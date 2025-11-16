import struct
import time
import json
import sys
import asyncio
from datetime import datetime
import os

from bleak import BleakClient, BleakScanner
import paho.mqtt.publish as publish


## BLE RELATED SETTINGS
# UUID of the characteristic sending notifications (from Arduino sketch)
UUID_CHR_ACK			=  "555A0002-0010-467A-9538-01F0652C74E8"
UUID_CHR_SENSOR	  		=  "555A0002-0030-467A-9538-01F0652C74E8"
UUID_CHR_TIMEST  	    =  "555A0002-0034-467A-9538-01F0652C74E8"
UUID_CHR_CAMPCONF	   	=  "555A0002-0035-467A-9538-01F0652C74E8"
UUID_CHR_ACTIVITY	   	=  "555A0002-0040-467A-9538-01F0652C74E8"  # start and stop

UUID_BATTERY_SRV	    =  "180F" # "0000180F-0000-1000-8000-00805f9b34fb"
UUID_NOTIFY_BATTERY		=  "2A19" # "00002A19-0000-1000-8000-00805f9b34fb"

# Protocol defines
START_CAMPAIGN	  		=  0x0
STOP_CAMPAIGN	   		=  0xF
SLEEP_CAMPAIGN          =  0xFF
END_CAMPAIGN            =  0xBB
FREQ                    =  0xAA
ACK_OK			  		=  0x00
ACK_KO                  =  0xFF

CSV_BUFFER_SIZE = 256

CAMPAIGN_TEMPLATE       = {"init_counter": 0, "sampling_frequency": 10}

def sensor_collect_callback_wrapper(name: str):
    def sensor_collect_callback(_, data: bytearray):

        if not is_imu_ready[name].is_set():
            # If an IMU gets disconnected, it may have been in acquisition mode when the disconnection occurred.
            # Therefore, if you receive data from an IMU currently marked as not ready, you should assume
            # it's active and mark it as ready.
            is_imu_ready[name].set()

        timest = int(time.time())
        nht, ax, ay, az = struct.unpack("<H3h", data)
        s = f"{timest},{name},{nht},{ax},{ay},{az}"

        STORE_FUNC(s)  # store_mqtt, store_csv or print

    return sensor_collect_callback


def ack_callback_wrapper(name: str):
    def ack_callback(_, data: bytearray):
        if data[0] == ACK_OK:
            is_imu_ready[name].set()
    return ack_callback


def battery_level_callback_wrapper(name: str):
    def battery_level_callback(_, data: bytearray):
        print(data)

    return battery_level_callback

def disconnection_callback_wrapper(name: str):
    def disconnection_callback(_: BleakClient):
        print(f"[warning] device {name} disconnected")
        available_imus.pop(name)
        
        # Disconnections can also occur during client shutdown.
        # In such cases, we should not attempt to reconnect the IMU.
        if not quitting.is_set(): 
            asyncio.create_task(reconnect(name))

    return disconnection_callback

async def reconnect(name: str) -> None:

    """
    The reconnection process works as follows:
    - The device is scanned, and if found within the timeout period, a connection is established and
      the device is marked as not ready.
    - Then, the system waits for the sensor_collect_callback to be triggered (i.e., for data to be received).
      If data arrives within a time window equal to the sampling period + 1 second, the device is marked as ready
      and the computation continues.
      If no data is received during that interval, it indicates that the IMU requires reconfiguration.    
    """
    
    err = 0
    if await discover(name):
        if await setup_campaign(name, reconnect=True):
            await start(name)
        else:
            print(f"[warning] {name} has been resetted. Quitting...")
            err = 1
    else:
        print(f"[warning] {name} discover timed out. Quitting...")
        err = 1

    if err:
        # If the IMU is not found within the timeout or something occured during the configuration, exit safely
        print("[info] press ENTER to exit")
        await broadcast(sleep)
        await safe_close()


async def discover(name: str) -> bool:
    dev = await BleakScanner.find_device_by_name(name, timeout=TIMEOUT)
    if dev is None:
        print(f"[warning] can't find device: {name}")
        return False

    print(f"[info] found device: {name}")
    client = BleakClient(dev, disconnected_callback=disconnection_callback_wrapper(name))
    if await connect(name, client):
        print(f"[info] connected to {name}")
        available_imus[name] = client
        return True

    return False

async def connect(name:str, client: BleakClient) -> bool:
    await client.connect()
    if client.is_connected:
        is_imu_ready[name] = asyncio.Event() # mark IMU as not-ready (event not set)
        await subscribe(client, UUID_CHR_SENSOR, sensor_collect_callback_wrapper(name))
        await subscribe(client, UUID_CHR_ACK, ack_callback_wrapper(name))
        await subscribe(client, UUID_NOTIFY_BATTERY, battery_level_callback_wrapper(name))

        return True

    return False

async def setup_campaign(name: str, reconnect=False) -> None:

    attempt = 0
    while attempt <= 2:

        try:
            # Wait for the device to be set as ready.
            await asyncio.wait_for(is_imu_ready[name].wait(), int(1 / CAMPAIGN_TEMPLATE["sampling_frequency"]) + 1)
            return True

        except asyncio.TimeoutError:
            # No coroutine has set the device as ready, so the only option is to configure it
            # and wait for the ack_callback to mark it as ready.
            if reconnect:
                return False
            
            print(f"[info] {name} is not configured")
            await send_timestamp(name)
            await send_campaign_parameters(name)
            attempt += 1

    return False # after 2 configuration attempts

async def send_timestamp(name: str) -> None:
    client = available_imus.get(name)
    if client is None:
        return

    timestamp = int(time.time())
    value = struct.pack(">i", timestamp)
    await client.write_gatt_char(UUID_CHR_TIMEST, value)
    print(f"[info] sent timestamp: {timestamp}")

async def send_campaign_parameters(name: str) -> None:
    client = available_imus.get(name)
    if client is None:
        return
    co = CAMPAIGN_TEMPLATE["init_counter"]
    fr = CAMPAIGN_TEMPLATE["sampling_frequency"]
    data = bytearray(co.to_bytes(2, byteorder='little') + fr.to_bytes(1, byteorder='little'))
    await client.write_gatt_char(UUID_CHR_CAMPCONF, data, len(data))
    print(f"[info] sent campaign parameters:\n {CAMPAIGN_TEMPLATE}")

async def send_sampling_frequency(name: str, freq: int=1) -> None:
    client = available_imus.get(name)
    if client is None:
        return

    data = bytearray(FREQ.to_bytes(1, byteorder='little') + freq.to_bytes(1, byteorder='little'))
    await client.write_gatt_char(UUID_CHR_CAMPCONF, data, len(data))
    print (f"[info] frequency updated to {freq}")

async def disconnect(name: str) -> None:
    client = available_imus.get(name)
    if client is None:
        return None

    await client.disconnect()

async def start(name: str) -> None:
    client = available_imus.get(name)
    if client is None:
        return None

    data = bytearray([START_CAMPAIGN])
    await client.write_gatt_char(UUID_CHR_ACTIVITY, data)
    print(f"[info] sent START to {name} (0x{START_CAMPAIGN:02X})")

async def stop(name: str):
    client = available_imus.get(name)
    if not client:
        return

    data = bytearray([STOP_CAMPAIGN])
    await client.write_gatt_char(UUID_CHR_ACTIVITY, data)
    print(f"[info] sent STOP to {name} (0x{STOP_CAMPAIGN:02X})")

async def end(name: str):
    client = available_imus.get(name)
    if not client:
        return

    data = bytearray([END_CAMPAIGN])
    await client.write_gatt_char(UUID_CHR_ACTIVITY, data)
    print(f"[info] sent END to {name} (0x{END_CAMPAIGN:02X})")


async def sleep(name: str):
    client = available_imus.get(name)
    if not client:
        return

    data = bytearray([SLEEP_CAMPAIGN])
    await client.write_gatt_char(UUID_CHR_ACTIVITY, data)
    print(f"[info] sent SLEEP to {name} (0x{SLEEP_CAMPAIGN:02X})")

async def subscribe(client: BleakClient, uuid: str, callback):
    await client.start_notify(uuid, callback)

async def broadcast(func, **kwargs) -> None:
    # Execute func for all IMUs in parallel
    tasks = [asyncio.create_task(func(name, **kwargs)) for name, _ in available_imus.items()]
    await asyncio.gather(*tasks)

def flush_csv_buffer():
    if len(csv_buffer) > 0:
        csv_writer.write('\n'.join(csv_buffer) + '\n')
        csv_buffer.clear()

    csv_writer.flush()

async def safe_close() -> None:
    
    """
    This function notifies other coroutines that the process is terminating, disconnects all connected IMUs,
    and if the csv_buffer is not empty, flushes it by writing all data to disk.
    """

    quitting.set()
    await broadcast(disconnect)
    available_imus.clear()
    if STORE_METHOD == "csv":
        if not csv_writer.closed:
            flush_csv_buffer()
            csv_writer.close()

def store_mqtt(data: str) -> None:
    # TODO: could raise an exception
    publish.single(topic=MQTT_TOPIC, payload=data, hostname=MQTT_BROKER, port=MQTT_PORT)

def store_csv(data: str) -> None:
    # Use of a buffer with a predefined length to avoid overloading the RAM."
    if len(csv_buffer) >= CSV_BUFFER_SIZE:
        flush_csv_buffer()
        return

    csv_buffer.append(data)

async def async_input(prompt: str = "") -> str:
    return await asyncio.to_thread(input, prompt)  # non-blocking input

def generate_csv_filename() -> str:
    today_str = datetime.now().strftime("%Y%m%d")
    all_files = os.listdir(CSV_STORE_DIR)

    matching_files = [f for f in all_files if f.startswith(today_str) and f.endswith(".csv")]

    existing_numbers = []
    for fn in matching_files:
        parts = fn.replace(".csv", "").split("_")
        if len(parts) == 2 and parts[0] == today_str and parts[1].isdigit():
            existing_numbers.append(int(parts[1]))

    # Determine next available number
    next_number = max(existing_numbers, default=0) + 1
    next_number_str = f"{next_number:02d}"  # zero-padded

    # Generate filename
    fn = f"{today_str}_{next_number_str}.csv"
    full_path = os.path.join(CSV_STORE_DIR, fn)
    return full_path

async def main():
    global csv_writer

    try:
        tasks = [discover(name) for name in IMUS]
        results = await asyncio.gather(*tasks)

        if sum(results) == len(available_imus):
            tasks = [setup_campaign(name) for name in available_imus.keys()]
            results = await asyncio.gather(*tasks)
        else:
            return

        while True:
            line = await async_input(">> ")
            if quitting.is_set():
                return
            if not line: continue

            line = line.split()
            cmd = line[0]
            argv = line[1:]
            if cmd == "stop":
                await broadcast(stop)
                if STORE_METHOD == "csv":
                    flush_csv_buffer()

            elif cmd == "start":
                await broadcast(start)
                if STORE_METHOD == "csv":
                    csv_writer = open(CSV_FILENAME, "a", newline='', encoding="utf-8")

            elif cmd == "freq":
                freq = argv[0]
                try:
                    freq = int(freq)
                    if freq < 0 or freq > 0xFF:
                        raise ValueError

                except (TypeError, ValueError):
                    print (f"[error] invalid frequency: {freq}")
                    continue

                CAMPAIGN_TEMPLATE["sampling_frequency"] = freq
                await broadcast(send_sampling_frequency, freq=freq)

            elif cmd == "shutdown":
                await broadcast(end)
                break
            elif cmd == "quit":
                await broadcast(sleep)
                break
            else:
                print(f"[error] unknown command: {cmd}")

        await safe_close()

    except asyncio.CancelledError:
        await broadcast(sleep)
        await safe_close()

if __name__ == '__main__':
    available_imus = dict()
    is_imu_ready = dict()
    csv_buffer = list()
    csv_writer = None
    quitting = asyncio.Event()

    if len(sys.argv) > 1:
        filename = sys.argv[1]
        with open(filename, 'r') as f:
            config = json.load(f)
    else:
        print ("Usage: python ble-central.py <config-filename>")
        sys.exit(1)

    IMUS = config["imus"]
    TIMEOUT = config["timeout"]
    CAMPAIGN_TEMPLATE["init_counter"] = config["init_counter"]
    CAMPAIGN_TEMPLATE["sampling_frequency"] = config["sampling_frequency"]

    STORE_METHOD = config["store_method"]  # mqtt, csv
    if STORE_METHOD == "mqtt":
        MQTT_BROKER = config["mqtt_broker"]
        MQTT_PORT = config["mqtt_port"]
        MQTT_TOPIC = config["mqtt_topic"]
        STORE_FUNC = store_mqtt  # macro

    elif STORE_METHOD == "csv":
        CSV_STORE_DIR = config["csv_store_dir"]
        CSV_FILENAME = generate_csv_filename()
        csv_writer = open(CSV_FILENAME, "a", newline="", encoding="utf-8")
        csv_writer.write("Timestamp,IMU,Counter,Acceleration X,Acceleration Y,Acceleration Z\n")
        STORE_FUNC = store_csv  # macro

    else:
        print("[error] unknown store method")
        STORE_FUNC = print  # macro

    asyncio.run(main())
