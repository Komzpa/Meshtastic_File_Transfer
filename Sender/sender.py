"""Land_and_air
Connect 2 Radios to the USB ports running meshtastic and then run the file. A progress bar should show up
100%|██████████| 36/36 [02:52<00:00,  4.79s/packet] (ex. progress bar) - Long Fast(4 sec send delay) 50 bytes/sec
100%|██████████| 36/36 [01:06<00:00,  1.84s/packet] - Medium Fast(1.6 sec sending delay) 138 bytes/sec
100%|██████████| 36/36 [00:15<00:00,  2.32packet/s] - Short Fast(.4 sec sending delay) 300-500 bytes/sec
"""
import logging
import os
import sys

sys.path.append('..')
from serial.serialutil import SerialException
import time
from meshtastic.serial_interface import SerialInterface
from meshtastic.util import findPorts
from pubsub import pub
import argparse
from File_Class_Manager import FileTransManager

LOGGER = logging.getLogger(__name__)


def _configure_logging():
    level_name = os.environ.get('FILE_TRANSFER_LOG_LEVEL', 'INFO').upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )
Text_Queue = []
Queue = []


def main(interface):
    args = parser.parse_args()
    # Args to be used
    time_delay = float(args.time_delay)
    use_dir = args.use_dir
    auto_restart = args.auto_restart
    size = 0
    paths = []
    if use_dir:
        list_paths = os.listdir(args.path)
        for i, fname in enumerate(list_paths):
            path = args.path + '/' + fname
            if not fname.startswith('.') and os.path.isfile(path):
                size += os.path.getsize(path)
                paths.append(path)

    else:
        paths = [args.path]
        size += os.path.getsize(args.path)
    LOGGER.debug('Prepared transfer paths: %s', paths)
    try:
        paths = sorted(paths)
        send_time = time_delay * size/232 * 1.1
        send_hrs = int(send_time // 60**2)
        send_mins = int(send_time - send_hrs*60**2)//60
        send_secs = round(send_time - send_hrs*60**2 - send_mins*60)
        LOGGER.info(
            'Estimated transfer duration %sh %sm %ss for %s bytes with delay %ss',
            send_hrs,
            send_mins,
            send_secs,
            size,
            time_delay,
        )
        if 'n' in input(f'Transfer will take approx. {send_hrs}hrs {send_mins}mins {send_secs}s. \n'
                        'Continue?(y/n)>>').lower():
            raise KeyboardInterrupt
        manager = FileTransManager(interface, send_delay=time_delay, auto_restart=auto_restart)  # Sender
        # Selecting the destination(to be changed)
        print('Select the destination below')
        nodes = dict(interface.nodes)
        nodes.pop(interface.getMyNodeInfo()['user']['id'], None)
        keys = list(nodes.keys())
        for i, key in enumerate(keys):
            print(f"{i+1}: {key} - {nodes[key]['user']['shortName']}")
        index = input('>>')
        selected = keys[int(index)-1]
        destination_id = selected
        # destination_id = interface_2.getMyNodeInfo()['user']['id']
        print(f"Starting transfer of {args.path} to {nodes[selected]['user']['shortName']}")
        LOGGER.info('Beginning transfer of %s to %s (%s)', args.path, nodes[selected]['user']['shortName'], destination_id)
        manager.send_new_files(paths, destination_id)
        looping = True
        while looping:
            time.sleep(.05)  # For performance lol
            # Sending update
            manager.update_all()
            if Queue:  # Handle binary data
                name, packet = Queue.pop(0)
                payload = packet['decoded']['payload']
                LOGGER.debug('Processing %s control/data packet from %s', len(payload) if payload else 0, name)
                manager.new_data_packet(bytearray(payload))
            if Text_Queue:  # handle text data
                name, packet = Text_Queue.pop(0)
                text = packet['decoded']['text']
                LOGGER.info('Text received from %s: %s', name, text)

            if len(manager.transfer_objects) == 0:
                looping = False
        interface.close()
    except KeyboardInterrupt:
        sys.exit('\nUser Interrupted')


def on_receive(packet, interface): # called when a packet arrives
    # print(packet['decoded']['portnum'], interface.getShortName())
    # print(f'received_1: {packet["decoded"]["payload"]}')
    LOGGER.debug(
        'Inbound packet on %s from %s (%s)',
        packet['decoded'].get('portnum'),
        packet.get('fromId'),
        interface.getShortName(),
    )
    if packet['decoded']['portnum'] == 'IP_TUNNEL_APP':
        Queue.append((interface.getShortName(), packet))
    elif packet['decoded']['portnum'] == 'TEXT_MESSAGE_APP':
        Text_Queue.append((interface.getShortName(), packet))


if __name__ == '__main__':
    _configure_logging()
    parser = argparse.ArgumentParser(
        prog='Meshtastic File Sender',
        description='Sends a file or directory to another node running the receiver program',)
    parser.add_argument('-t', '--time_delay', default=5, type=float, help='Time between sending packets')
    parser.add_argument('-d', '--use_dir', action='store_true', help='Sends a directory of files instead')
    parser.add_argument('-r', '--auto_restart', action='store_true', help='Automatically restart transfer upon '
                                                                    'failure(good for large transfers)')
    parser.add_argument('-p', '--path', required=True, help='Path to find the file from(must be less than'
                                                            '59kb')
    ports = findPorts(True)
    print(f'Number of Radios: {len(ports)}')
    interface = None
    if ports:
        for port in ports:
            try:
                interface = SerialInterface(devPath=port)
                print(f'connected to {interface.getShortName()} on {port}')
                break
            except (BlockingIOError, SerialException):
                pass
    if interface:
        pub.subscribe(on_receive, "meshtastic.receive")
        main(interface)
    else:
        sys.exit('Error: No Radio Available')
