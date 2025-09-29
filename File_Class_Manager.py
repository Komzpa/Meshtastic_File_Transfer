import logging
import os.path
import random
import file_classes
from Packaging_Data import decode_initial_req

LOGGER = logging.getLogger(__name__)


class FileTransManager:
    def __init__(self, interface, send_delay=7, packet_len=200, destination=None, auto_restart=False):
        self.transfer_objects = {}  # id: file_sender/file_receiver
        self.interface = interface
        self.send_delay = send_delay
        self.packet_len = packet_len
        self.file_list = []
        self.destination = destination
        self.done = False
        self.restart = auto_restart

    def update_all(self):
        """Calls update method on each object"""
        deleted_keys = []
        for key in self.transfer_objects.keys():
            LOGGER.debug('Updating transfer object %s (%s)', key, type(self.transfer_objects[key]).__name__)
            self.transfer_objects[key].update()
            if self.transfer_objects[key].kill:
                deleted_keys.append(key)

        for key in deleted_keys:
            finished = self.transfer_objects.pop(key)
            LOGGER.info('Transfer %s (%s) marked for cleanup. Finished=%s', key, finished.name, finished.finished)
            if not finished.finished:
                if not self.restart:
                    result = input('retry?(y/n)>>')
                else:
                    result = ""
                if 'y' in result.lower() or self.restart:
                    print('restarting ', finished.name)
                    LOGGER.warning('Restarting transfer for %s (%s)', finished.name, key)
                    self.file_list.insert(0, finished.name)

        if len(self.transfer_objects) == 0:
            if self.file_list:
                file = self.file_list.pop(0)
                if os.path.exists(file) and self.destination:
                    LOGGER.info('Starting queued transfer of %s to %s', file, self.destination)
                    self.send_new_file(file, self.destination)
            else:
                self.done = True
                LOGGER.debug('No active transfers remaining')

    def new_data_packet(self, packet):
        """Called to process new data packet"""
        if packet[0] == bytearray('f'.encode('utf8'))[0]:
            f_id = packet[4]
            transfer = self.transfer_objects.get(f_id)
            if transfer:
                LOGGER.debug('Routing control packet type %s to transfer %s', packet[5], f_id)
                transfer.manage_com_packet(packet)
            else:
                print(f'Received control packet for unknown transfer id {f_id}, ignoring')
                LOGGER.warning('Control packet for unknown transfer id %s: %s', f_id, packet)
        elif packet[0] in self.transfer_objects.keys():
            # print(f'file packet received for {int(packet[0])}: {packet}')
            LOGGER.debug('Routing data packet #%s to transfer %s', packet[1], packet[0])
            self.transfer_objects[packet[0]].add_packet(packet)
        else:
            print(f'something went Wrong: {packet}')
            LOGGER.error('Received unrecognized packet prefix %s: %s', packet[0], packet)

    def new_req_packet(self, initial_req, sending_id, timeout=100):
        """Called to make new file_receiving packet based on a request packet"""
        file_name, f_id, num = decode_initial_req(initial_req)
        LOGGER.info('New transfer request %s id=%s packets=%s from %s', file_name, f_id, num, sending_id)
        if self.restart:
            print(f'Automatically Accepted {file_name} with a size of {round(num*self.packet_len / 1000, 2)}kb.')
            LOGGER.debug('Auto-accept enabled, proceeding with %s', file_name)
        elif 'y' in input(f'Receive {file_name} with an approx size of {round(num*self.packet_len/1000,2)}kb?(y/n)\n>>'):
            print('Accepted')
        else:
            LOGGER.info('Transfer request %s was declined', f_id)
            return
        self.transfer_objects[f_id] = file_classes.FileTransferReceiver(file_name, f_id, num, self.interface,
                                                                        sending_id, timeout=timeout)
        LOGGER.debug('Transfer receiver created for %s (%s)', file_name, f_id)

    def send_new_file(self, file_name, destination):
        """Called to make new file sending object"""
        file_id = random.randint(0, 256)
        while file_id == bytearray('f'.encode('utf8'))[0] or file_id in self.transfer_objects.keys():
            file_id = random.randint(0, 256)
        print(f'Sending {os.path.basename(file_name) or file_name}...')
        LOGGER.info('Preparing new transfer %s (%s) to %s', file_name, file_id, destination)
        self.transfer_objects[file_id] = file_classes.FileTransferSender(file_name, file_id, self.interface,
                                                                         destination, self.send_delay, self.packet_len,
                                                                         disable_bar=False)

    def send_new_files(self, file_names: list, destination=None):
        """Called to make new file sending object"""
        if destination:
            self.destination = destination
            LOGGER.debug('Destination set to %s', destination)
        for file in sorted(file_names):
            self.file_list.append(file)
            LOGGER.debug('Queued %s for transfer', file)
