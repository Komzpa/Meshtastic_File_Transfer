import logging
import os.path
import random
import file_classes
from Packaging_Data import decode_initial_req

LOGGER = logging.getLogger(__name__)


class FileTransManager:
    def __init__(self, interface, send_delay=7, packet_len=200, destination=None, auto_restart=False):
        self.transfer_objects = {}  # (direction, peer, id): file_sender/file_receiver
        self.interface = interface
        self.send_delay = send_delay
        self.packet_len = packet_len
        self.file_list = []
        self.destination = destination
        self.done = False
        self.restart = auto_restart

    def _make_key(self, direction: str, peer_id, file_id: int):
        """Generate a dictionary key for a transfer direction/peer combination."""
        return direction, peer_id or '', int(file_id)

    def update_all(self):
        """Calls update method on each object"""
        deleted_keys = []
        for key in list(self.transfer_objects.keys()):
            transfer = self.transfer_objects[key]
            LOGGER.debug('Updating transfer object %s (%s)', key, type(transfer).__name__)
            transfer.update()
            if transfer.kill:
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

    def new_data_packet(self, packet, from_id=None):
        """Called to process new data or control packet"""
        if not packet:
            LOGGER.warning('Received empty packet from %s, ignoring', from_id)
            return

        control_prefix = bytearray('f'.encode('utf8'))[0]
        if packet[0] == control_prefix:
            f_id = packet[4]
            key = self._make_key('recv', from_id, f_id)
            transfer = self.transfer_objects.get(key)
            if transfer:
                LOGGER.debug('Routing control packet type %s from %s to receiver %s', packet[5], from_id, key)
                transfer.manage_com_packet(packet)
                return
            key = self._make_key('send', from_id, f_id)
            transfer = self.transfer_objects.get(key)
            if transfer:
                LOGGER.debug('Routing control packet type %s from %s to sender %s', packet[5], from_id, key)
                transfer.manage_com_packet(packet)
                return
            print(f'Received control packet for unknown transfer id {f_id}, ignoring')
            LOGGER.warning('Control packet for unknown transfer id %s from %s: %s', f_id, from_id, packet)
            return

        key = self._make_key('recv', from_id, packet[0])
        transfer = self.transfer_objects.get(key)
        if transfer:
            LOGGER.debug('Routing data packet #%s from %s to receiver %s', packet[1], from_id, key)
            transfer.add_packet(packet)
        else:
            print(f'something went Wrong: {packet}')
            LOGGER.error('Received data packet for unknown transfer id %s from %s: %s', packet[0], from_id, packet)

    def new_req_packet(self, initial_req, sending_id, timeout=100):
        """Called to make new file_receiving packet based on a request packet"""
        file_name, f_id, num = decode_initial_req(initial_req)
        LOGGER.info('New transfer request %s id=%s packets=%s from %s', file_name, f_id, num, sending_id)
        key = self._make_key('recv', sending_id, f_id)
        existing = self.transfer_objects.get(key)
        if existing:
            LOGGER.info('Duplicate transfer request for %s (%s) from %s; resending acknowledgement', file_name, f_id, sending_id)
            existing.resend_initial_ack()
            return

        if self.restart:
            print(f'Automatically Accepted {file_name} with a size of {round(num*self.packet_len / 1000, 2)}kb.')
            LOGGER.debug('Auto-accept enabled, proceeding with %s', file_name)
        elif 'y' in input(f'Receive {file_name} with an approx size of {round(num*self.packet_len/1000,2)}kb?(y/n)\n>>'):
            print('Accepted')
        else:
            LOGGER.info('Transfer request %s was declined', f_id)
            return
        self.transfer_objects[key] = file_classes.FileTransferReceiver(
            file_name,
            f_id,
            num,
            self.interface,
            sending_id,
            timeout=timeout,
        )
        LOGGER.debug('Transfer receiver created for %s (%s) from %s', file_name, f_id, sending_id)

    def send_new_file(self, file_name, destination):
        """Called to make new file sending object"""
        file_id = random.randint(0, 255)
        key = self._make_key('send', destination, file_id)
        while file_id == bytearray('f'.encode('utf8'))[0] or key in self.transfer_objects:
            file_id = random.randint(0, 255)
            key = self._make_key('send', destination, file_id)
        print(f'Sending {os.path.basename(file_name) or file_name}...')
        LOGGER.info('Preparing new transfer %s (%s) to %s', file_name, file_id, destination)
        self.transfer_objects[key] = file_classes.FileTransferSender(
            file_name,
            file_id,
            self.interface,
            destination,
            self.send_delay,
            self.packet_len,
            disable_bar=False,
        )

    def send_new_files(self, file_names: list, destination=None):
        """Called to make new file sending object"""
        if destination:
            self.destination = destination
            LOGGER.debug('Destination set to %s', destination)
        for file in sorted(file_names):
            self.file_list.append(file)
            LOGGER.debug('Queued %s for transfer', file)
