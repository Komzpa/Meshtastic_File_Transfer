import os
import time
import Packaging_Data
from meshtastic import portnums_pb2
import tqdm

class FileTransferReceiver:
    """Class used to store and handle incoming file packets and should be created when the first request packet is
    acknowledged. Added to as the packets come in. Checked when the last packet number arrives or when a timeout is
    reached"""
    def __init__(self, file_name, file_id, num_packets, interface, sending_id, timeout=30, disable_bar=False):
        self.name = file_name
        self.id = file_id
        self.num_packets = int(num_packets)
        self.interface = interface
        self.sending_id = sending_id
        self.packet_dict = {}
        self.timeout = timeout + 10
        self.last_packet = time.time()
        self.progress_bar = tqdm.tqdm(total=num_packets, unit='packet', disable=disable_bar)
        initial_ack = Packaging_Data.make_status_packet(file_id, 1)  # Make ack array
        self.send_data(bytes(initial_ack))
        self.kill = False
        self.finished = False
        self.saved = False

    def update(self):
        if time.time()-self.last_packet > self.timeout:
            if self.get_missing_nums():  # Try to see if all the packets somehow made it
                print(f'File Transfer "{self.name}" Failed')
                self.progress_bar.close()
                self.kill = True
            else:
                self.save_to_file()
                self.kill = True
                self.progress_bar.close()

    def add_packet(self, packet: bytearray):
        """Send byte array received from radio and it will parse it and save it to the packet_dict
        -Returns true if it added it and false if it didn't"""
        self.last_packet = time.time()
        packet.pop(0)  # gives file id
        num = packet.pop(0)  # Packet index
        if num < self.num_packets:
            is_new_packet = num not in self.packet_dict
            self.packet_dict[num] = bytes(packet)
            ack_packet = Packaging_Data.make_status_packet(self.id, 5, opt_data=[num])
            self.send_data(ack_packet)
            if is_new_packet:
                self.progress_bar.update(1)
                if len(self.packet_dict) == self.num_packets:
                    self.save_to_file()
            return True
        return False

    def manage_com_packet(self, packet: bytearray):
        """Receives and Responds to fcom Packets"""
        self.last_packet = time.time()
        packet_type = packet[5]
        if packet_type == 2:  # Done Transmitting
            missing_packets = self.get_missing_nums()
            if missing_packets:  # Ask for missing packets
                # print(missing_packets)
                ret_packet = Packaging_Data.make_status_packet(self.id, 3, opt_data=missing_packets)
            else:  # Let it know all packets are received
                ret_packet = Packaging_Data.make_status_packet(self.id, 4)
                self.save_to_file()
                self.kill = True
            self.send_data(ret_packet)

    def get_missing_nums(self):
        """Returns a list of missing packets based on the indexes stored in the packet_dict"""
        missing_nums = []
        for num in range(self.num_packets):
            if num not in self.packet_dict.keys():
                missing_nums.append(num)
        return missing_nums

    def save_to_file(self):
        check = self.get_missing_nums()
        if len(check) == 0 and not self.saved:
            try:
                self.progress_bar.close()
                dir_name = os.path.dirname(self.name)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                with open(self.name, 'wb') as fi:
                    for num in range(self.num_packets):
                        fi.write(self.packet_dict[num])
                self.finished = True
                self.saved = self.finished
            except OSError as exc:
                print(f'Failed to save {self.name}: {exc}')
                self.kill = True
                return False
            return True
        return False

    def send_data(self, data):
        """Sends data over interface to destination"""
        try:
            self.interface.sendData(bytes(data), destinationId=self.sending_id,
                                    portNum=portnums_pb2.IP_TUNNEL_APP, wantAck=False)
        except Exception as exc:
            print(f'Failed to send control packet for {self.name}: {exc}')


class FileTransferSender:
    """Class used to communicate and send files with a desired target. Sends Initial Packet, receives ack packets and
    sends packets and a confirmation packet at the end."""
    def __init__(self, file_name, file_id: int, interface, destination_id, send_delay=10, packet_len=200,
                 disable_bar=True):
        self.name = file_name
        self.id = file_id  # 0 Reserved for file meta packets
        self.interface = interface
        self.destination_id = destination_id
        self.packet_len = packet_len
        data_dict = Packaging_Data.split_data(file_name, packet_len)  # Unlabeled
        self.data_dict = Packaging_Data.package_data(data_dict, self.id)  # Labeled
        self.delay = max(send_delay, 0.1)
        self.retry_timeout = max(self.delay * 2, 3)
        self.window_size = max(1, min(5, len(self.data_dict)))
        self.packet_num = len(self.data_dict)
        self.last_send = time.time()
        self.last_activity = time.time()
        # Modes- 0: Waiting for initial ack, 2: Sending Data, 3: Waiting for completion confirmation
        self.mode = 0
        self.kill = False
        self.finished = False
        self.to_send = []
        self.pending_packets = {}
        self.acknowledged_packets = set()
        self.finish_sent = False
        # Send initial Req packet
        self.send_initial()
        self.disable_bar = disable_bar
        self.progress_bar = tqdm.tqdm(total=self.packet_num, unit='packet', disable=self.disable_bar)

    def send_initial(self):
        # Sends initial packet
        init_str = Packaging_Data.make_initial_req(self.name, len(self.data_dict), self.id)
        try:
            self.interface.sendText(init_str, destinationId=self.destination_id)
            self.last_activity = time.time()
        except Exception as exc:
            print(f'Failed to send initial request for {self.name}: {exc}')
            self.kill = True

    def update(self):
        """-Sees if it has a packet to send and can send a packet and sends one if it can
        - Deletes itself if it's been too long and not heard anything"""
        now = time.time()
        if self.kill:
            return

        if self.mode == 0:
            if now - self.last_activity > self.retry_timeout:
                self.send_initial()
        elif self.mode == 2:
            self._fill_window()
            self._retry_pending(now)
            if (not self.to_send and not self.pending_packets and
                    len(self.acknowledged_packets) == self.packet_num and not self.finish_sent):
                self._send_finish()
        elif self.mode == 3:
            if not self.finish_sent:
                self._send_finish()
            elif now - self.last_activity > self.retry_timeout:
                self.finish_sent = False

        if now - self.last_activity > self.retry_timeout * 4:
            print('failed Send Timeout - no activity detected')
            self.progress_bar.close()
            self.kill = True

    def manage_com_packet(self, packet: bytearray):
        """Returns a list of missing packets based on the indexes stored in the packet_dict
        packet_types = {0: Deny initial Request, 1: Confirm Initial Request, 2: Done Transmitting,
        3: Need Packets(list Packets after one byte at a time), 4: Received all Packets(finished),
        5: Packet receipt acknowledgement(list Packets after one byte at a time)}
        inputs:
            - packet = bytes(0, c, o, m, file_num, packet_type, opt data...)

        Mainly Adds to the sending queue in accordance with the received packet or deletes the object
        """
        packet_type = packet[5]
        self.last_activity = time.time()
        if packet_type == 0:
            print('Sending Denied')
            self.progress_bar.close()
            self.kill = True
        elif packet_type == 1:  # Receiver ready for data
            if self.mode == 0:
                self.mode = 2
                self.to_send = list(self.data_dict.keys())
        elif packet_type == 3:  # Receiver needs specific packets
            needed_packets = list(packet[6:])
            for num in needed_packets:
                if num in self.pending_packets:
                    self.pending_packets.pop(num, None)
                if num in self.acknowledged_packets:
                    self.acknowledged_packets.discard(num)
                if num not in self.to_send and num in self.data_dict:
                    self.to_send.append(num)
            self.finish_sent = False
            if self.mode != 2:
                self.mode = 2
        elif packet_type == 4:  # Receiver confirms completion
            self.progress_bar.close()
            print(f'Confirmed File Transfer #{self.id} Complete')
            self.finished = True
            self.kill = True
        elif packet_type == 5:  # Receiver acknowledges individual packets
            acknowledged = list(packet[6:])
            for num in acknowledged:
                if num in self.pending_packets:
                    self.pending_packets.pop(num, None)
                if num not in self.acknowledged_packets and num < self.packet_num:
                    self.acknowledged_packets.add(num)
                    self.progress_bar.update(1)
            if self.mode == 0:
                self.mode = 2

    def _fill_window(self):
        while self.to_send and len(self.pending_packets) < self.window_size and not self.kill:
            packet_index = self.to_send.pop(0)
            if packet_index in self.acknowledged_packets:
                continue
            self._send_packet(packet_index)

    def _retry_pending(self, now):
        for packet_index, timestamp in list(self.pending_packets.items()):
            if now - timestamp > self.retry_timeout:
                self.pending_packets.pop(packet_index, None)
                if packet_index not in self.to_send:
                    self.to_send.append(packet_index)

    def _send_packet(self, packet_index):
        data = self.data_dict.get(packet_index)
        if data is None:
            return
        try:
            self.interface.sendData(bytes(data), portNum=portnums_pb2.IP_TUNNEL_APP,
                                    destinationId=self.destination_id, wantAck=False)
            sent_time = time.time()
            self.pending_packets[packet_index] = sent_time
            self.last_send = sent_time
            self.last_activity = sent_time
        except Exception as exc:
            print(f'Failed to send packet {packet_index} for {self.name}: {exc}')
            if packet_index not in self.to_send:
                self.to_send.append(packet_index)

    def _send_finish(self):
        finish_packet = Packaging_Data.make_status_packet(self.id, 2)
        try:
            self.interface.sendData(bytes(finish_packet), portNum=portnums_pb2.IP_TUNNEL_APP,
                                    destinationId=self.destination_id, wantAck=False)
            self.finish_sent = True
            self.last_send = time.time()
            self.last_activity = self.last_send
            self.mode = 3
        except Exception as exc:
            print(f'Failed to send completion notice for {self.name}: {exc}')
            self.finish_sent = False
