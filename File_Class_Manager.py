from __future__ import annotations

import logging
import random
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import file_classes
from Packaging_Data import decode_initial_req

LOGGER = logging.getLogger(__name__)


class FileTransManager:
    """Coordinate active transfers and user prompts."""

    def __init__(
        self,
        interface,
        send_delay: float = 7,
        packet_len: int = 200,
        destination: Optional[str] = None,
        auto_restart: bool = False,
    ) -> None:
        self.transfer_objects: Dict[Tuple[str, str, int], object] = {}
        self.interface = interface
        self.send_delay = send_delay
        self.packet_len = packet_len
        self.file_list: list[str] = []
        self.destination = destination
        self.done = False
        self.restart = auto_restart

    def _make_key(self, direction: str, peer_id: Optional[str], file_id: int) -> Tuple[str, str, int]:
        """Generate a dictionary key for a transfer direction/peer combination."""

        return direction, peer_id or "", int(file_id)

    def update_all(self) -> None:
        """Advance the state of all managed transfers."""

        deleted_keys = []
        for key, transfer in list(self.transfer_objects.items()):
            LOGGER.debug("Updating transfer object %s (%s)", key, type(transfer).__name__)
            transfer.update()
            if getattr(transfer, "kill", False):
                deleted_keys.append(key)

        for key in deleted_keys:
            finished = self.transfer_objects.pop(key)
            LOGGER.info("Transfer %s (%s) marked for cleanup. Finished=%s", key, finished.name, finished.finished)
            if not finished.finished:
                if not self.restart:
                    result = input("retry?(y/n)>>")
                else:
                    result = ""
                if "y" in result.lower() or self.restart:
                    print("restarting ", finished.name)
                    LOGGER.warning("Restarting transfer for %s (%s)", finished.name, key)
                    self.file_list.insert(0, finished.name)

        if not self.transfer_objects:
            if self.file_list:
                file = Path(self.file_list.pop(0))
                if file.exists() and self.destination:
                    LOGGER.info("Starting queued transfer of %s to %s", file, self.destination)
                    self.send_new_file(str(file), self.destination)
            else:
                self.done = True
                LOGGER.debug("No active transfers remaining")

    def _is_embedded_initial_request(self, packet: bytearray) -> bool:
        """Return True if the payload contains an inline initial request."""

        return len(packet) > 6 and packet[0] == ord("!") and packet[1:6] == b"fcom,"

    def _lookup_short_name(self, node_id: Optional[str]) -> str:
        """Best effort lookup of a node's short name for logging."""

        if not node_id:
            return "Unknown"

        nodes = getattr(self.interface, "nodes", {}) or {}
        node_info = nodes.get(node_id, {})
        user_info = node_info.get("user", {}) if isinstance(node_info, dict) else {}
        short_name = user_info.get("shortName")
        return short_name or node_id

    def new_data_packet(self, packet: bytearray, from_id: Optional[str] = None) -> None:
        """Process a new data or control packet."""

        if not packet:
            LOGGER.warning("Received empty packet from %s, ignoring", from_id)
            return

        if self._is_embedded_initial_request(packet):
            try:
                request = packet.decode("utf8")
            except UnicodeDecodeError:
                request = packet.decode("utf8", errors="ignore")
            LOGGER.info("Detected embedded initial request from %s: %s", from_id, request)
            self.new_req_packet(request, from_id)
            return

        control_prefix = bytearray("f".encode("utf8"))[0]
        if packet[0] == control_prefix:
            f_id = packet[4]
            key = self._make_key("recv", from_id, f_id)
            transfer = self.transfer_objects.get(key)
            if transfer:
                LOGGER.debug("Routing control packet type %s from %s to receiver %s", packet[5], from_id, key)
                transfer.manage_com_packet(packet)
                return
            key = self._make_key("send", from_id, f_id)
            transfer = self.transfer_objects.get(key)
            if transfer:
                LOGGER.debug("Routing control packet type %s from %s to sender %s", packet[5], from_id, key)
                transfer.manage_com_packet(packet)
                return
            print(f"Received control packet for unknown transfer id {f_id}, ignoring")
            LOGGER.warning("Control packet for unknown transfer id %s from %s: %s", f_id, from_id, packet)
            return

        key = self._make_key("recv", from_id, packet[0])
        transfer = self.transfer_objects.get(key)
        if transfer:
            LOGGER.debug("Routing data packet #%s from %s to receiver %s", packet[1], from_id, key)
            transfer.add_packet(packet)
        else:
            try:
                text_payload = packet.decode("utf8")
            except UnicodeDecodeError:
                text_payload = packet.decode("utf8", errors="replace")

            if text_payload and any(ch.isprintable() for ch in text_payload):
                sender_name = self._lookup_short_name(from_id)
                LOGGER.info(
                    "Received non-transfer text payload from %s (%s): %s",
                    sender_name,
                    from_id,
                    text_payload,
                )
            else:
                LOGGER.debug(
                    "Ignoring payload for unknown transfer id %s from %s: %s",
                    packet[0] if packet else "unknown",
                    from_id,
                    packet,
                )

    def new_req_packet(self, initial_req, sending_id: Optional[str], timeout: int = 100) -> None:
        """Create a new receiver object based on an incoming request."""

        try:
            file_name, f_id, num = decode_initial_req(initial_req)
        except (ValueError, KeyError) as exc:
            LOGGER.warning("Failed to decode initial request from %s: %s (%s)", sending_id, initial_req, exc)
            return
        LOGGER.info("New transfer request %s id=%s packets=%s from %s", file_name, f_id, num, sending_id)
        key = self._make_key("recv", sending_id, f_id)
        existing = self.transfer_objects.get(key)
        if existing:
            LOGGER.info(
                "Duplicate transfer request for %s (%s) from %s; resending acknowledgement",
                file_name,
                f_id,
                sending_id,
            )
            existing.resend_initial_ack()
            return

        size_kb = round(num * self.packet_len / 1000, 2)
        if self.restart:
            print(f"Automatically Accepted {file_name} with a size of {size_kb}kb.")
            LOGGER.debug("Auto-accept enabled, proceeding with %s", file_name)
        elif "y" in input(f"Receive {file_name} with an approx size of {size_kb}kb?(y/n)\n>>"):
            print("Accepted")
        else:
            LOGGER.info("Transfer request %s was declined", f_id)
            return

        self.transfer_objects[key] = file_classes.FileTransferReceiver(
            file_name,
            f_id,
            num,
            self.interface,
            sending_id,
            timeout=timeout,
        )
        LOGGER.debug("Transfer receiver created for %s (%s) from %s", file_name, f_id, sending_id)

    def _generate_file_id(self, destination: str) -> Tuple[int, Tuple[str, str, int]]:
        """Return a random, unused file id and its associated transfer key."""

        control_value = bytearray("f".encode("utf8"))[0]
        while True:
            file_id = random.randint(0, 255)
            key = self._make_key("send", destination, file_id)
            if file_id != control_value and key not in self.transfer_objects:
                return file_id, key

    def send_new_file(self, file_name: str, destination: str) -> None:
        """Start a new outgoing transfer."""

        file_id, key = self._generate_file_id(destination)
        display_name = Path(file_name).name or file_name
        print(f"Sending {display_name}...")
        LOGGER.info("Preparing new transfer %s (%s) to %s", file_name, file_id, destination)
        self.transfer_objects[key] = file_classes.FileTransferSender(
            file_name,
            file_id,
            self.interface,
            destination,
            self.send_delay,
            self.packet_len,
            disable_bar=False,
        )

    def send_new_files(self, file_names: Iterable[str], destination: Optional[str] = None) -> None:
        """Queue multiple files for transfer."""

        if destination:
            self.destination = destination
            LOGGER.debug("Destination set to %s", destination)
        for file in sorted(file_names):
            self.file_list.append(file)
            LOGGER.debug("Queued %s for transfer", file)
