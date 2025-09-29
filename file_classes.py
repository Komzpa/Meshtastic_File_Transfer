from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace
import time
from pathlib import Path

from typing import Dict, Iterable, List, MutableMapping, Optional, Sequence, Set

import tqdm
from meshtastic import portnums_pb2

import Packaging_Data

LOGGER = logging.getLogger(__name__)


class ChunkProgressDisplay:
    """Render a chunk-by-chunk status view for transfers."""

    def __init__(
        self,
        total: int,
        label: str,
        char_map: MutableMapping[str, str],
        default_status: str,
        disable: bool = False,
    ) -> None:
        # Store the configuration used to render the progress output.
        self.enabled = not disable and total > 0
        self.label = label
        self.char_map = dict(char_map)
        self.default_status = default_status
        self.total = total
        self.statuses: List[str] = [default_status for _ in range(total)]
        self._last_width = 0
        self._rendered_once = False
        self.closed = False
        # Failing fast makes invalid configuration easier to diagnose.
        if self.enabled and default_status not in self.char_map:
            raise ValueError(f"Unknown default status '{default_status}' for {label}")
        if self.enabled:
            # Provide a legend so users can interpret the characters at a glance.
            legend = ", ".join(
                f"{symbol}={status.replace('_', ' ')}"
                for status, symbol in self.char_map.items()
            )
            if legend:
                print(f"{self.label} legend -> {legend}")
            # Render the initial empty progress view.
            self._render(initial=True)

    def _apply_status(self, index: int, status: str) -> bool:
        if status not in self.char_map or not 0 <= index < self.total:
            return False
        if self.statuses[index] == status:
            return False
        self.statuses[index] = status
        return True

    def set_status(self, index: int, status: str) -> None:
        """Set the status for a single chunk and refresh the view."""

        if not self.enabled or self.closed:
            return
        if self._apply_status(index, status):
            self._render()

    def set_many(self, indices: Iterable[int], status: str) -> None:
        """Set the status for multiple chunks in one render."""

        if not self.enabled or self.closed:
            return
        updated = False
        for index in indices:
            updated = self._apply_status(index, status) or updated
        if updated:
            self._render()

    def set_all(self, status: str) -> None:
        """Set the status for every chunk."""

        if not self.enabled or self.closed or status not in self.char_map:
            return
        if all(current == status for current in self.statuses):
            return
        self.statuses = [status for _ in range(self.total)]
        self._render()

    def _render(self, initial: bool = False) -> None:
        if not self.enabled or self.closed:
            return
        # Build the textual progress line using the configured characters.
        progress = "".join(self.char_map.get(status, "?") for status in self.statuses)
        line = f"{self.label}: {progress}"
        prefix = "" if initial and not self._rendered_once else "\r"
        sys.stdout.write(prefix + line)
        if len(line) < self._last_width:
            sys.stdout.write(" " * (self._last_width - len(line)))
        sys.stdout.flush()
        self._last_width = len(line)
        self._rendered_once = True

    def close(self, final_status: str | None = None) -> None:
        """Finalize the display and move to the next terminal line."""

        if not self.enabled or self.closed:
            return
        # Optionally normalise the final state before printing a newline.
        if final_status is not None and final_status in self.char_map:
            self.statuses = [final_status for _ in range(self.total)]
        self._render()
        sys.stdout.write("\n")
        sys.stdout.flush()
        self.closed = True

# File transfer traffic now uses the same core port number as the initial
# request message so radios treat the packets as standard application data.
TRANSFER_PORTNUM = portnums_pb2.TEXT_MESSAGE_APP

RECEIVER_STATUS_CHARS = {
    "waiting": ".",
    "received": "#",
    "stored": "*",
    "missing": "!",
    "failed": "x",
}

SENDER_STATUS_CHARS = {
    "pending": ".",
    "in_flight": ">",
    "awaiting_retry": "!",
    "acknowledged": "#",
    "failed": "x",
}

# Maintain a ``tqdm`` attribute for compatibility with existing tests.
tqdm = SimpleNamespace(tqdm=ChunkProgressDisplay)


class FileTransferReceiver:
    """Track and persist packets received for a single file transfer."""

    def __init__(
        self,
        file_name: str,
        file_id: int,
        num_packets: int,
        interface,
        sending_id: str,
        timeout: float = 30,
        disable_bar: bool = False,
    ) -> None:
        self.name = file_name
        self.id = file_id
        self.num_packets = int(num_packets)
        self.interface = interface
        self.sending_id = sending_id
        self.packet_dict: Dict[int, bytes] = {}
        self.timeout = timeout + 10
        self.last_packet = time.time()
        self.progress_display = ChunkProgressDisplay(
            num_packets,
            label=f"Receiving {self.name}",
            char_map=RECEIVER_STATUS_CHARS,
            default_status="waiting",
            disable=disable_bar,
        )
        self.retry_interval = max(5, min(self.timeout / 2, 60))
        self.last_control_sent = 0.0
        self.kill = False
        self.finished = False
        self.saved = False
        self._send_initial_ack()

    def update(self) -> None:
        """Refresh the receiver state and handle retransmission or timeouts."""

        now = time.time()
        if now - self.last_packet > self.timeout:
            if self.get_missing_nums():
                print(f'File Transfer "{self.name}" Failed')
                LOGGER.error("Transfer %s (%s) timed out", self.name, self.id)
                self.progress_display.set_many(self.get_missing_nums(), "missing")
                self.progress_display.close()
                self.kill = True
            else:
                self.save_to_file()
                self.kill = True
        elif not self.packet_dict and now - self.last_control_sent > self.retry_interval:
            LOGGER.warning(
                "No packets received yet for %s (%s), resending initial acknowledgement",
                self.name,
                self.id,
            )
            self._send_initial_ack()

    def add_packet(self, packet: bytearray) -> bool:
        """Process an incoming data packet."""

        if len(packet) < 2:
            LOGGER.warning("Ignoring truncated packet for %s (%s)", self.name, self.id)
            return False

        self.last_packet = time.time()
        packet_id = packet.pop(0)
        packet_index = packet.pop(0)
        if packet_id != self.id:
            LOGGER.debug("Packet for unexpected id %s received by %s", packet_id, self.id)
            return False

        if packet_index >= self.num_packets:
            LOGGER.debug("Ignoring out-of-range packet %s for %s", packet_index, self.name)
            return False

        is_new_packet = packet_index not in self.packet_dict
        self.packet_dict[packet_index] = bytes(packet)
        LOGGER.debug("Received packet #%s for %s (%s)", packet_index, self.name, self.id)
        ack_packet = Packaging_Data.make_status_packet(self.id, 5, opt_data=[packet_index])
        self._send_control_packet(ack_packet, description=f"ack #{packet_index}")
        if is_new_packet:
            self.progress_display.set_status(packet_index, "received")
            if len(self.packet_dict) == self.num_packets:
                self.save_to_file()
        return True

    def manage_com_packet(self, packet: bytearray) -> None:
        """Handle a control packet sent by the transfer peer."""

        self.last_packet = time.time()
        packet_type = packet[5]
        if packet_type == 2:  # Done Transmitting
            missing_packets = self.get_missing_nums()
            if missing_packets:
                LOGGER.info("Transfer %s (%s) missing packets: %s", self.name, self.id, missing_packets)
                ret_packet = Packaging_Data.make_status_packet(self.id, 3, opt_data=missing_packets)
                self.progress_display.set_many(missing_packets, "missing")
            else:
                ret_packet = Packaging_Data.make_status_packet(self.id, 4)
                self.save_to_file()
                self.kill = True
            self._send_control_packet(ret_packet, description="completion update")

    def get_missing_nums(self) -> List[int]:
        """Return a list of missing packet indices."""

        return [num for num in range(self.num_packets) if num not in self.packet_dict]

    def save_to_file(self) -> bool:
        """Persist received packets to ``self.name`` in the original order."""

        if self.saved or self.get_missing_nums():
            return False

        try:
            path = Path(self.name)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("wb") as destination:
                for index in range(self.num_packets):
                    destination.write(self.packet_dict[index])
            self.progress_display.set_all("stored")
            self.progress_display.close()
            self.finished = True
            self.saved = True
            LOGGER.info("Saved file %s (%s packets)", self.name, self.num_packets)
        except OSError as exc:
            print(f"Failed to save {self.name}: {exc}")
            LOGGER.exception("Failed to save %s: %s", self.name, exc)
            self.kill = True
            self.progress_display.close(final_status="failed")
            return False
        return True

    def _send_initial_ack(self) -> None:
        initial_ack = Packaging_Data.make_status_packet(self.id, 1)
        self._send_control_packet(initial_ack, description="initial ack")

    def resend_initial_ack(self) -> None:
        LOGGER.info("Resending initial acknowledgement for %s (%s)", self.name, self.id)
        self._send_initial_ack()

    def _send_control_packet(self, data: Sequence[int], description: str = "control") -> None:
        """Send ``data`` over ``self.interface`` to ``self.sending_id``."""

        try:
            self.interface.sendData(
                bytes(data),
                destinationId=self.sending_id,
                portNum=TRANSFER_PORTNUM,
                wantAck=True,
            )
            self.last_control_sent = time.time()
            LOGGER.debug("Sent %s packet for %s (%s)", description, self.name, self.id)
        except Exception as exc:  # pragma: no cover - interface errors are environment specific
            print(f"Failed to send control packet for {self.name}: {exc}")
            LOGGER.exception("Failed to send %s packet for %s (%s): %s", description, self.name, self.id, exc)


class FileTransferSender:
    """Coordinate packetised file transmission to a peer."""

    def __init__(
        self,
        file_name: str,
        file_id: int,
        interface,
        destination_id: str,
        send_delay: float = 10,
        packet_len: int = 200,
        disable_bar: bool = True,
    ) -> None:
        self.name = file_name
        self.display_name = os.path.basename(file_name) or file_name
        self.id = file_id  # 0 Reserved for file meta packets
        self.interface = interface
        self.destination_id = destination_id
        self.packet_len = packet_len
        data_dict = Packaging_Data.split_data(file_name, packet_len)
        self.data_dict = dict(Packaging_Data.package_data(data_dict, self.id))
        self.delay = max(send_delay, 0.1)
        self.retry_timeout = max(self.delay * 2, 3)
        self.window_size = max(1, min(5, len(self.data_dict)))
        self.packet_num = len(self.data_dict)
        self.last_send = 0.0
        self.last_activity = time.time()
        self.mode = 0  # 0: Waiting for initial ack, 2: Sending Data, 3: Waiting for completion confirmation
        self.kill = False
        self.finished = False
        self.to_send: List[int] = []
        self.pending_packets: MutableMapping[int, float] = {}
        self.acknowledged_packets: Set[int] = set()
        self.finish_sent = False
        self.disable_bar = disable_bar
        self.progress_display = ChunkProgressDisplay(
            self.packet_num,
            label=f"Sending {self.display_name}",
            char_map=SENDER_STATUS_CHARS,
            default_status="pending",
            disable=self.disable_bar,
        )
        self._min_retry_timeout = max(self.delay * 2, 3)
        self._max_retry_timeout = 300.0
        self.retry_timeout = self._min_retry_timeout
        self._smoothed_rtt: Optional[float] = None
        self.send_initial()

    def send_initial(self) -> None:
        """Send the initial transfer request."""

        init_str = Packaging_Data.make_initial_req(self.display_name, len(self.data_dict), self.id)
        try:
            self.interface.sendText(
                init_str,
                destinationId=self.destination_id,
                wantAck=True,
            )
            self.last_activity = time.time()
            LOGGER.info("Sent initial request for %s (%s packets) to %s", self.name, self.packet_num, self.destination_id)
        except Exception as exc:  # pragma: no cover - interface errors are environment specific
            print(f"Failed to send initial request for {self.name}: {exc}")
            LOGGER.exception("Failed to send initial request for %s (%s): %s", self.name, self.id, exc)
            self.kill = True
            self.progress_display.close(final_status="failed")

    def update(self) -> None:
        """Advance the sender state machine and enforce retry logic."""

        now = time.time()
        if self.kill:
            return

        if self.mode == 0:
            if now - self.last_activity > self.retry_timeout:
                LOGGER.warning("No acknowledgement yet for %s (%s), resending request", self.name, self.id)
                self.send_initial()
        elif self.mode == 2:
            self._fill_window()
            self._retry_pending(now)
            if (
                not self.to_send
                and not self.pending_packets
                and len(self.acknowledged_packets) == self.packet_num
                and not self.finish_sent
            ):
                self._send_finish()
        elif self.mode == 3:
            if not self.finish_sent:
                self._send_finish()
            elif now - self.last_activity > self.retry_timeout:
                LOGGER.warning("No completion ack for %s (%s), resending finish notice", self.name, self.id)
                self.finish_sent = False

        if now - self.last_activity > self.retry_timeout * 4:
            print("failed Send Timeout - no activity detected")
            LOGGER.error("Send timeout for %s (%s)", self.name, self.id)
            self.progress_display.close(final_status="failed")
            self.kill = True

    def manage_com_packet(self, packet: bytearray) -> None:
        """Handle control packets from the receiver."""

        packet_type = packet[5]
        self.last_activity = time.time()
        LOGGER.debug("Received control packet type %s for %s (%s)", packet_type, self.name, self.id)
        if packet_type == 0:
            print("Sending Denied")
            LOGGER.error("Transfer %s (%s) denied by receiver", self.name, self.id)
            self.progress_display.close(final_status="failed")
            self.kill = True
        elif packet_type == 1:
            if self.mode == 0:
                self.mode = 2
                self.to_send = list(self.data_dict.keys())
                LOGGER.info("Receiver accepted %s (%s packets queued)", self.name, len(self.to_send))
        elif packet_type == 3:
            needed_packets = list(packet[6:])
            for num in needed_packets:
                self.pending_packets.pop(num, None)
                self.acknowledged_packets.discard(num)
                if num not in self.to_send and num in self.data_dict:
                    self.to_send.append(num)
                self.progress_display.set_status(num, "awaiting_retry")
            self.finish_sent = False
            if self.mode != 2:
                self.mode = 2
            LOGGER.info("Receiver requested retransmit of %s packets for %s", len(needed_packets), self.name)
        elif packet_type == 4:
            self.progress_display.close()
            print(f"Confirmed File Transfer #{self.id} Complete")
            LOGGER.info("Receiver confirmed completion of %s (%s)", self.name, self.id)
            self.finished = True
            self.kill = True
        elif packet_type == 5:
            acknowledged = list(packet[6:])
            now = time.time()
            for num in acknowledged:
                sent_time = self.pending_packets.pop(num, None)
                if num not in self.acknowledged_packets and num < self.packet_num:
                    self.acknowledged_packets.add(num)
                    self.progress_display.set_status(num, "acknowledged")

                if sent_time is not None:
                    rtt = max(0.0, now - sent_time)
                    self._update_retry_timeout(rtt)
            LOGGER.debug("Acknowledged packets for %s: %s", self.name, acknowledged)
            if self.mode == 0:
                self.mode = 2

    def _fill_window(self) -> None:
        while self.to_send and len(self.pending_packets) < self.window_size and not self.kill:
            now = time.time()
            if self.last_send and now - self.last_send < self.delay:
                remaining = self.delay - (now - self.last_send)
                LOGGER.debug(
                    "Delaying send of next packet for %s (%s); %.2fs remaining in rate limit",
                    self.name,
                    self.id,
                    max(0, remaining),
                )
                break
            packet_index = self.to_send.pop(0)
            if packet_index in self.acknowledged_packets:
                continue
            LOGGER.debug("Sending packet #%s for %s (%s)", packet_index, self.name, self.id)
            self._send_packet(packet_index)

    def _retry_pending(self, now: float) -> None:
        for packet_index, timestamp in list(self.pending_packets.items()):
            elapsed = now - timestamp
            if elapsed > self.retry_timeout:
                self.pending_packets.pop(packet_index, None)
                self._backoff_retry_timeout(elapsed)
                if packet_index not in self.to_send:
                    self.to_send.append(packet_index)
                self.progress_display.set_status(packet_index, "awaiting_retry")
                LOGGER.warning("Packet #%s for %s timed out; rescheduling", packet_index, self.name)

    def _send_packet(self, packet_index: int) -> None:
        data = self.data_dict.get(packet_index)
        if data is None:
            LOGGER.error("Attempted to send missing packet #%s for %s", packet_index, self.name)
            return
        try:
            self.interface.sendData(
                bytes(data),
                portNum=TRANSFER_PORTNUM,
                destinationId=self.destination_id,
                wantAck=True,
            )
            sent_time = time.time()
            self.pending_packets[packet_index] = sent_time
            self.last_send = sent_time
            self.last_activity = sent_time
            self.progress_display.set_status(packet_index, "in_flight")
            LOGGER.debug("Packet #%s queued for delivery (%s)", packet_index, self.name)
        except Exception as exc:  # pragma: no cover - interface errors are environment specific
            print(f"Failed to send packet {packet_index} for {self.name}: {exc}")
            LOGGER.exception("Failed to send packet #%s for %s: %s", packet_index, self.name, exc)
            if packet_index not in self.to_send:
                self.to_send.append(packet_index)
            self.progress_display.set_status(packet_index, "awaiting_retry")

    def _send_finish(self) -> None:
        finish_packet = Packaging_Data.make_status_packet(self.id, 2)
        try:
            self.interface.sendData(
                bytes(finish_packet),
                portNum=TRANSFER_PORTNUM,
                destinationId=self.destination_id,
                wantAck=True,
            )
            self.finish_sent = True
            self.last_send = time.time()
            self.last_activity = self.last_send
            self.mode = 3
            LOGGER.info("Sent completion notice for %s (%s)", self.name, self.id)
        except Exception as exc:  # pragma: no cover - interface errors are environment specific
            print(f"Failed to send completion notice for {self.name}: {exc}")
            LOGGER.exception("Failed to send completion notice for %s (%s): %s", self.name, self.id, exc)
            self.finish_sent = False

    def _backoff_retry_timeout(self, elapsed: float) -> None:
        """Increase ``retry_timeout`` to better match the observed network delay."""

        proposed = max(self.retry_timeout * 1.5, elapsed * 2, self._min_retry_timeout)
        if proposed > self.retry_timeout:
            self.retry_timeout = min(proposed, self._max_retry_timeout)
            LOGGER.debug(
                "Retry timeout for %s increased to %.2fs after %.2fs elapsed",
                self.name,
                self.retry_timeout,
                elapsed,
            )

    def _update_retry_timeout(self, observed_rtt: float) -> None:
        """Adapt ``retry_timeout`` based on an ``observed_rtt`` sample."""

        if observed_rtt <= 0:
            return
        if self._smoothed_rtt is None:
            self._smoothed_rtt = observed_rtt
        else:
            alpha = 0.2
            self._smoothed_rtt = (1 - alpha) * self._smoothed_rtt + alpha * observed_rtt

        target = max(self._min_retry_timeout, self._smoothed_rtt * 2.5, observed_rtt * 2.5)
        capped = min(target, self._max_retry_timeout)
        if capped < self.retry_timeout:
            self.retry_timeout = max(capped, self._min_retry_timeout)
        else:
            self.retry_timeout = capped
        LOGGER.debug(
            "Retry timeout for %s adjusted to %.2fs based on RTT %.2fs",
            self.name,
            self.retry_timeout,
            observed_rtt,
        )
