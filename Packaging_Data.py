"""Utilities for packaging file transfer payloads.

All helper functions in this module take care of the fiddly byte handling that
is required to split a file into Meshtastic packets and wrap or unwrap the
protocol metadata.  Keeping the logic centralised here makes the sender and
receiver classes significantly easier to read and audit.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Mapping, Sequence, Tuple


PacketDict = Dict[int, bytearray]
PacketMapping = Mapping[int, bytes]


def split_data(path: str | Path, packet_size: int = 230) -> PacketDict:
    """Split ``path`` into packet-sized bytearrays.

    The return value is a dictionary keyed by packet index.  A dictionary is
    used because the ordering matters and the data structure mirrors the way
    the sender and receiver internally track missing packets.
    """

    if packet_size <= 0:
        raise ValueError("packet_size must be a positive integer")

    packets: PacketDict = {}
    path = Path(path)
    with path.open("rb") as source:
        index = 0
        while True:
            chunk = bytearray(source.read(packet_size))
            if not chunk:
                break
            packets[index] = chunk
            index += 1
    return packets


def package_data(byte_dict: PacketDict, file_id_num: int) -> PacketMapping:
    """Return a new mapping where each packet is prefixed with protocol bytes."""

    packaged: Dict[int, bytes] = {}
    for packet_num, payload in byte_dict.items():
        packet = bytearray(payload)
        packet.insert(0, packet_num)
        packet.insert(0, file_id_num)
        packaged[packet_num] = bytes(packet)
    return packaged


def send_packets_dict_to_file(byte_dict: PacketMapping, file_name: str = "Sending/packets.txt") -> None:
    """Persist packet payloads in ``byte_dict`` to ``file_name``."""

    with Path(file_name).open("wb") as destination:
        for payload in byte_dict.values():
            destination.write(bytes(payload))


def make_initial_req(file_name: str, packet_num: int, file_id: int) -> str:
    """Return the initial request string for a file transfer."""

    return f"!fcom,file:{file_name},packets:{packet_num},id:{file_id}"


def decode_initial_req(message: bytes | bytearray | str) -> Tuple[str, int, int]:
    """Extract the file name, packet count and file ID from ``message``."""

    if isinstance(message, (bytes, bytearray)):
        string = message.decode("utf8", errors="ignore")
    else:
        string = str(message)

    string = string.strip()
    if not string.startswith("!fcom,"):
        raise ValueError(f"Unsupported initial request format: {message!r}")

    fields = string.split(",")
    values = {}
    for field in fields[1:]:
        try:
            key, value = field.split(":", 1)
        except ValueError as exc:
            raise ValueError(f"Malformed field in initial request: {field!r}") from exc
        values[key] = value

    try:
        file_name = values["file"]
        file_id = int(values["id"])
        packet_count = int(values["packets"])
    except KeyError as exc:
        raise ValueError(f"Missing field in initial request: {exc.args[0]}") from exc

    return file_name, file_id, packet_count


def make_status_packet(file_id: int, packet_type: int, opt_data: Sequence[int] | None = None) -> bytearray:
    """Build a status packet according to the Meshtastic file-transfer protocol."""

    packet = bytearray(b"fcom")
    packet.append(file_id)
    packet.append(packet_type)
    if opt_data:
        packet.extend(int(value) & 0xFF for value in opt_data)
    return packet


def send_packets_list_to_file(byte_list: Iterable[bytes], file_name: str = "Sending/radio_packets.txt") -> None:
    """Persist raw packet payloads from ``byte_list`` to ``file_name``."""

    with Path(file_name).open("wb") as destination:
        for payload in byte_list:
            destination.write(bytes(payload))

