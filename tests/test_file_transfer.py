import heapq
import os
import sys
import types
from unittest.mock import patch


# Provide a minimal stub for the optional meshtastic dependency so the
# core transfer logic can be imported without the real radio bindings.
if "meshtastic" not in sys.modules:
    meshtastic_module = types.ModuleType("meshtastic")
    portnums_module = types.ModuleType("meshtastic.portnums_pb2")
    portnums_module.IP_TUNNEL_APP = 1
    meshtastic_module.portnums_pb2 = portnums_module
    sys.modules["meshtastic"] = meshtastic_module
    sys.modules["meshtastic.portnums_pb2"] = portnums_module


TEST_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if TEST_ROOT not in sys.path:
    sys.path.insert(0, TEST_ROOT)


import Packaging_Data
import file_classes


class FakeTime:
    def __init__(self):
        self._now = 0.0

    def time(self):
        return self._now

    def sleep(self, delta):
        self._now += delta

    def advance(self, delta):
        self._now += delta


class DummyProgressBar:
    def __init__(self, *args, **kwargs):
        pass

    def update(self, *_args, **_kwargs):
        pass

    def close(self):
        pass


class FakeInterface:
    def __init__(self, node_id, network):
        self.node_id = node_id
        self.network = network
        self.sent_texts = []

    def sendData(self, data, destinationId, portNum=None, wantAck=False):
        del portNum, wantAck  # Unused in tests
        self.network.send_data(self.node_id, destinationId, data)

    def sendText(self, text, destinationId, wantAck=False):
        self.sent_texts.append((destinationId, text))
        self.network.send_text(self.node_id, destinationId, text)


class FakeNetwork:
    def __init__(self, clock, base_delay=0.0):
        self.clock = clock
        self.base_delay = base_delay
        self._events = []
        self._handlers = {}
        self.data_hook = None
        self.text_hook = None

    def register(self, node_id, on_text=None, on_data=None):
        self._handlers[node_id] = {"text": on_text, "data": on_data}

    def send_text(self, src, dest, text):
        deliver = True
        delay = self.base_delay
        if self.text_hook:
            hook_result = self.text_hook(src, dest, text)
            if hook_result:
                deliver = hook_result.get("deliver", True)
                delay += hook_result.get("delay", 0.0)
        if deliver:
            heapq.heappush(self._events, (self.clock.time() + delay, dest, "text", src, text))

    def send_data(self, src, dest, data):
        deliver = True
        delay = self.base_delay
        if self.data_hook:
            hook_result = self.data_hook(src, dest, data)
            if hook_result:
                deliver = hook_result.get("deliver", True)
                delay += hook_result.get("delay", 0.0)
        if deliver:
            heapq.heappush(self._events, (self.clock.time() + delay, dest, "data", src, data))

    def deliver(self):
        while self._events and self._events[0][0] <= self.clock.time():
            _, dest, kind, src, payload = heapq.heappop(self._events)
            handler = self._handlers.get(dest)
            if not handler:
                continue
            callback = handler.get(kind)
            if callback:
                callback(payload, src)


class ReceiverNode:
    def __init__(self, interface):
        self.interface = interface
        self.receiver = None

    def on_text(self, text, src):
        if self.receiver is None:
            file_name, file_id, packet_count = Packaging_Data.decode_initial_req(text)
            self.receiver = file_classes.FileTransferReceiver(
                file_name,
                file_id,
                packet_count,
                self.interface,
                src,
                timeout=60,
                disable_bar=True,
            )

    def on_data(self, data, _src):
        if self.receiver is None:
            return
        if data.startswith(b"fcom"):
            self.receiver.manage_com_packet(bytearray(data))
        else:
            self.receiver.add_packet(bytearray(data))


def run_transfer(sender, receiver_node, network, clock, step=0.2, max_steps=2000):
    for _ in range(max_steps):
        network.deliver()
        sender.update()
        if receiver_node.receiver:
            receiver_node.receiver.update()
        network.deliver()
        if sender.finished and receiver_node.receiver and receiver_node.receiver.finished:
            return
        clock.advance(step)
    raise AssertionError("transfer did not complete in time")


def build_transfer(tmp_path, packet_len=120, send_delay=0.2, base_delay=0.0, data_hook=None):
    payload = os.urandom(packet_len * 4 + 37)
    source_path = tmp_path / "sample.bin"
    source_path.write_bytes(payload)

    fake_time = FakeTime()
    network = FakeNetwork(fake_time, base_delay=base_delay)
    network.data_hook = data_hook

    sender_interface = FakeInterface("sender", network)
    receiver_interface = FakeInterface("receiver", network)
    receiver_node = ReceiverNode(receiver_interface)
    network.register("receiver", on_text=receiver_node.on_text, on_data=receiver_node.on_data)

    return payload, fake_time, network, sender_interface, receiver_node, source_path, send_delay


def test_transfer_completes_on_open_channel(tmp_path):
    payload, fake_time, network, sender_iface, receiver_node, source_path, send_delay = build_transfer(
        tmp_path, packet_len=100, send_delay=0.1
    )
    with patch("file_classes.time", fake_time), patch("file_classes.tqdm.tqdm", DummyProgressBar):
        sender = file_classes.FileTransferSender(
            str(source_path),
            42,
            sender_iface,
            "receiver",
            send_delay=send_delay,
            packet_len=100,
            disable_bar=True,
        )
        network.register(
            "sender",
            on_data=lambda data, _src: sender.manage_com_packet(bytearray(data)),
        )
        run_transfer(sender, receiver_node, network, fake_time)

    assert sender.finished
    assert receiver_node.receiver is not None and receiver_node.receiver.finished
    assert source_path.read_bytes() == payload


def test_initial_request_uses_basename(tmp_path):
    _payload, fake_time, network, sender_iface, _receiver_node, source_path, send_delay = build_transfer(
        tmp_path, packet_len=64, send_delay=0.1
    )

    with patch("file_classes.time", fake_time), patch("file_classes.tqdm.tqdm", DummyProgressBar):
        file_classes.FileTransferSender(
            str(source_path),
            7,
            sender_iface,
            "receiver",
            send_delay=send_delay,
            packet_len=64,
            disable_bar=True,
        )

    assert sender_iface.sent_texts, "no initial request was sent"
    _dest, message = sender_iface.sent_texts[0]
    assert f"!fcom,file:{source_path.name}," in message


def test_transfer_handles_out_of_order_delivery(tmp_path):
    def out_of_order_hook(src, dest, data):
        if src == "sender" and dest == "receiver" and not data.startswith(b"fcom"):
            packet_index = data[1]
            return {"delay": (packet_index % 3) * 0.05}
        return {}

    payload, fake_time, network, sender_iface, receiver_node, source_path, send_delay = build_transfer(
        tmp_path, packet_len=90, send_delay=0.1, data_hook=out_of_order_hook
    )
    with patch("file_classes.time", fake_time), patch("file_classes.tqdm.tqdm", DummyProgressBar):
        sender = file_classes.FileTransferSender(
            str(source_path),
            42,
            sender_iface,
            "receiver",
            send_delay=send_delay,
            packet_len=90,
            disable_bar=True,
        )
        network.register(
            "sender",
            on_data=lambda data, _src: sender.manage_com_packet(bytearray(data)),
        )
        run_transfer(sender, receiver_node, network, fake_time)

    assert sender.finished
    assert receiver_node.receiver is not None and receiver_node.receiver.finished
    assert source_path.read_bytes() == payload


def test_transfer_recovers_from_packet_loss(tmp_path):
    drops = {}

    def lossy_hook(src, dest, data):
        if src == "sender" and dest == "receiver" and not data.startswith(b"fcom"):
            packet_index = data[1]
            drops.setdefault(packet_index, 0)
            if packet_index == 1 and drops[packet_index] == 0:
                drops[packet_index] += 1
                return {"deliver": False}
            drops[packet_index] += 1
        return {}

    payload, fake_time, network, sender_iface, receiver_node, source_path, send_delay = build_transfer(
        tmp_path, packet_len=80, send_delay=0.2, data_hook=lossy_hook
    )
    with patch("file_classes.time", fake_time), patch("file_classes.tqdm.tqdm", DummyProgressBar):
        sender = file_classes.FileTransferSender(
            str(source_path),
            42,
            sender_iface,
            "receiver",
            send_delay=send_delay,
            packet_len=80,
            disable_bar=True,
        )
        network.register(
            "sender",
            on_data=lambda data, _src: sender.manage_com_packet(bytearray(data)),
        )
        run_transfer(sender, receiver_node, network, fake_time, step=0.5, max_steps=4000)

    assert drops.get(1, 0) >= 2  # ensure the lost packet was retried
    assert sender.finished
    assert receiver_node.receiver is not None and receiver_node.receiver.finished
    assert source_path.read_bytes() == payload


def test_transfer_completes_with_link_delay(tmp_path):
    payload, fake_time, network, sender_iface, receiver_node, source_path, send_delay = build_transfer(
        tmp_path, packet_len=70, send_delay=0.2, base_delay=0.6
    )
    with patch("file_classes.time", fake_time), patch("file_classes.tqdm.tqdm", DummyProgressBar):
        sender = file_classes.FileTransferSender(
            str(source_path),
            42,
            sender_iface,
            "receiver",
            send_delay=send_delay,
            packet_len=70,
            disable_bar=True,
        )
        network.register(
            "sender",
            on_data=lambda data, _src: sender.manage_com_packet(bytearray(data)),
        )
        run_transfer(sender, receiver_node, network, fake_time, step=0.3, max_steps=4000)

    assert sender.finished
    assert receiver_node.receiver is not None and receiver_node.receiver.finished
    assert source_path.read_bytes() == payload

