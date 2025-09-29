import sys
import types


if "meshtastic" not in sys.modules:
    meshtastic_module = types.ModuleType("meshtastic")
    sys.modules["meshtastic"] = meshtastic_module
else:
    meshtastic_module = sys.modules["meshtastic"]


serial_module = sys.modules.get("meshtastic.serial_interface")
if serial_module is None:
    serial_module = types.ModuleType("meshtastic.serial_interface")

    class SerialInterface:  # pragma: no cover - stub for import side effects
        def __init__(self, *args, **kwargs):
            raise RuntimeError("SerialInterface stub should not be instantiated in tests")

    serial_module.SerialInterface = SerialInterface
    sys.modules["meshtastic.serial_interface"] = serial_module


util_module = sys.modules.get("meshtastic.util")
if util_module is None:
    util_module = types.ModuleType("meshtastic.util")

    def findPorts(_=None):  # pragma: no cover - stub for import side effects
        return []

    util_module.findPorts = findPorts
    sys.modules["meshtastic.util"] = util_module


from Receiver import receiver


class DummyInterface:
    def __init__(self, name='radio'):
        self._name = name

    def getShortName(self):
        return self._name


def _reset_queues():
    receiver.Queue.clear()
    receiver.Text_Queue.clear()


def test_on_receive_routes_text_messages():
    _reset_queues()
    packet = {
        'decoded': {
            'portnum': receiver.TRANSFER_PORT_NAME,
            'text': 'hello world',
            'payload': bytearray(b'hello world'),
        },
        'fromId': '!abcd',
    }
    iface = DummyInterface()

    receiver.on_receive(packet, iface)

    assert receiver.Queue == []
    assert receiver.Text_Queue == [(iface.getShortName(), packet)]


def test_on_receive_routes_binary_payloads():
    _reset_queues()
    binary_packet = {
        'decoded': {
            'portnum': receiver.TRANSFER_PORT_NAME,
            'payload': bytearray(b'\x01\x02\x03'),
        },
        'fromId': '!peer',
    }
    iface = DummyInterface('peer_radio')

    receiver.on_receive(binary_packet, iface)

    assert receiver.Text_Queue == []
    assert receiver.Queue == [(iface.getShortName(), binary_packet)]

