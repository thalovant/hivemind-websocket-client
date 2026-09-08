import json
import sys
from inspect import signature

from bitstring import BitArray, BitStream

from hivemind_bus_client.exceptions import UnsupportedProtocolVersion
from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessageType, HiveMessage, HiveMindBinaryPayloadType
from hivemind_bus_client.util import compress_payload, decompress_payload, cast2bytes, bytes2str

PROTOCOL_VERSION = 1  # integer, a version increase signals new functionality added
                      # version 0 is the original hivemind protocol, 1 supports handshake + binary


_INT2TYPE = {0: HiveMessageType.HANDSHAKE,
             1: HiveMessageType.BUS,
             2: HiveMessageType.SHARED_BUS,
             3: HiveMessageType.BROADCAST,
             4: HiveMessageType.PROPAGATE,
             5: HiveMessageType.ESCALATE,
             6: HiveMessageType.HELLO,
             7: HiveMessageType.QUERY,
             8: HiveMessageType.CASCADE,
             9: HiveMessageType.PING,
             10: HiveMessageType.RENDEZVOUS,
             11: HiveMessageType.THIRDPRTY,
             12: HiveMessageType.BINARY}


def get_bitstring(hive_type=HiveMessageType.BUS, payload=None,
                  compressed=None, hivemeta=None,
                  binary_type=HiveMindBinaryPayloadType.UNDEFINED,
                  proto_version=PROTOCOL_VERSION, versioned=False):
    if proto_version <= 1:
        if compressed is None:  # auto
            unc = _get_bitstring_v1(hive_type, payload, False, hivemeta, binary_type, versioned)
            comp = _get_bitstring_v1(hive_type, payload, True, hivemeta, binary_type, versioned)
            if len(unc) <= len(comp):
                return unc
            return comp
        return _get_bitstring_v1(hive_type, payload, bool(compressed), hivemeta, binary_type, versioned)
    raise UnsupportedProtocolVersion(f"Max Supported Version: {PROTOCOL_VERSION}")


def _get_bitstring_v1(hive_type=HiveMessageType.BUS, payload=None,
                      compressed=True, hivemeta=None,
                      binary_type=HiveMindBinaryPayloadType.UNDEFINED, versioned=False):
    # there are 12 hivemind message main types
    typemap = {v: k for k, v in _INT2TYPE.items()}
    binmap = {e: e.value for e in HiveMindBinaryPayloadType}

    s = BitArray()
    s.append(f'uint:1={int(1)}')  # always start with a 1, 0s to the left for padding so it can be cast to bytes
    s.append(f'uint:1={int(versioned)}')  # 1 bit unsigned integer - requires protocol version
    if versioned:
        s.append(f'uint:8={PROTOCOL_VERSION}')
    s.append(f'uint:5={typemap.get(hive_type, 11)}')  # 5 bit unsigned integer - the hive msg type
    s.append(f'uint:1={int(bool(compressed))}')  # 1 bit unsigned integer - payload is zlib compressed

    # NOTE: hivemind meta is reserved TBD arbitrary data
    hivemeta = cast2bytes(hivemeta or {}, compressed)
    s.append(f'uint:8={len(hivemeta)}')  # 8 bit unsigned integer - N of bytes for metadata
    s.append(hivemeta)  # arbitrary hivemind meta

    # when payload is binary data meant to be passed along raw and not parsed
    if hive_type == HiveMessageType.BINARY:
        # 4 bit unsigned integer - integer indicating pseudo format of bin content
        s.append(f'uint:4={binmap.get(binary_type, 0)}')
    # the remaining bits are the payload
    else:
        if hasattr(payload, "serialize"):
            payload = payload.serialize()
        payload = cast2bytes(payload, compressed)

    s.append(payload)

    # pad
    while len(s) % 8 != 0:
        s.insert(f'uint:1={int(0)}', 0)

    return s


def decode_bitstring(bitstr):
    s = BitStream(bitstr)
    pad = False
    while not pad:
        pad = s.read(1).bool
    versioned = s.read(1).bool
    if versioned:
        proto_version = s.read(8).uint
    else:
        proto_version = PROTOCOL_VERSION
    if proto_version <= 1:
        return _decode_bitstring_v1(s)
    raise UnsupportedProtocolVersion(f"Max Supported Version: {PROTOCOL_VERSION}")


def _decode_bitstring_v1(s):
    binmap = {e: e.value for e in HiveMindBinaryPayloadType}

    hive_type = _INT2TYPE.get(s.read(5).uint, 11)
    compressed = s.read(1).bool  # note: bool(BitStream) checks length (always True), .bool reads the actual bit

    metalen = s.read(8).uint * 8
    meta = s.read(metalen)

    # TODO standardize hivemind meta
    meta = json.loads(bytes2str(meta.bytes, compressed))

    is_bin = hive_type == HiveMessageType.BINARY
    bin_type = HiveMindBinaryPayloadType.UNDEFINED
    if is_bin:
        bin_type = binmap.get(s.read(4).uint, 0)

    payload_len = len(s) - s.pos
    payload = s.read(payload_len)

    if not is_bin:
        payload = bytes2str(payload.bytes, compressed)
    else:
        payload = payload.bytes

    return HiveMessage(hive_type, payload,
                       metadata=meta, bin_type=bin_type)


def mycroft2bitstring(msg, compressed=False):
    if isinstance(msg, str):
        msg = Message.deserialize(msg)
    return get_bitstring(HiveMessageType.BUS, payload=msg, hivemeta=msg.context, compressed=compressed)


if __name__ == "__main__":
    d = {e: e.value for e in HiveMindBinaryPayloadType}
    from hivemind_bus_client.message import Message

    text = """The Mycroft project is also working on and selling smart speakers that run its software. All of its hardware is open-source, released under the CERN Open Hardware Licence.
Its first hardware project was the Mark I, targeted primarily at developers. Its production was partially funded through a Kickstarter campaign, which finished successfully. Units started shipping out in April 2016.
Its most recent hardware project is the Mark II, intended for general usage, not just for developers. Unlike the Mark I, the Mark II is equipped with a screen, being able to relay information both visually as well as acoustically. As with the Mark I, the Mark II's production was partially funded through a Kickstarter campaign, which wrapped up in February 2018, hitting almost 8 times its original goal. As of February 2021, the Mark II had not yet begun shipping to crowd-funders, though shipping of the Development Kit was imminent.
Mycroft announced that a third hardware project, Mark III, will be offered through Kickstarter, and that an entire product line of Mark I, II, and III will be released to stores by November, 2019"""

    payload = Message("speak", {"utterance": text})

    json_plod = HiveMessage(HiveMessageType.BUS, payload)
    n_json_bits = len(json_plod.serialize().encode("utf-8")) * 8
    # 10096 - naive json2bytes

    bitstr = get_bitstring(hive_type=HiveMessageType.BUS,
                           payload=payload,
                           compressed=False)
    n_unc_bits = len(bitstr)
    # 9494  - uncompressed HM

    bitstr = get_bitstring(hive_type=HiveMessageType.BUS,
                           payload=payload,
                           compressed=True)
    n_enc_bits = len(bitstr)
    # 4886 - compressed HM  (small strings might actually become larger)

    decoded = decode_bitstring(bitstr)
    print(decoded)

    payload = HiveMessage(HiveMessageType.BUS,
                          payload=Message("speak", {"utterance": "RED ALERT"}))
    bitstr = get_bitstring(hive_type=HiveMessageType.BROADCAST,
                           payload=payload,
                           compressed=False)

    decoded = decode_bitstring(bitstr)
    print(decoded)

    compressed = compress_payload(text).hex()


    # 789c5590c16e84300c44ef7cc51c5ba942bdee1fb4528ffb03261848156c9418e8fe7d9daebab0b72863cfbcf1758a05c32ac1a20afc6d1363c971a67c4314e33c506098bae0eaacfd9a18945446ecd126343d079d97cca5bcbc3e9c5a5c9f8c33db9aa5a0bb1943bb6f0ee66ffc6f4677abc13d19a119e3c65223a3810a16ca34b39354533e3c27d748d4f7f231834029718fc41b27ec530c8e18542c6bba97e31f6331e870a457de4fcf92bfc6a3bb746c3b3bc47bc5b8b4f8d29d8b61a3b4b27f36c5487aefa719a2672337e971c149efeae253d4471c2b7385b9633a4b739a78c39899ec3122a3dff9c4ebf54e776c7f0106a5a377

    def measure_compression(text):
        text = text.encode("utf-8")
        text_size = sys.getsizeof(text)
        print("N bytes of original text", text_size)

        compressed = compress_payload(text)
        csize = sys.getsizeof(compressed)
        print("N bytes of compressed text", csize)

        decompressed = decompress_payload(compressed)
        dsize = sys.getsizeof(decompressed)
        print("N bytes of decompressed text", dsize)

        sdiff = text_size - csize
        print("Difference of N bytes", sdiff)

        print("N bytes reduced by", sdiff * 100 / text_size, "%")

        return sdiff * 100 / text_size


    measure_compression(text)
    # N bytes of original text 1153
    # N bytes of compressed text 588
    # N bytes of decompressed text 1153
    # Difference of N bytes 565
    # N bytes reduced by 49.00260190806591 %
