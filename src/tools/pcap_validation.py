"""Classic PCAP structural validation helpers."""

import struct


def validate_pcap_structure(pcap_path):
    """Validate classic PCAP record boundaries and captured lengths."""
    magic_endianness = {
        b"\xd4\xc3\xb2\xa1": "<",
        b"\xa1\xb2\xc3\xd4": ">",
        b"\x4d\x3c\xb2\xa1": "<",
        b"\xa1\xb2\x3c\x4d": ">",
    }

    try:
        with open(pcap_path, "rb") as f:
            global_header = f.read(24)
            if len(global_header) != 24:
                return False, f"global_header_size={len(global_header)}"

            endian = magic_endianness.get(global_header[:4])
            if endian is None:
                return False, f"unsupported_magic={global_header[:4].hex()}"

            _, major, minor, _, _, snaplen, _ = struct.unpack(
                f"{endian}IHHIIII",
                global_header,
            )
            if (major, minor) != (2, 4):
                return False, f"unsupported_version={major}.{minor}"
            if snaplen <= 0:
                return False, f"invalid_snaplen={snaplen}"

            packet_index = 0
            while True:
                record_offset = f.tell()
                record_header = f.read(16)
                if not record_header:
                    break
                if len(record_header) != 16:
                    return False, (
                        f"truncated_record_header index={packet_index + 1} "
                        f"offset={record_offset} size={len(record_header)}"
                    )

                _, _, captured_length, original_length = struct.unpack(
                    f"{endian}IIII",
                    record_header,
                )
                packet_index += 1
                if captured_length > snaplen:
                    return False, (
                        f"captured_length={captured_length}>snaplen={snaplen} "
                        f"index={packet_index} offset={record_offset}"
                    )
                if captured_length > original_length:
                    return False, (
                        f"captured_length={captured_length}>original_length={original_length} "
                        f"index={packet_index} offset={record_offset}"
                    )

                packet_data = f.read(captured_length)
                if len(packet_data) != captured_length:
                    return False, (
                        f"truncated_packet index={packet_index} offset={record_offset} "
                        f"expected={captured_length} actual={len(packet_data)}"
                    )

            if packet_index == 0:
                return False, "no_packets"
    except OSError as e:
        return False, f"{type(e).__name__}: {e}"

    return True, ""
