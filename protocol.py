"""Length-prefixed JSON messages over TCP."""

import json
import struct

MAX_FRAME_SIZE = 1024 * 1024  # 1 MiB per message


class ProtocolError(Exception):
    """The peer sent an invalid or incomplete message."""


def recv_exact(sock, size):
    """Read exactly size bytes; return None for EOF before any bytes."""
    data = bytearray()

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            if not data:
                return None
            raise ProtocolError("Connection closed during a message.")

        data.extend(chunk)

    return bytes(data)


def reject_constant(value):
    raise ValueError(f"Invalid JSON constant: {value}")


def encode_message(message):
    """Convert a JSON object into a header and UTF-8 payload."""
    if not isinstance(message, dict):
        raise ProtocolError("Messages must be JSON objects.")

    try:
        payload = json.dumps(
            message,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ProtocolError("Message cannot be encoded as JSON.") from exc

    if not 0 < len(payload) <= MAX_FRAME_SIZE:
        raise ProtocolError("Message size is outside the allowed range.")

    return struct.pack("!I", len(payload)) + payload


def send_message(sock, message):
    """sendall handles partial socket writes."""
    sock.sendall(encode_message(message))


def recv_message(sock):
    """Read one framed JSON object; return None on clean disconnect."""
    header = recv_exact(sock, 4)
    if header is None:
        return None

    size = struct.unpack("!I", header)[0]
    if not 0 < size <= MAX_FRAME_SIZE:
        raise ProtocolError("Invalid frame length.")

    payload = recv_exact(sock, size)
    if payload is None:
        raise ProtocolError("Connection closed before the payload.")

    try:
        message = json.loads(
            payload.decode("utf-8"),
            parse_constant=reject_constant,
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ProtocolError("Payload must contain valid UTF-8 JSON.") from exc

    if not isinstance(message, dict):
        raise ProtocolError("Payload must be a JSON object.")

    return message
