"""Concurrent verification and TCP framing checks.

Run against a freshly started server with no other clients.
"""

import socket
import struct
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from protocol import MAX_FRAME_SIZE, encode_message, recv_message, send_message

HOST = "127.0.0.1"
PORT = 9090
WORKERS = 20


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def connect():
    return socket.create_connection((HOST, PORT), timeout=30)


def exchange(sock, request):
    send_message(sock, request)
    response = recv_message(sock)
    check(response is not None, "Server disconnected before responding.")
    return response


def worker(number, run_id, barrier):
    key = f"{run_id}:key:{number}"
    value = {"worker": number, "text": "Hello, café!"}
    task = f"{run_id}:task:{number}"

    barrier.wait(timeout=30)

    with connect() as sock:
        response = exchange(
            sock, {"action": "PUT", "key": key, "value": value}
        )
        check(response.get("ok") is True, f"PUT failed: {response}")

        response = exchange(sock, {"action": "GET", "key": key})
        check(
            response.get("ok") is True and response.get("value") == value,
            f"GET returned incorrect data: {response}",
        )

        response = exchange(sock, {"action": "ENQUEUE", "value": task})
        check(response.get("ok") is True, f"ENQUEUE failed: {response}")

        response = exchange(sock, {"action": "DEQUEUE", "timeout": 2})
        check(response.get("ok") is True, f"DEQUEUE failed: {response}")

        # A shared queue can return another worker's task.
        # Validate the full collection after all workers finish.
        return number, response["value"]


def protocol_checks(run_id):
    key = f"{run_id}:fragmented"

    with connect() as sock:
        frame = encode_message(
            {"action": "PUT", "key": key, "value": "fragmented"}
        )

        # Send the header and payload in small pieces.
        for offset in range(0, len(frame), 2):
            sock.sendall(frame[offset:offset + 2])
            time.sleep(0.001)

        check(recv_message(sock) == {"ok": True}, "Fragmented PUT failed.")

        # Two complete frames in one write must yield two responses.
        sock.sendall(
            encode_message({"action": "GET", "key": key})
            + encode_message({"action": "GET", "key": key})
        )

        for _ in range(2):
            check(
                recv_message(sock) == {"ok": True, "value": "fragmented"},
                "Combined frames were decoded incorrectly.",
            )

        response = exchange(
            sock, {"action": "GET", "key": f"{run_id}:missing"}
        )
        check(response.get("error") == "NOT_FOUND", "Missing-key check failed.")

        response = exchange(sock, {"action": "UNKNOWN"})
        check(response.get("error") == "BAD_REQUEST", "Action check failed.")

        response = exchange(sock, {"action": "DEQUEUE", "timeout": 0})
        check(response.get("error") == "EMPTY", "Queue is not empty.")

        # Sequential requests must preserve FIFO order.
        for item in ("first", "second"):
            check(
                exchange(sock, {"action": "ENQUEUE", "value": item})
                == {"ok": True},
                "FIFO enqueue failed.",
            )

        for item in ("first", "second"):
            check(
                exchange(sock, {"action": "DEQUEUE"})
                == {"ok": True, "value": item},
                "FIFO order was not preserved.",
            )

    # Invalid JSON inside an otherwise valid frame.
    with connect() as sock:
        payload = b"{invalid"
        sock.sendall(struct.pack("!I", len(payload)) + payload)
        response = recv_message(sock)
        check(
            response is not None
            and response.get("error") == "PROTOCOL_ERROR",
            "Malformed JSON was not rejected.",
        )

    # Reject oversized frames before trying to read their payload.
    with connect() as sock:
        sock.sendall(struct.pack("!I", MAX_FRAME_SIZE + 1))
        response = recv_message(sock)
        check(
            response is not None
            and response.get("error") == "PROTOCOL_ERROR",
            "Oversized frame was not rejected.",
        )

    # Disconnect partway through a header and then a payload.
    for incomplete in (b"\x00\x00", struct.pack("!I", 100) + b"{"):
        with connect() as sock:
            sock.sendall(incomplete)

    # The server must still accept valid clients afterward.
    with connect() as sock:
        check(
            exchange(sock, {"action": "GET", "key": key})
            == {"ok": True, "value": "fragmented"},
            "Server failed after incomplete client messages.",
        )


def main():
    run_id = uuid.uuid4().hex
    barrier = threading.Barrier(WORKERS)
    started = time.monotonic()

    print(f"[*] Launching {WORKERS} concurrent client worker threads...")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [
            pool.submit(worker, number, run_id, barrier)
            for number in range(WORKERS)
        ]
        results = [future.result() for future in futures]

    returned_tasks = [task for _, task in results]
    expected_tasks = {
        f"{run_id}:task:{number}" for number in range(WORKERS)
    }

    check(
        len(returned_tasks) == WORKERS
        and set(returned_tasks) == expected_tasks,
        "Tasks were lost, duplicated, or unexpectedly present.",
    )

    for number, _ in sorted(results):
        print(f"[Worker Thread {number:02d}] Transaction verified successfully.")

    elapsed = time.monotonic() - started
    print(f"[+] All {WORKERS} workers completed execution in {elapsed:.3f} seconds.")

    protocol_checks(run_id)
    print("[+] Framing, FIFO, validation, and disconnection checks passed.")


if __name__ == "__main__":
    main()
