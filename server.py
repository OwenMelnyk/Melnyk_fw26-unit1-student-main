"""Concurrent TCP key-value and task-queue server."""

import logging
import math
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from protocol import ProtocolError, recv_message, send_message
from storage import Storage

HOST = "0.0.0.0"
PORT = 9090
MAX_WORKERS = 10
MAX_CONNECTIONS = 30
CLIENT_TIMEOUT = 10
MAX_DEQUEUE_WAIT = 2

storage = Storage()
stopping = threading.Event()


def error(code, message):
    return {"ok": False, "error": code, "message": message}


def process_request(request):
    action = request.get("action")

    if action not in ("PUT", "GET", "ENQUEUE", "DEQUEUE"):
        return error("BAD_REQUEST", "Unknown or missing action.")

    if action in ("PUT", "GET"):
        key = request.get("key")
        if not isinstance(key, str) or not key:
            return error("BAD_REQUEST", "key must be a nonempty string.")

    if action == "PUT":
        if "value" not in request:
            return error("BAD_REQUEST", "PUT requires a value.")

        storage.put(request["key"], request["value"])
        return {"ok": True}

    if action == "GET":
        found, value = storage.get(request["key"])
        if not found:
            return error("NOT_FOUND", "The key does not exist.")
        return {"ok": True, "value": value}

    if action == "ENQUEUE":
        if "value" not in request:
            return error("BAD_REQUEST", "ENQUEUE requires a value.")

        if not storage.enqueue(request["value"]):
            return error("QUEUE_FULL", "The task queue is full.")
        return {"ok": True}

    timeout = request.get("timeout", 0)
    if (
        type(timeout) not in (int, float)
        or not 0 <= timeout <= MAX_DEQUEUE_WAIT
        or not math.isfinite(timeout)
    ):
        return error(
            "BAD_REQUEST",
            f"timeout must be between 0 and {MAX_DEQUEUE_WAIT} seconds.",
        )

    found, value = storage.dequeue(timeout)
    if not found:
        return error("EMPTY", "No task became available.")
    return {"ok": True, "value": value}


def handle_client(conn, address):
    with conn:
        conn.settimeout(CLIENT_TIMEOUT)

        try:
            while not stopping.is_set():
                request = recv_message(conn)
                if request is None:
                    return

                send_message(conn, process_request(request))

        except ProtocolError as exc:
            # An invalid frame ends this connection, since its framing can no longer be trusted.
            try:
                send_message(conn, error("PROTOCOL_ERROR", str(exc)))
            except (OSError, ProtocolError):
                pass

        except OSError:
            # Timeouts, connection resets and broken pipes affect this client only.
            logging.debug("Client disconnected: %s", address)


def main():
    stopping.clear()
    slots = threading.BoundedSemaphore(MAX_CONNECTIONS)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((HOST, PORT))
            listener.listen(MAX_CONNECTIONS)
            listener.settimeout(0.5)

            print(f"[*] Server initialized on {HOST}:{PORT}", flush=True)
            print(
                f"[*] Thread Pool initialized with "
                f"max_workers={MAX_WORKERS}...",
                flush=True,
            )

            try:
                while not stopping.is_set():
                    if not slots.acquire(timeout=0.5):
                        continue

                    try:
                        conn, address = listener.accept()
                    except socket.timeout:
                        slots.release()
                        continue
                    except BaseException:
                        slots.release()
                        raise

                    try:
                        future = pool.submit(handle_client, conn, address)
                    except BaseException:
                        conn.close()
                        slots.release()
                        raise

                    future.add_done_callback(lambda finished: slots.release())

            except KeyboardInterrupt:
                print("\n[*] Stopping server...", flush=True)
            finally:
                stopping.set()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()
