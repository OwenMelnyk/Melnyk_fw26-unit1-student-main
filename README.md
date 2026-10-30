# Unit 1: Bare-Metal IPC & Custom Protocol Design

Welcome to **Unit 1** of COSC 3657 (Distributed Systems). In this project, you adopt the mindset of a **System Programmer** to construct a concurrent, networked key-value and task-queue server from first principles.

Instead of relying on high-level HTTP/gRPC frameworks that abstract away the network layer, you will work directly with POSIX TCP socket primitives, design a custom length-prefixed stream framing protocol, implement thread-safe storage using explicit synchronization locks, and build a worker thread pool to handle concurrent client operations safely.

**The spec is based on using Python as the implementation language but please feel free to choose the language of your preference.**

---

## Background and Objectives

### Problem Statement

Modern high-level frameworks hide essential network realities—such as TCP byte-stream fragmentation, thread pool exhaustion, partial writes, and race conditions—leaving engineers unable to debug low-level transport failures.
### How to Resolve It

Construct an in-memory key-value server from first principles without external frameworks:
* Implement explicit fixed-length byte framing ($\text{Frame} = [\text{4-byte Length}] \;\vert{}\vert{}\; [\text{JSON Payload}]$).
* Design a bounded thread pool worker manager to process concurrent client requests safely.
* Protect shared state using fine-grained reader-writer locks (`ReentrantReadWriteLock`).


### Student Experience

Students work directly with raw socket streams. They trigger deliberate edge cases (burst clients, abrupt TCP disconnections, buffer overflow) to observe data corruption and thread deadlocks before implementing synchronization guards.


---

## 📚 Textbook Theoretical Alignment (CDK5 Mapping)

This project directly translates theoretical concepts from *Distributed Systems: Concepts and Design (5th Edition)* (Coulouris, Dollimore, Kindberg, and Blair) into working system code.

| Project Component | Distributed Systems Core Concept | Primary Textbook Reference |
| --- | --- | --- |
| **Stream Unmarshalling (`protocol.py`)** | Stream Sockets vs. Datagrams, Stream Fragmentation, Partial Reads | **CDK5 Chapters 3 & 4** (Networking & Interprocess Communication)

 |
| **Binary Header (`protocol.py`)** | External Data Representation (EDR), Byte Order (Big-Endian), Marshalling | **CDK5 Chapter 4** (Interprocess Communication)

 |
| **State Storage (`storage.py`)** | Shared Memory Primitives, Mutual Exclusion (`Mutex`), Condition Variables | **CDK5 Chapter 7** (Operating System Support)

 |
| **Concurrent Engine (`server.py`)** | Bounded Thread Pools vs. Thread-per-Request, Socket System Calls (`bind`, `listen`, `accept`) | **CDK5 Chapters 4 & 7** (IPC & Process Synchronization)

 |
| **Stress Harness (`client_stress_test.py`)** | Concurrency Failures, Memory Race Conditions, Client Disconnection Handling | **CDK5 Chapters 2 & 4** (System Models & Failure Handling)

 |

---

## 🛠️ Architecture & Protocol Specification

TCP provides a reliable stream of ordered bytes but does **not** preserve application message boundaries. To prevent **stream fragmentation** (where messages merge or split across buffer reads), all communication over port `9090` must conform to the following explicit framing protocol:

```text
+------------------------------------+-------------------------------------------+
| 4-Byte Big-Endian Header           | Variable-Length Payload                   |
| (Unsigned 32-bit Integer Length N) | (UTF-8 Encoded JSON String, N Bytes)      |
+------------------------------------+-------------------------------------------+
| 0x00 0x00 0x00 0x1F                | {"action": "PUT", "key": "k", ...}        |
+------------------------------------+-------------------------------------------+

```

1. **Header Field:** A 4-byte unsigned integer in Network Byte Order (Big-Endian, struct format `!I`) denoting payload size $N$ in bytes.


2. **Payload Field:** An $N$-byte string containing a valid JSON object specifying an operational action (`PUT`, `GET`, `ENQUEUE`, `DEQUEUE`).

---

## 📁 Repository Structure

```text
unit1-devcontainer/
├── .devcontainer/
│   └── devcontainer.json       # OS-independent workspace setup
├── protocol.py                 # Binary stream framing & marshalling helper
├── storage.py                  # Thread-safe KV store & Condition-backed Queue
├── server.py                   # Multi-threaded TCP socket server
├── client_stress_test.py       # Concurrent verification test harness
└── README.md                   # Project documentation & instructions

```

---

## 🚀 Quick Start (Dev Container / Codespaces)

This project is pre-configured to run inside a standardized Debian Linux environment using **VS Code Dev Containers** or **GitHub Codespaces**, guaranteeing identical functionality across Windows, macOS, and Linux.

### Option A: Local VS Code with Docker

1. Open Docker Desktop or OrbStack on your computer.
2. Open this directory in VS Code: `code .`
3. When prompted, click **"Reopen in Container"** (or press `F1` $\rightarrow$ `Dev Containers: Reopen in Container`).

### Option B: Cloud Execution with GitHub Codespaces

1. Push this repository to GitHub.
2. Click **Code** $\rightarrow$ **Codespaces** $\rightarrow$ **Create codespace on main**.

---

## 🧪 Step-by-Step Testing & Verification Instructions

### 1. Start the Concurrent IPC Server

Inside the Dev Container terminal, start the multi-threaded server listening on all network interfaces (`0.0.0.0:9090`):

```bash
python server.py

```

*Expected Output:*

```text
[*] Server initialized on 0.0.0.0:9090
[*] Thread Pool initialized with max_workers=10...

```

### 2. Verify Port Binding

Open a secondary terminal split (`Ctrl+Shift+\``) and verify that port `9090`is in a`LISTEN` state:

```bash
lsof -i :9090

```

*Expected Output:*

```text
COMMAND   PID   USER   FD   TYPE DEVICE SIZE/OFF NODE NAME
python3  1234 vscode    3u  IPv4  23456      0t0  TCP *:9090 (LISTEN)

```

### 3. Run the Concurrent Client Stress Harness

In the secondary terminal, execute the 20-thread concurrent stress tester to verify that thread synchronization locks and message framing hold up under concurrent load:

```bash
python client_stress_test.py

```

*Expected Output:*

```text
[*] Launching 20 concurrent client worker threads...
[Worker Thread 00] Transaction verified successfully.
[Worker Thread 01] Transaction verified successfully.
...
[Worker Thread 19] Transaction verified successfully.
[+] All 20 workers completed execution in 0.082 seconds.

```

---

## 📝 Student Deliverables Checklist

To complete Unit 1, submit your repository containing:

* [ ] Complete implementation of `protocol.py` handling partial socket reads (`recv_exact`).
* [ ] Complete implementation of `storage.py` wrapped in explicit `threading.Lock` and `threading.Condition` primitives.
* [ ] Complete implementation of `server.py` utilizing `concurrent.futures.ThreadPoolExecutor`.
* [ ] Terminal screenshot proving all 20 worker threads in `client_stress_test.py` execute without race condition failures or unhandled socket exceptions.

And a live presentation to walk through the implementations will be arranged for the evaluation.