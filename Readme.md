# 🌐 Distributed File System using gRPC
A lightweight Distributed File System (DFS) built using gRPC and Protocol Buffers (Protobuf).

The system enables clients to upload, download, and delete files across multiple worker nodes coordinated by a central master server.

Each worker node manages its own local storage while the master handles metadata and routing.
Clients communicate with the system using defined gRPC services.


# 📁 Project Structure
DistributedFileSystemProject/

│── file_system.proto               # Protobuf file defining gRPC services and messages

│── file_system_pb2.py              # Generated protobuf definitions

│── file_system_pb2_grpc.py         # Generated gRPC service stubs

│── master.py                       # Master node: manages workers and metadata

│── worker.py                       # Worker node: handles storage and file I/O

│── client.py                       # Client: performs file operations through gRPC

│── requirements.txt                # Python dependencies

│── logs/                           # Execution logs for master and workers

│── worker_storage/                 # Simulated distributed storage directories

│── storage1/, storage2/, ...       # Local storage folders


# ⚙️ Prerequisites
Python 3.10 or later

pip package manager

To install dependencies, run: pip install -r requirements.txt

If using a virtual environment, activate it before running the command above.


# 🚀 Getting Started
## 🖥️ Step 1: Start the Master Server
Launch the master node (it manages file metadata and coordination): python master.py

## 🗄️ Step 2: Start Worker Nodes
Run multiple workers on different ports to simulate distributed storage:

python worker.py --port=50051

python worker.py --port=50052

python worker.py --port=50053

Each worker will automatically create its own storage directory (e.g., worker_localhost_50051/, worker_localhost_50052/, etc.).

## 💻 Step 3: Run the Client
Once the master and workers are active, open a new terminal and execute file operations:

➕ Upload a File

python client.py upload <file_path>

⬇️ Download a File

python client.py download <file_name>

❌ Delete a File

python client.py delete <file_name>

All activities and status messages are recorded in the logs/ directory.


# 🧠 Design Overview
## Master Node
Maintains file metadata, assigns workers for uploads, and coordinates downloads/deletions.

## Worker Nodes
Responsible for storing, retrieving, and deleting files on their local disk.

## Client
Interacts with the master node through gRPC calls, enabling distributed file operations seamlessly.

## Protobuf Definitions
The communication protocol is defined in file_system.proto, ensuring strict type safety and efficient serialization.


# 🧾 Logging
Each component (Master and Workers) generates its own log files in the logs/ folder.

Logs contain details such as request timestamps, status messages, and node-level activities.


# 🧪 Testing Notes
The project can be fully tested locally by running multiple worker processes with unique ports.

To simulate worker failure, stop one of the worker nodes and observe the master’s handling behavior.

Modify and extend this setup to explore consistency, replication, or load-balancing strategies.


# 🧰 Tools and Technologies

| **Component** | **Technology**        |
| ------------- | --------------------- |
| Language      | Python                |
| Communication | gRPC                  |
| Serialization | Protocol Buffers      |
| Logging       | Python Logging Module |
| Architecture  | Master–Worker Model   |

# 📘 License
This project is intended for academic and research use.
You are free to modify and extend it for your own experiments in distributed systems, data consistency, or fault tolerance.