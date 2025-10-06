import grpc
import file_system_pb2
import file_system_pb2_grpc
import os
import time
import psutil
from src.common.config import *
from concurrent import futures
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

logger = setup_logger('worker')

class FileStorage:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
        
    def store_file(self, filename, version, data):
        file_path = os.path.join(self.base_dir, f"{filename}_{version}")
        with open(file_path, 'wb') as f:
            f.write(data)
        return True
        
    def get_file(self, filename, version=None):
        if version:
            file_path = os.path.join(self.base_dir, f"{filename}_{version}")
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    return f.read()
        else:
            # Find latest version
            matching_files = [f for f in os.listdir(self.base_dir) 
                            if f.startswith(filename + "_")]
            if matching_files:
                latest = max(matching_files, 
                    key=lambda x: os.path.getctime(
                        os.path.join(self.base_dir, x)))
                with open(os.path.join(self.base_dir, latest), 'rb') as f:
                    return f.read()
        return None
    
class WorkerService(file_system_pb2_grpc.FileSystemServicer):
    def __init__(self, storage_dir):
        self.storage_dir = storage_dir
        # if not os.path.exists(storage_dir):
        #     os.makedirs(storage_dir, exist_ok=True)
        os.makedirs(storage_dir, exist_ok=True)
        self.storage = FileStorage(storage_dir)
        self.file_types = {}
        self.last_health_check = datetime.now()
        
        # Register with master on startup
        self.register_with_master()
        logger.info(f"Worker initialized with storage directory: {storage_dir}")

    def register_with_master(self):
        try:
            channel = grpc.insecure_channel(MASTER_ADDRESS)
            stub = file_system_pb2_grpc.FileSystemStub(channel)
            
            # Generate key pair
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            public_key = private_key.public_key()
            
            pem_public_key = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ).decode()
            
            # Get worker address from storage dir name
            worker_addr = os.path.basename(self.storage_dir).replace('worker_', '').replace('_', ':')
            
            request = file_system_pb2.WorkerRegistration(
                address=worker_addr,
                total_capacity=psutil.disk_usage(self.storage_dir).total,
                public_key=pem_public_key
            )
            
            response = stub.RegisterWorker(request)
            logger.info(f"Worker registration response: {response.message}")
            
        except Exception as e:
            logger.error(f"Failed to register with master: {str(e)}")

    def get_storage_metrics(self):
        total_size = 0
        file_types = {}
        for root, _, files in os.walk(self.storage_dir):
            for f in files:
                file_path = os.path.join(root, f)
                total_size += os.path.getsize(file_path)
                ext = os.path.splitext(f)[1]
                file_types[ext] = file_types.get(ext, 0) + 1
        return total_size, len(files), file_types

    def GetHealth(self, request, context):
        logger.info("Health check request received")
        try:
            total_size, total_files, file_types = self.get_storage_metrics()
            disk = psutil.disk_usage(self.storage_dir)
            cpu_load = psutil.cpu_percent() / 100.0

            return file_system_pb2.HealthResponse(
                status="HEALTHY",
                metrics=file_system_pb2.SystemMetrics(
                    total_files=total_files,
                    total_storage=total_size,
                    system_load=cpu_load,
                    file_types=file_types
                )
            )
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return file_system_pb2.HealthResponse(status="UNHEALTHY")
        
    def UploadFile(self, request, context):
        logger.info(f"Worker received upload request for: {request.filename}")
        try:
            # Check if we have enough space
            required_space = len(request.data)
            if not self.check_storage_space(required_space):
                return file_system_pb2.FileResponse(
                    status="Insufficient storage space"
                )

            success = self.storage.store_file(
                request.filename,
                request.version,
                request.data
            )
            
            if success:
                # Update metrics
                ext = os.path.splitext(request.filename)[1]
                self.file_types[ext] = self.file_types.get(ext, 0) + 1
                return file_system_pb2.FileResponse(status="File uploaded")
            
            return file_system_pb2.FileResponse(status="Upload failed")
            
        except Exception as e:
            logger.error(f"Upload error: {str(e)}")
            return file_system_pb2.FileResponse(status=f"Error: {str(e)}")

    def DownloadFile(self, request, context):
        logger.info(f"Worker received download request for: {request.filename}")
        try:
            data = self.storage.get_file(request.filename, request.version)
            if data:
                return file_system_pb2.FileResponse(
                    status="File downloaded",
                    data=data
                )
            return file_system_pb2.FileResponse(status="File not found")
        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return file_system_pb2.FileResponse(status=f"Error: {str(e)}")

    def DeleteFile(self, request, context):
        logger.info(f"Delete request for file: {request.filename}")
        try:
            files_to_delete = [
                f for f in os.listdir(self.storage_dir) 
                if f.startswith(request.filename + "_")
            ]
            
            if not files_to_delete:
                error_msg = "File not found"
                logger.error(error_msg)
                return file_system_pb2.FileResponse(status=error_msg)
            
            deleted_count = 0
            for file in files_to_delete:
                file_path = os.path.join(self.storage_dir, file)
                os.remove(file_path)
                deleted_count += 1
                
                # Update metrics
                ext = os.path.splitext(file)[1]
                if ext in self.file_types:
                    self.file_types[ext] -= 1
                    if self.file_types[ext] == 0:
                        del self.file_types[ext]
            
            logger.info(f"Successfully deleted {deleted_count} versions")
            return file_system_pb2.FileResponse(
                status=f"Deleted {deleted_count} versions"
            )
        except Exception as e:
            error_msg = f"Delete failed: {str(e)}"
            logger.error(error_msg)
            return file_system_pb2.FileResponse(status=error_msg)

    def check_storage_space(self, required_space):
        try:
            disk = psutil.disk_usage(self.storage.base_dir)
            return disk.free >= required_space
        except:
            return False
        
    def SyncDirectory(self, request, context):
        logger.info(f"Directory sync request for path: {request.path}")
        try:
            base_path = os.path.join(self.storage_dir, request.path)
            if not os.path.exists(base_path):
                yield file_system_pb2.FileStatus(
                    status="error",
                    message=f"Directory {request.path} not found"
                )
                return

            for root, _, files in os.walk(base_path):
                if not request.recursive and root != base_path:
                    continue
                    
                for file in files:
                    relative_path = os.path.relpath(
                        os.path.join(root, file), 
                        self.storage_dir
                    )
                    yield file_system_pb2.FileStatus(
                        filename=relative_path,
                        status="synced",
                        message="File synchronized"
                    )
                    
        except Exception as e:
            logger.error(f"Directory sync failed: {str(e)}")
            yield file_system_pb2.FileStatus(
                status="error",
                message=f"Sync failed: {str(e)}"
            )

def serve(port, storage_dir):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_system_pb2_grpc.add_FileSystemServicer_to_server(
        WorkerService(storage_dir), server)
    server.add_insecure_port(f'[::]:{port}')
    logger.info(f"Starting worker server on port {port}...")
    server.start()
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info("Worker shutting down...")
        server.stop(0)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python worker.py <port> <storage_dir>")
        sys.exit(1)
    
    port = sys.argv[1]
    storage_dir = sys.argv[2]
    serve(port, storage_dir)