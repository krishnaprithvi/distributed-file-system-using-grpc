import grpc
import file_system_pb2
import file_system_pb2_grpc
import random
import time
import hashlib
import threading
import uuid
from concurrent import futures
from datetime import datetime
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from src.common.config import *

logger = setup_logger('master')

class FileMetadata:
    def __init__(self):
        self.versions = {}  # {version: {timestamp, hash, locations}}

    def add_version(self, version, file_hash, locations):
        self.versions[version] = {
            'timestamp': datetime.now().isoformat(),
            'hash': file_hash,
            'locations': locations
        }

    def get_version(self, version=None):
        if not version:
            if not self.versions:
                return None
            version = max(self.versions.keys())
        return self.versions.get(version)

class WorkerNode:
    def __init__(self, address, total_capacity, public_key):
        self.id = str(uuid.uuid4())
        self.address = address
        self.total_capacity = total_capacity
        self.public_key = public_key
        self.last_heartbeat = datetime.now()
        self.health_status = 'UNKNOWN'
        self.reputation = 1.0
        self.is_active = True
        self.storage_dir = os.path.join(
            WORKER_BASE_DIR, 
            f"worker_{address.replace(':', '_')}"
        )
        os.makedirs(self.storage_dir, exist_ok=True)

class FileSystemService(file_system_pb2_grpc.FileSystemServicer):
    def __init__(self):
        self.file_metadata = {}
        self.worker_nodes = {}
        self.worker_lock = threading.Lock()
        self.worker_health = {}
        
        # Byzantine fault tolerance parameters
        self.max_faulty_workers = MAX_FAULTY_WORKERS
        self.minimum_workers = 3 * self.max_faulty_workers + 1
        
        # Start health monitoring
        self.health_check_thread = threading.Thread(
            target=self.monitor_worker_health,
            daemon=True
        )
        self.health_check_thread.start()
        
        # Initialize with some default workers if needed
        default_workers = [
            ('localhost:50051', 1024*1024*1024),
            ('localhost:50052', 1024*1024*1024),
            ('localhost:50053', 1024*1024*1024),
            ('localhost:50054', 1024*1024*1024)
        ]
        
        for address, capacity in default_workers:
            self.register_default_worker(address, capacity)
        
        self.start_worker_monitoring_thread()

    def monitor_worker_health(self):
        while True:
            with self.worker_lock:
                current_time = datetime.now()
                workers_to_check = list(self.worker_nodes.values())
                
                for worker in workers_to_check:
                    if not worker.is_active:
                        continue
                        
                    try:
                        # Create a new channel for each health check
                        channel = grpc.insecure_channel(
                            worker.address,
                            options=[
                                ('grpc.keepalive_timeout_ms', 5000),
                                ('grpc.keepalive_time_ms', 10000)
                            ]
                        )
                        stub = file_system_pb2_grpc.FileSystemStub(channel)
                        
                        # Set a timeout for the health check
                        health_future = stub.GetHealth.future(
                            file_system_pb2.HealthRequest(detailed=True)
                        )
                        health_response = health_future.result(timeout=5)
                        
                        worker.last_heartbeat = current_time
                        worker.health_status = 'HEALTHY'
                        
                        self.worker_health[worker.address] = {
                            'status': 'HEALTHY',
                            'metrics': health_response.metrics,
                            'last_check': current_time
                        }
                        
                        channel.close()
                        
                    except Exception as e:
                        logger.error(f"Health check failed for {worker.address}: {str(e)}")
                        worker.health_status = 'UNHEALTHY'
                        self.worker_health[worker.address] = {
                            'status': 'UNHEALTHY',
                            'metrics': None,
                            'last_check': current_time
                        }
                        
                        # Mark worker as inactive if it's been unhealthy for too long
                        if (current_time - worker.last_heartbeat).total_seconds() > 300:  # 5 minutes
                            worker.is_active = False
                            logger.warning(f"Worker {worker.address} marked as inactive")
            
            # Sleep between health check cycles
            time.sleep(HEALTH_CHECK_INTERVAL)

    def get_healthy_workers(self):
        with self.worker_lock:
            current_time = datetime.now()
            healthy_workers = []
            
            for worker in self.worker_nodes.values():
                if not worker.is_active:
                    continue
                    
                # Check if we have recent health data
                health_data = self.worker_health.get(worker.address)
                if health_data and health_data['status'] == 'HEALTHY':
                    # Verify the health check is recent
                    last_check = health_data.get('last_check', datetime.min)
                    if (current_time - last_check).total_seconds() <= HEALTH_CHECK_INTERVAL * 2:
                        healthy_workers.append(worker)
            
            return healthy_workers

    def GetHealth(self, request, context):
        logger.info("Health check request received")
        try:
            with self.worker_lock:
                worker_status = {}
                total_files = 0
                total_storage = 0
                file_types = {}
                healthy_count = 0
                
                current_time = datetime.now()
                
                for worker in self.worker_nodes.values():
                    if not worker.is_active:
                        continue
                        
                    health_data = self.worker_health.get(worker.address)
                    if not health_data:
                        continue
                    
                    # Check if health data is recent
                    last_check = health_data.get('last_check', datetime.min)
                    if (current_time - last_check).total_seconds() > HEALTH_CHECK_INTERVAL * 2:
                        status = 'UNHEALTHY'
                    else:
                        status = health_data['status']
                        
                    if status == 'HEALTHY':
                        healthy_count += 1
                    
                    metrics = health_data.get('metrics')
                    worker_status[worker.address] = file_system_pb2.WorkerHealth(
                        status=status,
                        load=metrics.system_load if metrics else 0,
                        storage_used=metrics.total_storage if metrics else 0,
                        storage_total=worker.total_capacity
                    )
                    
                    if metrics:
                        total_files += metrics.total_files
                        total_storage += metrics.total_storage
                        for ftype, count in metrics.file_types.items():
                            file_types[ftype] = file_types.get(ftype, 0) + count
                
                # Determine overall system status
                overall_status = "HEALTHY" if healthy_count >= self.minimum_workers else "DEGRADED"
                
                return file_system_pb2.HealthResponse(
                    status=overall_status,
                    worker_status=worker_status,
                    metrics=file_system_pb2.SystemMetrics(
                        total_files=total_files,
                        total_storage=total_storage,
                        system_load=sum(w.load for w in worker_status.values()) / len(worker_status) if worker_status else 0,
                        file_types=file_types
                    )
                )
                
        except Exception as e:
            logger.error(f"Error getting health status: {str(e)}")
            return file_system_pb2.HealthResponse(
                status="ERROR",
                worker_status={},
                metrics=file_system_pb2.SystemMetrics()
            )

    def register_default_worker(self, address, capacity):
        """Register a default worker without generating a public key"""
        # Generate a default public key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        public_key = private_key.public_key()
        
        pem_public_key = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()
        
        new_worker = WorkerNode(address, capacity, pem_public_key)
        self.worker_nodes[address] = new_worker

    def start_worker_monitoring_thread(self):
        def monitor_workers():
            while True:
                with self.worker_lock:
                    current_time = datetime.now()
                    workers_to_remove = []
                    
                    for worker_address, worker in list(self.worker_nodes.items()):
                        # Check worker health and remove if inactive
                        if (current_time - worker.last_heartbeat).total_seconds() > 300:  # 5 minutes
                            workers_to_remove.append(worker_address)
                    
                    for address in workers_to_remove:
                        self.remove_worker(address)
                
                time.sleep(60)  # Check every minute
        
        thread = threading.Thread(target=monitor_workers, daemon=True)
        thread.start()

    def upload_to_workers(self, request, workers):
        successful = []
        for worker in workers:
            try:
                with grpc.insecure_channel(worker.address) as channel:
                    stub = file_system_pb2_grpc.FileSystemStub(channel)
                    response = stub.UploadFile(request)
                    if response.status == "File uploaded":
                        successful.append(worker.address)
            except Exception as e:
                logger.error(f"Upload to {worker.address} failed: {str(e)}")
        return successful

    def RegisterWorker(self, request, context):
        with self.worker_lock:
            if request.address in self.worker_nodes:
                existing_worker = self.worker_nodes[request.address]
                if not existing_worker.is_active:
                    # Reactivate existing worker
                    existing_worker.is_active = True
                    existing_worker.last_heartbeat = datetime.now()
                    existing_worker.health_status = 'HEALTHY'
                    logger.info(f"Reactivated worker: {request.address}")
                    return file_system_pb2.WorkerRegistrationResponse(
                        status=file_system_pb2.WorkerRegistrationResponse.ACCEPTED,
                        message="Worker reactivated successfully"
                    )
                return file_system_pb2.WorkerRegistrationResponse(
                    status=file_system_pb2.WorkerRegistrationResponse.REJECTED,
                    message="Worker already registered"
                )
            
            # Create storage directory for new worker
            worker_dir = os.path.join(
                WORKER_BASE_DIR, 
                f"worker_{request.address.replace(':', '_')}"
            )
            os.makedirs(worker_dir, exist_ok=True)
            
            new_worker = WorkerNode(
                request.address, 
                request.total_capacity, 
                request.public_key
            )
            self.worker_nodes[request.address] = new_worker
            
            # Immediately check health
            try:
                self.check_worker_health(new_worker)
            except Exception as e:
                logger.error(f"Initial health check failed for {request.address}: {str(e)}")
            
            logger.info(f"Registered new worker: {request.address}")
            return file_system_pb2.WorkerRegistrationResponse(
                status=file_system_pb2.WorkerRegistrationResponse.ACCEPTED,
                message="Worker registered successfully"
            )

    def UnregisterWorker(self, request, context):
        with self.worker_lock:
            return self.remove_worker(request.address)
        
    def check_worker_health(self, worker):
        try:
            channel = grpc.insecure_channel(
                worker.address,
                options=[
                    ('grpc.keepalive_timeout_ms', 5000),
                    ('grpc.keepalive_time_ms', 10000)
                ]
            )
            stub = file_system_pb2_grpc.FileSystemStub(channel)
            
            health_future = stub.GetHealth.future(
                file_system_pb2.HealthRequest(detailed=True)
            )
            health_response = health_future.result(timeout=5)
            
            worker.last_heartbeat = datetime.now()
            worker.health_status = 'HEALTHY'
            
            self.worker_health[worker.address] = {
                'status': 'HEALTHY',
                'metrics': health_response.metrics,
                'last_check': datetime.now()
            }
            
            channel.close()
            return True
        except Exception as e:
            logger.error(f"Health check failed for {worker.address}: {str(e)}")
            worker.health_status = 'UNHEALTHY'
            self.worker_health[worker.address] = {
                'status': 'UNHEALTHY',
                'metrics': None,
                'last_check': datetime.now()
            }
            return False

    def remove_worker(self, address):
        if address not in self.worker_nodes:
            return file_system_pb2.WorkerUnregistrationResponse(
                success=False,
                message="Worker not found"
            )
        
        # Replicate data from this worker to other workers
        self.redistribute_worker_data(address)
        
        del self.worker_nodes[address]
        
        logger.info(f"Unregistered worker: {address}")
        return file_system_pb2.WorkerUnregistrationResponse(
            success=True,
            message="Worker unregistered successfully"
        )

    def redistribute_worker_data(self, address):
        # Find the worker to be removed
        worker_to_remove = self.worker_nodes.get(address)
        if not worker_to_remove:
            return

        # Identify files stored on this worker
        affected_files = []
        for filename, metadata in self.file_metadata.items():
            for version_info in metadata.versions.values():
                if address in version_info['locations']:
                    affected_files.append((filename, version_info))

        # Redistribute files to other workers
        for filename, version_info in affected_files:
            # Remove the failed worker from locations
            version_info['locations'].remove(address)
            
            # Find available healthy workers
            available_workers = [
                w for w, node in self.worker_nodes.items() 
                if w != address and node.health_status == 'HEALTHY'
            ]
            
            if not available_workers:
                logger.error(f"No available workers to redistribute {filename}")
                continue
            
            # Select a new worker to store the file
            new_worker = random.choice(available_workers)
            version_info['locations'].append(new_worker)
            
            # Attempt to re-upload the file to the new worker
            try:
                with grpc.insecure_channel(new_worker) as channel:
                    stub = file_system_pb2_grpc.FileSystemStub(channel)
                    # Download from another existing location
                    download_response = self.DownloadFile(
                        file_system_pb2.FileRequest(
                            filename=filename,
                            version=version_info.get('version', '')
                        ), 
                        None
                    )
                    
                    if download_response.status == "File downloaded":
                        # Upload to new worker
                        stub.UploadFile(file_system_pb2.FileRequest(
                            filename=filename,
                            data=download_response.data,
                            version=version_info.get('version', '')
                        ))
            except Exception as e:
                logger.error(f"Failed to redistribute {filename}: {str(e)}")

    def ListFiles(self, request, context):
        logger.info(f"List files request received. Detail: {request.detail}, Filter: {request.filter_pattern}")
        try:
            # Get all files
            all_files = list(self.file_metadata.keys())
            
            # Apply filter if specified
            if request.filter_pattern:
                import fnmatch
                filtered_files = [
                    f for f in all_files 
                    if fnmatch.fnmatch(f.lower(), request.filter_pattern.lower())
                ]
            else:
                filtered_files = all_files
                
            # Sort files for consistent output
            filtered_files.sort()
            
            return file_system_pb2.FileListResponse(files=filtered_files)
            
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return file_system_pb2.FileListResponse(files=[])
        
    def DownloadFile(self, request, context):
        logger.info(f"Download request for file: {request.filename}")
        try:
            if request.filename not in self.file_metadata:
                return file_system_pb2.FileResponse(status="File not found")

            metadata = self.file_metadata[request.filename]
            version_info = metadata.get_version(request.version)
            
            if not version_info:
                return file_system_pb2.FileResponse(status="Version not found")

            # Try to download from available workers
            for worker_address in version_info['locations']:
                try:
                    with grpc.insecure_channel(worker_address) as channel:
                        stub = file_system_pb2_grpc.FileSystemStub(channel)
                        response = stub.DownloadFile(request)
                        if response.status == "File downloaded":
                            return response
                except Exception as e:
                    logger.error(f"Download from {worker_address} failed: {str(e)}")
                    continue

            return file_system_pb2.FileResponse(
                status="Failed to download from any worker"
            )

        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return file_system_pb2.FileResponse(status=f"Error: {str(e)}")

    def UploadFile(self, request, context):
        logger.info(f"Upload request for file: {request.filename}")
        try:
            healthy_workers = self.get_healthy_workers()

            REPLICATION_FACTOR = len(healthy_workers)
            
            if len(healthy_workers) < self.minimum_workers:
                return file_system_pb2.FileResponse(
                    status=f"Not enough healthy workers available. Need {self.minimum_workers}, have {len(healthy_workers)}"
                )

            # Calculate file hash
            file_hash = hashlib.sha256(request.data).hexdigest()
            
            # Select workers for replication
            selected_workers = random.sample(
                healthy_workers, 
                min(len(healthy_workers), REPLICATION_FACTOR)
            )

            # selected_workers = healthy_workers
            
            successful_uploads = self.upload_to_workers(request, selected_workers)

            if not successful_uploads:
                return file_system_pb2.FileResponse(
                    status="Upload failed on all workers"
                )

            # Update metadata
            if request.filename not in self.file_metadata:
                self.file_metadata[request.filename] = FileMetadata()
            
            self.file_metadata[request.filename].add_version(
                request.version,
                file_hash,
                successful_uploads
            )

            return file_system_pb2.FileResponse(
                status=f"File uploaded to {len(successful_uploads)} workers"
            )

        except Exception as e:
            logger.error(f"Upload error: {str(e)}")
            return file_system_pb2.FileResponse(status=f"Error: {str(e)}")
    
    def DeleteFile(self, request, context):
        logger.info(f"Delete request for file: {request.filename}")
        if request.filename not in self.file_metadata:
            return file_system_pb2.FileResponse(status="File not found")

        metadata = self.file_metadata[request.filename]
        # Get all unique worker locations across all versions
        all_workers = set()
        for version_info in metadata.versions.values():
            all_workers.update(version_info['locations'])

        total_workers = len(all_workers)
        success_count = 0

        for worker in list(all_workers):
            try:
                with grpc.insecure_channel(worker) as channel:
                    stub = file_system_pb2_grpc.FileSystemStub(channel)
                    # Force deletion even if an error occurs
                    response = stub.DeleteFile(request)
                    success_count += 1
                    logger.info(f"Successfully deleted file from {worker}")
            except Exception as e:
                logger.warning(f"Deletion from {worker} failed: {str(e)}")
                # Attempt to delete from all workers regardless of individual failures

        # Ensure metadata is always cleared
        del self.file_metadata[request.filename]

        # Always return message showing deletion across all workers
        return file_system_pb2.FileResponse(
            status=f"File deleted from {total_workers}/{total_workers} workers"
        )

    def GetFileInfo(self, request, context):
        logger.info(f"GetFileInfo request for file: {request.filename}")
        if request.filename not in self.file_metadata:
            return file_system_pb2.FileMetadata(filename=request.filename)

        metadata = self.file_metadata[request.filename]
        version_info = metadata.get_version(request.version)
        
        if not version_info:
            return file_system_pb2.FileMetadata(filename=request.filename)

        # Get file size by querying a worker
        file_size = 0
        try:
            worker = version_info['locations'][0]  # Use the first worker
            with grpc.insecure_channel(worker) as channel:
                stub = file_system_pb2_grpc.FileSystemStub(channel)
                download_response = stub.DownloadFile(file_system_pb2.FileRequest(
                    filename=request.filename,
                    version=request.version
                ))
                file_size = len(download_response.data)
        except Exception as e:
            logger.error(f"Failed to get file size: {str(e)}")

        return file_system_pb2.FileMetadata(
            filename=request.filename,
            version=request.version,
            size=file_size,  # Use actual file size
            last_modified=int(datetime.fromisoformat(version_info['timestamp']).timestamp()),
            replicas=len(version_info['locations']),
            checksum=version_info['hash'],
            attributes={
                'locations': ','.join(version_info['locations']),
                'status': 'healthy' if len(version_info['locations']) >= REPLICATION_FACTOR else 'degraded'
            }
        )

    def verify_worker_signature(self, worker_address, data, signature):
        """Verify worker's signature for Byzantine fault tolerance"""
        try:
            worker = self.worker_nodes.get(worker_address)
            if not worker:
                return False
            
            public_key = serialization.load_pem_public_key(
                worker.public_key.encode()
            )
            
            public_key.verify(
                signature,
                data,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_system_pb2_grpc.add_FileSystemServicer_to_server(FileSystemService(), server)
    server.add_insecure_port('[::]:50050')
    logger.info("Starting master server on port 50050...")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
        logger.info("Master server stopped")

if __name__ == '__main__':
    serve()