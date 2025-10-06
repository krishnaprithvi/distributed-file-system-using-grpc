import click
import grpc
import file_system_pb2
import file_system_pb2_grpc
import os
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from datetime import datetime
from src.common.config import *

console = Console()

class FileSystemClient:
    def __init__(self, master_address):
        self.channel = grpc.insecure_channel(master_address)
        self.stub = file_system_pb2_grpc.FileSystemStub(self.channel)

    def register_worker(self, address, capacity, public_key):
        request = file_system_pb2.WorkerRegistration(
            address=address,
            total_capacity=capacity,
            public_key=public_key
        )
        return self.stub.RegisterWorker(request)

    def unregister_worker(self, address):
        request = file_system_pb2.WorkerUnregistration(address=address)
        return self.stub.UnregisterWorker(request)

    def upload_file(self, filename, data, version):
        print(f"Attempting to upload {filename}")
        try:
            request = file_system_pb2.FileRequest(
                filename=filename,
                data=data,
                version=version
            )
            response = self.stub.UploadFile(request)
            print(f"Upload response: {response.status}")
            return response.status
        except Exception as e:
            print(f"Upload error: {str(e)}")
            return f"Error: {str(e)}"

    def download_file(self, filename):
        print(f"Attempting to download {filename}")
        try:
            request = file_system_pb2.FileRequest(filename=filename)
            response = self.stub.DownloadFile(request)
            if response.status == "File downloaded":
                with open(f'downloaded_{filename}', 'wb') as f:
                    f.write(response.data)
                print(f"File downloaded: downloaded_{filename}")
            else:
                print(f"Download failed: {response.status}")
            return response.status
        except Exception as e:
            print(f"Download error: {str(e)}")
            return f"Error: {str(e)}"

    def delete_file(self, filename):
        print(f"Attempting to delete {filename}")
        try:
            request = file_system_pb2.FileRequest(filename=filename)
            response = self.stub.DeleteFile(request)
            print(f"Delete response: {response.status}")
            return response.status
        except Exception as e:
            print(f"Delete error: {str(e)}")
            return f"Error: {str(e)}"

    def list_files(self):
        try:
            # Using FileListResponse instead of Empty
            response = self.stub.ListFiles(file_system_pb2.FileListResponse())
            if response.files:
                print("Available files:", response.files)
            return response.files
        except Exception as e:
            print(f"List files error: {str(e)}")
            return []
        
    def get_file_info(self, filename):
        try:
            request = file_system_pb2.FileRequest(filename=filename)
            response = self.stub.GetFileInfo(request)
            return response
        except Exception as e:
            console.print(f"[red]Error getting file info: {str(e)}[/red]")
            return None

    def get_health(self, detailed=False):
        try:
            request = file_system_pb2.HealthRequest(detailed=detailed)
            return self.stub.GetHealth(request)
        except Exception as e:
            console.print(f"[red]Error getting health status: {str(e)}[/red]")
            return None

    def sync_directory(self, path, recursive=False):
        try:
            request = file_system_pb2.DirectoryRequest(
                path=path,
                recursive=recursive
            )
            return self.stub.SyncDirectory(request)
        except Exception as e:
            console.print(f"[red]Error syncing directory: {str(e)}[/red]")
            return None

@click.group()
def cli():
    """Distributed File System CLI"""
    pass

@cli.command()
@click.argument('filename')
@click.option('--version', default="v1.0", help='Version tag for the file')
def upload(filename, version):
    """Upload a file to the distributed system"""
    try:
        with open(filename, 'rb') as f:
            data = f.read()
        
        client = FileSystemClient('localhost:50050')
        status = client.upload_file(filename, data, version)
        
        table = Table(title="Upload Result")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Filename", filename)
        table.add_row("Version", version)
        table.add_row("Status", status)
        console.print(table)
        
    except FileNotFoundError:
        console.print(f"[red]Error: File {filename} not found[/red]")
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")

@cli.command()
@click.argument('filename')
def download(filename):
    """Download a file from the distributed system"""
    client = FileSystemClient('localhost:50050')
    status = client.download_file(filename)
    
    if "Error" not in status:
        console.print(f"[green]{status}[/green]")
    else:
        console.print(f"[red]{status}[/red]")

@cli.command()
@click.argument('filename')
def delete(filename):
    """Delete a file from the distributed system"""
    client = FileSystemClient('localhost:50050')
    status = client.delete_file(filename)
    
    if "Error" not in status:
        console.print(f"[green]{status}[/green]")
    else:
        console.print(f"[red]{status}[/red]")

@cli.command()
@click.option('--detail', is_flag=True, help='Show detailed file listing')
@click.option('--filter', 'filter_pattern', default="", help='Filter files by pattern')
def list(detail, filter_pattern):
    """List all files in the system"""
    client = FileSystemClient('localhost:50050')
    try:
        # Create a ListRequest with the provided options
        request = file_system_pb2.ListRequest(
            detail=detail,
            filter_pattern=filter_pattern
        )
        response = client.stub.ListFiles(request)
        
        if response.files:
            table = Table(title="Available Files")
            table.add_column("Filename", style="cyan")
            
            for filename in response.files:
                table.add_row(filename)
            
            console.print(table)
            
            if detail:
                for filename in response.files:
                    info = client.get_file_info(filename)
                    if info:
                        console.print(f"\nDetails for {filename}:")
                        console.print(f"Version: {info.version}")
                        console.print(f"Size: {info.size:,} bytes")
                        console.print(f"Replicas: {info.replicas}")
        else:
            console.print("[yellow]No files found in the system[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error listing files: {str(e)}[/red]")

@cli.command()
@click.argument('filename')
def info(filename):
    """Get detailed information about a file"""
    client = FileSystemClient(MASTER_ADDRESS)
    info = client.get_file_info(filename)
    
    # Check if file exists and has valid metadata
    if not info or not (info.version or info.size or info.replicas or info.checksum):
        console.print(f"[red]No such file available: {filename}[/red]")
        return
    
    table = Table(title=f"File Information: {filename}")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Version", info.version)
    table.add_row("Size", f"{info.size:,} bytes")
    table.add_row("Replicas", str(info.replicas))
    table.add_row("Last Modified", 
        datetime.fromtimestamp(info.last_modified).strftime(
            '%Y-%m-%d %H:%M:%S'
        ))
    table.add_row("Checksum", info.checksum)
    
    for key, value in info.attributes.items():
        table.add_row(key.capitalize(), value)
    
    console.print(table)

@cli.command()
@click.option('--detailed', is_flag=True, help='Show detailed health information')
def health(detailed):
    """Check system health status"""
    client = FileSystemClient(MASTER_ADDRESS)
    health = client.get_health(detailed)
    
    if health:
        table = Table(title="System Health Status")
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="yellow")
        
        table.add_row("Overall", health.status, "")
        
        for worker, status in health.worker_status.items():
            details = f"Load: {status.load*100:.1f}%, Storage: {status.storage_used}/{status.storage_total} bytes"
            table.add_row(f"Worker: {worker}", status.status, details)
        
        metrics = health.metrics
        table.add_row(
            "System Metrics",
            f"Files: {metrics.total_files}",
            f"Storage: {metrics.total_storage:,} bytes"
        )
        
        console.print(table)
        
        if detailed and metrics.file_types:
            type_table = Table(title="File Type Distribution")
            type_table.add_column("Extension", style="cyan")
            type_table.add_column("Count", style="green")
            
            for ext, count in metrics.file_types.items():
                type_table.add_row(ext or "No Extension", str(count))
            
            console.print(type_table)

@cli.command()
@click.argument('path')
@click.option('--recursive', is_flag=True, help='Sync directories recursively')
def sync(path, recursive):
    """Synchronize a directory with the distributed system"""
    client = FileSystemClient(MASTER_ADDRESS)
    
    with Progress() as progress:
        task = progress.add_task("[cyan]Syncing...", total=None)
        file_count = 0
        
        for status in client.sync_directory(path, recursive):
            if status.status == "error":
                console.print(f"[red]{status.message}[/red]")
                continue
                
            progress.update(task, advance=1)
            file_count += 1
            console.print(
                f"[green]Synced:[/green] {status.filename} - {status.message}")
        
        progress.update(task, total=file_count, completed=file_count)
    
    console.print(f"[green]Successfully synchronized {file_count} files[/green]")

@cli.group()
def worker():
    """Worker management commands"""
    pass

@worker.command('add')
@click.argument('address')
@click.option('--capacity', default=1024*1024*1024, help='Total storage capacity in bytes')
def add_worker(address, capacity):
    """Add a new worker node to the distributed system"""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    # Generate a key pair for the worker
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()
    
    # Convert public key to PEM format
    pem_public_key = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()

    client = FileSystemClient(MASTER_ADDRESS)
    try:
        request = file_system_pb2.WorkerRegistration(
            address=address,
            total_capacity=capacity,
            public_key=pem_public_key
        )
        
        response = client.stub.RegisterWorker(request)
        
        if response.status == file_system_pb2.WorkerRegistrationResponse.ACCEPTED:
            console.print(f"[green]Worker {address} added successfully[/green]")
        else:
            console.print(f"[red]Failed to add worker: {response.message}[/red]")
    except Exception as e:
        console.print(f"[red]Error adding worker: {str(e)}[/red]")

@worker.command('remove')
@click.argument('address')
def remove_worker(address):
    """Remove a worker node from the distributed system"""
    client = FileSystemClient(MASTER_ADDRESS)
    try:
        request = file_system_pb2.WorkerUnregistration(address=address)
        response = client.stub.UnregisterWorker(request)
        
        if response.success:
            console.print(f"[green]Worker {address} removed successfully[/green]")
        else:
            console.print(f"[red]Failed to remove worker: {response.message}[/red]")
    except Exception as e:
        console.print(f"[red]Error removing worker: {str(e)}[/red]")

if __name__ == '__main__':
    cli()