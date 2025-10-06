import logging
import os
from datetime import datetime

# System Configuration
MASTER_ADDRESS = 'localhost:50050'
WORKER_BASE_DIR = os.path.join(os.getcwd(), 'worker_storage')
MIN_WORKERS = 4  # Minimum workers needed (3f + 1 where f=1)
REPLICATION_FACTOR = 3  # Minimum replicas for each file
MAX_FAULTY_WORKERS = 1  # Maximum faulty workers tolerated (f)
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
HEALTH_CHECK_INTERVAL = 5  # seconds
WORKER_TIMEOUT = 5  # seconds before marking worker as unhealthy

# Logging Configuration
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # File handler
    file_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'{name}_{datetime.now().strftime("%Y%m%d")}.log')
    )
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger