import sys
import os

root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, root_dir)

from utils.logger import logger
from utils.variables import Variables
from producer import connect_and_stream

var = Variables()

wikimedia_url = var["source"]["wikimedia_url"]
max_retries = var["source"]["max_retries"]

def main():
    logger.info("Producer running")
    connect_and_stream(API_URL=wikimedia_url,max_retries=max_retries)


if __name__ == "__main__":
    main()