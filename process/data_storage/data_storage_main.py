import logging
import multiprocessing
import time

from configuration.config import Config
from utils.logger_config import LoggerConfig


class DataStorage(object):
    """ Class for Data Processing """

    def __init__(self, log_obj: logging.Logger, data_storage_queue: multiprocessing.Queue) -> None:
        """ Constructor to process all initialization process

            Args:
                log_obj (object): Log object
                data_storage_queue (object): Queue from the processing stage.
        """

        self.log = log_obj
        self.data_storage_queue = data_storage_queue

    def setup_data_storage(self) -> None:
        """ Setup for data Processing phase """

        self.log.info(f"Setup phase for data storage phase completed")

    def run_data_storage(self) -> None:
        """ Read processed items from processed_data_queue and print them. """

        log_obj = LoggerConfig.create_logger("storage_process", Config.LOG_FILE)
        while True:
            if self.data_storage_queue.empty():
                log_obj.debug("In Sleep")
                time.sleep(1)
                continue

            item = self.data_storage_queue.get()
            print(item)
            self.log.info(item)
            if item is None:
                log_obj.info("Data Storage: No more data to store.")
                break
            log_obj.info(f"Data Storage received: {item}")
