import logging
import time


class DataStorage(object):
    """ Class for Data Processing """

    def __init__(self, log_obj, data_storage_queue):
        """ Constructor to process all initialization process

            Args:
                log_obj (object): Log object
        """

        self.log = log_obj
        self.data_storage_queue = data_storage_queue

    def setup_data_storage(self):
        """ Setup for data Processing phase """

        self.log.info(f"Setup phase for data storage phase completed")

    def run_data_storage(self):
        """
        Read processed items from processed_data_queue and print them.
        Replace the print logic with actual database insertion when ready.
        """

        logger = logging.getLogger("main_app")
        while True:
            if self.data_storage_queue.empty():
                time.sleep(1)
                continue

            item = self.data_storage_queue.get()
            print(item)
            if item is None:
                logger.info("Data Storage: No more data to store.")
                break
            logger.debug(f"Data Storage received: {item}")