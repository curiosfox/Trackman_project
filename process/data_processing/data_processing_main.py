import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

from configuration.config import Config
from utils.logger_config import LoggerConfig


class DataProcessing(object):
    """ Class for Data Processing """

    def __init__(self, log_obj: logging.Logger, data_process_queue: multiprocessing.Queue,
                 data_storage_queue: multiprocessing.Queue) -> None:
        """ Constructor to process all initialization process

            Args:
                log_obj (object): Log object
                data_process_queue (object): Queue from the acquisition stage.
                data_storage_queue (object): Queue to send processed data to storage stage.
        """

        self.log = log_obj
        self.data_process_queue = data_process_queue
        self.data_storage_queue = data_storage_queue
        self.usd_vs_dkk = Config.USD_VS_DKK

    def setup_data_processing(self) -> None:
        """ Setup for data Processing phase """

        self.log.info(f"Setup phase for data processing phase completed")

    def process_data_item(self, item: dict) -> dict:
        """ Process a single data item, adding a price in DKK.

        Args:
            item (dict): A data item with keys 'symbol', 'price', 'timestamp'.

        Returns:
            dict: Updated data item with additional 'price (DKK)' key.
        """

        log_obj = LoggerConfig.create_logger("data_process", Config.LOG_FILE)
        if item is not None and "price" in item:
            item["price (DKK)"] = item["price"] * self.usd_vs_dkk  # Danish krone value vs USD
        else:
            log_obj.error(f"Error occurred during data processing:{item}")
            raise Exception(f"Queue data error :{item}")
        log_obj.debug(f"Data processed value:{item}")
        return item

    def run_data_processing(self, max_workers: int = 10) -> None:
        """ Continuously read from data_process_queue, process items using a ThreadPoolExecutor,
        and push the results into data_storage_queue.

        Args:
            max_workers (int): Max number of threads for parallel processing.
        """

        log_obj = LoggerConfig.create_logger("data_process", Config.LOG_FILE)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            while True:
                item = self.data_process_queue.get()
                if item is None:
                    break
                future = executor.submit(self.process_data_item, item)
                futures.append(future)
            for f in futures:
                try:
                    result = f.result()
                    self.data_storage_queue.put(result)
                except Exception as e:
                    log_obj.error(f"Error processing data item: {e}")

        self.data_storage_queue.put(None)
