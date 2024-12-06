from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue


from config import Config


class DataProcessing(object):
    """ Class for Data Processing """

    def __init__(self, log_obj, data_process_queue, data_storage_queue):
        """ Constructor to process all initialization process

            Args:
                log_obj (object): Log object
        """

        self.log = log_obj
        self.data_process_queue = data_process_queue
        self.data_storage_queue = data_storage_queue
        self.usd_vs_dkk = Config.USD_VS_DKK

    def setup_data_processing(self):
        """ Setup for data Processing phase """

        self.log.info(f"Setup phase for data processing phase completed")

    def process_data_item(self, item):
        """
        Simulate processing the item.
        For example, compute a mock metric:
        moving_average = price * 1.01
        Add fields, transform data, etc.
        """

        if item is not None and "price" in item:
            item["price (DKK)"] = item["price"] * self.usd_vs_dkk  # Danish krone value vs USD
        else:
            raise Exception(f"Queue data error :{item}")
        return item

    def run_data_processing(self, max_workers=10):
        """
        Read raw data items from raw_data_queue, process them using a ThreadPoolExecutor,
        and put the processed results into processed_data_queue.
        Place None into processed_data_queue when done.

        max_workers: The maximum number of threads. If the queue size grows,
                     more of these workers will be utilized, effectively scaling up.
        """

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
                    print(f"Error processing data item: {e}")

        self.data_storage_queue.put(None)
