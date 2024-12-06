import logging
import multiprocessing
import threading
import time

from yahoo_fin import stock_info

from configuration.config import Config
from utils.logger_config import LoggerConfig


class DataAcquisition(object):
    """ Class for data acquisition process """

    def __init__(self, log_obj: logging.Logger, data_process_queue: multiprocessing.Queue) -> None:
        """ Constructor to process all initialization process

            Args:
                log_obj (object): Log object
                data_process_queue (object): Multiprocessing Queue object
        """

        self.log = log_obj
        self.data_process_queue = data_process_queue
        self.stock_symbols = Config.SYMBOLS
        self.api_url = None

    def setup_data_acquisition(self) -> None:
        """ Setup all data Acquisition phase

            Steps:
                1. Prepare the API endpoints and ensure that the API is reachable.
                2. For the Yahoo Finance API, we’ll attempt a test request to validate connectivity.
        """
        try:
            self.log.debug(f"Testing live data capture for stock price: "
                           f"{stock_info.get_live_price(self.stock_symbols[0])}")
            self.log.info("Data Acquisition setup completed successfully.")
        except Exception as e:
            self.log.error(f"Failed to setup Data Acquisition: {e}")
            raise Exception(f"Error occurred during API validation :{e}")

    @staticmethod
    def fetch_stock_data(symbol) -> dict:
        """ Fetch stock data for a single symbol using yahoo_fin.

        Args:
            symbol (str): The stock symbol to fetch data for.

        Returns:
            dict: A dictionary with keys 'symbol', 'price', and 'timestamp'.
        """

        data = stock_info.get_live_price(symbol)
        return {
            "symbol": symbol,
            "price": data,
            "timestamp": time.time()
        }

    def acquisition_worker(self, symbol_subset: list, queue: multiprocessing.Queue, time_limit: int = 10,
                           fetch_interval: int = 1) -> None:
        """ Worker thread function to continuously fetch data for given symbols until time_limit expires.

        Args:
            symbol_subset (list[str]): A subset of symbols to fetch.
            queue (multiprocessing.Queue): Queue to store fetched data.
            time_limit (int): Duration in seconds for continuous fetching.
            fetch_interval (int): Delay between successive fetches in seconds.
        """

        log_obj = LoggerConfig.create_logger("acq_process", Config.LOG_FILE)
        start_time = time.time()
        while (time.time() - start_time) < time_limit:  # set time limit for now
            for symbol in symbol_subset:
                try:
                    data = self.fetch_stock_data(symbol)
                    log_obj.debug(data)
                    queue.put(data)
                except Exception as e:
                    log_obj.error(f"Error fetching data for {symbol}: {e}")
            time.sleep(fetch_interval)

    def run_data_acquisition_process(self, thread_count=2) -> None:
        """ Run the data acquisition process by spawning multiple threads.

            Args:
                thread_count (int): Number of threads to use for concurrent data fetching.
        """

        chunk_size = max(1, len(self.stock_symbols) // thread_count)
        symbol_chunks = [self.stock_symbols[i:i + chunk_size] for i in range(0, len(self.stock_symbols), chunk_size)]

        threads = []
        for chunk in symbol_chunks:
            t = threading.Thread(target=self.acquisition_worker, args=(chunk, self.data_process_queue))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        self.data_process_queue.put(None)
