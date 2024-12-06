import threading
import time

from yahoo_fin import stock_info

from config import Config


class DataAcquisition(object):
    """ Class for data acquisition process """

    def __init__(self, log_obj, data_process_queue):
        """ Constructor to process all initialization process

            Args:
                log_obj (object): Log object
        """

        self.log = log_obj
        self.data_process_queue = data_process_queue
        self.stock_symbols = Config.SYMBOLS
        self.api_url = None

    def setup_data_acquisition(self):
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

    def fetch_stock_data(self, symbol):
        """
        Fetch stock data for a single symbol using yahoo_fin.
        Returns a dictionary with keys: symbol, price, timestamp
        extracted from the quote data.
        """

        data = stock_info.get_live_price("AAPL")
        return {
            "symbol": symbol,
            "price": data,
            "timestamp": time.time()
        }

    def acquisition_worker(self, symbol_subset, queue, time_limit=10, fetch_interval=1):
        """

        :param symbol_subset:
        :param queue:
        :param time_limit:
        :return:
        """

        start_time = time.time()
        while (time.time() - start_time) < time_limit:  # set time limit for now
            for symbol in symbol_subset:
                try:
                    data = self.fetch_stock_data(symbol)
                    queue.put(data)
                except Exception as e:
                    print(f"Error fetching data for {symbol}: {e}")
            time.sleep(fetch_interval)

    def run_data_acquisition_process(self, thread_count= 2):

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
