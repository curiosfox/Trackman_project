from multiprocessing import Process
from multiprocessing import Queue

from configuration.config import Config
from process.data_acquisition.data_acquisition_main import DataAcquisition
from process.data_processing.data_processing_main import DataProcessing
from process.data_storage.data_storage_main import DataStorage
from utils.logger_config import LoggerConfig


class MainApp(object):
    """ Main application class responsible for orchestrating the data pipeline.

        This class sets up and runs three separate processes:
        1. Data Acquisition
        2. Data Processing
        3. Data Storage

        Each process communicates via multiprocessing queues:
        - Data Acquisition → Data Process Queue
        - Data Processing → Data Storage Queue
    """

    def __init__(self) -> None:
        """ Initialize the main application with required queues and loggers."""

        self.data_process_queue = Queue()
        self.data_storage_queue = Queue()
        self.log = LoggerConfig.create_logger("main_app", Config.LOG_FILE)
        self.log.debug("Initializing MainApp and creating multiprocessing queues.")
        self.data_acq_obj = DataAcquisition(log_obj=self.log, data_process_queue=self.data_process_queue)
        self.data_proc_obj = DataProcessing(log_obj=self.log, data_process_queue=self.data_process_queue,
                                            data_storage_queue=self.data_storage_queue)
        self.data_stor_obj = DataStorage(log_obj=self.log, data_storage_queue=self.data_storage_queue)

    def setup(self) -> None:
        """ Setup phase for the entire app """

        try:
            self.log.info("Starting the application setup phase")
            self.data_acq_obj.setup_data_acquisition()
            self.data_proc_obj.setup_data_processing()
            self.data_stor_obj.setup_data_storage()
            self.log.info(f"Setup phase of the application has completed successfully")
        except Exception as ex:
            self.log.error(f"Error occurred during setup phase :{ex}")

    def acquisition_process(self) -> None:
        """ Target function for the data acquisition process."""

        self.log.info("Acquisition process starting...")
        self.data_acq_obj.run_data_acquisition_process(thread_count=2)
        self.log.info("Acquisition process completed.")

    def processing_process(self) -> None:
        """ Target function for the data processing."""

        self.log.info("Processing process starting...")
        self.data_proc_obj.run_data_processing(max_workers=10)
        self.log.info("Processing process completed.")

    def storage_process(self) -> None:
        """ Target function for the data storage process."""

        self.log.info("Storage process starting...")
        self.data_stor_obj.run_data_storage()
        self.log.info("Storage process completed.")

    def run(self) -> None:
        """ Run the main application pipeline by starting all processes."""

        self.setup()
        self.log.info("Starting Main application")
        p_acq = Process(target=self.acquisition_process)
        p_proc = Process(target=self.processing_process)
        p_store = Process(target=self.storage_process)
        try:
            p_acq.start()
            p_proc.start()
            p_store.start()
            self.log.debug("All processes started, waiting for completion.")

            p_acq.join()
            p_proc.join()
            p_store.join()
            self.log.info("Completed the entire process")
        except Exception as e:
            self.log.error(f"Exception in running processes: {e}", exc_info=True)
        finally:
            self.log.info("Main application run is now complete.")

if __name__ == "__main__":
    main_obj = MainApp()
    main_obj.run()
