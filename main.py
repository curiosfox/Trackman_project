from multiprocessing import Process
from multiprocessing import Queue

from config import Config
from process.data_acquisition.data_acquisition_main import DataAcquisition
from process.data_processing.data_processing_main import DataProcessing
from process.data_storage.data_storage_main import DataStorage
from utils.logger_config import LoggerConfig


class MainApp(object):
    """ Main App class for controlling system processes """

    def __init__(self) -> None:
        """ Constructor for Main App class """

        data_process_queue = Queue()
        data_storage_queue = Queue()
        self.log = LoggerConfig.create_logger("main_app", Config.LOG_FILE)
        self.data_acq_obj = DataAcquisition(log_obj=self.log, data_process_queue=data_process_queue)
        self.data_proc_obj = DataProcessing(log_obj=self.log, data_process_queue=data_process_queue,
                                            data_storage_queue=data_storage_queue)
        self.data_stor_obj = DataStorage(log_obj=self.log, data_storage_queue=data_storage_queue)

    def setup(self) -> None:
        """ Setup phase for the entire app """

        try:
            self.log.info("Starting the application setup phase")
            self.data_acq_obj.setup_data_acquisition()
            self.data_proc_obj.setup_data_processing()
            self.data_stor_obj.setup_data_storage()
        except Exception as ex:
            self.log.error(f"Error occurred during setup phase :{ex}")
        self.log.info(f"Setup phase of the application has completed successfully")

    def acquisition_process(self):
        # This runs in its own process
        self.data_acq_obj.run_data_acquisition_process(thread_count=2)

    def processing_process(self):
        # This runs in its own process
        self.data_proc_obj.run_data_processing(max_workers=10)

    def storage_process(self):
        # This runs in its own process
        self.data_stor_obj.run_data_storage()

    def run(self) -> None:
        """ Runs the main app """

        self.setup()
        self.log.info("Starting Main application")
        p_acq = Process(target=self.acquisition_process)
        p_proc = Process(target=self.processing_process)
        p_store = Process(target=self.storage_process)

        p_acq.start()
        p_proc.start()
        p_store.start()

        p_acq.join()
        p_proc.join()
        p_store.join()
        self.log.info("Completed the entire process")

if __name__ == "__main__":
    main_obj = MainApp()
    main_obj.run()
