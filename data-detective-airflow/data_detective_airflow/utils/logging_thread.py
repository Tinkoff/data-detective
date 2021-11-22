import time
from datetime import timedelta
from threading import Thread

from airflow.utils.log.logging_mixin import LoggingMixin


class LoggingThread(LoggingMixin):
    def __init__(self, context, interval=300):
        super().__init__(context=context)
        self.interval = interval
        self.dag_id = context['dag'].dag_id
        self.task_id = context['task'].task_id
        self.still_running = True
        self.start_time = time.perf_counter()

        thread = Thread(target=self.run, args=(), daemon=True)
        thread.start()

    def run(self):
        while self.still_running:
            time.sleep(self.interval)
            if self.still_running:
                time_running = timedelta(seconds=time.perf_counter() - self.start_time)
                self.log.info(f"DAG '{self.dag_id}' task '{self.task_id}'" f' still running {time_running}')

    def stop(self):
        self.still_running = False
