import copy
import json
import logging
import threading
import time
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass
from threading import Thread
import requests
import socket

from conductor.conductor import WFClientMgr

logger = logging.getLogger(__name__)

DEFAULT_TASK_DEFINITION = {
    "name": "",
    "retryCount": 0,
    "ownerEmail": "example@example.com",
    "timeoutSeconds": 60,
    "timeoutPolicy": "TIME_OUT_WF",
    "retryLogic": "FIXED",
    "retryDelaySeconds": 0,
    "responseTimeoutSeconds": 60
}

hostname = socket.gethostname()


@dataclass
class RegisteredWorkerTask:
    task_type: str
    exec_function: callable


class TaskSource:
    def __init__(self):
        self.lock = threading.Lock()
        self.task_types = {}
        self.task_types_list = None

        self.actual_uuid = None
        self.filtered_queue = {}
        self.actual_task_types_running = defaultdict(int)
        self.last_task_position = 0

    def register_task_type(self, task_type, exec_function):
        self.task_types[task_type] = RegisteredWorkerTask(
            task_type, exec_function
        )
        self.task_types_list = list(self.task_types)

    def handle_tasks(self, queue):
        with self.lock:
            self.actual_uuid = uuid.uuid4()
            self.filtered_queue = {
                key: value
                for key, value in queue.items()
                if key in self.task_types.keys() and value > 0
            }

    def round_robin_task_types(self):
        task_type = self.task_types_list[self.last_task_position]
        self.last_task_position += 1

        if self.last_task_position == len(self.task_types):
            self.last_task_position = 0

        return task_type

    def get_next_task(self, last_task_type):
        with self.lock:

            if last_task_type:
                self.actual_task_types_running[last_task_type] -= 1

            if len(self.filtered_queue) == 0:
                return None

            task_type = None
            while task_type not in self.filtered_queue:
                task_type = self.round_robin_task_types()

            self.actual_task_types_running[task_type] += 1

            registered_task: RegisteredWorkerTask = self.task_types[task_type]

            self.filtered_queue[task_type] -= 1
            if self.filtered_queue[task_type] <= 0:
                self.filtered_queue.pop(task_type, None)

            next_worker = NextWorkerTask()
            next_worker.task_type = task_type
            next_worker.exec_function = registered_task.exec_function
            next_worker.poll_uuid = self.actual_uuid
            return next_worker

    def task_not_found_anymore(self, task_not_found):
        if self.actual_uuid == task_not_found.poll_uuid:
            with self.lock:
                self.filtered_queue.pop(task_not_found.task_type, None)


class NextWorkerTask:
    def __init__(self):
        self.task_type = None
        self.exec_function = None
        self.poll_uuid = None


class FrinxConductorWrapper:
    def __init__(self, server_url, max_thread_count, polling_interval=0.1, worker_id=None, headers=None):
        # Synchronizes access to self.queues by producer thread (in read_queue) and consumer threads (in tasks_in_queue)
        self.lock = threading.Lock()
        self.queues = {}
        self.conductor_task_url = server_url + "/metadata/taskdefs"
        self.headers = headers
        self.consumer_worker_count = max_thread_count
        self.task_source = TaskSource()

        self.polling_interval = polling_interval

        wfcMgr = WFClientMgr(server_url, headers=headers)
        self.taskClient = wfcMgr.taskClient
        self.worker_id = worker_id or hostname

    def start_workers(self):
        for i in range(self.consumer_worker_count):
            thread = Thread(target=self.consume_task)
            thread.daemon = True
            thread.start()

        logger.info("Starting a queue polling")
        fail_count = 0
        while True:
            try:
                time.sleep(float(self.polling_interval))
                queues_temp = self.taskClient.getTasksInQueue("all")
                self.task_source.handle_tasks(queues_temp)
                fail_count = 0
            except Exception:
                logger.error(
                    f"Unable to read a queue info after {fail_count} attempts", exc_info=True
                )
                self.task_source.handle_tasks({})
                fail_count = +1
                if fail_count > 10:
                    exit(1)

    # Consume_task is executing tasks in the queue. The tasks are selected by round-robin from all task types.
    # If there is no task for processing, the thread is in sleeping for a defined interval.
    def consume_task(self):
        last_task_type = None
        while True:
            next_task: NextWorkerTask = self.task_source.get_next_task(last_task_type)
            last_task_type = None

            if not next_task:
                time.sleep(float(self.polling_interval))
                continue

            last_task_type = next_task.task_type

            polled_task = self.taskClient.pollForTask(
                next_task.task_type, self.worker_id
            )

            if polled_task is None:
                self.task_source.task_not_found_anymore(next_task)
                continue

            logger.info(
                "Polled for a task %s of type %s", polled_task["taskId"], next_task.task_type
            )

            # Check if task input is externalized and if so, download the input
            polled_task = self.replaceExternalPayloadInput(polled_task)
            if polled_task is None:
                # Error replacing external payload
                continue

            self.execute(polled_task, next_task.exec_function)


    def replaceExternalPayloadInput(self, task):
        # No external payload placeholder present, just return original task
        if self.taskClient.EXTERNAL_INPUT_KEY not in task:
            return task

        location = {}
        try:
            # Get the exact uri from conductor where the payload is stored
            location = self.taskClient.getTaskInputExternalPayloadLocation(task[self.taskClient.EXTERNAL_INPUT_KEY])
            if 'uri' not in location:
                raise Exception("Unexpected output for external payload location: %s" % location)

            # Replace placeholder with real output
            task.pop(self.taskClient.EXTERNAL_INPUT_KEY)
            task['inputData'] = requests.get(location['uri'], headers=self.taskClient.headers).json()
            return task

        except Exception:
            logger.error("Unable to download external task input: %s for path: %s", task["taskId"], location, exc_info=True)
            self.handleTaskException(task)
            return None


    def register(self, task_type, task_definition, exec_function):
        if task_definition is None:
            task_definition = copy.deepcopy(DEFAULT_TASK_DEFINITION)
        else:
            task_definition = {**DEFAULT_TASK_DEFINITION, **task_definition}

        task_definition["name"] = task_type

        logger.debug("Registering a task of type %s with definition %s", task_type, task_definition)
        task_meta = copy.deepcopy(task_definition)
        task_meta["name"] = task_type
        try:
            res = requests.post(
                self.conductor_task_url, data=json.dumps([task_meta]), headers=self.headers
            )
        except Exception:
            logger.error("Unable to register a task", exc_info=True)

        self.task_source.register_task_type(task_type, exec_function)

    def execute(self, task, exec_function):
        try:
            logger.info("Executing a task %s", task["taskId"])
            resp = exec_function(task)
            if resp is None:
                error_msg = "Task execution function MUST return a response as a dict with status and output fields"
                raise Exception(error_msg)
            task["status"] = resp["status"]
            task["outputData"] = resp.get("output", {})
            task["logs"] = resp.get('logs', [])
            self.taskClient.updateTask(task)
        except Exception:
            self.handleTaskException(task)

    def handleTaskException(self, task):
        logger.error("Unable to execute a task %s", task["taskId"], exc_info=True)
        error_info = traceback.format_exc().split("\n")[:-1]
        task["status"] = "FAILED"
        task["outputData"] = {
            "Error while executing task": task.get("taskType", ""),
            "traceback": error_info,
        }
        task['logs'] = ["Logs: %s" % traceback.format_exc()]
        try:
            self.taskClient.updateTask(task)
        except Exception:
            logger.error(
                "Unable to update a task %s, it may have timed out",
                task["taskId"],
                exc_info=True,
            )
