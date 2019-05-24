import concurrent.futures
import urllib.request
import time
import random
from flask import (Blueprint, g, abort)
from inspect import isfunction
from .airctrl.airctrl import AirClient
from kafka import KafkaProducer
from datetime import datetime
import json

class TaskRunner:
     
    def __init__(self, fn, interval = 10, executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)):
        if not isfunction(fn):
            raise TypeError("fn should be function")
        if interval < 1 or executor is None:
            raise ValueError("incorrect value passed")    
        self.__fn = fn
        self.__interval = interval
        self.__executor = executor
        self.__future = None
        self.__run = True

    def __infinitRun(self):
        while self.__run:
            self.__fn()
            time.sleep(self.__interval)
        return True

    def start(self):
        if self.__future is None:
            self.__future = self.__executor.submit(self.__infinitRun)
        return self.__future

    def stop(self):
        if self.__future is not None:
            self.__run = False
        return self.__future

    def status(self):
        status = {'interval' : self.__interval}
        if self.__future is not None and self.__run is True:
            status['status'] = {True: 'running', False: 'failed'}[self.__future.running()]
        elif self.__future is not None and self.__run is False:
            status['status'] = {True: 'stopping', False: 'stopped'}[self.__future.running()]
        else:
            status['status'] = 'not started'
        return status

bp = Blueprint('puller', __name__, url_prefix='/puller')
executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
c = AirClient('192.168.1.4')
tr = TaskRunner(lambda: c.print_status(),interval=5,executor=executor)
producer = None#KafkaProducer(bootstrap_servers='192.168.1.6:32768,192.168.1.6:32769',value_serializer=lambda m: json.dumps(m).encode('ascii'))

def pull_status(debug=False):
    status = c.get_status(debug)
    status['timestamp']=datetime.now().isoformat()
    producer.send('air-stats-input', status)

def close_executor(e=None):
    if (tr is not None):
        tr.stop()
    executor.shutdown(False)

def init_app(app):
    c.load_key()
    app.teardown_appcontext(close_executor)

@bp.route('/status')
def status():
    return f'TaskRunner working status = {tr.status()}'

@bp.route('/start', methods=('POST',))
def start():
    tr.start()
    return '',202

@bp.route('/stop', methods=('POST',))
def stop():
    result = tr.stop()
    if result is None:
        abort(403, "TaskRunner not started yet")
    else:
        return '',202