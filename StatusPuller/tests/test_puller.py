import pytest
import time

def test_status(client):
    response = client.get('/puller/status')
    assert b"TaskRunner working status = False" in response.data

def test_start_stop(client):
    assert client.post('puller/start').status_code == 202
    response = client.get('/puller/status')
    assert b"TaskRunner working status = True" in response.data 
    assert client.post('puller/stop').status_code == 202
    # waiting for TaskRunner to shutdown
    time.sleep(2)
    response = client.get('/puller/status')
    assert b"TaskRunner working status = False" in response.data 