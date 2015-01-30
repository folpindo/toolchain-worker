#!/bin/env python

import gearman, json, requests, threading, logging
from paramiko import *
from phpserialize import *

class RestClient:
    def init(self):
        params = self.params
        self.base_url = params['base_url']
        self.version = params['version']
        self.service = params['service']
        self.method = params['method']
        self.init_url()
    def set_base_url(self, url):
        self.base_url = url
    def set_version(self, version):
        self.version = version
    def set_service(self, service):
        self.service = service
    def set_method(self, method):
        self.method = method
    def set_params(self, params):
        self.params = params
    def init_url(self):
        valid = True
        if self.base_url is None:
            valid = False
        if self.version is None:
            valid = False
        if self.service is None:
            valid = False
        if self.method is None:
            valid = False
        self.service_url = "%s/%s/%s/%s" % (self.base_url, self.version, self.service, self.method)
        return valid
    def call_url(self):
        response = None
        response = requests.post(self.service_url, data=self.params)
        self.response = response
        return response
    
class MiniThreads(threading.Thread):
    def set_job(self, job):
        self.job = job
    def set_exit_status(self, status):
        self.exit_status = status
    def get_exit_status(self):
        return self.exit_status
    def run(self):
        logging.debug('running')
        worker = Worker()
        worker.set_params(self.job)
        if worker.check_params():		
            worker.run_job(self)
        return
    
class Worker:
    def set_ssh_client(self, client):
        self.ssh_client = client
    def get_ssh_client(self):
        return self.ssh_client
    def set_rest_client(self, client):
        self.rest_client = client
    def get_rest_client(self):
        return self.rest_client
    def set_params(self, params):
        self.params = params
    def get_params(self):
        return self.params
    def check_params(self):
        try:
            params = None
            if self.params is None:
                return False
            params = self.params
            username = params['username']
            password = params['password']
            hostname = params['hostname']
            command = params['command']
            base_url = params['base_url']
            version = params['version']
            service = params['service']
            method = params['method']
            return True
        except IndexError:
            return False
    def init(self):
        if self.check_params:
            params = None
            params = self.params
    def run_job(self, th):
        params = self.params
        sshclient = SshClient()
        sshclient.init_connection()
        sshclient.set_params(params)
        result = sshclient.execute_command(th)	
        logging.debug('Executing on %s (thread: %s)' % (th.job['username'],threading.currentThread().getName()))
        client = RestClient()
        client.set_params(params)
        #client.set_params({"data":{"criteria":{}}})
        client.init()
        response = None
        response = client.call_url()
        if response is not None:
            print json.dumps(json.loads(response.text), indent=4, sort_keys=True)
            
class SshClient:
    def set_params(self, params):
        self.params = params
    def init_connection(self):
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        ssh.load_system_host_keys()
        #ssh.load_host_keys("~/.ssh/known_hosts")
        return ssh
    def init_properties(self):
        params = self.params
        self.base_url = params['base_url']
        self.version = params['version']
        self.service = params['service']
        self.method = params['method']
        self.username = params['username']
        self.password = params['password']
        self.hostname = params['hostname']
        self.command = params['command']
    def execute_command(self, th):
        ssh = self.init_connection()
        self.init_properties()
        ssh.connect(self.hostname, username=self.username, password=self.password)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(self.command)
        exit_status = ssh_stdout.channel.recv_exit_status()
        th.set_exit_status(exit_status)
        print "COMMAND OUTPUT:",ssh_stdout.read()
        print "status: %s" % th.get_exit_status()
        return ssh_stdin, ssh_stdout, ssh_stderr
    
def task_listener(gearman_worker, gearman_job):
    data = loads(gearman_job.data)
    objData = json.loads(json.dumps(data))
    my_threads = []
    hosts = objData['hosts']
    for i in hosts:
        t = MiniThreads(name='Thread for: %s' % (hosts[i]['hostname']))
        t.set_job(hosts[i])
        my_threads.append(t)
        t.start()
        t.join()
    for es in my_threads:
        status = "Failed"
        if es.get_exit_status() == 0:
            status = "Success"
        print "thread: %s, status: %s" % (es.getName(), status)
    return gearman_job.data[::-1]

class MyGearmanWorker(gearman.GearmanWorker):
    counter_init = False
    counter = 0
    def init_counter(self):
        if self.counter_init is None:
            self.counter_init = True
            self.counter = 0
            
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s',)
    gm_worker = MyGearmanWorker(['alphard:4730'])
    gm_worker.set_client_id('python-worker')
    gm_worker.register_task('toolchain-worker', task_listener)
    gm_worker.work()
