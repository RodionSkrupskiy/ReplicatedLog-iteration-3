import os
import time
import grpc
import ReplicatedLog_pb2
import ReplicatedLog_pb2_grpc
from concurrent import futures
import sys
from threading import Thread, Lock, Condition
from queue import Queue



logs = []
slave_ports = [50052, 50053]
slave_hosts = ['slave1', 'slave2']
id = 0

waiting_parameter = 5.0
delays = [0.5, 1, 5, 10, 20]

quorum = 2
quorum_state = False
        
        
def logslaves(host, port, msg, id, delay, latch):
    '''
    Connect to slave, check accessibility, retry if needed, count delivered messages
    :param id: id of message to be replicated
    :param delay: delay id for iterating delays array
    :return:
    '''
    with grpc.insecure_channel(f'{host}:{port}') as channel:
        client = ReplicatedLog_pb2_grpc.PostRequestServiceStub(channel)
        slave_request = ReplicatedLog_pb2.POST(w=id, msg=msg)
        try:
            response = client.PostRequest(slave_request)
        except:
            time.sleep(delays[delay])
            if delay == 4:
                delay -= 1
            logslaves(host, port, msg, id, delay+1, latch)
            return 0
        latch.count_down()
        return 1
                
              
class Logger(ReplicatedLog_pb2_grpc.PostRequestServiceServicer):
    def PostRequest(self, request, context):
        global id, quorum_state
        if not quorum_state:
            return ReplicatedLog_pb2.POSTResponse(
                msg='No quorum nodes are available. Master node in read-only mode')
        id += 1
        logs.append(request.msg)
        threads = []
        latch = CountDownLatch(request.w - 1)
        for i in range(len(slave_ports)):
            thread = Thread(target=logslaves, args=(slave_hosts[i], slave_ports[i], request.msg, id, 0, latch))
            thread.start()
            threads.append(thread)
        latch.__await__()
        for thread in threads:
            thread.join(waiting_parameter)


        return ReplicatedLog_pb2.POSTResponse(msg=f'Master and Slaves have recived msg, w={request.w}')
        
        
         
class SendLogs(ReplicatedLog_pb2_grpc.GetRequestServiceServicer):
    '''
    Send local logs to Client
    '''
    def GetRequest(self, request, context):
        return ReplicatedLog_pb2.GETResponse(data=logs)
        
        
        
def heartbeat_node(slave_host, port):
    '''
    Check node for accessibility
    :return: int 1-node is alive, 0-node is dead
    '''
    with grpc.insecure_channel(f'{slave_host}:{port}') as channel:
        client = ReplicatedLog_pb2_grpc.AskHeartBeatServiceStub(channel)
        request_to_slave = ReplicatedLog_pb2.AskHeartBeat()
        try:
            return client.HeartBeatRequest(request_to_slave).heartbeat
        except:
            return 0

def check_quorum(n):
    '''
    Check for quorum every 0.5sec, if system doesn't have quorum,
    master goes to read-only mode
    :param n: int quorum
    '''
    global quorum_state
    while True:
        active_nodes = [heartbeat_node(slave_hosts[i], slave_ports[i]) for i in range(len(slave_ports))]
        quorum_state = sum(active_nodes) >= n-1
        #print(quorum_state)
        time.sleep(0.5)


class HeartBeat(ReplicatedLog_pb2_grpc.AskHeartBeatsServiceServicer):
    '''
    HeartBeat API
    '''
    def HeartBeatRequest(self, request, context):
        '''
        Check all slaves accessibility
        :return: array with addresses and array with heartbeats: 1-alive; 0-dead
        '''
        heartbeats = []
        address = []
        for i in range(len(slave_ports)):
            address.append(f'{slave_hosts[i]}:{slave_ports[i]}')
            heartbeats.append(heartbeat_node(slave_hosts[i], slave_ports[i]))
        return ReplicatedLog_pb2.HeartBeats(address=address, heartbeats=heartbeats)        


class CountDownLatch(object):
    def __init__(self, count=1):
        self.count = count
        self.lock = Condition()

    def count_down(self):
        self.lock.acquire()
        self.count -= 1
        if self.count <= 0:
            self.lock.notifyAll()
        self.lock.release()

    def __await__(self):
        self.lock.acquire()
        while self.count > 0:
            self.lock.wait()
        self.lock.release()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    ReplicatedLog_pb2_grpc.add_PostRequestServiceServicer_to_server(Logger(), server)
    ReplicatedLog_pb2_grpc.add_GetRequestServiceServicer_to_server(SendLogs(), server)
    ReplicatedLog_pb2_grpc.add_AskHeartBeatsServiceServicer_to_server(HeartBeat(), server)
    server.add_insecure_port("master:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    # Before starting servers, check quorum
    Thread(target=check_quorum, args=(quorum, )).start()
    serve()
    
    
    
