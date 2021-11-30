import random
import time
import grpc
from queue import Queue
import ReplicatedLog_pb2
import ReplicatedLog_pb2_grpc
from concurrent import futures
import sys



ipaddress = sys.argv[1]
host = int(sys.argv[2])
logs = []
id = 0
q = Queue()
        
def log(msg_id, msg):
    global id
    if msg_id == id + 1: # append right message
        logs.append(msg)
        id += 1
        for i in range(q.qsize()): # check the queue of wrong order messages
            log(*q.get())
    elif msg_id < id + 1: # check if duplicate
        pass
    else: # add to queue wrong order message
        q.put((msg_id, msg))
            
        
class SlaveLogger(ReplicatedLog_pb2_grpc.PostRequestServiceServicer):
    '''
    Log message, simulated internal server error and latency
    '''
    def PostRequest(self, request, context):
        sleep = random.randint(0, 4) # realization of random latency
        time.sleep(sleep)
        log(request.w, request.msg)
        if sleep == 0:
            raise Exception('RandomInternalServerError')
        return ReplicatedLog_pb2.POSTResponse(msg='1')


class SlaveSendLogs(ReplicatedLog_pb2_grpc.GetRequestServiceServicer):
    '''
    Send logs API.
    '''
    def GetRequest(self, request, context):
        return ReplicatedLog_pb2.GETResponse(data=logs)
        
class SlaveSendHeartBeat(ReplicatedLog_pb2_grpc.AskHeartBeatServiceServicer):
    '''
    Send heartbeat API.
    '''
    def HeartBeatRequest(self, request, context):
        return ReplicatedLog_pb2.HeartBeat(heartbeat=1)

    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    ReplicatedLog_pb2_grpc.add_PostRequestServiceServicer_to_server(SlaveLogger(), server)
    ReplicatedLog_pb2_grpc.add_GetRequestServiceServicer_to_server(SlaveSendLogs(), server)
    ReplicatedLog_pb2_grpc.add_AskHeartBeatServiceServicer_to_server(SlaveSendHeartBeat(), server)
    server.add_insecure_port(f"{ipaddress}:{host}")
    server.start()
    server.wait_for_termination()



if __name__ == "__main__":
    serve()









