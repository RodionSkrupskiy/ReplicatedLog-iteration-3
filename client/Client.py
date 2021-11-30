import os
import grpc
import ReplicatedLog_pb2
import ReplicatedLog_pb2_grpc
import argparse



slave_ports = ['50052', '50053']

def post():
    with grpc.insecure_channel('172.17.0.1:50051') as channel:
        client = ReplicatedLog_pb2_grpc.PostRequestServiceStub(channel)
        request = ReplicatedLog_pb2.POST(w=int(input('Enter write concern parameter: ')), msg=input('Enter values to log: '))
        response = client.PostRequest(request)
        print(response)

def get_master():
    with grpc.insecure_channel('172.17.0.1:50051') as channel:
        client = ReplicatedLog_pb2_grpc.GetRequestServiceStub(channel)
        request = ReplicatedLog_pb2.GET(msg='1')
        response = client.GetRequest(request)
        print(response.data)
	    
	    
def get_slaves(port):
    with grpc.insecure_channel(f'172.17.0.1:{port}') as channel:
        client = ReplicatedLog_pb2_grpc.GetRequestServiceStub(channel)
        request = ReplicatedLog_pb2.GET(msg='1')
        try:
            response = client.GetRequest(request)
        except:
            return f'node {port} not responding.'
        return response.data   
        
        
def get_heartbeats():
    with grpc.insecure_channel(f'172.17.0.1:50051') as channel:
        client = ReplicatedLog_pb2_grpc.AskHeartBeatsServiceStub(channel)
        request = ReplicatedLog_pb2.AskHeartBeat()
        response = client.HeartBeatRequest(request)
        return list(zip(response.address, response.heartbeats))
        
      	


# for real application can be used argparse.ArgumentParser
user_input = input('Enter POST, GET, hb or q').lower()
while user_input != 'q':
    if user_input == 'post':
        print('Calling POST')
        post()
        
    elif user_input == 'get':
        ask_node = input('Enter where you want logs from: m, s, s1 or s2 ').lower()
        if ask_node == 'm':
            print('calling GET')
            get_master()
        elif ask_node == 's':
            print('calling GET')
            for i in slave_ports:
                print(f'Slave {i}', get_slaves(i))
        elif ask_node == 's1':
            print('calling GET')
            print(get_slaves(50052))
        elif ask_node == 's2':
            print('calling GET')
            print(get_slaves(50053))
            
    elif user_input == 'hb':
        print(get_heartbeats())
        
    else:
        print('Wrong input')
    user_input = input('Enter POST, GET, hb or q ').lower()


