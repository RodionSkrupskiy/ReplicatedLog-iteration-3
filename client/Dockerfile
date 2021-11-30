FROM python
RUN mkdir /service
COPY client/Client.py /service/Client.py
COPY client/ReplicatedLog.proto /service/ReplicatedLog.proto
COPY client/requirements.txt /service/requirements.txt
WORKDIR /service
RUN python -m pip install --upgrade pip
RUN python -m pip install -r /service/requirements.txt
RUN python -m grpc_tools.protoc -I ./ --python_out=. --grpc_python_out=. ./ReplicatedLog.proto
EXPOSE 50053
ENTRYPOINT ["python", "Client.py"]
