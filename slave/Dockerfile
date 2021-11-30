FROM python
RUN mkdir /service
COPY Slave.py /service/Slave.py
COPY ReplicatedLog.proto /service/ReplicatedLog.proto
COPY requirements.txt /service/requirements.txt
WORKDIR /service
RUN python -m pip install --upgrade pip
RUN python -m pip install -r /service/requirements.txt
RUN python -m grpc_tools.protoc -I ./ --python_out=. --grpc_python_out=. ./ReplicatedLog.proto
#EXPOSE 50052
ENTRYPOINT ["python", "Slave.py"]
