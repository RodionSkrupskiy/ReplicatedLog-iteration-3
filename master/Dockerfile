FROM python
RUN mkdir /service
#for docker-compose-from-img use master/ before files u want to copy
COPY Master.py /service/Master.py
COPY ReplicatedLog.proto /service/ReplicatedLog.proto
COPY requirements.txt /service/requirements.txt
WORKDIR /service
RUN python -m pip install --upgrade pip
RUN python -m pip install -r /service/requirements.txt
RUN python -m grpc_tools.protoc -I ./ --python_out=. --grpc_python_out=. ./ReplicatedLog.proto
ENTRYPOINT ["python", "Master.py"]
