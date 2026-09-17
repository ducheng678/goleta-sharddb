FROM python:3.13-slim
WORKDIR /work
COPY . .
ENTRYPOINT ["python", "-m", "sharddb.devhost"]
