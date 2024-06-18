# syntax=docker/dockerfile:1
FROM python:3.10-slim
WORKDIR /mtp
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
EXPOSE 8008
COPY . .
CMD ["flask", "run"]