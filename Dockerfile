FROM ubuntu

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update

RUN apt install -y python3 python3-pip

RUN pip3 install agavepy

WORKDIR /scripts
COPY *.py .
COPY *.sh .

CMD [ "/bin/python3", "upload.py" ]