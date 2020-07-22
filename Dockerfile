FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
	python3 \
	python3-networkx \
	python3-seaborn \
	python3-setuptools \
	python3-pygraphviz \
	python3-pip \
	ca-certificates \
	graphviz

COPY . /analysis

WORKDIR /analysis

RUN python3 setup.py install

RUN pip3 install pandas==1.0.5

ENTRYPOINT ["./run_analysis.sh"]
