FROM ubuntu:20.04

RUN apt-get update && apt-get install -y \
	python3 \
	python3-networkx \
	python3-seaborn \
	python3-setuptools \
	python3-pygraphviz \
	ca-certificates \
	graphviz

COPY . /analysis

WORKDIR /analysis

RUN python3 setup.py install

ENTRYPOINT ["./run_analysis.sh"]
