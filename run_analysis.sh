#!/bin/bash

set -uio pipefail

if [ -z "${1:-}" ]; then
	echo "$0 <INPUT LOGS> <OUTPUT ANALYSIS>"
	exit
fi

if [ -z "${2:-}" ]; then
	echo "$0 <INPUT LOGS> <OUTPUT ANALYSIS>"
	exit
fi

mkdir -p "$2"

if [ -d "$1" ]; then
	file_dump="$2/dump.json"
	echo "Dumping logs to $file_dump"
	python scripts/dump_logs.py "$1" "$file_dump"
else
	file_dump="$1"
	echo "Using logdump at $file_dump"
fi

python ./scripts/analysis_function_tree.py "$file_dump" > "$2/function_tree.txt"
python ./plots/execution_time.py "$file_dump" "$2"
python ./plots/function_graph.py --style modern "$file_dump" "$2/function_graph.png"
