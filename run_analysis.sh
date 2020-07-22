#!/bin/bash

set -euo pipefail

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
	python3 scripts/dump_logs.py "$1" "$file_dump"
else
	file_dump="$1"
	echo "Using logdump at $file_dump"
fi

python3 ./scripts/analysis_function_tree.py "$file_dump" > "$2/function_tree.txt"
python3 ./scripts/export.py "$file_dump" "$2/dump.csv"
python3 ./plots/execution_time.py "$file_dump" "$2"
python3 ./plots/function_graph.py --style modern "$file_dump" "$2/function_graph.png"
