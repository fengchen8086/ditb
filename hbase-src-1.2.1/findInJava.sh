#!/bin/bash

if [ $# -lt 1 ]; then
	echo "Usage: ./findInJava.sh str"
else
	find ./ -name "*.java" | xargs grep -in "$@"
fi

