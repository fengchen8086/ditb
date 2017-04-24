#!/bin/bash


find ./ -name "*" -type f | xargs grep "Exception" | grep -v "KeeperException" | grep -v "BindException" | grep -v "EndOfStreamException"
