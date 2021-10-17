#!/bin/sh
echo "Init localstack Kinesis"
awslocal kinesis create-stream --stream-name local-retrytest-stream --shard-count 1
