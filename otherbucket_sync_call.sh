#!/bin/bash

# JSON object to pass to Lambda Function
numworkers=$1
data="test.txt"
#data=$2
bucket="project.bucket.raw.text.loose"

json=$(jq -n --arg bucket "$bucket" --arg data "$data" --argjson workers $numworkers '{bucket: $bucket,data: $data, workers: $workers}')
#json={"\"bucket\"":"\"$bucket\",\"data\"":"\"$data\",\"workers\"":$numworkers}

echo "Message being sent:"
echo $json | jq
echo ""

time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://lwos1t6wqk.execute-api.us-east-1.amazonaws.com/test`
echo ""

echo ""
echo "JSON RESULT:"
echo $output
echo "JQ RESULT"
wc=$(echo "$output" | jq '.word_counts')
echo $wc

