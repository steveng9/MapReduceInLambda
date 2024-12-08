#!/bin/bash

# JSON object to pass to Lambda Function
numworkers=$1
data="Moby-Dick.txt"
bucket="test.bucket.462562f24.skg"
json={"\"bucket\"":"\"$bucket\",\"data\"":"\"$data\",\"workers\"":$numworkers}

echo "Message being sent:"
echo $json | jq
echo ""

time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://jbtnbc5gyb.execute-api.us-east-1.amazonaws.com/async_project_stage`
echo ""

echo ""
echo "JSON RESULT:"
echo $output
echo "JQ RESULT"
wc=$(echo "$output" | jq '.word_counts')
echo $wc

