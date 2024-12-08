#!/bin/bash

# JSON object to pass to Lambda Function
numworkers=$1
data="Moby-Dick.txt"
bucket="test.bucket.462562f24.skg"
json={"\"bucket\"":"\"$bucket\",\"data\"":"\"$data\",\"workers\"":$numworkers}

echo "Message being sent:"
echo $json | jq
echo ""

time output=`aws lambda invoke --invocation-type RequestResponse --cli-binary-format raw-in-base64-out --function-name project_test_script --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""

echo ""
echo "JSON RESULT:"
echo $output
echo "JQ RESULT"
wc=$(echo "$output" | jq '.word_counts')
echo $wc

