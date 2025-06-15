#!/bin/bash

urls=(
  "http://0.0.0.0:8080/products/search?search_term=cerveza"
  "http://0.0.0.0:8080/products/b4d8b3c5-482e-40f1-b225-7bdb72c0cbc4"
)
expected_codes=(200 200)

for i in "${!urls[@]}"; do
  url="${urls[$i]}"
  expected="${expected_codes[$i]}"
  status=$(curl -s -o /dev/null -w "%{http_code}" "$url")
  if [ "$status" -eq "$expected" ]; then
    echo "SUCCESS: $url returned $expected"
  else
    echo "FAIL: $url returned $status (expected $expected)"
  fi
done

# Check the third URL expecting 400
url="http://0.0.0.0:8080/products/search?search_term="
expected=400
status=$(curl -s -o /dev/null -w "%{http_code}" "$url")
if [ "$status" -eq "$expected" ]; then
  echo "SUCCESS: $url returned $expected"
else
  echo "FAIL: $url returned $status (expected $expected)"
fi
