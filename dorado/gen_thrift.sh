#!/bin/bash

# dorado-test-api
files=$(ls dorado-test/dorado-test-api/src/main/resources/thrift/)
for filename in $files
do
    echo $filename
    thrift --gen java dorado-test/dorado-test-api/src/main/resources/thrift/$filename
done
cp -rf gen-java/* dorado-test/dorado-test-api/src/main/java/
rm -rf gen-java

# dorado-protocol-octo
files=$(ls dorado-protocol/dorado-protocol-octo/src/main/resources/idl/)
for filename in $files
do
    echo $filename
    thrift --gen java dorado-protocol/dorado-protocol-octo/src/main/resources/idl/$filename
done
cp -rf gen-java/* dorado-protocol/dorado-protocol-octo/src/main/java/
rm -rf gen-java
