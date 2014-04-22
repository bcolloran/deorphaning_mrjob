#!/bin/sh

rm -rf classes 2> /dev/null
rm naive.jar 2> /dev/null

mkdir classes
javac -classpath `hadoop classpath` -d classes *java
jar -cvf naive.jar -C classes/ .
