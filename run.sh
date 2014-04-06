#! /bin/bash

for((i=1; i<=100; i++));
do
java -jar client.jar localhost 51000 jobs.jar jobs.Hello 
done
