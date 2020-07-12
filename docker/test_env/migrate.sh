#!/bin/bash

while ! migrate -path=/migrations/ -database "mysql://root:password@tcp(mysql:3306)/ortelius_test" up; do
    sleep 1
done
