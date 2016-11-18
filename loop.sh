#!/bin/bash

# Helpful script to run tests in loop

while mvn test -Dtest=LmdbDocManagerTest;
do
    echo ...
done
