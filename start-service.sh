#!/bin/bash
# run script for development
mvn clean package -DskipTests -q
java -jar target/starter-1.0.0-SNAPSHOT-fat.jar
