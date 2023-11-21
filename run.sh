#!/bin/bash
docker run --network=apache-storm_default -it --rm -v ./build/libs/apache-storm-1.0-SNAPSHOT.jar:/topology.jar storm storm jar /topology.jar org.example.Main
