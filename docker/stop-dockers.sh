#!/bin/bash
docker kill master nifi slave1 slave2 slave3 hbase-docker
docker rm nifi master slave1 slave2 slave3 hbase-docker
docker network rm apache_network
