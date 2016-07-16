#!/bin/sh -ex

rabbitmqctl add_vhost /test
rabbitmqctl add_user test test
rabbitmqctl set_permissions -p /test test ".*"  ".*"  ".*"

