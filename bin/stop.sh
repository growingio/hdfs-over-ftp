#!/usr/bin/env bash

if [ -n "$(jps -m | grep HdfsOverFtpServer)" ]
then
  jps -m | grep HdfsOverFtpServer | awk '{print $1}' | xargs kill
fi