#!/bin/bash

# The script will install pyenv and setup python 3.10.10 as your default version.


# We are going to install Python 3.10.10 in order to be compatible with Glue
# as shown here: https://github.com/awslabs/aws-glue-libs
apt-get -y update && apt-get -y upgrade 
apt-get -y install python3.10
ln -s /usr/bin/python3.10 /usr/bin/python 


# Installing nodejs
apt-get -y install nodejs


