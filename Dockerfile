# Dockerfile for the execution of the CDK deployment
FROM ubuntu:22.04

WORKDIR /home/biontechdemo/

# We are going to install Python 3.10.10 in order to be compatible with Glue
# Installing nodejs as dependency of CDK
RUN apt-get -y update && apt-get -y upgrade 
RUN apt-get -y install python3.10 unzip
RUN ln -s /usr/bin/python3.10 /usr/bin/python 

RUN apt-get -y install curl
RUN curl -sL https://deb.nodesource.com/setup_16.x -o nodesource_setup.sh
RUN sh nodesource_setup.sh
RUN apt install nodejs

RUN node -v
RUN apt-get install less
RUN apt install -y python3-pip

# Docker
RUN apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
RUN mkdir -m 0755 -p /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
 
RUN apt-get -y update
RUN apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
RUN apt-get -y install zip jq

COPY . ./

# Installing AWS CLI
RUN unzip ./utilities/awscliv2.zip && ./aws/install

# Installing CDK
RUN npm install -g aws-cdk

# Installing Python Libraries
RUN pip install -r requirements.txt
