#! /usr/bin/env bash
git clone https://github.com/ACONCEPT/generate_project_data
git clone https://github.com/ACONCEPT/postgreslib
git clone https://github.com/ACONCEPT/producer
git clone https://github.com/ACONCEPT/consumer
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update -yq
sudo apt-get install -yq software-properties-common python3.6 python3-pip python3-venv
pip3 install --update pip
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
echo "export PROJECT_HOME='"$(pwd)"'" >> ~/.bashrc
echo "export "PATH=$PATH:$(pwd)/bin"" >> ~/.bashrc
chmod +x bin/
source ~/.bashrc

