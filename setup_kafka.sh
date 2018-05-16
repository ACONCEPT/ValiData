sudo apt-get -y update
sudo apt-get install -y default-jre zookeeperd
sudo adduser --system --no-create-home --disabled-password --disabled-login kafka
mkdir Downloads
cd Downloads/
wget "http://www-eu.apache.org/dist/kafka/1.0.1/kafka_2.12-1.0.1.tgz"
#sudo netstat -plunt
sudo mkdir /opt/kafka
sudo chown $USER:$USER /opt/kafka
echo export KAFKA_HOME=/opt/kafka >> ~/.profile
source ~/.profile
sudo tar -xvzf kafka_2.12-1.0.1.tgz --directory $KAFKA_HOME --strip-components 1
sudo mkdir /var/lib/kafka
sudo mkdir /var/lib/kafka/data
sudo chown -R kafka:nogroup /opt/kafka
sudo chown -R kafka:nogroup /var/lib/kafka
sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

echo "alias zookeeper_shell='bash $KAFKA_HOME/bin/zookeeper-shell.sh localhost:2181'" >> ~/.bashrc
echo "alias edit_kafka_server='vim /opt/kafka/config/server.properties'" >> ~/.bashrc
echo "export PATH=$PATH:'~/bin'" >> ~/.bashrc
echo "alias ks='start_kafkaserver'" >> ~/.bashrc
echo "alias rks='sudo pkill -f kafka'" >> ~/.bashrc
echo "alias publicip='dig +short myip.opendns.com @resolver1.opendns.com'" >> ~/.bashrc

mkdir -p ~/bin
touch ~/bin/start_kafkaserver
echo "# /usr/bin bash" >> bin/start_kafkaserver
echo "nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &" >> ~/bin/start_kafkaserver
echo "# /usr/bin bash" >> ~/bin/list_topics
echo "$KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper localhost:2181" >> ~/bin/list_topics
chmod +x ~/bin/list_topics
chmod +x ~/bin/start_kafkaserver
source ~/.bashrc

