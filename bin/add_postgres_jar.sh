# /usr/env/bin bash
mkdir -p ~/Downloads
cd ~/Downloads
wget https://jdbc.postgresql.org/download/postgresql-42.2.2.jar -O postgresql-42.2.2.jar
sudo mv postgresql-42.2.2.jar $SPARK_HOME/jars/

