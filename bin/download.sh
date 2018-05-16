cd $PROJECT_HOME
git pull origin ${1}
cd postgreslib
git pull origin ${1}
cd ..
cd consumer
git pull origin ${1}
cd ..
cd producer
git pull origin ${1}
cd ..
cd postgreslib
git pull origin ${1}
cd ..
