ES=/Volumes/Data/elastic-testing/elasticsearch-0.19.11
#ES=/usr/share/elasticsearch
sudo $ES/bin/plugin remove reindex
mvn -DskipTests clean package
FILE=`ls ./target/elasticsearch-*zip`
sudo $ES/bin/plugin -url file:$FILE -install reindex

/Volumes/Data/elastic-testing/elasticsearch-0.19.11/bin/elasticsearch
#sudo service elasticsearch restart
