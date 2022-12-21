# Associar os containers do airflow e mysql a essa rede

echo "Associando mysql_db ao network"
docker network connect mysql mysql_db

echo "Associando containers airflow ao network"
docker container ls -aq --filter name=webserver-1
docker container ls -aq --filter name=scheduler-1
docker container ls -aq --filter name=triggerer-1
docker container ls -aq --filter name=postgres-1 