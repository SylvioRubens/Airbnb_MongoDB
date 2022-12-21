# Excluindo qualquer instância da rede mysql que existir
echo "Excluindo qualquer network mysql"
docker network rm mysql  

echo "Recriando network mysql"
# criar uma rede para comunicação dos containers mysql e airflow, chamada mysql
docker network create mysql 

echo "running docker compose for mysql database"
start cmd /k docker compose up

echo "running astro dev start for deploying the airflow aplication"
start cmd /k astro dev start

echo "Airflow and mysql deployed"

timeout /t 17

Powershell.exe -executionpolicy remotesigned -File  creating_network.ps1

