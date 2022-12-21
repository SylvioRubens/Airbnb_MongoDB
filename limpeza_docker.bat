echo "Parando todos os containers"
Powershell.exe -executionpolicy remotesigned -Command "docker container stop $(docker container ls -q)"

echo "Limpando variáveis docker"
Powershell.exe -executionpolicy remotesigned -Command "docker system prune"

timeout /t 10