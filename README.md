### Projeto Airbnb usando mongoDB

Esse projeto é desenvolvido como entrega final da primeira unidade da aula de "Camadas e Serviços de Consumo de Dados" da pós graduação em Engenharia de dados da Puc Minas.

A atividade proposta por nós, alunos, é consumir a base Sample_airbnb disponível no mongoDB Server, persistindo os dados como arquivo Json em um local que seria considerado como nossa zona raw. Após isso, os dados são consumidos novamente, estruturando a infomação do Json em uma tabela e populando em um banco mysql. E por fim, disponibilizando em uma tabela de consumo, considerada como nossa refined, onde disponibilizamos apenas os dados necessários para os KPIs propostos, levantando, os scores das revisões de casas do airbnb.

Toda a arquitetura será feita em containers dockers, construídos pelo docker-compose.yml, a orquestração dos pipelines será feita pela imagem docker do airflow e disponibilizadas em um container do mysql. A informação publicada na tabela trusted, será consumida por um dashboard PowerBI.

Para rodar o programa que sobe todos os containers:

* Deve-se rodar o arquivo "run_application.bat". Esse arquivo é o responsável por subir todos os containers (airflow e mysql database), e cria uma rede chamada mysql, onde ela é a rede usada para comunicação entre os containers.
* O arquivo run_application irá subir os containers, criar a rede mysql e após isso, irá rodar o "creating_network.ps1", que é um arquivo de execução do powershell, que irá alocar os containers existentes do airflow e mysql na rede.
* Após isso, a execução deve ser feita pela interface web do airflow.