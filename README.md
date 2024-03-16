## Sobre o Projeto

A intenção deste projeto foi de estudar e entender o funcionamento de uma construçõa de pipeline end-to-end.

O inicio dele é feito a partir da extração de dados de uma api, onde então utilizamos o airflow junto ao postgree 
para construir uma dag que irá enviar estes dados para nosso broker(kafka) em conjunto ao Zookeeper,
posteriormente a isto utilizamos o control-center e o schema registry para fazer a visualização da transferência destes dados via streaming 
e então fazemos uma conexão com o spark em uma arquitetura de master <-> worker e por fim fazendo o streaming destes dados ao Cassandra.

## Arquitetura.

![Streaming](https://github.com/EricMencarini/Data_Streaming/assets/133675044/d9636503-f90f-40f7-a6ec-05f3fbaf00d9)

## Ferramentas/Tecnologias

*Python
*Docker
*API(randomuser.me)
*Airflow
*Postgree
*Kafka em conjunto com o Zookeper
*Control Center
*Schema Registry
*Spark
*Cassandra

## Pré-requisitos.

* Python versão3.11+
* Pip
* Utilize o comando pip install -r requirements.txt para importação das biblioteca.
* Docker


## Notas.
* Agradecimentos ao Yusuf do @airscholar por boa parte dos ensinamentos ao construir o projeto.
