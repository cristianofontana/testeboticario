
# Airflow Cristiano

Clone o repositório e execute os seguintes comandos para o airflow ser iniciado em: localhost:8080

```sh
$ cd testeboticario
$ docker-compose up --build
```

### Estrutura do Projeto

Na pasta dags encontran-se todas as ETLs que são responsaveis pela ingestão das bases, modelagem, e extração de dados do twitter. 

dags -> ETLs
sql -> sql utilizado para criação das tabelas 
bases -> bases de dados utilizadas no teste

### Pontos de melhoria

No futuro pode-se: 
    * Armazenar as keys em um lugar seguro e criptografado.
    * Armazenar os dados raw em uma store na nuvem (s3, Cloud Store)
    * Fazer uso da ferramenta Cloud Composer em produção
