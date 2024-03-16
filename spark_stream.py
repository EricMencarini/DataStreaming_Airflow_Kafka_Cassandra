###############################################################################################
#Arquivo responsável por gerar a conexão do spark com o Cassandra                             #
###############################################################################################

import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

'''
Faz a criação do Keyspace(Similar a um Schema)
'''
def cria_keyspace(session):
    session.execute("""
         CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("KeySpace criado com sucesso!")

'''
Faz a criação da tabela dentro do cassandra.
'''
def cria_tabela(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        Id UUID PRIMARY KEY,
        Nome TEXT,
        Sobrenome TEXT,
        Gênero TEXT,
        Endereço TEXT,
        CEP TEXT,
        Email TEXT,
        Usuário TEXT,
        Data TEXT,
        Telefone TEXT,
        Foto TEXT);
    """)

    print("Tabela criada com sucesso!")

'''
Insere os dados dos usuários na tabela do Cassandra.
'''
def insere_dados(session, **kwargs):
    print("Fazendo upload dos dados...")

    user_id = kwargs.get('Id')
    first_name = kwargs.get('Nome')
    last_name = kwargs.get('Sobrenome')
    gender = kwargs.get('Gênero')
    address = kwargs.get('Endereço')
    postcode = kwargs.get('CEP')
    email = kwargs.get('Email')
    username = kwargs.get('Usuário')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('Data')
    phone = kwargs.get('Telefone')
    picture = kwargs.get('Foto')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(Id, Nome, Sobrenome, Gênero, Endereço, 
                CEP0, Email, Usuário, dob, Data, Telefone, Foto)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Dados inseridos para {first_name} {last_name}")

    except Exception as e:
        logging.error(f'Não foi possível inserir os dados devido a :{e}')

'''
Cria uma conexão com o Spark.
'''
def cria_conexao_spark():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("Erro ao se conectar")
        logging.info("Conexão com o spark realizada com sucesso.")
    except Exception as e:
        logging.error(f"Não foi possível se conectar a uma sessão do spark {e}")

    return s_conn

'''
Cria uma conexão com o Kafka usando o Spark.
'''
def cria_conexao_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Dataframe do Kafka criado com sucesso")
    except Exception as e:
        logging.warning(f"Dataframe do kafka não pode ser criado devido a : {e}")

    return spark_df

'''
Cria uma conexão com o Cassandra.
'''
def cria_conexao_cassandra():
    try:
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Não foi possível estabelecer uma conexão com o cassandra devido a: {e}")
        return None

'''
Cria um DataFrame do Spark a partir dos dados recebidos do Kafka.
'''
def cria_df_kafka(spark_df):
    schema = StructType([
        StructField("Id", StringType(), False),
        StructField("Nome", StringType(), False),
        StructField("Sobrenome", StringType(), False),
        StructField("Gênero", StringType(), False),
        StructField("Endereço", StringType(), False),
        StructField("CEP", StringType(), False),
        StructField("Email", StringType(), False),
        StructField("Usuário", StringType(), False),
        StructField("Data", StringType(), False),
        StructField("Telefone", StringType(), False),
        StructField("Foto", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


'''
Inicializa conexões com o Spark, Kafka e Cassandra, cria um streaming de dados do Kafka para o Cassandra via Spark, e aguarda a conclusão do streaming.
'''
if __name__ == "__main__":
    # Inicializa a conexão do spark
    spark_conn = cria_conexao_spark()

    if spark_conn is not None:
        spark_df = cria_conexao_kafka(spark_conn)
        selection_df = cria_df_kafka(spark_df)
        session = cria_conexao_cassandra()

        if session is not None:
            cria_keyspace(session)
            cria_tabela(session)

            logging.info("Inicializando o streaming...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()