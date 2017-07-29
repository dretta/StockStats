import os
import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from urllib.error import HTTPError

try:
    from googlefinance import getQuotes
    from kafka import KafkaProducer, KafkaConsumer, TopicPartition, KafkaClient
    from kafka.errors import KafkaError, KafkaTimeoutError, KafkaUnavailableError
    from pyspark import SparkContext
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
    from pyspark.sql import SparkSession, DataFrame
    from kafka.consumer.fetcher import ConsumerRecord
    from cassandra import util, cluster, protocol
except ImportError as ie:
    print("Missing module named {}".format(ie.msg.split()[-1]))


def getStocks(producer, symbols):
    """
    Gets stock data via googlefinance API for each symbol
    @param producer: KafkaProducer Object
    @param symbols: The symbols of the stocks to retrieve fata about
    @return List of successful results
    """      

    try:
        prices = getQuotes(symbols)
    except HTTPError as httpe:
        if httpe.code == 400:
            raise Exception("Bad Symbol input")
        else:
            raise Exception("Bad Http Connetion to googlefinance API")
    except Exception as e:
        raise Exception("Failed to obtain stock data:\n{0}".format(str(e)))

    results = []
    for price in prices:
        symbol = price['StockSymbol']
        try:
            results.append(producer.send(topic = topic_name, value = json.dumps(price)))
        except (KafkaError, KafkaTimeoutError) as ke:
            print("Failed to send stock data to KafkaProducer for symbol {0}:\n{1}".format(symbol,str(ke)))
        except Exception as e:
            print("Failed to obtain stock data for symbol:\n{0}".format(symbol, str(e)))
    return results


if __name__ == '__main__':
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell'
    
    parser = argparse.ArgumentParser()
    parser.add_argument('symbols', type=str, nargs='+', help = 'the symbols of the stocks')
    parser.add_argument('-s', '--skip-service-checks', action='store_true', help='Skip checking to see if the Kafka and Cassandra services are running')
    parser.add_argument('-k', '--keyspace', type=str, default='ks', help='The name of the keyspace used for Cassandra, default is "ks"')
    parser.add_argument('-t', '--table', type=str, default='stocks', help='The name of the table used for Cassandra, default is "stocks"')
    parser.add_argument('-b', '--brokers', type=str, nargs='+', default=["localhost:9092"], help='The brokers used for Kafka')
    parser.add_argument('-g', '--group', type=str, default='my-group', help='The group name used for all kafka brokers')
    parser.add_argument('-n', '--topic-name', type=str, default='stock-data', help='The name of topic used for kafka messaging')

    # Get Command Line arugments
    args = parser.parse_args()
    symbols = args.symbols
    skip_service = args.skip_service_checks
    keyspace = args.keyspace
    table = args.table
    kafka_broker = args.brokers
    group = args.group
    topic_name = args.topic_name

    #Check to see if all the required services are running
    if not skip_service: 
        try:
            client = KafkaClient(kafka_broker)
            client.close()        
        except KafkaUnavailableError as kue:
            raise Exception("No Kafka Service Found")

        try:
            c = cluster.Cluster(connect_timeout=30)
            session = c.connect() 
            session.shutdown()
        except cluster.NoHostAvailable as nha:
            raise Exception("No Cassandra Service Found")
        except Exception:
            pass

    # Create KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id=group, enable_auto_commit=False)
    tp = TopicPartition(topic_name, 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)
    print("Initial Offset is {}".format(last_offset))

    # Create KafkaProducer
    producer = KafkaProducer(bootstrap_servers = kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Start Spark Streaming
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 5)

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic_name], {'metadata.broker.list': kafka_broker[0]})

    parsers = directKafkaStream.map(lambda v: v)
    parsers.pprint()

    ssc.start()
    result = getStocks(producer, symbols)
    ssc.stop(stopSparkContext=False, stopGraceFully=True)

    if not result:
        raise Exception("All Stock Symbols failed to write to Kafka")

    # Collect message count in topic
    countDF = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker[0]) \
        .option("startingOffsets", "earliest") \
        .option("subscribe", topic_name) \
        .load()
    print("New Stock Record Size: {0}".format(countDF.count()))

    producer.flush(10)
    producer.close()

    # Send Data to Cassandra
    try:
        cassandra_cluster = cluster.Cluster(connect_timeout=30)
    except OperationTimedOut as ope:
        raise Exception("Cassandra connection took too long")

    session = cassandra_cluster.connect()
    keyspace_statement = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true" % (keyspace)    
    try:
        session.execute(keyspace_statement)
    except protocol.SyntaxException as se:
        raise Exception("Do not use keywords for keyspace name")
    session.set_keyspace(keyspace)

    table_statement = "CREATE TABLE IF NOT EXISTS {0} (symbol text, last_trade_date_time timestamp, change_percent double, change_price double, last_close_price double, last_trade_price double, last_trade_size int, stock_index text, PRIMARY KEY (symbol, last_trade_date_time)) WITH CLUSTERING ORDER BY (last_trade_date_time DESC)".format(table)
    try:
        session.execute(table_statement)
    except protocol.SyntaxException as se:
        raise Exception("Do not use keywords for table name")
    table_columns = session.execute("SELECT * FROM {}".format(table)).column_names
    for required_column in ['symbol', 'last_trade_date_time', 'change_percent', 'change_price', 'last_close_price', 'last_trade_price', 'last_trade_size', 'stock_index']:
        if required_column not in table_columns:
            print("Column Mismatch, Remaking table")
            session.execute("DROP TABLE {0}".format(table))
            session.execute(table_statement)
            break

    records = consumer.poll(sys.maxsize)
    for record in records[tp]:
        decoded_str = record.value.decode('utf-8')
        stock = json.loads(json.loads(decoded_str))
        statement = "INSERT INTO {0} (symbol, last_trade_date_time, change_price, change_percent, last_close_price, last_trade_price, last_trade_size, stock_index) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) USING TIMESTAMP %s".format(table)
        lastTradeDateTime = datetime.strptime(stock['LastTradeDateTime'], "%Y-%m-%dT%H:%M:%SZ")
        change = float(stock['Change'])
        changePercent = float(stock['ChangePercent'])
        lastClosePrice = float(stock['PreviousClosePrice'])
        lastTradePrice = float(stock['LastTradePrice'])
        print("change({0} -> {1}), changePercent({2} -> {3}), lastClosePrice({4} -> {5}), lastTradePrice({6} -> {7})".format(stock['Change'], change, stock['ChangePercent'], changePercent,  stock['PreviousClosePrice'], lastClosePrice, stock['LastTradePrice'], lastTradePrice))
        lastTradeSize = int(stock['LastTradeSize'])
        epochTime = int((lastTradeDateTime - datetime.utcfromtimestamp(0)).total_seconds() * 1000000)
        session.execute(statement, [stock['StockSymbol'], lastTradeDateTime, change, changePercent, lastClosePrice, lastTradePrice, lastTradeSize, stock['Index'], epochTime])
    
    consumer.close()