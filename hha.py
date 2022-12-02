from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import date, datetime
from pyspark.sql.functions import udf, when
from pyspark.sql import functions as F
from pyspark.sql.types import *
from functools import reduce

import threading
import configparser
import time
import os 
import sys
import ipaddress
import logging
import re
import consul

os.environ["HADOOP_USER_NAME"] = "hadoop"
IpToNetwork = udf(lambda ip_int: str(int(ipaddress.IPv4Network(str(ipaddress.IPv4Address(ip_int))+"/24", strict=False).network_address))) 

# udf_IpNetwork = udf(lambda ip_int: str(ipaddress.IPv4Address(ip_int)), StringType()) 

# ******************************************************************************************************************
class consulData ():
    def __init__(self, host, port, path):
        self.zonelist = set()
        self.consul_dir = path
        self.consul_host = host
        self.consul_port = port
        os.environ["CONSUL_HTTP_ADDR"] = self.consul_host+":"+str(self.consul_port)
        self.consul_conn = consul.Consul(host=self.consul_host, port=self.consul_port)


# ******************************************************************************************************************        
    def get_zones(self):
        return self.zonelist


# ******************************************************************************************************************
    def fill_zones (self):
        """
        Заполняет set списком ip/net из consul за которыми необходимо следить
        """
        while True:
            prefixs = list()
            try:
                consul_conn = consul.Consul(host=self.consul_host, port=self.consul_port)
                consul_kvs = consul_conn.kv.get(key=self.consul_dir, recurse=True)
                old_zonelist_len = len(self.zonelist)
                self.zonelist.clear()                
            except:
                logging.error(f"Error: Try get consul keys. Check access to Consul server and check exist path {self.consul_dir}")
                sys.exit(0)

            for keys in consul_kvs[1]:
                if keys['Value'] != None:
                    prefixs += str((keys['Value']).decode('utf-8')).split("\n")
            for prefix in prefixs:
                comment_str = re.match(r'#', prefix) # Поиск закомментированных строк
                if comment_str is None:
                    try:
                        ip_to_int = int(ipaddress.IPv4Address(prefix))
                        self.zonelist.add(ip_to_int)
                    except:
                        logging.warning(f"in zones wrong format ip {prefix}")
            zonelist_len = len(self.zonelist)                        
            if zonelist_len != old_zonelist_len:
                res_len = zonelist_len - old_zonelist_len
                logging.info(f"Change count client prefix on {res_len}. Total: {zonelist_len}")
            time.sleep(300)
# ******************************************************************************************************************


# *******************************************************************************************************
# Read config
# *******************************************************************************************************
class cfg ():
    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        config = configparser.ConfigParser()
        params = config.read(f"{dir_path}/hha.conf")
        if len(params) == 0:
            logging.error('Error: Not found config file histogramm.conf. Stop programm.')
            sys.exit(0)
        self.hdfs_host = str(config.get('hdfs','host'))
        self.hdfs_port = config.get('hdfs','port')
        self.hdfs_file_dir = config.get('hdfs','file_dir')
        self.hdfsusername = config.get('hdfs','hdfsusername')
        self.spark_host = config.get('spark','spark_host')
        self.spark_port = config.get('spark','spark_port')
        self.cluster_gbmemmory = config.get('spark','cluster_gbmemmory')
        self.cluster_countproc = config.get('spark','cluster_countproc')
        self.app_name = config.get('spark','app_name')
        log_dir = config.get('logs','log_dir')
        log_file_name = config.get('logs','log_file_name')
        self.log_path = log_dir+'/'+log_file_name
        self.sleepInterval = int(config.get('other','sleepInterval'))
        self.LimitNewData = int(config.get('other','LimitNewData'))
        self.LimitNewDataNet = int(config.get('other','LimitNewDataNet'))
        self.quotientAmplification = int(config.get('other','quotientAmplification'))
        self.LimitDetectTimeSec = int(config.get('other','LimitDetectTimeSec'))
        self.consul_host = config.get('consul','consul_host')
        self.consul_port = config.get('consul','consul_port')
        self.consul_dir = config.get('consul','consul_dir')
# ******************************************************************************************************************
        

class hha():
    def __init__(self):
        self.startTime = 0
        self.endTime = 0
        self.prev_starttime = 0
        self.prev_endtime = 0
        self.GlobalRowList = {}
        self.hfw = HadoopFileWorker ()
    
# ******************************************************************************************************************    
    def recreateDF(self, origDF):
        """ Пересоздаем датафрейм. Операция медленная. Но Если этого не делать,то работая с датафреймами 
            pyspark  делает операцию readparquet, что еще медленнее
            После пересоздания датафрейма pyspark  делает операцию readExistRDD
        Args:
            origDF (DataFrame): pyspark DataFrame
        Returns:
            Dataframe: pyspark DataFrame
        """
        return self.hfw.spark.createDataFrame(origDF.rdd, origDF.schema)


# ******************************************************************************************************************
    def FiltrDataByInterval(self):
        """ Выборка/Фильтр данных различных протоколов за два промежутка времени(текущий и некоторое время назад), для последуещего анализа 
        Args:
            protocolData (DataFrame): DataFrame из файла (paruqet)
        """
        protocolData = self.hfw.ReadData("level_",1)
        if protocolData == False:
            return 0

        short_cur_df = protocolData.filter( (protocolData.timestamp > self.startTime)) \
            .select( F.col("timestamp"),  F.col("subagent_id"), F.col("num_protocol"), F.col("CountPkt"), F.col("type_proto"), F.col("dst_ip")) 
        cur_df = short_cur_df.groupBy("num_protocol", "type_proto", "dst_ip").agg(F.avg("CountPkt").cast(IntegerType()).alias("sum_val") ) \
                    .select( F.col("num_protocol"),  F.col("type_proto"), F.col("sum_val"), F.col("dst_ip"))
        protocolData.unpersist()

        protocolData = self.hfw.ReadData("level_",2)
        short_prev_df = protocolData.filter( (protocolData.timestamp < self.prev_starttime) )
        prev_df = short_prev_df.groupBy(["num_protocol", "type_proto", "dst_ip"]).agg(F.avg("CountPkt").cast(IntegerType()).alias("sum_val") ) \
            .select( F.col("num_protocol").alias("prev_num_protocol"),  F.col("type_proto").alias("prev_type_proto"), F.col("sum_val").alias("prev_sum_val"), F.col("dst_ip").alias("prev_dst_ip")) 
        protocolData.unpersist()
        

        start_time = time.time()
        cur_df = self.recreateDF(cur_df)
        prev_df = self.recreateDF(prev_df)
        print("--- recreate DF %s seconds ---" % int(time.time() - start_time))       


        cur_df_net = cur_df.select(cur_df.num_protocol, cur_df.type_proto, cur_df.sum_val, cur_df.dst_ip) \
                    .withColumn("dst_net",IpToNetwork(F.col("dst_ip")) ) \
                    .groupBy("num_protocol", "type_proto", "dst_net").agg(F.avg("sum_val").cast(IntegerType()).alias("sum_val") ) \
                    .select( F.col("num_protocol"),  F.col("type_proto"), F.col("sum_val"), F.col("dst_net"))

        prev_df_net = prev_df.select(prev_df.prev_num_protocol,  prev_df.prev_type_proto, prev_df.prev_sum_val, prev_df.prev_dst_ip) \
                    .withColumn("prev_dst_net",IpToNetwork(prev_df.prev_dst_ip) ) \
                    .groupBy(["prev_num_protocol", "prev_type_proto", "prev_dst_net"]).agg(F.avg("prev_sum_val").cast(IntegerType()).alias("prev_sum_val") ) \
            .select( F.col("prev_num_protocol").alias("prev_num_protocol"),  F.col("prev_type_proto").alias("prev_type_proto"), F.col("prev_sum_val").alias("prev_sum_val"), F.col("prev_dst_net").alias("prev_dst_net")) 
            
        JoinData_net = cur_df_net.join(prev_df_net, (cur_df_net.num_protocol == prev_df_net.prev_num_protocol) & (cur_df_net.type_proto == prev_df_net.prev_type_proto) & (cur_df_net.dst_net == prev_df_net.prev_dst_net), "left") \
                    .select(
                            cur_df_net.num_protocol, cur_df_net.type_proto, cur_df_net.sum_val, cur_df_net.dst_net,
                            prev_df_net.prev_num_protocol.alias("prev_num_protocol"),
                            prev_df_net.prev_type_proto.alias("prev_type_proto"),
                            prev_df_net.prev_sum_val.alias("prev_sum_val"),
                            prev_df_net.prev_dst_net.alias("prev_dst_net"),
                            when((F.col("prev_sum_val")/F.col("sum_val")) > cfg_item.quotientAmplification, cfg_item.LimitNewDataNet) \
                            .otherwise( F.col("prev_sum_val")).alias("prev_sum_val2")
                    ).na.fill(cfg_item.LimitNewDataNet, ["prev_sum_val"]).na.fill(cfg_item.LimitNewDataNet, ["prev_sum_val2"]) 

                    


        JoinData = cur_df.join(prev_df, (cur_df.num_protocol == prev_df.prev_num_protocol) & (cur_df.type_proto == prev_df.prev_type_proto) & (cur_df.dst_ip == prev_df.prev_dst_ip), "left") \
                    .select(
                            cur_df.num_protocol, cur_df.type_proto, cur_df.sum_val, cur_df.dst_ip,
                            prev_df.prev_num_protocol.alias("prev_num_protocol"),
                            prev_df.prev_type_proto.alias("prev_type_proto"),
                            prev_df.prev_sum_val.alias("prev_sum_val"),
                            prev_df.prev_dst_ip.alias("prev_dst_ip"),
                            when((F.col("prev_sum_val")/F.col("sum_val") > cfg_item.quotientAmplification) & (F.col("prev_sum_val") > cfg_item.LimitNewData), cfg_item.LimitNewData) \
                            .otherwise( F.col("prev_sum_val") ).alias("prev_sum_val2")

                    ).na.fill(cfg_item.LimitNewData, ["prev_sum_val"]).na.fill(cfg_item.LimitNewData, ["prev_sum_val2"])
        
        
        
        amplificationData_net = JoinData_net.filter( ((JoinData_net.sum_val/JoinData_net.prev_sum_val2) > cfg_item.quotientAmplification)) \
                            .select(JoinData_net.num_protocol, JoinData_net.type_proto, JoinData_net.prev_sum_val2.alias("sum_val"), JoinData_net.dst_net.alias("dst_ip"))


        # amplificationData_net = JoinData_net.filter( (JoinData_net.type_proto.isNotNull() ) & ((JoinData_net.sum_val/JoinData_net.prev_sum_val2) > cfg_item.quotientAmplification)) \
        #                     .select(JoinData_net.num_protocol, JoinData_net.type_proto, JoinData_net.prev_sum_val2.alias("sum_val"), JoinData_net.dst_net.alias("dst_ip"))
        
        
        # amplificationData = JoinData.filter( (JoinData.type_proto.isNotNull() ) & ((JoinData.sum_val/JoinData.prev_sum_val2) > cfg_item.quotientAmplification)) \
        #                     .select(JoinData.num_protocol, JoinData.type_proto, JoinData.prev_sum_val2.alias("sum_val"), JoinData.dst_ip)

        amplificationData = JoinData.filter(((JoinData.sum_val/JoinData.prev_sum_val2) > cfg_item.quotientAmplification)) \
                            .select(JoinData.num_protocol, JoinData.type_proto, JoinData.prev_sum_val2.alias("sum_val"), JoinData.dst_ip)
        
        # dfs = [amplificationData,NewData,amplificationData_net]
        dfs = [amplificationData,amplificationData_net]
        abNormalData = reduce(DataFrame.unionAll, dfs)
        JoinData.unpersist()
        amplificationData.unpersist()
        amplificationData_net.unpersist()
        # abNormalData = amplificationData.union(NewData).union(amplificationData_net)
        self.attackAction(abNormalData)
  

# ******************************************************************************************************************
    def attackAction(self, AttackData):
        """ конвертируем DataFrame с всплесками в список и если ip/net находится под наблюдение (список ip/net из консула),
            то генерим правило
        Args:
            AttackData (DataFrame): Датафрэйм со списком ip/net, номером протокола, типом протокола по которым был 'всплеск' 
        """
        
        curTime = int(time.time())
        collectData = AttackData.collect()   
        AttackData.unpersist()
        for num_protocol, type_proto, prev_sum_val, dst_ip in collectData:
            isSet = self.GlobalRowList.get((num_protocol, type_proto, int(dst_ip)))
            if isSet == None:
                # Добавляем обнаруженные данные в словарь, ставим время обнаружения, чтобы исключить повторную обработку элемента
                if int(dst_ip) in cons_data.get_zones():
                    self.GlobalRowList[(num_protocol, type_proto, int(dst_ip))] = curTime
                    ip = str(ipaddress.IPv4Address(int(dst_ip)))
                    print (" *****  IP Generate Rule ->" , num_protocol, type_proto, ip,  prev_sum_val, dst_ip)
                    logging.info(f"Generate Rule for type protocol {type_proto} number protocol {num_protocol} ip = {ip}")
                    self.GenerateRule()
        # Удаляем обнаруженные элементы у которых время обнаружения больше LimitDetectTimeSec
        self.GlobalRowList = {key:value for key, value in self.GlobalRowList.items() if (curTime - value) < cfg_item.LimitDetectTimeSec}
        
        
# ******************************************************************************************************************        
    def GenerateRule(self):
        pass        

# ******************************************************************************************************************
class HadoopFileWorker ():
    def __init__(self):
        config = cfg_item
        self.hadoophost = config.spark_host
        self.hadoopport = config.spark_port
        self.hadoopdir = config.hdfs_file_dir
        # self.spark = SparkSession.builder.appName("Python Spark histogramm analyzer").getOrCreate()
        self.spark = SparkSession.builder\
            .master(f"spark://{config.spark_host}:{config.spark_port}")\
            .appName(f"{config.app_name}")\
            .config('spark.executor.memory', f"{config.cluster_gbmemmory}gb")\
            .config("spark.driver.memory", "15g") \
            .config("spark.cores.max", f"{config.cluster_countproc}")\
            .getOrCreate()
        self.spark.conf.set("spark.sql.adaptive.enabled",True)    
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled",True)
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",True)    
        self.sc = self.spark.sparkContext
        self.spark.sparkContext.setLogLevel('ERROR')
        URI           = self.sc._gateway.jvm.java.net.URI
        FileSystem    = self.sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        Configuration = self.sc._gateway.jvm.org.apache.hadoop.conf.Configuration
        self.fs = FileSystem.get(URI(f"hdfs://{config.hdfs_host}:{config.hdfs_port}"), Configuration())


# ******************************************************************************************************************
    def ReadData (self, prefix, numberFile):
        """ Чтение файла с hadoop кластера в формате parquet
        Имя файла состоит из prefix_сегодняшняя_дата_в_формате_unixtimestamp
        Args:
            prefix (string): приставка к имени файла
        Returns:
            DataFrame
        """
        InputPath = []
        start_time = time.time()
        today = date.today()
        cur_date = int(datetime.strptime(today.strftime("%d/%m/%Y"), "%d/%m/%Y").timestamp())
        filename = prefix+str(cur_date)


        for i in range(numberFile):
            secInHour = i * 3600    
            ts = int(time.time())
            round_hour = int(ts/3600) * 3600 - secInHour
            filename = prefix + str(round_hour)
            
            if self.fs.exists(self.spark._jvm.org.apache.hadoop.fs.Path(f"/{cfg_item.hdfs_file_dir}/{filename}")):
                readfilename = f"hdfs://{cfg_item.hdfs_host}:{cfg_item.hdfs_port}/{cfg_item.hdfs_file_dir}/{filename}"
                InputPath.append(readfilename)
        try:
            # parquetFile = self.sqlContext.read.parquet(f"hdfs://{cfg_item.hdfs_host}:{cfg_item.hdfs_port}/{cfg_item.hdfs_file_dir}/{filename}")
            parquetFile = self.spark.read.parquet(*InputPath)
        except BaseException as e:
            # print(e.args[0])
            # logging.error(e.args[0])
            # sys.exit(0)
            logging.warning("Can't read parquet file")
            time.sleep(10)
            return False
        print("--- Read data %s seconds ---" % int(time.time() - start_time))        
        return parquetFile

def main():
    ad = hha ()
    while True:
        print(datetime.now())
        start_time = time.time()
        ad.startTime = int(time.time()) - 90
        ad.endTime = ad.startTime + 60
        ad.prev_starttime = int(time.time())-300
        ad.prev_endtime = ad.prev_starttime + 60
        ad.FiltrDataByInterval()
        print("--- summary %s seconds ---" % int(time.time() - start_time))        
        print("******************************************************************\n")
        time.sleep(cfg_item.sleepInterval)


# ******************************************************************************************************************
if __name__ == "__main__":
    
    cfg_item = cfg()
    try:
        logging.basicConfig(filename=cfg_item.log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    except BaseException as e:
        logging.error(e)
        sys.exit(0)
    os.environ["HADOOP_USER_NAME"] = cfg_item.hdfsusername
    logging.info("Start monitoring DMA")
    cons_data = consulData(cfg_item.consul_host,cfg_item.consul_port, cfg_item.consul_dir)
    consul_thred = threading.Thread(target=cons_data.fill_zones)
    main_proc = threading.Thread(target=main)    
    consul_thred.start()
    main_proc.start()
    main_proc.join()
    consul_thred.join()

