# Analyse des anomalies : Identifiez les mois ou les années qui ont enregistré des températures 
# ou des niveaux d'humidité significativement différents de la moyenne. Cela peut aider à détecter 
# des événements météorologiques exceptionnels ou des phénomènes climatiques inhabituels.
import re

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import (avg, expr, stddev, desc, abs, col)


sc = SparkContext("local[*]", "tp1")
sqlContext = SQLContext(sc)

# Charger les fichiers de données à partir du chemin spécifié dans HDFS
data = sc.textFile("/user/cloudera/data")

# Valeur utilisée pour les enregistrements manquants
missing = "+9999"
# Codes acceptés pour la température et l'humidité
code = "[01459]"


rdd = data.map(lambda row: (
        row[15:23],     # date
        row[28:34],     # latitude
        row[34:41],     # longitude
        row[46:51],     # altitude
        row[87:92],     # temperature
        row[92:93],     # quality code
        row[93:98],     # humidity
        row[98:99],     # quality code
        )).filter(lambda row: (
            re.match(code, row[5]) and      # temperature quality code
            re.match(code, row[7]) and      # humidity quality code
            row[4] != missing and           # temperature
            row[6] != missing               # humidity
            )).map(lambda row: (
                row[0],         # date
                row[1],         # latitude
                row[2],         # longitude
                int(row[3]),    # altitude
                int(row[4]),    # temperature
                int(row[6])     # humidity
                ))


# Définition du schéma pour le DataFrame
schema = StructType([
    StructField("date", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("altitude", IntegerType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])


# Créer un DataFrame à partir de RDD en utilisant le schéma spécifié
df = sqlContext.createDataFrame(rdd, schema=schema)
# Sélectionner les colonnes pertinentes pour l'analyse
df = df.select("date", "temperature", "humidity")


# Ajouter une colonne "year" au DataFrame en extrayant les 4 premiers caractères de la colonne "date"
df = df.withColumn("year", expr("substring(date, 1, 4)").cast("integer"))
# Ajouter une colonne "month" au DataFrame en extrayant les caractères 5 et 6 de la colonne "date"
df = df.withColumn("month", expr("substring(date, 5, 2)").cast("integer"))


# Calculer la moyenne et l'écart-type de la température et de l'humidité par mois
monthly_stats = df.groupBy("year", "month") \
                 .agg(avg("temperature").alias("avg_temperature"),
                      avg("humidity").alias("avg_humidity"),
                      stddev("temperature").alias("stddev_temperature"),
                      stddev("humidity").alias("stddev_humidity"))


# Calculer les anomalies en comparant les valeurs aux moyennes +/- un seuil (par exemple, 2 fois l'écart-type)
threshold = 2  # Seuil pour définir ce qui est considéré comme une anomalie


# Calculer les limites supérieures et inférieures pour détecter les anomalies
monthly_stats = monthly_stats.withColumn("upper_temperature", col("avg_temperature") + threshold * col("stddev_temperature"))
monthly_stats = monthly_stats.withColumn("lower_temperature", col("avg_temperature") - threshold * col("stddev_temperature"))
monthly_stats = monthly_stats.withColumn("upper_humidity", col("avg_humidity") + threshold * col("stddev_humidity"))
monthly_stats = monthly_stats.withColumn("lower_humidity", col("avg_humidity") - threshold * col("stddev_humidity"))


# Identifier les mois ou les années avec des anomalies de température ou d'humidité
anomalies = monthly_stats.filter(
                (abs(col("avg_temperature") - col("upper_temperature")) > threshold * col("stddev_temperature")) |
                (abs(col("avg_temperature") - col("lower_temperature")) > threshold * col("stddev_temperature")) |
                (abs(col("avg_humidity") - col("upper_humidity")) > threshold * col("stddev_humidity")) |
                (abs(col("avg_humidity") - col("lower_humidity")) > threshold * col("stddev_humidity"))
                ).orderBy("year", "month")


# Afficher les mois et les années avec des anomalies
anomalies.show()

# +----+-----+-------------------+-------------------+------------------+------------------+------------------+-------------------+------------------+-------------------+
# |year|month|    avg_temperature|       avg_humidity|stddev_temperature|   stddev_humidity| upper_temperature|  lower_temperature|    upper_humidity|     lower_humidity|
# +----+-----+-------------------+-------------------+------------------+------------------+------------------+-------------------+------------------+-------------------+
# |1928|    1|  13.71078431372549| -6.313725490196078|   43.166549991909| 42.78096442109682|100.04388429754349| -72.62231567009252| 79.24820335199756| -91.87565433238971|
# |1928|    3|  36.13907284768212| -5.403973509933775|61.003829359049185|43.817223220699724| 158.1467315657805| -85.86858587041624| 82.23047293146567| -93.03841995133322|
# |1928|    6| 152.08247422680412|  94.72680412371135| 65.67791879833496| 45.51595521760211|  283.438311823474|  20.72663663013421|185.75871455891559|  3.694893688507122|
# |1928|    7| 193.28508771929825| 118.30701754385964|  71.2080302015897| 37.78332084130572|335.70114812247766|  50.86902731611886| 193.8736592264711|   42.7403758612482|
# |1928|   11|   52.0296052631579| 30.674342105263158| 46.27093170269806| 44.82075632545203|144.57146866855402| -40.51225814223822|120.31585475616723|-58.967170545640904|
# |1929|    1| -47.34228187919463| -67.15883668903803| 51.03618321810454| 53.50763162942145| 54.73008455701445|-149.41464831540372| 39.85642656980487| -174.1740999478809|
# |1929|    2|-102.96587926509186|-132.29921259842519|   82.147885843162| 78.20923168290939|61.329892421232145|-267.26165095141585|24.119250767393595|  -288.717675964244|
# |1930|    1|  67.44589585172109|  44.60105913503972| 39.51391549179456|  37.4046820463833|146.47372683531023|-11.581935131868036|119.41042322780632| -30.20830495772688|
# |1930|    2| 48.894940880015504|   20.7197131226982|51.962548511785464|50.306360203499885|152.82003790358644| -55.03015614355542|121.33243352969797| -79.89300728430158|
# |1930|    4| 102.15065666041276|  72.93245778611632|59.147207862395206| 60.83955020981194|220.44507238520316|-16.143759064377647| 194.6115582057402| -48.74664263350756|
# |1930|    5|  128.8082788671024|  95.91358024691358| 61.00286743429984| 60.13163766763926|250.81401373570208|  6.802543998502713| 216.1768555821921| -24.34969508836494|
# |1930|   10| 124.14434601354873|  95.43546986277575|58.132802253006346| 52.72054717089215|240.40995051956142|  7.878741507536034|200.87656420456005|-10.005624479008546|
# |1931|    1| 34.156363636363636| 12.986285714285714| 58.71831194084857| 57.41401481362679| 151.5929875180608| -83.28026024533351| 127.8143153415393|-101.84174391296787|
# |1931|    2|  28.44598400360482|  7.072547031654838|63.049571466185206| 62.26887981478369|154.54512693597525| -97.65315892876559| 131.6103066612222|-117.46521259791254|
# |1931|    5|  141.0571884684008|    96.643065076533| 59.35217208937177| 54.19592407688352|259.76153264714435| 22.352844289657256|205.03491323030005|-11.748783077234037|
# |1931|    6| 158.74386998669453| 115.49030602547045| 52.10191654071362| 48.87226601304609|262.94770306812177|   54.5400369052673|213.23483805156263| 17.745773999378272|
# |1931|    7|  172.6622586617297|  130.8734902583091| 52.77789649127588|  44.1306725907862|278.21805164428145|  67.10646567917794| 219.1348354398815| 42.612145076736695|
# |1931|    8|  163.4691705514997| 124.82689770428358| 55.04232860670162| 49.34186789532698|273.55382776490296| 53.384513338096454|223.51063349493754| 26.143161913629626|
# |1931|    9| 127.65869781762203|  95.37779588879833| 56.61970217940485| 54.71436246067046|240.89810217643173| 14.419293458812334|204.80652081013926|-14.050929032542584|
# |1931|   11|  72.16273736354462| 47.888634070268786|60.366915800752494| 56.17548585611666| 192.8965689650496| -48.57109423796037| 160.2396057825021| -64.46233764196452|
# +----+-----+-------------------+-------------------+------------------+------------------+------------------+-------------------+------------------+-------------------+
