# cloudera-quickstart

https://www.ncei.noaa.gov/data/global-hourly/doc/
https://www.ncei.noaa.gov/data/global-hourly/archive/isd/

```bash
hadoop jar "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar" \
 -files "mapper.py,reducer.py" \
 -mapper mapper.py \
 -reducer reducer.py \
 -input "/user/cloudera/data/*" \
 -output "/user/cloudera/$(date '+%Y%m%d%H%M%S')"
```

```bash
# https://community.cloudera.com/t5/Support-Questions/How-can-I-resolve-java-net-BindException-Address-already-in/td-p/101002
pyspark --conf spark.ui.port=4041
```