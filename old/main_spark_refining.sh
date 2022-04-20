echo "Begining of Main Spark"
echo $PYSPARK_PYTHON
echo $SPARK_HOME
echo $PYTHONPATH
echo "Technical environment configuration file: $1"
echo "Scope : $2"

technical_conf_file="./conf/$1.yml"
echo "The technical configuration file is $technical_conf_file"

sudo pip-3.6 install -r requirements.txt
zip -x main_data_apo_refining.py -x \*libs\* -r main_data_apo_refining_src.zip ./src/

spark-submit --deploy-mode client --master yarn \
	--conf 'spark.executorEnv.PYTHONPATH=/usr/lib/spark/python/lib/py4j-src.zip:/usr/lib/spark/python/:<CPS>{{PWD}}/pyspark.zip<CPS>{{PWD}}/py4j-src.zip' \
	--conf spark.yarn.isPython=true \
	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	--py-files main_data_apo_refining_src.zip \
	main_data_apo_refining.py -c $technical_conf_file -l ./conf/loggin-console.json -s $2

echo  $? > code_status

echo "End of Main Spark"
