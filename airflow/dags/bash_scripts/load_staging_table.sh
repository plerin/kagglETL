spark-submit --driver-class-path $SPARK_HOME/jars/redshift-jdbc42-2.1.0.1.jar \
--jars $SPARK_HOME/jars/redshift-jdbc42-2.1.0.1.jar \
$AIRFLOW_HOME/dags/python_scripts/{{params.python_script}} {{ params.s3_bucket }} {{ params.s3_key }} \
{{ params.aws_key }} {{ params.aws_secret_key }} {{ params.redshift_conn_string }} \
{{ params.db_user }} {{params.db_pass}} --conf "fs.s3a.multipart.size=1048576"