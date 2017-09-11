#!/bin/bash
#author:ranzechen
#function:依次获取hdfs上/data/shoudan/日期/每个流水目录中的文件----规则:先取流水中以AERRN结尾文件，没有的话再取以AERR结尾的文件

#time=`date -d "2 day ago" +"%Y%m%d"`

if [ $# != 1 ] ; then
	echo "USAGE: sh aerrn_aerr_path.sh date"
	exit 1
fi

time=$1
hdfs_path="hdfs://100.1.1.39:8020"

dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
for i in $dir_path
do
	file_path_aerrn=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep "AERRN$" | grep "IND"`
        if [ $? -ne 0 ];then
			file_path_aerr=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep  "AERR$" | grep "IND"`
			if [ $? -ne 0 ];then
				echo "not find *AERRN *AERR path in $i"
			fi
		fi

	if [ -n "$file_path_aerrn" ];then
		for aerrn in $file_path_aerrn
		do
				jigouhao=`echo $aerrn | awk -F '/' '{print$5}'`
				filename=`echo $aerrn | awk -F '/' '{print$6}'`
				echo ">>>>>Spark aerrn app"
				echo ">>>>>data source:"$hdfs_path$aerrn $time $jigouhao $filename

				/usr/local/spark/bin/spark-submit  --executor-memory 8g --total-executor-cores 10 --driver-memory 8g --master spark://100.1.1.39:7077 --conf spark.network.timeout=3000 --class data.spark.batch.messageparse.app.parseMessageErrApp /home/bigdata/ranzechen/message/data.spark.batch-0.0.1-SNAPSHOT.jar $hdfs_path$aerrn $time $jigouhao $filename

				if [ $? -ne 0 ];then
					exit 0
				fi
		done
	fi

	if [ -n "$file_path_aerr" ];then
		echo ">>>>>Spark aerr app"
		for aerr in $file_path_aerr
		do
			jigouhao=`echo $aerr | awk -F '/' '{print$5}'`
			filename=`echo $aerr | awk -F '/' '{print$6}'`
			echo ">>>>>data source:"$hdfs_path$aerr $time $jigouhao $filename

			/usr/local/spark/bin/spark-submit --executor-memory 8g --total-executor-cores 10 --driver-memory 8g --master spark://100.1.1.39:7077 --conf spark.network.timeout=3000 --class data.spark.batch.messageparse.app.parseMessageErrApp /home/bigdata/ranzechen/message/data.spark.batch-0.0.1-SNAPSHOT.jar $hdfs_path$aerr $time $jigouhao $filename

			if [ $? -ne 0 ];then
                   		 exit 0
                	fi
		done
	fi
done