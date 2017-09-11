#!/bin/bash
#author:ranzechen
#function:依次获取hdfs上/data/shoudan/日期/每个流水目录中的文件----规则:先取流水中以ACOMN结尾文件，没有的话再取以ACOM结尾的文件，在没有的话取以ACOMA结尾的文件

#time=`date -d "2 day ago" +"%Y%m%d"`

if [ $# != 1 ] ; then 
	echo "USAGE: sh acomn_acom_path.sh date"  
	exit 1
fi 

time=$1
hdfs_path="hdfs://175.10.100.14:8020"

dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
for i in $dir_path
do
	file_path_acomn=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep "ACOMN$" | grep "IND"`
        if [ $? -ne 0 ];then
			file_path_acom=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep  "ACOM$" | grep "IND"`
			if [ $? -ne 0 ];then
	               echo "not find *ACOMN *ACOM path in $i"
			fi
		fi

	if [ -n "$file_path_acomn" ];then
		for acomn in $file_path_acomn
		do
			jigouhao=`echo $acomn | awk -F '/' '{print$5}'`
		        filename=`echo $acomn | awk -F '/' '{print$6}'`
			echo ">>>>>Spark acomn app"
			echo ">>>>>data source:"$hdfs_path$acomn $time $jigouhao $filename
					
			/usr/local/spark/bin/spark-submit --executor-memory 8g --total-executor-cores 10 --driver-memory 8g --master spark://100.1.1.39:7077 --conf spark.network.timeout=3000 --class data.spark.batch.messageparse.app.parseMessageAcomApp /home/bigdata/ranzechen/message/data.spark.batch-0.0.1-SNAPSHOT.jar $hdfs_path$acomn $time $jigouhao $filename
			
			if [ $? -ne 0 ];then
				exit 0
			fi
		done
	fi

	if [ -n "$file_path_acom" ];then
		
		for acom in $file_path_acom
		do
			jigouhao=`echo $acom | awk -F '/' '{print$5}'`
                        filename=`echo $acom | awk -F '/' '{print$6}'`
			echo ">>>>>Spark acom app"
			echo ">>>>>data source:"$hdfs_path$acom $time $jigouhao $filename

			/usr/local/spark/bin/spark-submit --executor-memory 8g --total-executor-cores 10 --driver-memory 8g --master spark://100.1.1.39:7077 --conf spark.network.timeout=3000 --class data.spark.batch.messageparse.app.parseMessageAcomApp /home/bigdata/ranzechen/message/data.spark.batch-0.0.1-SNAPSHOT.jar $hdfs_path$acom $time $jigouhao $filename
			
			if [ $? -ne 0 ];then
                   		 exit 0
                	fi
		done
	fi
done
