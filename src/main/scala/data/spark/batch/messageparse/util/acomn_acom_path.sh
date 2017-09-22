#!/bin/bash
#author:ranzechen
#function:依次获取hdfs上/data/shoudan/日期/每个流水目录中的文件----规则:先取流水中以ACOMN结尾文件，没有的话再取以ACOM结尾的文件，在没有的话取以ACOMA结尾的文件

if [ $# != 1 ] ; then 
	time=`date -d "2 day ago" +"%Y%m%d"`
else
	time=$1
fi

hdfs_path="hdfs://100.1.1.39:8020"

dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
for i in $dir_path
do
	echo "reading dirname is :"$i
	file_path_alfee=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep "ALFEE$"`
	file_path_acomn=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep "ACOMN$" | grep "IND"`
        if [ $? -eq 0 ];then
			for acomn in $file_path_acomn
			do
				if [ -n "$acomn" ];then
					jigouhao=`echo $acomn | awk -F '/' '{print$5}'`
					filename=`echo $acomn | awk -F '/' '{print$6}'`
					echo ">>>>>Spark acomn app"
					echo ">>>>>data source:"$hdfs_path$acomn $time $jigouhao $filename $file_path_alfee
					
					#/usr/local/spark/bin/spark-submit  --num-executors 4 --executor-memory 8g --executor-cores 4 --driver-memory 8g --master spark://100.1.1.39:7077 --conf spark.network.timeout=3000 --class data.spark.batch.messageparse.app.parseMessageAcomApp /home/bigdata/ranzechen/message/data.spark.batch-0.0.1-SNAPSHOT.jar $hdfs_path$acomn $time $jigouhao $filename $file_path_alfee ./config.json
		
				fi
			done
		else
		file_path_acom=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep  "ACOM$" | grep "IND"`
			if [ $? -eq 0 ];then
				for acom in $file_path_acom
				do
					if [ -n "$acom" ];then
						jigouhao=`echo $acom | awk -F '/' '{print$5}'`
       		        	filename=`echo $acom | awk -F '/' '{print$6}'`
						echo ">>>>>Spark acom app"
						echo ">>>>>data source:"$hdfs_path$acom $time $jigouhao $filename $file_path_alfee
	
						#/usr/local/spark/bin/spark-submit  --num-executors 4 --executor-memory 8g --executor-cores 4 --driver-memory 8g --master spark://100.1.1.39:7077 --conf spark.network.timeout=3000 --class data.spark.batch.messageparse.app.parseMessageAcomApp /home/bigdata/ranzechen/message/data.spark.batch-0.0.1-SNAPSHOT.jar $hdfs_path$acom $time $jigouhao $filename $file_path_alfee ./config.json
		
					fi
				done
		      
			else 
				echo "not find *ACOMN *ACOM path in $i"
			fi
		fi


done



