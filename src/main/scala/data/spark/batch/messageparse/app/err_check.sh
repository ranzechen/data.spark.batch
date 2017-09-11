#!/bin/bash
#author:ranzechen
#核对hdfs和es中的数据条数是否相等
if [ $# != 1 ] ; then
	time=`date -d "2 day ago" +"%Y%m%d"`
else
	time=$1
fi

#xiugai:
logpath=/home/kafka/check_err.log
espath=175.10.100.14:9200

aerrn_sum=0
aerr_sum=0
sum=0
echo "-------------------------------------------------------------------------------------------------------------------------------------------" >> $logpath
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
			echo ">>>>>data source:"$aerrn $time
			aerrn_count=`hadoop fs -cat $aerrn | wc -l`
			aerrn_path=$aerrn
			aerrn_checktime=`date +"%Y/%m/%d %H:%m:%S"`
			aerrn_dirdate=$time
			echo -e "{FileHdfsPath:$aerrn_path},{FileCount:$aerrn_count},{FileDirDate:$aerrn_dirdate},{CheckFileDate:$aerrn_checktime}" >> $logpath
			aerrn_sum=`expr $aerrn_count + $aerrn_sum`
		done
	fi

	if [ -n "$file_path_aerr" ];then
		for aerr in $file_path_aerr
		do
			echo ">>>>>data source:"$aerr $time
			aerr_count=`hadoop fs -cat $aerr | wc -l`
			aerr_path=$aerr
			aerr_checktime=`date +"%Y/%m/%d %H:%m:%S"`
			aerr_dirdate=$time
			echo -e "{FileHdfsPath:$aerr_path},{FileCount:$aerr_count},{FileDirDate:$aerr_dirdate},{CheckFileDate:$aerr_checktime}" >> $logpath
			aerr_sum=`expr $aerr_count + $aerr_sum`
		done
	fi

	sum=`expr $aerrn_sum + $aerr_sum`
done

es_index=aerr_$(echo $time | cut -c 1-6)
es_type=$time
es_count=`curl -s "http://$espath/$es_index/$es_type/_count" | awk -F ',' '{print $1}' | awk -F ':' '{print $2}'`
if [ $sum == $es_count ];then
	status="success"
else
	status="fail"
fi
echo -e ">>>>>{HdfsSumCount:$sum},{EsIndex:$es_index},{EsType:$es_type},{EsCount:$es_count},{CheckStatus:$status}" >> $logpath

