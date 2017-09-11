#!/bin/bash
#author:ranzechen
#核对hdfs和es中的数据条数是否相等
if [ $# != 1 ] ; then
	time=`date -d "2 day ago" +"%Y%m%d"`
else
	time=$1
fi
#xiugai:
logpath=/home/kafka/check_com.log
espath=175.10.100.14:9200

acomn_sum=0
acom_sum=0
sum=0
dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
echo "-------------------------------------------------------------------------------------------------------------------------------------------" >> $logpath
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
			echo ">>>>>data source:"$acomn $time
			acomn_count=`hadoop fs -cat $acomn | wc -l`
			acomn_path=$acomn
			acomn_checktime=`date +"%Y/%m/%d %H:%m:%S"`
			acomn_dirdate=$time
			echo -e "{FileHdfsPath:$acomn_path},{FileCount:$acomn_count},{FileDirDate:$acomn_dirdate},{CheckFileDate:$acomn_checktime}" >> $logpath
			acomn_sum=`expr $acomn_count + $acomn_sum`
		done
	fi

	if [ -n "$file_path_acom" ];then
		for acom in $file_path_acom
		do
			echo ">>>>>data source:"$acom $time
			acom_count=`hadoop fs -cat $acom | wc -l`
                        acom_path=$acom
                        acom_checktime=`date +"%Y/%m/%d %H:%m:%S"`
                        acom_dirdate=$time
			echo -e "{FileHdfsPath:$acom_path},{FileCount:$acom_count},{FileDirDate:$acom_dirdate},{CheckFileDate:$acom_checktime}" >> $logpath
			acom_sum=`expr $acom_count + $acom_sum`
		done
	fi

	sum=`expr $acomn_sum + $acom_sum`
done

es_index=acom_$(echo $time | cut -c 1-6)
es_type=$time
es_count=`curl -s "http://$espath/$es_index/$es_type/_count" | awk -F ',' '{print $1}' | awk -F ':' '{print $2}'`
if [ $sum == $es_count ];then
	status="success"
else
	status="fail"
fi
echo -e ">>>>>{HdfsSumCount:$sum},{EsIndex:$es_index},{EsType:$es_type},{EsCount:$es_count},{CheckStatus:$status}" >> $logpath

