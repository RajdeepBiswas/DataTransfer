#!/bin/bash
##########################################################################################################
#Author: Rajdeep Biswas
#Date:6/27/2017
#Description:
#Moves data in and out of Amazon S3 storage to Hadoop Cluster
#Primary Functions: export and import
#Uses configuration files to store S3 keys (Owned by root)
#Needs root access to execute
#Example usage
#./datamove.sh export /tmp/tomcatLog dir conf_datamove_devs3.conf
#./datamove.sh import /tmp/tomcatLog dir conf_datamove_devs3.conf
#./datamove.sh export testraj db conf_datamove_devs3.conf
#./datamove.sh import testraj db conf_datamove_devs3.conf
##########################################################################################################
if [ $# -ne 4 ]
then
	echo -e "\nError: Exactly three arguments are allowed. Like export dbtest db OR import dbtest dir\n"
	exit 1
elif [[ ! "$1" =~ ^(export|import)$ ]]
then
	echo -e "\nError: First argument needs to be export or import. Full argument set like export dbtest db OR import dbtest dr\n"
	exit 1
elif [[ ! "$3" =~ ^(db|dir)$ ]]
then
	echo -e "\nError: Last argument needs to be db or dr. Full argument set like export dbtest db OR import dbtest dr\n"
	exit 1
fi

baseDir=/root/scripts/dataCopy
operation=$1
hiveDBName=$2
copyType=$3
confFile=$4

function logsetup {
	ts=$(date +%Y_%m_%d_%H_%M_%S)
	LOGFILE="$baseDir/$hiveDBName/datamove_$ts.log"
	exec > >(tee -a $LOGFILE)
	exec 2>&1
}

function log {
	echo "[$(date +%Y/%m/%d:%H:%M:%S)]: $*"
}

function exportDB {

	log "Step1: Getting list of tables"

	hive -e "USE $hiveDBName; SHOW tables;" > $hiveDBName/${hiveDBName}_alltables.txt

	log "Step2: Fetching Hive table DDLs"

	log "DROP DATABASE IF EXISTS $hiveDBName CASCADE;" > $hiveDBName/${hiveDBName}_all_tables_DDL.txt
	log "CREATE DATABASE $hiveDBName;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
	log "USE $hiveDBName;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt

	while read line
	do
		log "Processing table $line"
		hive -e "USE $hiveDBName; SHOW CREATE TABLE ${hiveDBName}.$line;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
		echo ";" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt

		hive -e "USE $hiveDBName; SHOW PARTITIONS $line;" > $hiveDBName/tmp_part.txt
		while read tablepart
		do
			partname=`echo ${tablepart/=/=\"}`
			echo "ALTER TABLE ${hiveDBName}.$line ADD PARTITION ($partname\");" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
			echo "ANALYZE TABLE ${hiveDBName}.$line PARTITION ($partname\") COMPUTE STATISTICS;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
		done < $hiveDBName/tmp_part.txt

		checklib=$(hive -e "USE $hiveDBName; DESCRIBE EXTENDED $line;" |grep serializationLib)
		if [[ $checklib == *"OpenCSVSerde"* ]]
		then
			echo "ALTER TABLE ${hiveDBName}.$line SET SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
			escapechar="\"escapeChar\"=\"\\\\\""
			quotechar="\"quoteChar\"=\"\\\"\""
			seperatorchar="\"separatorChar\"=\",\""
			echo "ALTER TABLE ${hiveDBName}.$line SET SERDEPROPERTIES ($escapechar, $quotechar, $seperatorchar);" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
		fi


	done <	$hiveDBName/${hiveDBName}_alltables.txt

	sed -i.bak '/LOCATION/,+1 d' $hiveDBName/${hiveDBName}_all_tables_DDL.txt

	log " Wrote DDLs to $hiveDBName/${hiveDBName}_all_tables_DDL.txt"

	if [ -e /tmp/${hiveDBName}_all_tables_DDL.txt ]
	then
		rm -f /tmp/${hiveDBName}_all_tables_DDL.txt
		sudo -u hdfs hdfs dfs -rm /tmp/${hiveDBName}_all_tables_DDL.txt
	fi

	cp $hiveDBName/${hiveDBName}_all_tables_DDL.txt /tmp

	sudo -u hdfs hdfs dfs -put /tmp/${hiveDBName}_all_tables_DDL.txt /tmp/${hiveDBName}_all_tables_DDL.txt

	sudo -u hdfs hadoop distcp	-D fs.s3a.server-side-encryption-algorithm=${fs_s3a_server_side_encryption_algorithm} \
	-D fs.s3a.secret.key=${fs_s3a_secret_key} \
	-D fs.s3a.access.key=${fs_s3a_access_key} \
	-p -update hdfs:///tmp/${hiveDBName}_all_tables_DDL.txt ${s3bucket}/${hiveDBName}/${hiveDBName}_all_tables_DDL.txt

	sudo -u hdfs hadoop distcp	-D fs.s3a.server-side-encryption-algorithm=${fs_s3a_server_side_encryption_algorithm} \
	-D fs.s3a.secret.key=${fs_s3a_secret_key} \
	-D fs.s3a.access.key=${fs_s3a_access_key} \
	-p -update hdfs:///apps/hive/warehouse/${hiveDBName}.db ${s3bucket}/apps/hive/warehouse/${hiveDBName}.db

}

function exportDIR {

	sudo -u hdfs hadoop distcp -D fs.s3a.server-side-encryption-algorithm=${fs_s3a_server_side_encryption_algorithm} \
	-D fs.s3a.secret.key=${fs_s3a_secret_key} \
	-D fs.s3a.access.key=${fs_s3a_access_key} \
	-p -update hdfs:///${formattedDirName} ${s3bucket}/${formattedDirName}
}


function importDB {

	log "Step1: Running disributed copy."

	sudo -u hdfs hadoop distcp -D fs.s3a.server-side-encryption-algorithm=${fs_s3a_server_side_encryption_algorithm} \
	-D fs.s3a.secret.key=${fs_s3a_secret_key} \
	-D fs.s3a.access.key=${fs_s3a_access_key} \
	-p -update ${s3bucket}/${hiveDBName}/${hiveDBName}_all_tables_DDL.txt hdfs:///tmp/${hiveDBName}_all_tables_DDL.txt

	log "Step2: Running database import."

	if [ -e /tmp/${hiveDBName}_all_tables_DDL.txt ]
	then
		rm -f /tmp/${hiveDBName}_all_tables_DDL.txt
	fi

	sudo -u hdfs hdfs dfs -get /tmp/${hiveDBName}_all_tables_DDL.txt /tmp/${hiveDBName}_all_tables_DDL.txt

	if [ ! -e $hiveDBName ]
	then
		mkdir $hiveDBName
	fi

	if [ -e $hiveDBName/${hiveDBName}_all_tables_DDL.txt ]
	then
		rm -f $hiveDBName/${hiveDBName}_all_tables_DDL.txt
	fi
	chmod 755 /tmp/${hiveDBName}_all_tables_DDL.txt

	cp -f /tmp/${hiveDBName}_all_tables_DDL.txt $hiveDBName/${hiveDBName}_all_tables_DDL.txt

	sudo -u hive hive -f /tmp/${hiveDBName}_all_tables_DDL.txt
	cat /tmp/${hiveDBName}_all_tables_DDL.txt | grep ANALYZE > /tmp/${hiveDBName}_all_tables_ANALYZE.txt


	sudo -u hdfs hadoop distcp -D fs.s3a.server-side-encryption-algorithm=${fs_s3a_server_side_encryption_algorithm} \
	-D fs.s3a.secret.key=${fs_s3a_secret_key} \
	-D fs.s3a.access.key=${fs_s3a_access_key} \
	-p -update ${s3bucket}/apps/hive/warehouse/${hiveDBName}.db hdfs:///apps/hive/warehouse/${hiveDBName}.db

	sudo -u hdfs hdfs dfs -chown -R hive:hdfs /apps/hive/warehouse/${hiveDBName}.db
	sudo -u hive hive -f /tmp/${hiveDBName}_all_tables_ANALYZE.txt
}

function importDIR {
	sudo -u hdfs hadoop distcp -D fs.s3a.server-side-encryption-algorithm=${fs_s3a_server_side_encryption_algorithm} \
	-D fs.s3a.secret.key=${fs_s3a_secret_key} \
	-D fs.s3a.access.key=${fs_s3a_access_key} \
	-p -update ${s3bucket}/${formattedDirName} hdfs:///${formattedDirName}
	sudo -u hdfs hdfs dfs -chown -R hive:hdfs /${formattedDirName}
}


##############
#MAIN
##############

cd $baseDir

if [ $copyType = 'dir' ]
then
	formattedDirName=$(echo $hiveDBName | sed 's/^\///')
	hiveDBName=$(basename $hiveDBName)
fi
if [ ! -e $hiveDBName ]
then
	mkdir $hiveDBName
fi

logsetup

log "$copyType $hiveDBName copy initiation..."

. conf/$confFile

if [ $operation = 'export' ]
then
	log "$copyType $hiveDBName export initiation..."
	if [ $copyType = 'db' ]
	then
		exportDB
	else
		exportDIR
	fi
	log "$hiveDBName Export Processing finished."
else
	log "$copyType $hiveDBName import initiation..."
	if [ $copyType = 'db' ]
	then
		importDB
	else
		importDIR
	fi
	log "$copyType $hiveDBName Import processing finished. Please verify the objects"
fi
