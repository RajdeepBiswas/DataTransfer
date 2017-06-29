# DataTransfer
Generic HDFS data and Hive Database transfer automation between any environment(Production/QA/Development) utilizing Amazon S3 storage


## Synopsis:
Exporting and importing data between different layers of environment like production, QA and development is a recurring task.
Due to security considerations this environments cannot talk to each other. Hence we are using Amazon S3 storage as an intermediate storage point for transferring data seamlessly across environments.
The automation of this task is expected to save close to 4 hours of manual intervention per occurrence.

## Code location:

**Place your scripts here:**

**Script:**
/root/scripts/dataCopy/datamove.sh
Configuration File:
/root/scripts/dataCopy/conf/conf_datamove_devs3.conf
Note: The name of the configuration files can be different for different S3 locations. This can be passed to the script. But it needs to be in conf folder under the /root/scripts/dataCopy directory.

## Usage:
**Scenario1:**  For exporting database from cluster1 to cluster2
Example database name: testdb
**In cluster1:**
sudo su root
cd /root/scripts/dataCopy/
./datamove.sh export testdb db conf_datamove_devs3.conf

After above execution finishes:

**In cluster2:**
sudo su root
cd /root/scripts/dataCopy/
./datamove.sh import testraj db conf_datamove_devs3.conf

**Scenario 2:** For exporting HDFS data (directory) from cluster1 to cluster2
Example directory name: /tmp/tomcatLog
**In cluster1:**
sudo su root
cd /root/scripts/dataCopy/
./datamove.sh export /tmp/tomcatLog dir conf_datamove_devs3.conf

After above execution finishes:

**In cluster2:**
sudo su root
cd /root/scripts/dataCopy/
./datamove.sh import /tmp/tomcatLog dir conf_datamove_devs3.conf
Note
The script can be run in background (nohup &) and the logs are stored inside the folder structure with database or directory name with timestamp.

## Logs:
[root@cluster1 tomcatLog]# pwd
/root/scripts/dataCopy/tomcatLog
[root@cluster1 tomcatLog]# ls -lrt
total 40
-rw-r--r--. 1 root root 4323 Jun 27 20:53 datamove_2017_06_27_20_52_42.log
-rw-r--r--. 1 root root 4358 Jun 27 20:54 datamove_2017_06_27_20_54_15.log
-rw-r--r--. 1 root root 4380 Jun 27 20:57 datamove_2017_06_27_20_57_31.log

[root@cluster1 tomcatLog]# head datamove_2017_06_27_21_29_24.log
[2017/06/27:21:29:24]: dir tomcatLog copy initiation...
[2017/06/27:21:29:24]: dir tomcatLog import initiation...

17/06/27 21:29:25 INFO tools.DistCp: Input Options: DistCpOptions{atomicCommit=false, syncFolder=true, deleteMissing=false, ignoreFailures=false, overwrite=false, skipCRC=false, blocking=true, numListstatusThreads=0, maxMaps=20, mapBandwidth=100, sslConfigurationFile='null', copyStrategy='uniformsize', preserveStatus=[REPLICATION, BLOCKSIZE, USER, GROUP, PERMISSION, CHECKSUMTYPE, TIMES], preserveRawXattrs=false, atomicWorkPath=null, logPath=null, sourceFileListing=null, sourcePaths=[s3a://s3.path/tmp/tomcatLog], targetPath=hdfs:/tmp/tomcatLog, targetPathExists=true, filtersFile='null'}
17/06/27 21:29:26 INFO impl.TimelineClientImpl: Timeline service address: http://cluster1:8188/ws/v1/timeline/
17/06/27 21:29:26 INFO client.RMProxy: Connecting to ResourceManager at test:8050
17/06/27 21:29:26 INFO client.AHSProxy: Connecting to Application History server at test:10200
17/06/27 21:29:28 INFO tools.SimpleCopyListing: Paths (files+dirs) cnt = 9; dirCnt = 0
17/06/27 21:29:28 INFO tools.SimpleCopyListing: Build file listing completed.
17/06/27 21:29:29 INFO tools.DistCp: Number of paths in the copy list: 9
17/06/27 21:29:29 INFO tools.DistCp: Number of paths in the copy list: 9
[root@cluster1 tomcatLog]#

