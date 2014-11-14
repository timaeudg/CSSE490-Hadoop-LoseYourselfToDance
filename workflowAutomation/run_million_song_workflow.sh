#!/bin/sh
#Run pig stuff
pig filterToHiveTSV.pig

#run hive stuff
hive -f msdAggregateCreateAndExport.hql;

#create mysql stuff
mysql -u root < ./millionSong.sql;

#sqoop the hive tables to mysql
sqoop export --connect jdbc:mysql://hadoop09.csse.rose-hulman.edu:3306/MillionSongData --username root -m 1 --table FullAggregates --export-dir /apps/hive/warehouse/millionsongdataset.db/allsongsaggregatedata --input-fields-terminated-by '\t';
sqoop export --connect jdbc:mysql://hadoop09.csse.rose-hulman.edu:3306/MillionSongData --username root -m 1 --table BottomTenPercentAggregates --export-dir /apps/hive/warehouse/millionsongdataset.db/bottomaggregatedata --input-fields-terminated-by '\t';
sqoop export --connect jdbc:mysql://hadoop09.csse.rose-hulman.edu:3306/MillionSongData --username root -m 1 --table TopTenPercentAggregates --export-dir /apps/hive/warehouse/millionsongdataset.db/topaggregatedata --input-fields-terminated-by '\t';
