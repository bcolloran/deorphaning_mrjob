register '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
register 'elephant-bird-core-4.3.jar';
register 'elephant-bird-hadoop-compat-4.3.jar';
register 'elephant-bird-pig-4.3.jar';

SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare SEQFILE_STORAGE 'com.twitter.elephantbird.pig.store.SequenceFileStorage';

 
fulldump = LOAD '$orig' USING org.apache.pig.piggybank.storage.SequenceFileLoader AS (key:chararray, value:chararray);
ids_to_fetch_raw = LOAD '$fetchids' USING PigStorage() AS (key:chararray, ign:chararray);
 
ids_to_fetch = ORDER ids_to_fetch_raw BY key;
 
common = JOIN fulldump by key, ids_to_fetch by key USING '$jointype';
todump = FOREACH common GENERATE $0, $1;
 
STORE todump INTO '$output' USING $SEQFILE_STORAGE ('-c $TEXT_CONVERTER', '-c $TEXT_CONVERTER');

