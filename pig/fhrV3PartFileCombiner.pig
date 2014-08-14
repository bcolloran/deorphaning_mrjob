register '/opt/cloudera/parcels/CDH/lib/pig/piggybank.jar';
register 'elephant-bird-core-4.3.jar';
register 'elephant-bird-hadoop-compat-4.3.jar';
register 'elephant-bird-pig-4.3.jar';


a = LOAD '$orig' USING org.apache.pig.piggybank.storage.SequenceFileLoader AS (key:chararray, val:chararray);
b = STORE a INTO '$output' USING com.twitter.elephantbird.pig.store.SequenceFileStorage ( '-c com.twitter.elephantbird.pig.util.TextConverter', '-c com.twitter.elephantbird.pig.util.TextConverter');