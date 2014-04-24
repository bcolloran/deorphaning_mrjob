from initialScan import ScanJob
import yaml
import os
import datetime

extractDate= datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")




if ".localfile" in os.listdir('.'):
    #a dummy file called ".localfile" can be added to your local dir to force inline mode
    args = ["-r","inline", "--jobconf", "mapred.reduce.tasks=1", "--output-dir", "testData/initialScanTmp","testData/fhrFullExtract_2014-04-14_part-m-08207_1k"]
else:
    args = ["-r","hadoop","--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=3","--verbose","--output-dir","/user/bcolloran/mrjobTest/"+extractDate,"hdfs:///user/bcolloran/data/fhrFullExtract_2014-04-14/part-m-08207"]

print args
mr_job = ScanJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    print yaml.dump(runner.counters(),default_flow_style=False)
