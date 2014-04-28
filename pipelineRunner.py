from initialScan import ScanJob
from linkDocsAndParts import linkDocsAndPartsJob
import yaml
import os
import datetime
import testingTools

extractDate=datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")

#a dummy file called ".localfile" can be added to your local dir to force inline mode
localRun = ".localfile" in os.listdir('.')

if localRun:
    args = ["-r","inline", "--jobconf", "mapred.reduce.tasks=1", "--output-dir", "testData/initialScanTmp","testData/fhrFullExtract_2014-04-14_part-m-08207_1k"]
else:
    rootPath = "/user/bcolloran/mrjobTest/"
    args = ["-r","hadoop","--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=3","--verbose","--output-dir",rootPath+extractDate,"hdfs:///user/bcolloran/data/fhrFullExtract_2014-04-14/part-m-08207"]

print args
mr_job = ScanJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    print runner.counters()
    print yaml.dump(runner.counters(),default_flow_style=False)


# extractDate="2014-04-25_18.28.38"

if localRun:
    testingTools.multipleOutputSim("testData/initialScanTmp")
    args = ["-r","inline",
        "--jobconf", "mapred.reduce.tasks=1",
        "--output-dir", "v2/kPart_vObjTouchingPart_1",
        "v2/kDoc_vPart_0"]
else:
    args = ["-r","hadoop",
        "--jobconf","mapred.reduce.tasks=3",
        "--verbose",
        "--strict-protocols",
        "--output-dir",rootPath+extractDate+"/v2/kPart_vObjTouchingPart_1",
        "hdfs://"+rootPath+extractDate+"/v2/kDoc_vPart_0"]

'''
+str( random.randint(1000,9999))

HADOOP_HOME=/opt/cloudera/parcels/CDH/ python linkDocsAndParts.py -r hadoop --jobconf mapred.reduce.tasks=3 --verbose --output-dir //user/bcolloran/mrjobTest/2014-04-25_18.28.38/v2/kPart_vObjTouchingPart_1 hdfs:///user/bcolloran/mrjobTest/2014-04-25_18.28.38/v2/kDoc_vPart_0

'''

print args
mr_job = linkDocsAndPartsJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    print runner.counters()
    print yaml.dump(runner.counters(),default_flow_style=False)
