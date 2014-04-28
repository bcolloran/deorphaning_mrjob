from initialScan import ScanJob
from linkDocsAndParts import linkDocsAndPartsJob
import yaml
import os
import datetime
import testingTools
import mrjob.logparsers
import mrjob.parse
import subprocess

extractDate=datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")

#a dummy file called ".localfile" can be added to your local dir to force inline mode
localRun = ".localfile" in os.listdir('.')

if localRun:
    args = ["-r","inline", "--jobconf", "mapred.reduce.tasks=1", "--output-dir", "testData/initialScanTmp","testData/fhrFullExtract_2014-04-14_part-m-08207_1k"]
else:
    rootPath = "/user/bcolloran/mrjobTest/"
    args = ["-r","hadoop","--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=3","--verbose","--output-dir",rootPath+extractDate,"hdfs:///user/bcolloran/data/fhrFullExtract_2014-04-14/part-m-08207"]

# print args
# mr_job = ScanJob(args=args)
# with mr_job.make_runner() as runner:
#     runner.run()
#     print runner.counters()
#     print yaml.dump(runner.counters(),default_flow_style=False)


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
extractDate="2014-04-28_19.22.25"

print args
mr_job = linkDocsAndPartsJob(args=args)
with mr_job.make_runner() as runner:
    # runner.run()
    # print runner.counters()
    # print yaml.dump(runner.counters(),default_flow_style=False)
    print rootPath+extractDate
    counters = mrjob.logparsers.scan_for_counters_in_files(rootPath+extractDate, runner, '0.20')
    runnercat = runner.cat("/user/bcolloran/mrjobTest/2014-04-28_19.22.25/_logs/history/job_201403060050_3193_1398712956710_bcolloran_streamjob4688214214206024345.jar")
    for cat in runnercat:
        print "cat:",cat
    print 'counters:',counters

    commandList = ["hdfs", "dfs", "-ls", "/user/bcolloran/mrjobTest/2014-04-28_19.22.25/_logs/history"]
    command = " ".join(commandList)
    p = subprocess.Popen(commandList,stdout=subprocess.PIPE)
    stdout,stderr = p.communicate()
    print "stdout:",stdout
    print "stderr:",stderr

    p = subprocess.Popen(["hdfs", "dfs", "-text", "/user/bcolloran/mrjobTest/2014-04-28_19.22.25/_logs/history/job_201403060050_3193_1398712956710_bcolloran_streamjob4688214214206024345.jar"],stdout=subprocess.PIPE)
    stdout,stderr = p.communicate()
    # print "stdout:",stdout
    # print "stderr:",stderr
    for line in stdout.split("\n"):
        print line[:200]
    # print mrjob.parse.parse_hadoop_counters_from_line(stdout, '0.20')
    print [ctr for ctr in mrjob.parse._parse_counters_0_20(stdout) if ctr[0] not in ["Map-Reduce Framework","File System Counters","Job Counters "]]
    # print mrjob.parse.parse_hadoop_counters_from_line(stdout, '0.18')

