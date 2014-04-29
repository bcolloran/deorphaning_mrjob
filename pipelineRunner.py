import os
import datetime
import testingTools
import getCounterLogs
import yaml

from initialScan import ScanJob
from linkDocsAndParts import linkDocsAndPartsJob
from relabelDocsWithLowestPart import relabelDocsJob
from join_docIdsInParts_to_tieBreakInfo import tieBreakInfoPerPartJob
from finalNaiveHeadRecordExtraction import headRecordExtractionJob

extractDate=datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")

#a dummy file called ".localfile" can be added to your local dir to force inline mode
localRun = ".localfile" in os.listdir('.')






# if localRun:
#     args = ["-r","inline", "--jobconf", "mapred.reduce.tasks=1", "--output-dir", "testData/initialScanTmp","testData/fhrFullExtract_2014-04-14_part-m-08207_1k"]
# else:
#     rootPath = "/user/bcolloran/mrjobTest/"
#     args = ["-r","hadoop","--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=3","--verbose","--output-dir",rootPath+extractDate,"hdfs:///user/bcolloran/data/fhrFullExtract_2014-04-14/part-m-08207"]
# print args
# mr_job = ScanJob(args=args)
# with mr_job.make_runner() as runner:
#     runner.run()
#     if localRun:
#         print runner.counters()
#         print yaml.dump(runner.counters(),default_flow_style=False)
#     else:
#         print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)

if localRun:
    testingTools.multipleOutputSim("testData/initialScanTmp")





if localRun:
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
print args
mr_job = linkDocsAndPartsJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    if localRun:
        # print runner.counters()
        print yaml.dump(runner.counters(),default_flow_style=False)
    else:
        print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)



if localRun:
    args = ["-r","inline",
        "--jobconf", "mapred.reduce.tasks=1",
        "--output-dir", "v2/kDoc_vPart_2",
        "v2/kPart_vObjTouchingPart_1"]
else:
    args = ["-r","hadoop",
        "--jobconf","mapred.reduce.tasks=3",
        "--verbose",
        "--strict-protocols",
        "--output-dir",rootPath+extractDate+"/v2/kDoc_vPart_2",
        "hdfs://"+rootPath+extractDate+"/v2/kPart_vObjTouchingPart_1"]
print args
mr_job = relabelDocsJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    if localRun:
        # print runner.counters()
        print yaml.dump(runner.counters(),default_flow_style=False)
    else:
        print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)





if localRun:
    args = ["-r","inline",
        "--jobconf", "mapred.reduce.tasks=1",
        "--output-dir", "v2/kPart_vObjTouchingPart_3",
        "v2/kDoc_vPart_2"]
else:
    args = ["-r","hadoop",
        "--jobconf","mapred.reduce.tasks=3",
        "--verbose",
        "--strict-protocols",
        "--output-dir",rootPath+extractDate+"/v2/kPart_vObjTouchingPart_3",
        "hdfs://"+rootPath+extractDate+"/v2/kDoc_vPart_2"]
print args
mr_job = linkDocsAndPartsJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    if localRun:
        # print runner.counters()
        print yaml.dump(runner.counters(),default_flow_style=False)
    else:
        print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)





if localRun:
    args = ["-r","inline",
        "--jobconf", "mapred.reduce.tasks=1",
        "--output-dir", "v2/kPart_vDocId-tieBreakInfo",
        "v2/kPart_vObjTouchingPart_3",
        "v2/kDocId_vTieBreakInfo"]
else:
    args = ["-r","hadoop",
        "--jobconf","mapred.reduce.tasks=3",
        "--verbose",
        "--strict-protocols",
        "--output-dir",rootPath+extractDate+"/v2/kDoc_vPart_2",
        "hdfs://"+rootPath+extractDate+"/v2/kPart_vObjTouchingPart_3",
        "hdfs://"+rootPath+extractDate+"/v2/kDocId_vTieBreakInfo"]
print args
mr_job = tieBreakInfoPerPartJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    if localRun:
        # print runner.counters()
        print yaml.dump(runner.counters(),default_flow_style=False)
    else:
        print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)





if localRun:
    args = ["-r","inline",
        "--jobconf", "mapred.reduce.tasks=1",
        "--output-dir", "v2/finalNaiveHeadRecord",
        "v2/kPart_vDocId-tieBreakInfo"]
else:
    args = ["-r","hadoop",
        "--jobconf","mapred.reduce.tasks=3",
        "--verbose",
        "--strict-protocols",
        "--output-dir",rootPath+extractDate+"/v2/finalNaiveHeadRecord",
        "hdfs://"+rootPath+extractDate+"/v2/kPart_vDocId-tieBreakInfo"]
print args
mr_job = headRecordExtractionJob(args=args)
with mr_job.make_runner() as runner:
    runner.run()
    if localRun:
        # print runner.counters()
        print yaml.dump(runner.counters(),default_flow_style=False)
    else:
        print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)