import os
import datetime
import testingTools
import getCounterLogs
import yaml

from initialScan import ScanJob
from linkDocsAndParts import linkDocsAndPartsJob
from relabelDocsWithLowestPart import relabelDocsJob
from join_docIdsInParts_to_tieBreakInfo import tieBreakInfoPerPartJob
from naiveHeadDocIdExtraction import headRecordExtractionJob
from finalRecordExtraction import finalRecordExtractionJob
from finalHeadDocIds import finalHeadDocIdsJob


extractDate=datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")

#a dummy file called ".localfile" can be added to your local dir to force inline mode
localRun = ".localfile" in os.listdir('.')





def jobRunner(job,jobArgs,outputPath,inputPaths,local):
    if type(inputPaths)!=type([]):
        inputPaths=[inputPaths]

    if local:
        args= ["-r","inline", "--jobconf", "mapred.reduce.tasks=2", "--output-dir",outputPath]+inputPaths
    else:
        inputPaths = ["hdfs://"+path for path in inputPaths]
        args= ["-r","hadoop"]+jobArgs+["--output-dir",outputPath]+inputPaths

    print " ".join(args)
    mr_job = job(args=args)
    with mr_job.make_runner() as runner:
        runner.run()
        if localRun:
            print yaml.dump(runner.counters(),default_flow_style=False)
        else:
            print getCounterLogs.getCountersFromHdfsDir(rootPath+extractDate)




if localRun:
    rootPath = "/data/mozilla/deorphaning_mrjob/testData/"+extractDate
    initDataPath="/data/mozilla/deorphaning_mrjob/testData/fhrFullExtract_2014-04-14_part-m-08207_1k"
else:
    rootPath = "/user/bcolloran/data/deorphTest/"+extractDate
    initDataPath="/user/bcolloran/data/fhrFullExtract_2014-04-21/part-m-00001"


jobRunner(ScanJob,["--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=3","--verbose"],outputPath=rootPath,inputPaths=initDataPath,local=localRun)
if localRun:
    testingTools.multipleOutputSim(rootPath)

jobRunner(linkDocsAndPartsJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+"/v2/kPart_vObjTouchingPart_1",inputPaths=rootPath+"/v2/kDoc_vPart_0",local=localRun)

jobRunner(relabelDocsJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+"/v2/kDoc_vPart_2",inputPaths=rootPath+"/v2/kPart_vObjTouchingPart_1",local=localRun)

jobRunner(linkDocsAndPartsJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+"/v2/kPart_vObjTouchingPart_3",inputPaths=rootPath+"/v2/kDoc_vPart_2",local=localRun)

jobRunner(tieBreakInfoPerPartJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],
    outputPath=rootPath+"/v2/kPart_vDocId-tieBreakInfo",
    inputPaths=[rootPath+"/v2/kPart_vObjTouchingPart_3",rootPath+"/v2/kDocId_vTieBreakInfo"],local=localRun)

jobRunner(headRecordExtractionJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+"/v2/naiveHeadRecordDocIds",inputPaths=rootPath+"/v2/kPart_vDocId-tieBreakInfo",local=localRun)

jobRunner(finalHeadDocIdsJob,
    ["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],
    outputPath=rootPath+"/v2/finalHeadDocIds",
    inputPaths=[rootPath+"/v2/naiveHeadRecordDocIds",rootPath+"/v2/unlinkable"],local=localRun)


