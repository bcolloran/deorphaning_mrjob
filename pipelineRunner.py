import os
import datetime
import testingTools
import getCounterLogs
import yaml
import smtplib
import datetime

from initialScan import ScanJob
from linkDocsAndParts import linkDocsAndPartsJob
from relabelDocsWithLowestPart import relabelDocsJob
from join_docIdsInParts_to_tieBreakInfo import tieBreakInfoPerPartJob
from naiveHeadDocIdExtraction import headRecordExtractionJob
# from finalRecordExtraction import finalRecordExtractionJob
from finalHeadDocIds import finalHeadDocIdsJob


extractDate=datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")

#a dummy file called ".localfile" can be added to your local dir to force inline mode
localRun = ".localfile" in os.listdir('.')





def jobRunner(job,jobArgs,outputPath,inputPaths,local):
    tic = datetime.datetime.now()
    if type(inputPaths)!=type([]):
        inputPaths=[inputPaths]

    if local:
        # print inputPaths
        inputPaths = [f for f in inputPaths if (os.path.isdir(f) or os.path.isfile(f))]
        # print inputPaths
        args= ["-r","inline", "--jobconf", "mapred.reduce.tasks=2", "--output-dir",outputPath]+inputPaths
    else:
        inputPaths = ["hdfs://"+path for path in inputPaths]
        args= ["-r","hadoop"]+jobArgs+["--output-dir",outputPath]+inputPaths

    argString= " ".join(args)+"\n"
    mr_job = job(args=args)
    outString = "==="+mr_job.__class__.__name__ +"===\n start time: " +str(tic)+"\n"+ argString 
    with mr_job.make_runner() as runner:
        runner.run()
        if localRun:
            outString+= yaml.dump(runner.counters(),default_flow_style=False)
        else:
            outString+= getCounterLogs.getCountersFromHdfsDir(outputPath)
    outString+= "job duration: " + str(datetime.datetime.now()-tic)+"\n\n"
    print outString
    return outString




if localRun:
    rootPath = "/data/mozilla/deorphaning_mrjob/testData/"+extractDate
    initDataPath="/data/mozilla/deorphaning_mrjob/testData/fhrFullExtract_2014-04-14_part-m-08207_1k"
else:
    rootPath = "/user/bcolloran/data/deorphTest/"+extractDate
    initDataPath="/user/bcolloran/data/fhrFullExtract_2014-04-21/part-m-000*"


logString= jobRunner(ScanJob,["--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=30","--verbose"],outputPath=rootPath,inputPaths=initDataPath,local=localRun)
if localRun:
    testingTools.multipleOutputSim(rootPath)


for verPath in ["/v2","/v3"]:
    logString+="\n============ logs for "+verPath[1:]+" records ============\n"
    logString+= jobRunner(linkDocsAndPartsJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/kPart_vObjTouchingPart_1",inputPaths=rootPath+verPath+"/kDoc_vPart_0",local=localRun)

    logString+= jobRunner(relabelDocsJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/kDoc_vPart_2",inputPaths=rootPath+verPath+"/kPart_vObjTouchingPart_1",local=localRun)

    logString+= jobRunner(linkDocsAndPartsJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/kPart_vObjTouchingPart_3",inputPaths=rootPath+verPath+"/kDoc_vPart_2",local=localRun)

    logString+= jobRunner(tieBreakInfoPerPartJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],
        outputPath=rootPath+verPath+"/kPart_vDocId-tieBreakInfo",
        inputPaths=[rootPath+verPath+"/kPart_vObjTouchingPart_3",rootPath+verPath+"/kDocId_vTieBreakInfo"],local=localRun)

    logString+= jobRunner(headRecordExtractionJob,["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/naiveHeadRecordDocIds",inputPaths=rootPath+verPath+"/kPart_vDocId-tieBreakInfo",local=localRun)

    logString+= jobRunner(finalHeadDocIdsJob,
        ["--jobconf","mapred.reduce.tasks=3","--verbose","--strict-protocols"],
        outputPath=rootPath+verPath+"/finalHeadDocIds",
        inputPaths=[rootPath+verPath+"/naiveHeadRecordDocIds",rootPath+verPath+"/unlinkable"],local=localRun)





sender = 'bcolloran@mozilla.com'
receivers = ['bcolloran@mozilla.com']
message = """From: mrjob batch bot <bcolloran@mozilla.com>
To: <bcolloran@mozilla.com>
Subject: mrjob DEORPHANING logs, %s UTC

"""%(extractDate)
if localRun:
    print message+logString
else:
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message+logString)         
        print "Successfully sent email"
    except smtplib.SMTPException:
        print "Error: unable to send email"
