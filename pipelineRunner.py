import os
import datetime
import testingTools
import getCounterLogs
import yaml
import smtplib
import subprocess

from initialScan import ScanJob
from linkDocsAndParts import linkDocsAndPartsJob
from relabelDocsWithLowestPart import relabelDocsJob
from join_docIdsInParts_to_tieBreakInfo import tieBreakInfoPerPartJob
from naiveHeadDocIdExtraction import headRecordExtractionJob
from finalHeadDocIds import finalHeadDocIdsJob


startTime=datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")

logString="pipeline started at "+startTime+"\n\n"

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
        try:
            runner.run()
        except:
            outString+="\nrunner failed.\ninput paths:%s\nargs:%s\n\n"%(str(inputPaths),str(args))
        try:
            if localRun:
                outString+= yaml.dump(runner.counters(),default_flow_style=False)
            else:
                outString+= getCounterLogs.getCountersFromHdfsDir(outputPath)
        except:
            outString+="\ncould not retrieve job logs\n"
    outString+= "job duration: " + str(datetime.datetime.now()-tic)+"\n\n"
    print outString
    return outString





if localRun:
    rootPath = "/data/mozilla/deorphaning_mrjob/testData/"+startTime
    initDataPath="/data/mozilla/deorphaning_mrjob/testData/fhrFullExtract_2014-04-14_part-m-08207_1k"
else:
    rootPath = "/user/bcolloran/deorphaningPipeline/"+startTime
    # initDataPath="/user/bcolloran/data/fhrFullExtract_2014-04-21/part-m-*"
    #the following gets the date of the most recent Monday
    nowDate=datetime.datetime.utcnow()
    extractDate = (nowDate- datetime.timedelta(days=nowDate.weekday())).isoformat()[0:10]
    initDataPath="/data/fhr/text/"+(extractDate.replace("-",""))
    # initDataPath="/data/fhr/text/20140505/part-m-0000*"


logString+= jobRunner(ScanJob,["--hadoop-arg","-libjars","--hadoop-arg","tinyoutputformat/naive.jar","--jobconf","mapred.reduce.tasks=4000","--jobconf","mapred.job.priority=NORMAL","--verbose"],outputPath=rootPath,inputPaths=initDataPath,local=localRun)
if localRun:
    testingTools.multipleOutputSim(rootPath)


for verPath in ["/v2","/v3"]:
    logString+="\n============ logs for "+verPath[1:]+" records ============\n"
    logString+= jobRunner(linkDocsAndPartsJob,["--jobconf","mapred.reduce.tasks=1000","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/kPart_vObjTouchingPart_1",inputPaths=rootPath+verPath+"/kDoc_vPart_0",local=localRun)

    logString+= jobRunner(relabelDocsJob,["--jobconf","mapred.reduce.tasks=200","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/kDoc_vPart_2",inputPaths=rootPath+verPath+"/kPart_vObjTouchingPart_1",local=localRun)

    logString+= jobRunner(linkDocsAndPartsJob,["--jobconf","mapred.reduce.tasks=200","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/kPart_vObjTouchingPart_3",inputPaths=rootPath+verPath+"/kDoc_vPart_2",local=localRun)

    logString+= jobRunner(tieBreakInfoPerPartJob,["--jobconf","mapred.reduce.tasks=200","--verbose","--strict-protocols"],
        outputPath=rootPath+verPath+"/kPart_vDocId-tieBreakInfo",
        inputPaths=[rootPath+verPath+"/kPart_vObjTouchingPart_3",rootPath+verPath+"/kDocId_vTieBreakInfo"],local=localRun)

    logString+= jobRunner(headRecordExtractionJob,["--jobconf","mapred.reduce.tasks=200","--verbose","--strict-protocols"],outputPath=rootPath+verPath+"/naiveHeadRecordDocIds",inputPaths=rootPath+verPath+"/kPart_vDocId-tieBreakInfo",local=localRun)

    logString+= jobRunner(finalHeadDocIdsJob,
        ["--jobconf","mapred.reduce.tasks=200","--verbose","--strict-protocols"],
        outputPath=rootPath+verPath+"/finalHeadDocIds",
        inputPaths=[rootPath+verPath+"/naiveHeadRecordDocIds",rootPath+verPath+"/unlinkable"],local=localRun)


logString+="\n============ record extraction pig script ============\n"
os.chdir("/home/bcolloran/pig/")
for verPathStr in ["v2","v3tmp"]:
    command = "pig -param orig=%(initDataPath)s -param fetchids=%(rootPath)s/%(verPathStr)s/finalHeadDocIds/part* -param jointype=merge -param output=deorphaned/%(extractDate)s/%(verPathStr)s fetch_reports.aphadke.pig" % locals()
    print command
    logString+=command+"\n"
    try:
        p=subprocess.call(command,shell=True)
        if p==0:
            logString+="pig extraction to path %s successful\n"%verPathStr
        else:
            logString+="pig extraction to path %s FAILED\n"%verPathStr
    except:
        logString+="pig extraction to path %s CALLER ERROR\n"%verPathStr

#consolidate v3 files.
logString+="\n============ v3 consolidation pig script ============\n"
try:
    command = "pig -param orig=/user/bcolloran/deorphaned/%(extractDate)s/v3tmp -param output=/user/bcolloran/deorphaned/%(extractDate)s/v3 fhrV3PartFileCombiner.pig" % locals()
    p=subprocess.call(command,shell=True)
    if p==0:
        logString+="v3 consolidation pig script successful\n"%verPathStr
    else:
        logString+="v3 consolidation pig script %s FAILED\n"%verPathStr
except:
    logString+="v3 consolidation pig script %s CALLER ERROR\n"%verPathStr


#remove old data
for path in ["deorphaned","deorphaningPipeline"]:
    try:
        oldExtractDate = (nowDate- datetime.timedelta(days=nowDate.weekday()+14)).isoformat()[0:10]
        command = "hdfs dfs -rmr /user/bcolloran/%(path)s/%(oldExtractDate)s*" % locals()
        p=subprocess.call(command,shell=True)
        if p==0:
            logString+="removed data from /user/bcolloran/%(path)s/%(oldExtractDate)s* \n" % locals()
        else:
            logString+="FAILED to remove data from /user/bcolloran/%(path)s/%(oldExtractDate)s* \n" % locals()
    except:
        logString+="CALLER ERROR while removing data from /user/bcolloran/%(path)s/%(oldExtractDate)s* \n" % locals()

    


logString+="\n\n\npipeline finished at "+datetime.datetime.utcnow().isoformat()[0:19].replace(":",".").replace("T","_")











sender = 'bcolloran@mozilla.com'
receivers = ['bcolloran@mozilla.com']
message = """From: mrjob batch bot <bcolloran@mozilla.com>
To: <bcolloran@mozilla.com>
Subject: mrjob DEORPHANING logs, %s UTC

"""%(startTime)
if localRun:
    print message+logString
else:
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message+logString)         
        print "Successfully sent email"
    except smtplib.SMTPException:
        print "Error: unable to send email"
