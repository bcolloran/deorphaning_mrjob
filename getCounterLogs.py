import subprocess
import yaml
import mrjob.parse

def getCountersFromHdfsDir(hdfsDir):
    commandList = ["hdfs", "dfs", "-ls", hdfsDir+"/_logs/history"]
    p = subprocess.Popen(commandList,stdout=subprocess.PIPE)
    stdout,stderr = p.communicate()
    fileList = [line.split(" ")[-1] for line in stdout.split("\n")]
    logFileName= [fileName for fileName in fileList if (hdfsDir in fileName and fileName[-9:]!="_conf.xml")][0]

    p = subprocess.Popen(["hdfs", "dfs", "-text", logFileName],stdout=subprocess.PIPE)
    stdout,stderr = p.communicate()

    jobLines = [line for line in stdout.split("\n") if line[:9]=="Job JOBID"]
    counters = [ctr for ctr in mrjob.parse._parse_counters_0_20(jobLines[-1]) if ctr[0] not in ["Map-Reduce Framework","File System Counters","Job Counters ","org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter"]]
    counterDict={}
    for ctr in counters:
        counterDict.setdefault(ctr[0],{}).setdefault(ctr[1],ctr[2])
    return yaml.dump(counterDict,default_flow_style=False)