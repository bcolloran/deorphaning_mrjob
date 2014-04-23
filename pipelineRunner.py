from initialScan import ScanJob
# from datetime import date, datetime, timedelta
# import os, shutil, csv
# from mrjob.job import MRJob
# from mrjob.launch import _READ_ARGS_FROM_SYS_ARGV
# import sys, codecs
# import traceback
# import mrjob
# import json
import yaml


argsInline = ["-r","inline", "--jobconf", "mapred.reduce.tasks=1", "--output-dir", "testData/initialScanTmp","testData/fhrFullExtract_2014-04-14_part-m-08207_1k"]

argsHadoop = ["-r","hadoop","--hadoop-arg","-libjars","--hadoop-arg","./naive.jar","--jobconf mapred.reduce.tasks=3","--verbose","--output-dir","/user/bcolloran/mrjobTest/tmp9","hdfs:///user/bcolloran/data/fhrFullExtract_2014-04-14/part-m-08207"]

print argsInline
mr_job = ScanJob(args=argsInline)
with mr_job.make_runner() as runner:
    runner.run()
    print yaml.dump(runner.counters(),default_flow_style=False)
