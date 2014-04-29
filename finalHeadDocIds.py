from mrjob.job import MRJob
import mrjob
import sys, codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)



#inputs to this job will be:
#unlinkable docids: (docId,"u")
#head record docIds: (docId,"h")

class finalHeadDocIdsJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.TextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mapper(self,docId, flag):
        self.increment_counter("MAPPER", "number of final output head docIds")
        if flag=="h":
            self.increment_counter("MAPPER", "number of linkable docIds")
        elif flag=="u":
            self.increment_counter("MAPPER", "number of unlinkable docIds")
        else:
            print flag
            raise ValueError()
        yield("",docId)



if __name__ == '__main__':
    finalHeadDocIdsJob.run()


