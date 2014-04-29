from mrjob.job import MRJob
import mrjob
import sys, codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)



#inputs to this job will be:
#full records: (docId,fhrPayload)
#unlinkable docids: (docId,"u")
#head record docIds: (docId,"h")

class finalRecordExtractionJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.TextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def mapper(self,docId, val):
        self.increment_counter("MAPPER", "input k/v pair")
        yield(docId,val)


    def reducer(self,docId, iter_jsonOrFlag):
        # not all docIds will be re-emitted; only emit jsons when there is a flag of either "u" or "h"
        emitJson=False
        for jsonOrFlag in iter_jsonOrFlag:
            if jsonOrFlag[0] in ["u","h"]:
                emitJson=True
            elif jsonOrFlag[0]=="{":
                jsonOut = jsonOrFlag
            else:
                raise ValueError()

        if emitJson:
            self.increment_counter("REDUCER", "final record out")
            yield(docId,jsonOut)
        else:
            self.increment_counter("REDUCER", "final records dropped")



if __name__ == '__main__':
    finalRecordExtractionJob.run()


