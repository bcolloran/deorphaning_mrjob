from mrjob.job import MRJob
import mrjob
import sys, codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)



class tieBreakInfoPerPartJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.TextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    '''
    This script will be passed in pairs like EITHER:
    (A)
    k: docId
    v: a "_" sep str of tieBreakInfo: date_numAppSessionsPreviousOnThisPingDate_currentSessionTime

    (B)
    k: partId
    v: docId

    the mapper will pass out pairs like:
    (A)
    k: docId
    v: tieBreakInfo
    (b)
    k: docId
    v: partId

    '''
    def mapper(self,partId_orDocId, docId_orTieBreakInfo):
        self.increment_counter("MAPPER", "input (k,v) pairs")
        if partId_orDocId[0]=="p":
            #handle case of partId keys first:
            self.increment_counter("MAPPER", "(docId,partId) out")
            yield(docId_orTieBreakInfo,partId_orDocId)
            
        elif partId_orDocId[0].lower() in list("0123456789abcdef"):
            self.increment_counter("MAPPER", "(docId,tieBreakInfo) out")
            yield(partId_orDocId,docId_orTieBreakInfo)
        else:
            print "bad input partId_orDocId:",docId_orPartSet
            raise ValueError()





    def reducer(self,docId,iterOfPartId_orTieBreakInfo):
        # for each docId, the iter should contain:
        #       partId
        #       tieBreakInfo tuple
        # for docIds that are on the server twice (exact copies),
        # there may be more than one copy of partId or tieBreakInfo
        # in this case, we'll just choose the last instance of each
        hasPart=False
        hasTieBreakInfo=False
        numItems=0
        for item in iterOfPartId_orTieBreakInfo:
            numItems+=1
            if item[0]=="p":
                partId=item
                hasPart=True
            elif "_" in item:
                tieBreakInfo=item
                hasTieBreakInfo=True
            else:
                print "bad reducer iter contents:",item
                raise ValueError()

        if numItems==2:
            self.increment_counter("REDUCER", "partId,(docId,tieBreakInfo)  out")
            yield(partId, docId+"|"+tieBreakInfo)
        else:
            if hasTieBreakInfo and hasPart:
                self.increment_counter("REDUCER", "partId,(docId,tieBreakInfo)  out")
                yield(partId,(docId,tieBreakInfo))
                self.increment_counter("REDUCER", "WARNING: docId with "+str(numItems)+" elts in val iter" )
            elif hasPart and (not hasTieBreakInfo):
                self.increment_counter("REDUCER", "ERROR: missing tieBreakInfo")
                self.increment_counter("REDUCER", "WARNING: docId with "+str(numItems)+" elts in val iter" )
            elif (not hasPart) and hasTieBreakInfo:
                self.increment_counter("REDUCER", "ERROR: missing partId")
                self.increment_counter("REDUCER", "WARNING: docId with "+str(numItems)+" elts in val iter" )




if __name__ == '__main__':
    tieBreakInfoPerPartJob.run()
