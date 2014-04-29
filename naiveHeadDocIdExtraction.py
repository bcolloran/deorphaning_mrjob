from mrjob.job import MRJob
import mrjob
import sys, codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
import random




class headRecordExtractionJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.TextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def mapper(self,partId, docId_tieBreakInfo):
        # this must be:
        #   k: "partId"
        #   v: "{docId}|{thisPingDate}_{numAppSessionsPreviousOnThisPingDate}_{currentSessionTime}"
        self.increment_counter("MAPPER", "input: docIds with tieBreakInfo (and partId)")
        yield(partId,docId_tieBreakInfo)


    def reducer(self,partId, iter_docIdAndTieBreakInfo):
        self.increment_counter("REDUCER", "number of parts")
        maxRecordDocIdList = None
        maxRecordTieBreakInfo = ("0000-00-00",0,0)
        # saveIter=[]
        for docIdAndTieBreakInfo in iter_docIdAndTieBreakInfo:
            docId, tieBreakInfoStr = docIdAndTieBreakInfo.split("|")
            tieBreakInfo = tuple(tieBreakInfoStr.split("_"))
            if tieBreakInfo>maxRecordTieBreakInfo:
                #if this is the maximal record, update the maxRecordTieBreakInfo and reset the maxRecordDocIdList
                # print partId,docId,tieBreakInfo
                maxRecordDocIdList=[docId]
                maxRecordTieBreakInfo=tieBreakInfo
            elif tieBreakInfo==maxRecordTieBreakInfo:
                #if this record is tied for maximal record, add it to the list of record tups that tie for max
                maxRecordDocIdList+=[docId]


        #not sure why this was happening, but for some record(s) maxRecordDocIdList was not being set, which means that for all records with the given fingerprint, it must be that:
        # (thisPingDate, numAppSessionsPreviousOnThisPingDate, currentSessionTime) < ("0000-00-00",0,0)
        # this should only be possible if there is a bad thisPingDate, in which case we will discard the fingerprint
        if maxRecordDocIdList:
            if len(maxRecordDocIdList)==1:
                docIdOut = maxRecordDocIdList[0]

                self.increment_counter("REDUCER", "unique naive head records")
                self.increment_counter("REDUCER", "FINAL HEAD RECORD docIds OUT")

            if len(maxRecordDocIdList)>1:
                docIdOut = random.choice(maxRecordDocIdList)

                self.increment_counter("REDUCER", "parts with records tied for naive head")
                self.increment_counter("REDUCER", "records tied for naive head",len(maxRecordDocIdList))
                self.increment_counter("REDUCER", "FINAL HEAD RECORD docIds OUT")

            yield(docIdOut, "h")




if __name__ == '__main__':
    headRecordExtractionJob.run()


