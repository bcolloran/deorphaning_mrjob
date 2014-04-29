from mrjob.job import MRJob
import mrjob
import sys, codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)



#for each part, this job finds all the docs and other parts that touch the first doc. then, out of thatset of

#partSet is conceptually a set, but will be implemented as a "|" separated set of strings for compatibility reasons
class relabelDocsJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.TextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def mapper(self,partId, docId_orPartSet):
        # docId_orPartSet will be like either
        #     "c232ecdc-a56a-41be-a78a-aadf748f9408"
        # OR a comma separated string like:
        #     p{id1},p{id2},...
        # want to pass out ONLY TUPLES
        if partId[0]!="p":
            raise ValueError()

        self.increment_counter("MAPPER", "(partId,docId_orPartSet) in")
        if docId_orPartSet[0]=="p":
            self.increment_counter("MAPPER", "(partId, partSet) out")
        else:
            self.increment_counter("MAPPER", "(partId, docId) out")
        yield(partId,docId_orPartSet)





    def combiner(self,partId,iterOfDocIdSet_orPartSet):
        # iterOfDocIdSet_orPartSet will be like 
        #     ["docId_1", "p_1|p_2|...", "p_n+1|p_n+2|...", "docId_2|docId_3|...", ...]
        # want to emit
        #     partId,  "docId_1,docId_2,..."
        # and
        #     partId,  "p_1,p_2,..."
        partSetOut = set()
        docSetOut = set()
        for docIdSet_orPartSet in iterOfDocIdSet_orPartSet:
            if docIdSet_orPartSet[0]=="p":
                partSetOut |= set(docIdSet_orPartSet.split("|"))
            elif docIdSet_orPartSet[0].lower() in list("0123456789abcdef"):
                docSetOut |= set(docIdSet_orPartSet.split("|"))
            else:
                print "bad docId_orPartSet:",docIdSet_orPartSet
                raise ValueError()

        if len(docSetOut)>0:
            yield(partId,"|".join(docSetOut))
        if len(partSetOut)>0:
            yield(partId,"|".join(docSetOut))




    def reducer(self,partId,iterOfDocIdSet_orPartSet):
        # iterOfDocIdSet_orPartSet will be like 
        #     [("docId_1"),('p_1','p_2',...),('p_n+1','p_n+2',...),("docId_2"),...]
        # want to emit
        #     [("docId_1",...),('p_1','p_2',...),...]
        self.increment_counter("REDUCER", "distinct parts at this iter")
        partSet = set([partId])
        docSet = set()

        for docIdSet_orPartSet in iterOfDocIdSet_orPartSet:
            if docIdSet_orPartSet=="p":
                partSet |= set(docIdSet_orPartSet.split("|"))
            elif docIdSet_orPartSet[0].lower() in list("0123456789abcdef"):
                docSet |= set(docIdSet_orPartSet.split("|"))
            else:
                print "bad docId_orPartSet:",docIdSet_orPartSet
                raise ValueError()

        lowPart=min(partSet)

        for docId in docSet:
            yield(docId,lowPart)
            self.increment_counter("REDUCER", "docIds out")


if __name__ == '__main__':
    relabelDocsJob.run()
