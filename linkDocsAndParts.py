from mrjob.job import MRJob
import mrjob



#partSet is conceptually a set, but will be implemented as a "|" separated set of strings for compatibility reasons
class linkDocsAndPartsJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol


    def mapper(self,docId, partId):
        self.increment_counter("MAPPER", "(docId,partId) in")
        yield docId, [partId]


    def combiner(self,docId, partIdIter):
        # partIdIter should always reach the combiner as an iter of lists like:
        #     [["pid_1"],["pid_2"],...].
        # need to union these tuples and emit ["pid_1","pid_2",...]
        partSetOut = set()
        for partIdList in partIdIter:
            partSetOut |= set(partIdList)
        yield(docId,list(partSetOut))



    def reducer(self,docId, partSetIter):
        # partIdIter should always reach the reducer as an iter of lists like:
        #     [["pid_1,1","pid_1,2",...],["pid_2"],["pid_3,1",...]...].

        linkedParts = set()
        for partSet in partSetIter:
            linkedParts |= set(partSet)

        lowPart = min(linkedParts)

        yield(lowPart,docId)
        self.increment_counter("REDUCER", "(lowPart,docId) out")

        if len(linkedParts)==1:
            #if there is only one part linked to this docId, there are no overlaps here, so no part-to-part touches are emitted
            self.increment_counter("REDUCER", "docs in only 1 part")
        else:
            #otherwise, return all the overlaps
            yield(lowPart,"|".join(linkedParts))
            self.increment_counter("REDUCER", "OVERLAPPING_PARTS")



if __name__ == '__main__':
    linkDocsAndPartsJob.run()

