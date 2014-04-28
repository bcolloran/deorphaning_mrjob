import simplejson as json

from mrjob.job import MRJob
# from mrjob.launch import _READ_ARGS_FROM_SYS_ARGV
import sys, codecs
import traceback
import mrjob

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

def dictToSortedTupList(objIn):
    if isinstance(objIn,dict):
        return [(key,dictToSortedTupList(val)) for key,val in sorted(objIn.items(),key=lambda item:item[0])]
    else:
        return objIn



def getDatePrintsAndTieBreakInfo_v2(payload,jobObj):
    #NOTE: we drop any packet without data.days entries. these cannot be fingerprinted/linked.
    try:
        dataDaysDates = payload["data"]["days"].keys()
    except:
        jobObj.increment_counter("MAP ERROR", "no dataDaysDates")
        jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
        return (None,None)
    try:
        thisPingDate = payload["thisPingDate"]
    except:
        jobObj.increment_counter("MAP ERROR", "no thisPingDate")
        jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
        return (None,None)

    try:
        numAppSessionsPreviousOnThisPingDate = len(payload["data"]["days"][thisPingDate]['org.mozilla.appSessions.previous']["main"])
    except KeyError:
        jobObj.increment_counter("MAP WARNING", "no ['...appSessions.previous']['main'] on thisPingDate")
        numAppSessionsPreviousOnThisPingDate = 0
    except TypeError:
        #was getting "TypeError: 'float' object is unsubscriptable" errors in the above. this should not happen, and must indicate a bad packet, which we will discard
        jobObj.increment_counter("MAP ERROR", "float instead of obj in ['...appSessions.previous']['main'] on thisPingDate  ")
        jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
        return (None,None)

    try:
        currentSessionTime=payload["data"]["last"]['org.mozilla.appSessions.current']["totalTime"]
    except KeyError:
        currentSessionTime = 0
        jobObj.increment_counter("MAP WARNING", "no currentSessionTime")

    #NOTE: we will use profile creation date to add further refinement to date collisions, but it is not required.
    try:
        profileCreation = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]
    except:
        profileCreation = "00000"
        jobObj.increment_counter("MAP WARNING", "no profileCreation")

    datePrints = []
    for date in dataDaysDates:
        try:
            # was getting  "AttributeError: 'float' object has no attribute 'keys'"
            if 'org.mozilla.appSessions.previous' in payload["data"]["days"][date].keys():
                datePrints.append( str(profileCreation)+"_"+date+"_"+str(hash(str(dictToSortedTupList(payload["data"]["days"][date])))) )
        except:
            jobObj.increment_counter("MAP ERROR", "$data$days[date] is a float")
            jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return (None,None)

    ### emit tieBreakInfo
    # jobObj.increment_counter("MAPPER", "(docId,tieBreakInfo) out")
    tieBreakInfo = "_".join([thisPingDate,str(numAppSessionsPreviousOnThisPingDate),str(currentSessionTime)])

    return (datePrints,tieBreakInfo)








def getDatePrintsAndTieBreakInfo_v3(payload,jobObj):
    #NOTE: we drop any packet without data.days entries. these cannot be fingerprinted/linked.
    try:
        dataDaysDates = payload["data"]["days"].keys()
    except:
        jobObj.increment_counter("MAP ERROR", "no dataDaysDates")
        jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
        return (None,None)
    try:
        thisPingDate = payload["thisPingDate"]
    except:
        jobObj.increment_counter("MAP ERROR", "no thisPingDate")
        jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
        return (None,None)
    try:
        currentEnvHash = payload['environments']['current']['hash']
    except:
        jobObj.increment_counter("MAP ERROR", "v3 without current env hash")
        jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
        return (None,None)


    try:
        numAppSessionsOnThisPingDate = len(payload['data']['days'][thisPingDate][currentEnvHash]['org.mozilla.appSessions'].get('normal',[0])) + len(payload['data']['days'][thisPingDate][currentEnvHash]['org.mozilla.appSessions'].get('abnormal',[]))
    except:
        jobObj.increment_counter("MAP WARNING", "v3: no numAppSessionsOnThisPingDate")
        numAppSessionsOnThisPingDate = 0
    try:
        currentSessionTime=payload['data']['days'][thisPingDate][currentEnvHash]['org.mozilla.appSessions'].get('normal',[0])[-1]["d"]
    except KeyError:
        currentSessionTime = 0
        jobObj.increment_counter("MAP WARNING", "v3: no currentSessionTime, KeyError")
    except TypeError:
        currentSessionTime = 0
        jobObj.increment_counter("MAP WARNING", "v3: no currentSessionTime, TypeError")
    #NOTE: we will use profile creation date to add further refinement to date collisions, but it is not required.
    try:
        profileCreation = payload['environments']['current']['org.mozilla.profile.age']['profileCreation']
    except:
        profileCreation = "00000"
        jobObj.increment_counter("MAP WARNING", "v3: no profileCreation")


    datePrints = []
    for date in dataDaysDates:
        try:
            # was getting  "AttributeError: 'float' object has no attribute 'keys'"
            datePrints.append( str(profileCreation)+"_"+date+"_"+str(hash(str(dictToSortedTupList(payload["data"]["days"][date])))) )
        except:
            jobObj.increment_counter("MAP ERROR", "v3: bad datePrints")
            jobObj.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return (None,None)

    ### emit tieBreakInfo
    # jobObj.increment_counter("MAPPER", "(docId,tieBreakInfo) out")
    tieBreakInfo = "_".join([thisPingDate,str(numAppSessionsOnThisPingDate),str(currentSessionTime)])

    return (datePrints,tieBreakInfo)










class ScanJob(MRJob):
    HADOOP_INPUT_FORMAT="org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    HADOOP_OUTPUT_FORMAT='test.NaiveMultiOutputFormatTwo'

    def configure_options(self):
        super(ScanJob, self).configure_options()

        # self.add_passthrough_option('--start-date', help = "Specify start date",
        #                             default = datetime.now().strftime("%Y-%m-%d"))



    def mapper(self, key, rawJsonIn):
        docId = key
        self.increment_counter("MAPPER", "INPUT (docId,payload)")

        try:
            payload = json.loads(rawJsonIn)
        except:
            # print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[0], sys.exc_info()[1]
            # traceback.print_exc(file = sys.stderr)
            self.increment_counter("MAP ERROR", "record failed to parse")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        try:
            fhrVer=str(payload["version"])
        except:
            print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[0], sys.exc_info()[1]
            traceback.print_exc(file = sys.stderr)
            self.increment_counter("MAP ERROR", "no version")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        if fhrVer == "2":
            self.increment_counter("MAPPER", "fhr v2 records")
            try:
                datePrints, tieBreakInfo = getDatePrintsAndTieBreakInfo_v2(payload,self)
            except:
                print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[0], sys.exc_info()[1]
                traceback.print_exc(file = sys.stderr)
                self.increment_counter("MAP ERROR", "getDatePrintsAndTieBreakInfo_v2 failed")
                return
        elif fhrVer=="3":
            # yield  "v"+fhrVer+"/kDocId_vPartId_init|"+docId, "p"+docId
            self.increment_counter("MAPPER", "fhr v3 records")
            datePrints, tieBreakInfo = getDatePrintsAndTieBreakInfo_v3(payload,self)
            # try:
            #     datePrints, tieBreakInfo = getDatePrintsAndTieBreakInfo_v3(payload,self)
            # except:
            #     print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[0], sys.exc_info()[1]
            #     traceback.print_exc(file = sys.stderr)
            #     self.increment_counter("MAP ERROR", "getDatePrintsAndTieBreakInfo_v3 failed")
            #     return
        else:
            self.increment_counter("MAP ERROR", "fhr version not 2 or 3")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        # yield  "v"+fhrVer+"/kDocId_vPartId_init|"+docId, "p"+docId
        ### emit datePrints
        if tieBreakInfo:
            if datePrints:
                for dp in datePrints:
                    self.increment_counter("MAPPER", "(datePrint;docId) out")
                    yield  "|".join(["kDatePrint_vDocId",fhrVer,dp]), docId
                #if there ARE datePrints, then the record IS linkable, so we'll need to emit tieBreakInfo
                self.increment_counter("MAPPER", "(docId;tieBreakInfo) out")
                yield  "|".join(["kDocId_vTieBreakInfo",fhrVer,docId]),tieBreakInfo
                # "PASSv"+fhrVer+"/kDocId_vTieBreakInfo|"+docId,  tieBreakInfo
            else:
                # NOTE: if there are NO date prints in a record, the record cannot be linked to any others. pass it through with it's own part already determined. no tieBreakInfo is needed
                self.increment_counter("MAPPER", "v"+fhrVer+" unlinkable; no datePrints")
                yield  "|".join(["unlinkable",fhrVer,docId]),   "p"+docId
                return
        else:
            #if there is no tieBreakInfo, the packet is bad.
            return


    def reducer(self, keyIn, valIter):
        kvType,fhrVer,key = keyIn.split("|")

        #pass tieBreakInfo and unlinkable k/v pairs straight through
        #k/v pairs recieved for "unlinkable" and "kDocId_vTieBreakInfo" should be unique EXCEPT in the case of identical docIds and exactly duplicated records. in these cases, it suffices to re-emit the first element of each list
        if kvType=="unlinkable" or kvType=="kDocId_vTieBreakInfo":
            self.increment_counter("REDUCER", "%s v%s passed through reducer"%(kvType,fhrVer))
            yield "v%s/%s|%s"%(fhrVer,kvType,key), list(valIter)[0]

        elif kvType== 'kDatePrint_vDocId':
            self.increment_counter("REDUCER", "datePrint key into reducer")
            # in this case, we have a datePrint key. this reducer does all the datePrint linkage and initializes the parts; choose the lowest corresponding docId to initialize the part. subsequent steps will link parts through docs, and relabel docs into the lowest connected part
            linkedDocIds = list(valIter)
            partNum = min(linkedDocIds)
            for docId in linkedDocIds:
                yield "v%s/kDoc_vPart_0|%s"%(fhrVer,docId),  "p"+partNum
                self.increment_counter("REDUCER", "kDoc_vPart_0 v"+fhrVer+", out from reducer")

        else:
            self.increment_counter("REDUCER ERROR", "bad key type")



if __name__ == '__main__':
    ScanJob.run()