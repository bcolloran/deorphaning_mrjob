import simplejson
import json

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

def dictToSortedStr(objIn):
    if isinstance(objIn,dict):
        return [str(key)+dictToSortedStr(val) for key,val in sorted(objIn.items())]
    else:
        return str(objIn)




def getDatePrintsAndTieBreakInfo(payload,jobObj,fhrVer):
    #NOTE: we drop any packet without data.days entries. these cannot be fingerprinted/linked.
    try:
        dataDaysDates = payload["data"]["days"].keys()
    except:
        jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "no dataDaysDates")
        jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "REJECTED RECORDS")
        return (None,None)
    try:
        thisPingDate = payload["thisPingDate"]
    except:
        jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "no thisPingDate")
        jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "REJECTED RECORDS")
        return (None,None)

    if fhrVer=="3":
        try:
            currentEnvHash = payload['environments']['current']['hash']
        except:
            jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "without current env hash")
            jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "REJECTED RECORDS")
            return (None,None)


    try:
        if fhrVer=="3":
            numAppSessionsOnThisPingDate = len(payload['data']['days'][thisPingDate][currentEnvHash]['org.mozilla.appSessions'].get('normal',[0])) + len(payload['data']['days'][thisPingDate][currentEnvHash]['org.mozilla.appSessions'].get('abnormal',[]))
        elif fhrVer=="2":
            numAppSessionsOnThisPingDate = len(payload["data"]["days"][thisPingDate]['org.mozilla.appSessions.previous']["main"])
    except:
        jobObj.increment_counter("v%s MAP WARNING"%fhrVer, "no numAppSessionsOnThisPingDate")
        numAppSessionsOnThisPingDate = 0

    try:
        if fhrVer=="3":
            currentSessionTime=payload['data']['days'][thisPingDate][currentEnvHash]['org.mozilla.appSessions'].get('normal',[0])[-1]["d"]
        elif fhrVer=="2":
            currentSessionTime=payload["data"]["last"]['org.mozilla.appSessions.current']["totalTime"]
    except KeyError:
        currentSessionTime = 0
        jobObj.increment_counter("v%s MAP WARNING"%fhrVer, "no currentSessionTime, KeyError")
    except TypeError:
        currentSessionTime = 0
        jobObj.increment_counter("v%s MAP WARNING"%fhrVer, "no currentSessionTime, TypeError")
    #NOTE: we will use profile creation date to add further refinement to date collisions, but it is not required.

    try:
        if fhrVer=="3":
            profileCreation = payload['environments']['current']['org.mozilla.profile.age']['profileCreation']
        elif fhrVer=="2":
            profileCreation = payload['data']['last']['org.mozilla.profile.age']['profileCreation']
    except KeyError:
        profileCreation = "00000"
        jobObj.increment_counter("v%s MAP WARNING"%fhrVer, "no profileCreation")


    datePrints = []
    for date in dataDaysDates:
        try:
            if fhrVer=="3":# was getting  "AttributeError: 'float' object has no attribute 'keys'"
                datePrints.append( str(profileCreation)+"_"+date+"_"+str(hash(simplejson.dumps(payload["data"]["days"][date],sort_keys=True))) )
            elif fhrVer=="2":
                if 'org.mozilla.appSessions.previous' in payload["data"]["days"][date].keys():
                    datePrints.append( str(profileCreation)+"_"+date+"_"+str(hash(simplejson.dumps(payload["data"]["days"][date],sort_keys=True))) )
        except:
            jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "bad datePrints")
            jobObj.increment_counter("v%s MAP ERROR"%fhrVer, "REJECTED RECORDS")
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

        self.increment_counter("MAPPER", "INPUT (docId,payload)")
        try:
            if any(char not in "abcdef0123456789-" for char in str(key).lower()):
                self.increment_counter("MAP ERROR", "docId contains invalid character")
                self.increment_counter("MAP ERROR", "REJECTED RECORDS")
                return
            else:
                docId = str(key).lower()
        except:
            self.increment_counter("MAP ERROR", "error checking docId for invalid characters")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        try:
            payload = simplejson.loads(rawJsonIn)
        except:
            #added this block because as a fall back:
            #simplejson was failing to parse ~70k records that jackson parsed ok
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
            self.increment_counter("v2 MAP INFO", "fhr v2 records")
            try:
                datePrints, tieBreakInfo = getDatePrintsAndTieBreakInfo(payload,self,fhrVer)
            except:
                print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[0], sys.exc_info()[1]
                traceback.print_exc(file = sys.stderr)
                self.increment_counter("v2 MAP ERROR", "getDatePrintsAndTieBreakInfo_v2 failed")
                return
        elif fhrVer=="3":
            # yield  "v"+fhrVer+"/kDocId_vPartId_init|"+docId, "p"+docId
            self.increment_counter("v3 MAP INFO", "fhr v3 records")
            datePrints, tieBreakInfo = getDatePrintsAndTieBreakInfo(payload, self, fhrVer)
        else:
            self.increment_counter("MAP ERROR", "fhr version not 2 or 3")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        # yield  "v"+fhrVer+"/kDocId_vPartId_init|"+docId, "p"+docId
        ### emit datePrints
        if tieBreakInfo:
            if datePrints:
                for dp in datePrints:
                    self.increment_counter("v%s MAP INFO"%fhrVer, "v%s (datePrint;docId) out"%fhrVer)
                    yield  "|".join(["kDatePrint_vDocId",fhrVer,dp]), docId
                #if there ARE datePrints, then the record IS linkable, so we'll need to emit tieBreakInfo
                self.increment_counter("v%s MAP INFO"%fhrVer, "v%s (docId;tieBreakInfo) out"%fhrVer)
                yield  "|".join(["kDocId_vTieBreakInfo",fhrVer,docId]),tieBreakInfo
                # "PASSv"+fhrVer+"/kDocId_vTieBreakInfo|"+docId,  tieBreakInfo
            else:
                # NOTE: if there are NO date prints in a record, the record cannot be linked to any others. pass it through with it's own part already determined. no tieBreakInfo is needed
                self.increment_counter("v%s MAP INFO"%fhrVer, "unlinkable; no datePrints")
                yield  "|".join(["unlinkable",fhrVer,docId]),   "u"
                return
        else:
            #if there is no tieBreakInfo, the packet is bad.
            return




    def reducer(self, keyIn, valIter):
        try:
            kvType,fhrVer,key = keyIn.split("|")
        except:
            # try:
            #     yield "errors/reducerError|%s"%str(keyIn).replace("|","_").replace("/","_"), str(list(valIter))
            # except:
            #     self.increment_counter("REDUCER ERROR", "bad reducer input, could not save to HDFS")
            #     return
            # self.increment_counter( "REDUCER ERROR", "bad reducer input, could not even come close to saving to HDFS" )
            try:
                self.increment_counter( "REDUCER ERROR", "bad reducer input: "+str(keyIn) )
                return
            except:
                self.increment_counter( "REDUCER ERROR", "bad reducer input" )
                return

        if fhrVer not in ["2","3"]:
            try:
                self.increment_counter( "REDUCER ERROR", "bad reducer version: "+str(keyIn) )
                return
            except:
                self.increment_counter( "REDUCER ERROR", "bad reducer version" )
                return

        if ("/" in key) or ("|" in key):
            try:
                self.increment_counter( "REDUCER ERROR", "bad reducer key: "+str(keyIn) )
                return
            except:
                self.increment_counter( "REDUCER ERROR", "bad reducer key" )
                return


        #pass tieBreakInfo and unlinkable k/v pairs straight through
        #k/v pairs recieved for "unlinkable" and "kDocId_vTieBreakInfo" should be unique EXCEPT in the case of identical docIds and exactly duplicated records. in these cases, it suffices to re-emit the first element of each list
        if kvType=="unlinkable" or kvType=="kDocId_vTieBreakInfo":
            self.increment_counter("v%s REDUCER"%fhrVer, "%s passed through reducer"%kvType)
            yield "v%s/%s|%s"%(fhrVer,kvType,key), next(valIter)

        elif kvType== 'kDatePrint_vDocId':
            self.increment_counter("v%s REDUCER"%fhrVer, "datePrint key into reducer")
            # in this case, we have a datePrint key. this reducer does all the datePrint linkage and initializes the parts; choose the lowest corresponding docId to initialize the part. subsequent steps will link parts through docs, and relabel docs into the lowest connected part
            linkedDocIds = list(valIter)
            partNum = min(linkedDocIds)
            for docId in linkedDocIds:
                yield "v%s/kDoc_vPart_0|%s"%(fhrVer,docId),  "p"+partNum
                self.increment_counter("v%s REDUCER"%fhrVer, "kDoc_vPart_0 out from reducer")

        else:
            self.increment_counter("REDUCER ERROR", "bad key type")



if __name__ == '__main__':
    ScanJob.run()