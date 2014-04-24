import os

def multipleOutputSim(path):
    os.chdir(path)
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    outFileStrs = dict()
    for inFileName in files:
        with open(inFileName) as inFile:
            for line in inFile:
                pathKey,val = line.replace('"',"").strip().split("\t")
                path,key = pathKey.split("|")
                # print line
                # print path,key,val
                if not path in outFileStrs:
                    outFileStrs[path]=[key+"\t"+val]
                else:
                    outFileStrs[path].append( "\n"+key+"\t"+val )

    for path in outFileStrs:
        print os.getcwd()
        print path
        if not os.path.exists(path):
            os.makedirs(path)
        with open(path+"/part-00000","w") as outFile:
            outFile.write( "".join(outFileStrs[path]) )
