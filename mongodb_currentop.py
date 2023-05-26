#!/usr/bin/env python3

import os,sys
import pymongo
import logging,time
PATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH)
from ding import dingError

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=time.strftime("%Y-%m-%d.log", time.localtime()),
                    filemode='a')

def main():
    info = []
    with open("mongodb.toml",'r') as f:        
        for l in f:
            if any(s in l for s in ["labels","mongodb_uri"]):
                info.append(l.strip())

    i = 0
    while i < len(info)//2:
        lables = info[2*i]
        mongodb_uri = info[2*i+1]
        i+=1

        try:
            client = pymongo.MongoClient(mongodb_uri,serverSelectionTimeoutMS=10000
            )

            query = [
                {"$currentOp": {"allUsers": True, "idleSessions": True}},
                {
                    "$match": {
                        "active": True,
                        "secs_running": {
                            "$gte": 5
                        },
                        "ns": {"$ne": "admin.$cmd"},
                        # "op": {"$in": allowed_ops},
                    }
                },
            ]

            with client.admin.aggregate(query) as cursor:
                for op in cursor:
                    msg = '''
【Mongodb 慢语句】
currentOpTime = %s
opid = %d
secs_running = %d
op = %s
ns = %s
command = %s
                    ''' % (op['currentOpTime'],op['opid'],op['secs_running'],op['op'],op['ns'],op['command'])
                    # print(msg)
                    dingError(msg)

                    # res = client.command({"killOp": 1, "op": op["opid"]})

        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    main()
    # pass
