import boto3, os, queue, threading

ddbclient   = boto3.client('dynamodb')
res         = []

ddbtable    = os.environ['ddbtable'] 
ddbpk       = os.environ['ddb_primary_key']
ddbsk       = os.environ['ddb_secondary_key']

# create a queue
q1     	    = queue.Queue()

# worker for queue jobs
def worker():
    while not q1.empty():
        del_items(q1.get())
        q1.task_done()

# scan all messages in the table
def get_scan(pkey, skey):
    scan    = []
    res     = []

    if skey != '':
        attribs = [pkey, skey]

    else:
        attribs = [pkey]

    # scan the table and retrieve only the neccesary attributes
    x = ddbclient.scan(TableName = ddbtable, AttributesToGet = attribs)
    for y in x['Items']:
        scan.append(y)

    while 'LastEvaluatedKey' in scan:
        x = ddbclient.scan(TableName = ddbtable, AttributesToGet = attribs, ExclusiveStartKey = x['LastEvaluatedKey'])
        for y in x['Items']:
            scan.append(y)

    # retrieve pk and sk values
    for y in scan:
        pkvalue = y[pkey]['S']
        skvalue = y[skey]['S']
        res.append([pkvalue, skvalue])
        q1.put([pkey, pkvalue, skey, skvalue])
                    
    for x in range(10):
        t = threading.Thread(target = worker)
        t.daemon = True
        t.start()
    q1.join()
    
def del_items(x):
    pktitle = x[0]
    pkvalue = x[1]
    sktitle = x[2]
    skvalue = x[3]
    
    ddbkey = {
        pktitle: {"S": pkvalue},
        sktitle: {"S": skvalue}
    }
    
    ddbclient.delete_item(
        TableName = ddbtable,
        Key = ddbkey
    )

    print("deleted "+ str(ddbkey))

# lambda handler
def handler(event, context):
    get_scan(ddbpk, ddbsk)
