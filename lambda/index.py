import boto3, os, queue, threading

ddbclient   = boto3.client('dynamodb')
res         = []

ddbtable    = os.environ['ddbtable'] 
ddbpk       = os.environ['ddbprimarykey']
ddbsk       = os.environ['ddbsecondarykey']

# create a queue
q1     	    = queue.Queue()

# worker for queue jobs
def worker():
    while not q1.empty():
        del_items(q1.get())
        q1.task_done()

# scan all messages in the table
def get_scan(pk, sk):
    scan    = []
    res     = []

    if sk != '':
        attribs = [pk, sk]

    else:
        attribs = [pk]

    # scan the table and retrieve only the neccesary attributes
    x = ddbclient.scan(TableName = ddbtable, AttributesToGet = attribs)
    scan.append(x['Items'])

    while 'LastEvaluatedKey' in scan:
        x = ddbclient.scan(TableName = ddbtable, AttributesToGet = attribs, ExclusiveStartKey = x['LastEvaluatedKey'])
        scan.append(x['Items'])

    # print results
    for x in scan:
        l = len(x)

    for y in range(l):
        pk1 = x[y][pk]['S']
        sk1 = x[y][sk]['S']
        res.append([pk1, sk1])
        q1.put([pk, pk1, sk, sk1])
                    
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
