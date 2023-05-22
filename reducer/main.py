import os
from google.cloud import firestore
client = firestore.Client()

REDUCER_ID = os.environ.get('REDUCER_ID')

def set_reducer_status(status):
    """
        Updated reducer status to Ready/Processing.
    """
    doc = client.collection("node-status").document(REDUCER_ID)
    doc.set({
            u'is_ready': status
        })
    if not status:
        print(f"Reducer Status: Processing")
    else:
        print(f"Reducer Status: Ready")

# def parse_event(event):
#     """
#         Returns content to be Reduced.
#     """
#     doc_id = event["value"]["fields"]["doc_id"]["stringValue"]
#     return doc_id

def get_doc_status(event):
    """
        Returns (boolean):
            True: if doc is already processed by some reducer
            False: if doc is not yet processed

    """
    doc_status = event["value"]["fields"]["is_processed"]["booleanValue"]
    print(f"Doc Status:{doc_status}, Type:{type(doc_status)}")
    return doc_status

def set_doc_status(status, resource_str):
    """
        Updated doc status to Processed/Not-Processed.
    """
    path_parts = resource_str.split('/documents/')[1].split('/')
    collection_path = path_parts[0]
    document_path = path_parts[1].strip(".")

    doc = client.collection(collection_path).document(document_path)
    doc.update({
            u'is_processed': status
        })

def process_data(event):

    doc_id = event["value"]["fields"]["doc_id"]["stringValue"]
    data = client.collection("mapper-output").document(doc_id).get()
    if data.exists:
        data = data.to_dict()
        data = data.get("data",[])
        out = {}
        for row in data:
            f, w = row["filename"], row["word"]
            if w in out:
                if f in out[w]:
                    out[w][f] += 1
                else:
                    out[w][f] = 1
            else:
                out[w] = {
                    f: 1
                }

        counter = 0
        for k,v in out.items():
            
            doc_ref = client.collection("inverted-index").document(k)
            doc_ref.set({
                "meta":v
            })
            counter += 1
        print(f"{REDUCER_ID} Processed: {counter} items.")
    else:
        print(f"Error: {REDUCER_ID}, given doc_id: {doc_id} doesn't exist.")


def store_reducer_output(mapped_output):
    """
        Storing reducer intermediate data.
    """
    try:
        for k,v in mapped_output.items():

            fs_handler = client.collection("reducer-output").document(k)
            doc = fs_handler.get()
            if doc.exists:
                fs_handler.update({u"data":firestore.ArrayUnion(v)})
            else:
                fs_handler.set({u"data":v})
        return "Successful"
    except Exception as e:
        print(f"Exception:{e}")
        return None


def hello_firestore(event, context):
    """Triggered by a change to a Firestore document.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    resource_string = context.resource
    print(f"Function triggered by change to: {resource_string}.")

    if not get_doc_status(event):

        print(f"Starting Reducer on: {REDUCER_ID}")
        
        # Setting status of reducer as busy
        set_reducer_status(False)

        # Starting Reducing Process
        process_data(event)
        set_doc_status(True, resource_string)
        
        # Setting status of reducer as Ready
        set_reducer_status(True)        
    else:
        print(f"Duplicate Trigger, Document has been processed already.")