import os
import hashlib
from google.cloud import firestore
client = firestore.Client()

MAPPER_ID = os.environ.get('MAPPER_ID')

def set_mapper_status(status):
    """
        Updated mapper status to Ready/Processing.
    """
    doc = client.collection("node-status").document(MAPPER_ID)
    doc.set({
            u'is_ready': status
        })
    if not status:
        print(f"Mapper Status: Processing")
    else:
        print(f"Mapper Status: Ready")

def parse_event(event):
    """
        Returns content to be Mapped.
    """
    data = event["value"]["fields"]["data"]["stringValue"]
    filename = event["value"]["fields"]["filename"]["stringValue"]
    offset = event["value"]["fields"]["offset"]["integerValue"]
    return filename, data, offset

def get_doc_status(event):
    """
        Returns (boolean):
            True: if doc is already processed by some mapper
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

def get_bucket_name(text):
    
    numbers = "0123456789"
    char = text.strip()[0]
    char = char.lower()
    if char in numbers:
        return str(hashlib.md5("1".encode()).hexdigest())
    f_name = str(hashlib.md5(char.encode()).hexdigest())
    return f_name

def process_data(filename, data, offset):

    out = {}
    counter = int(offset)
    for word in data.split():
        if word:
            # Grouping Data
            bucket = get_bucket_name(word)
            if bucket in out:
                out[bucket].append({
                    "word":word,
                    "filename":filename,
                    "position":counter
                })
            else:
                out[bucket] = [{
                    "word":word,
                    "filename":filename,
                    "position":counter
                }]
            counter += 1
    return out

def store_mapper_output(mapped_output):
    """
        Storing mapper intermediate data.
    """
    try:
        for k,v in mapped_output.items():

            fs_handler = client.collection("mapper-output").document(k)
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

        print(f"Starting Mapping on: {MAPPER_ID}")
        
        # Setting status of mapper as busy
        set_mapper_status(False)

        # Starting Mapping Process
        filename, data, offset = parse_event(event)
        mapped_output = process_data(filename, data, offset)

        # Store Mapper Output
        resp = store_mapper_output(mapped_output)
        if resp:
            set_doc_status(True, resource_string)
        
        # Setting status of mapper as Ready
        set_mapper_status(True)        
    else:
        print(f"Duplicate Trigger, Document has been processed already.")