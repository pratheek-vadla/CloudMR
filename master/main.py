import re
from time import sleep
import functions_framework
from google.cloud import storage
from google.cloud import firestore

firestore_client = firestore.Client()

def clean_data(text):
    
    clean_text = text.replace("\n"," ").replace("\t"," ").replace("\r", " ")
    clean_text = re.sub(r'[^A-Za-z0-9]+'," ", clean_text)
    clean_text = re.sub(r'\s+'," ",clean_text).strip()
    return clean_text

def split_text(input_text, chunks):

    out = []
    chunk_size = len(input_text)//chunks
    print(f"Input Size:{len(input_text)}, Chunk Size:{chunk_size}")
    offset = 0
    counter = 0
    position_offset = 0
    while counter < chunks and offset < len(input_text):
        # print(f"counter:{counter}")
        start = offset
        end = offset + chunk_size
        # Ensuring that the string is split at whitespace, not in between word.
        while end < len(input_text) and input_text[end] != " ":
            end += 1
        temp = input_text[start:end].strip()
        if not temp:
            break
        token_count = len(temp.split())
        out.append({
            "data":temp,
            "offset":position_offset
        })
        offset = end 
        counter += 1
        position_offset += token_count
    return out

def fetch_data_from_gcs(project_id, bucket_name, mapper_count):
    """
        Downloads a blob into memory.
    """
    out = []
    storage_client = storage.Client(project=project_id)
    try:
        for blob in storage_client.list_blobs(bucket_name):
            blob_name = blob.name
            # print(f"blob-name:{blob_name}")
            contents = blob.download_as_string()
            # print(f"content-type:{type(contents)}")
            contents = clean_data(str(contents).lower())
            if not contents:
                print("Contents not found!")
                break
            for chunk in split_text(contents, mapper_count):
                chunk["filename"] = blob_name
                out.append(chunk)
        return out
    except Exception as e:
        print(f"Exception In Fetch Data: {e}")
        return None

def fetch_data_from_firestore(collection, mapper_count):

    try:
        out = []
        docs = firestore_client.collection(collection).stream()
        for doc in docs:
            doc_id = doc.id
            data = doc.to_dict()
            contents = data.get("text","")
            # print(f"content-type:{type(contents)}")
            contents = clean_data(str(contents).lower())
            if not contents:
                print("Contents not found!")
                break
            for chunk in split_text(contents, mapper_count):
                chunk["filename"] = doc_id
                out.append(chunk)
        return out
    except Exception as e:
        print(f"Exception In Fetch Data: {e}")
        return None

# def send_data_to_mapper(mapper_id, data):

#     data["is_processed"] = False
#     update_time, doc_ref = firestore_client.collection(mapper_id).add(data)
#     print(f"Sent data to: {mapper_id} at: {update_time} doc_id: {doc_ref}")

# def send_data_to_reducer(reducer_id, data):

#     data["is_processed"] = False
#     update_time, doc_ref = firestore_client.collection(reducer_id).add(data)
#     print(f"Sent data to: {mapper_id} at: {update_time} doc_id: {doc_ref}")

def send_data_to_node(node_id, data):

    data["is_processed"] = False
    update_time, doc_ref = firestore_client.collection(node_id).add(data)
    print(f"Sent data to: {node_id} at: {update_time} doc_id: {doc_ref}")

def init_node_status(node_count, node_type):

    for i in range(node_count):
        node_id = f"{node_type}-{i}"
        print(f"Initialising {node_id}")
        doc = firestore_client.collection("node-status").document(node_id)
        doc.set({
            "is_ready":True
        })

    doc = firestore_client.collection("node-status").document(f"{node_type}-count")
    doc.set({
            "value":node_count
        })

def set_job_status(status):

    doc = firestore_client.collection("node-status").document("job_status")
    doc.set({
        "is_running":status
    })

def check_node_status(node_id):

    doc = firestore_client.collection("node-status").document(node_id)
    doc = doc.get()
    if doc.exists:
        doc = doc.to_dict()
        return doc.get("is_ready")
    return None

def get_firestore_doc_ids(collection):

    items = firestore_client.collection(collection).select(field_paths=[]).get()
    ids = [item.id for item in items]
    return ids


@functions_framework.http
def master(request):

    config_json = request.get_json()
    mappers = config_json["mappers"]
    reducers = config_json["reducers"]
    firestore_collection = config_json["firestore_collection"]
    # project_id = config_json["project_id"]

    print(f"request:{config_json}")

    init_node_status(node_count=mappers, node_type="mapper")

    set_job_status(True)

    chunks = fetch_data_from_firestore(firestore_collection, mappers*5)
    # print(f"Length of chunks: {len(chunks)}")

    ########## Mapping task ##########

    while len(chunks) != 0:
        
        for i in range(mappers):
            if len(chunks) == 0:
                break
            mapper_id = f"mapper-{i}"
            status = check_node_status(node_id=mapper_id)
            if status:
                data = chunks.pop(0)
                send_data_to_node(mapper_id, data)
            elif status == None:
                print(f"mapper-status for: {mapper_id} Not Found!")
            else:
                # print("Waiting for mappers to finish task..")
                pass
        
        sleep(1)


    ########## Reducing task ##########
    mapped_output = get_firestore_doc_ids("mapper-output")

    init_node_status(node_count=reducers, node_type="reducer")

    while len(mapped_output) != 0:

        for i in range(reducers):
            if len(mapped_output) == 0:
                break
            reducer_id = f"reducer-{i}"
            status = check_node_status(node_id=reducer_id)
            if status:
                doc_id = mapped_output.pop(0)
                data = {
                    "doc_id":doc_id
                }
                send_data_to_node(reducer_id, data)
            elif status == None:
                print(f"reducer-status for: {reducer_id} Not Found!")
            else:
                # print("Waiting for reducers to finish task..")
                pass
        
        sleep(1)

    set_job_status(False)
    
    return "Finished Creating Inverted Index!"