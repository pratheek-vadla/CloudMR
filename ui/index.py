import os
from flask import Flask, request, render_template
from firebase_admin import credentials, firestore, initialize_app

PORT = os.environ.get("UI_PORT")
FIRESTORE_CREDENTIALS="firebase-key.json"

cred = credentials.Certificate(FIRESTORE_CREDENTIALS)
app = initialize_app(cred)

firestore_client = firestore.client()

app = Flask(__name__)

def check_inverted_index_status():

    doc = firestore_client.collection("node-status").document("job_status").get()
    if doc.exists:
        status_doc = doc.to_dict()
        return status_doc.get("is_running")
    return None

def get_data_from_inverted_index(query):

    tokens = query.lower().split()
    out = {}
    for token in tokens:
        doc = firestore_client.collection("inverted-index").document(token).get()
        if doc.exists:
            doc = doc.to_dict()
            meta = doc.get("meta", {})
            for k,v in meta.items():
                if k in out:
                    out[k] += int(v)
                else:
                    out[k] = v
    return out

@app.route('/')
def my_form():
    job_status = check_inverted_index_status()
    return render_template('index.html', job_running=job_status, display_results=False, data=None)

@app.route('/status')
def job_status():
    job_status = check_inverted_index_status()
    if job_status != None:
        return str(job_status)
    return "Job Status Not Found"

@app.route('/handle_data', methods=['POST'])
def handle_data():
    text = request.form['query']
    print(text)
    if not text:
        return render_template("index.html", job_running=False, display_results=False, data=None, error_msg="Invalid/Empty Query!")

    results = get_data_from_inverted_index(text)
    # print(results)
    results = dict(sorted(results.items(), key=lambda x:x[1], reverse=True))
    print(results)
    resp = []
    for k,v in results.items():
        resp.append(
            {
                "book":k,
                "score":v
            }
        )
    print(resp)
    return render_template("index.html", job_running=False, display_results=True, data=resp)

if __name__ == "__main__":

    # app.run()
    app.run(host='0.0.0.0', port=PORT)