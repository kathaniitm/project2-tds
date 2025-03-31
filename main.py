from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
from fastapi import FastAPI, Query
from typing import Optional
from app.utils.openai_client import get_openai_response
from app.utils.file_handler import save_upload_file_temporarily
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


# Import the functions you want to test directly
from app.utils.functions import *

uploads_dir = os.path.join(os.getcwd(), "uploads")
if not os.path.exists(uploads_dir):
    os.makedirs(uploads_dir, exist_ok=True)
    print(f"Created uploads directory at: {uploads_dir}")


app = FastAPI(title="IITM Assignment API")


AIPROXY_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6IjI0ZjEwMDIzOTBAZHMuc3R1ZHkuaWl0bS5hYy5pbiJ9.5j9400SGrtncpZLZmrML6BuqlhZw18Oa9Q7q0PQO32E"
AIPROXY_URL = "https://aiproxy.sanand.workers.dev/openai/v1/embeddings"





# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/")
async def process_question(
    question: str = Form(...), file: Optional[UploadFile] = File(None)
):
    try:
        # Save file temporarily if provided
        temp_file_path = None
        if file:
            temp_file_path = await save_upload_file_temporarily(file)

        # Get answer from OpenAI
        answer = await get_openai_response(question, temp_file_path)

        return {"answer": answer}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# New endpoint for testing specific functions
@app.post("/debug/{function_name}")
async def debug_function(
    function_name: str,
    file: Optional[UploadFile] = File(None),
    params: str = Form("{}"),
):
    """
    Debug endpoint to test specific functions directly

    Args:
        function_name: Name of the function to test
        file: Optional file upload
        params: JSON string of parameters to pass to the function
    """
    try:
        # Save file temporarily if provided
        temp_file_path = None
        if file:
            temp_file_path = await save_upload_file_temporarily(file)

        # Parse parameters
        parameters = json.loads(params)

        # Add file path to parameters if file was uploaded
        if temp_file_path:
            parameters["file_path"] = temp_file_path

        # Call the appropriate function based on function_name
        if function_name == "analyze_sales_with_phonetic_clustering":
            result = await analyze_sales_with_phonetic_clustering(**parameters)
            return {"result": result}
        elif function_name == "calculate_prettier_sha256":
            # For calculate_prettier_sha256, we need to pass the filename parameter
            if temp_file_path:
                result = await calculate_prettier_sha256(temp_file_path)
                return {"result": result}
            else:
                return {"error": "No file provided for calculate_prettier_sha256"}
        else:
            return {
                "error": f"Function {function_name} not supported for direct testing"
            }

    except Exception as e:
        import traceback

        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/students")
async def get_students(class_: List[str] = Query(default=[], alias="class")):
    results = []
    file_path = "students_marks.csv"
    if os.path.exists(file_path):
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row["studentId"] = int(row["studentId"])
                results.append(row)
    if class_:
        results = [student for student in results if student["class"] in class_]
    return {"students": results}


class SearchRequest(BaseModel):
    docs: List[str]
    query: str

def get_embedding(text: str):
    """ Generate embedding for a given text using AIPROXY """
    headers = {
        "Authorization": f"Bearer {AIPROXY_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "input": text,
        "model": "text-embedding-3-small"
    }

    response = requests.post(AIPROXY_URL, headers=headers, json=data)
    
    if response.status_code == 200:
        return response.json()['data'][0]['embedding']
    else:
        raise HTTPException(status_code=response.status_code, detail=response.text)

def cosine_similarity(vec1, vec2):
    """ Compute cosine similarity between two vectors """
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

@app.post("/similarity")
async def get_similar_documents(request: SearchRequest):
    try:
        # Get embeddings for all documents
        doc_embeddings = [get_embedding(doc) for doc in request.docs]
        
        # Get embedding for query
        query_embedding = get_embedding(request.query)
        
        # Compute cosine similarity scores
        similarities = [cosine_similarity(query_embedding, doc_emb) for doc_emb in doc_embeddings]
        
        # Get top 3 matches
        top_indices = np.argsort(similarities)[::-1][:3]
        top_matches = [request.docs[i] for i in top_indices]

        return {"matches": top_matches}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Mount the "uploads" directory so that files can be served at /uploads/*
 

app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")




if __name__ == "__main__":
    import uvicorn
      

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
