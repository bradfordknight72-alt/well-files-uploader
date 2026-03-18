from fastapi import Depends, HTTPException, Header

API_KEY = "your-secret-team-key-2026"  # CHANGE THIS

def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key

@app.post("/upload", dependencies=[Depends(verify_api_key)])
async def upload_files(files: list[UploadFile] = File(...)):
    # ... rest of the function ...
