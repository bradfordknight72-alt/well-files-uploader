from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import shutil
import os
from pathlib import Path
import subprocess
from typing import List

app = FastAPI()

# Mount static folder (optional - for serving HTML/CSS/JS if you want a separate frontend)
# app.mount("/static", StaticFiles(directory="static"), name="static")

# Folders
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# Map filename keywords to script names (adjust as needed)
SCRIPT_MAP = {
    "recap": "recaps",
    "interval": "interval_details",
    "detail": "interval_details",
    "time": "time",
    "pason": "pason",
    "code": "pason",
}

# Your import scripts (full paths on Render - they will be in the same repo)
# These are relative to the app root
IMPORT_SCRIPTS = {
    "recaps": "./recapsGH.py",
    "interval_details": "./interval_detailsGH.py",
    "time": "./import_timeGH.py",
    "pason": "./import_pason_codesGH.py",
}

# Simple API key protection
API_KEY = "Momentum2012"  # CHANGE THIS TO A STRONG, SHARED KEY

def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return x_api_key

# Root page - simple drag-drop UI
@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Well Files Uploader</title>
        <style>
            body { font-family: Arial, sans-serif; text-align: center; padding: 40px; background: #f8f9fa; }
            #drop-zone {
                border: 3px dashed #007bff;
                border-radius: 12px;
                padding: 60px;
                margin: 40px auto;
                max-width: 700px;
                background: white;
                cursor: pointer;
                transition: all 0.3s;
            }
            #drop-zone.dragover { border-color: #28a745; background: #e8f5e9; }
            #status { margin-top: 30px; font-size: 1.2em; }
            #log { margin-top: 20px; background: #fff; border: 1px solid #ddd; padding: 15px; max-height: 300px; overflow-y: auto; text-align: left; font-family: monospace; white-space: pre-wrap; }
        </style>
    </head>
    <body>
        <h1>Drag & Drop Well Files Here</h1>
        <p>Supports .xlsx / .csv files — will run the correct import script automatically</p>
        <div id="drop-zone">Drop files here (or click to select)</div>
        <div id="status"></div>
        <div id="log"></div>

        <script>
            const dropZone = document.getElementById('drop-zone');
            const status = document.getElementById('status');
            const log = document.getElementById('log');

            function logMessage(msg, type = 'info') {
                const p = document.createElement('p');
                p.textContent = msg;
                if (type === 'error') p.style.color = 'red';
                if (type === 'success') p.style.color = 'green';
                log.appendChild(p);
                log.scrollTop = log.scrollHeight;
            }

            dropZone.addEventListener('dragover', e => {
                e.preventDefault();
                dropZone.classList.add('dragover');
            });

            dropZone.addEventListener('dragleave', () => {
                dropZone.classList.remove('dragover');
            });

            dropZone.addEventListener('drop', async e => {
                e.preventDefault();
                dropZone.classList.remove('dragover');
                status.textContent = "Uploading...";
                logMessage("Upload started...");

                const files = e.dataTransfer.files;
                if (files.length === 0) return;

                const formData = new FormData();
                for (const file of files) {
                    formData.append("files", file);
                }

                try {
                    const response = await fetch('/upload', {
                        method: 'POST',
                        body: formData,
                        headers: {
                            'x-api-key': 'your-secret-team-key-2026'  // CHANGE THIS TO MATCH API_KEY
                        }
                    });

                    const result = await response.json();

                    if (response.ok) {
                        status.textContent = "Import complete!";
                        logMessage(`Success: ${result.message}`, 'success');
                        result.details.forEach(d => logMessage(d));
                    } else {
                        status.textContent = "Import failed";
                        logMessage(`Error: ${result.detail}`, 'error');
                    }
                } catch (err) {
                    status.textContent = "Upload error";
                    logMessage(`Network error: ${err.message}`, 'error');
                }
            });

            // Click to select files (optional)
            dropZone.addEventListener('click', () => {
                const input = document.createElement('input');
                input.type = 'file';
                input.multiple = true;
                input.onchange = e => {
                    const files = e.target.files;
                    if (files.length === 0) return;
                    const dt = new DataTransfer();
                    for (const file of files) dt.items.add(file);
                    const dropEvent = new DragEvent('drop', { dataTransfer: dt });
                    dropZone.dispatchEvent(dropEvent);
                };
                input.click();
            });
        </script>
    </body>
    </html>
    """

@app.post("/upload")
async def upload_files(files: List[UploadFile] = File(...), api_key: str = Depends(verify_api_key)):
    results = []
    for file in files:
        file_path = UPLOAD_DIR / file.filename
        try:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(file.file, f)

            lower_name = file.filename.lower()

            script = None
            target_folder = None

            if 'recap' in lower_name:
                script = "recaps"
                target_folder = UPLOAD_DIR / "recaps"
            elif 'interval' in lower_name or 'detail' in lower_name:
                script = "interval_details"
                target_folder = UPLOAD_DIR / "interval_details"
            elif 'time' in lower_name:
                script = "time"
                target_folder = UPLOAD_DIR / "time"
            elif 'pason' in lower_name or 'code' in lower_name:
                script = "pason"
                target_folder = UPLOAD_DIR / "pason"
            else:
                results.append(f"{file.filename}: no matching folder/script")
                continue

            target_folder.mkdir(parents=True, exist_ok=True)
            target_path = target_folder / file.filename
            shutil.move(file_path, target_path)

            script_path = IMPORT_SCRIPTS.get(script)
            if not script_path or not os.path.exists(script_path):
                results.append(f"{file.filename}: script not found for {script}")
                continue

            # Run the script
            try:
                result = subprocess.run(
                    ["python", script_path],
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 min timeout per file
                )
                if result.returncode == 0:
                    results.append(f"{file.filename}: imported successfully ({script})")
                    results.append(result.stdout)
                else:
                    results.append(f"{file.filename}: import failed\n{result.stderr}")
            except subprocess.TimeoutExpired:
                results.append(f"{file.filename}: import timed out")
            except Exception as e:
                results.append(f"{file.filename}: error running {script} - {str(e)}")

            # Optional: delete file after processing
            # os.remove(target_path)

        except Exception as e:
            results.append(f"{file.filename}: upload error - {str(e)}")

    return {"message": f"Processed {len(files)} file(s)", "details": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
