from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse
import uvicorn
import shutil
import os
from pathlib import Path
from typing import List
import logging

# Import your scripts' main functions (after refactoring them)
from recapsGH import run_recaps_import
from interval_detailsGH import run_interval_import
from import_timeGH import run_time_import
from import_pason_codesGH import run_pason_import

app = FastAPI()

# Logging setup
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

# Upload directory
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

# Keyword → import function mapping
IMPORT_FUNCTIONS = {
    "recaps": run_recaps_import,
    "interval_details": run_interval_import,
    "time": run_time_import,
    "pason": run_pason_import,
}

# API key protection
API_KEY = "Momentum2012"

def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return x_api_key

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
                            'x-api-key': 'Momentum2012'
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
async def upload_files(files: List[UploadFile] = File(...), x_api_key: str = Header(None)):
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Files in current directory: {os.listdir('.')}")

    if x_api_key != "Momentum2012":
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    results = []

    for file in files:
        file_path = UPLOAD_DIR / file.filename
        try:
            with open(file_path, "wb") as f:
                shutil.copyfileobj(file.file, f)

            lower_name = file.filename.lower()
            script_key = None
            target_folder = None

            if 'recap' in lower_name:
                script_key = "recaps"
                target_folder = UPLOAD_DIR / "recaps"
            elif 'interval' in lower_name or 'detail' in lower_name:
                script_key = "interval_details"
                target_folder = UPLOAD_DIR / "interval_details"
            elif 'time' in lower_name:
                script_key = "time"
                target_folder = UPLOAD_DIR / "time"
            elif 'pason' in lower_name or 'code' in lower_name:
                script_key = "pason"
                target_folder = UPLOAD_DIR / "pason"
            else:
                results.append(f"{file.filename}: no matching folder/script")
                continue

            target_folder.mkdir(parents=True, exist_ok=True)
            target_path = target_folder / file.filename
            shutil.move(file_path, target_path)

            import_func = IMPORT_FUNCTIONS.get(script_key)
            if not import_func:
                results.append(f"{file.filename}: no matching function for {script_key}")
                continue

            logger.info(f"Running import function for {script_key}")

            try:
                result = import_func()  # Call the function directly
                results.append(f"{file.filename}: imported successfully ({script_key})")
                results.append(str(result) if result else "Done")
            except Exception as e:
                results.append(f"{file.filename}: import failed - {str(e)}")

        except Exception as e:
            results.append(f"{file.filename}: upload error - {str(e)}")

    return {"message": f"Processed {len(files)} file(s)", "details": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
