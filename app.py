from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from base64 import b64decode
import os
import secrets

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook

from extractor import extract_fields_from_workbook
from label_config import (
    add_label,
    ensure_required_labels,
    get_required_field_keys,
    init_database,
    load_label_config,
)
from storage import append_extraction, list_extractions

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_MB", "15")) * 1024 * 1024
UPLOAD_RETENTION_DAYS = int(os.getenv("UPLOAD_RETENTION_DAYS", "14"))
APP_USERNAME = os.getenv("APP_USERNAME", "").strip()
APP_PASSWORD = os.getenv("APP_PASSWORD", "").strip()
AUTH_ENABLED = bool(APP_USERNAME and APP_PASSWORD)

app = FastAPI(title="CERFA 2069-A-SD Extractor")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def ensure_authorized(request: Request) -> None:
    if not AUTH_ENABLED:
        return
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Basic "):
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "Basic"})
    encoded = auth_header[6:]
    try:
        decoded = b64decode(encoded).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception as exc:
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "Basic"}) from exc
    if not (
        secrets.compare_digest(username, APP_USERNAME)
        and secrets.compare_digest(password, APP_PASSWORD)
    ):
        raise HTTPException(status_code=401, headers={"WWW-Authenticate": "Basic"})


def cleanup_old_uploads() -> None:
    if UPLOAD_RETENTION_DAYS <= 0:
        return
    cutoff = datetime.now() - timedelta(days=UPLOAD_RETENTION_DAYS)
    for item in UPLOAD_DIR.glob("*"):
        if not item.is_file():
            continue
        modified = datetime.fromtimestamp(item.stat().st_mtime)
        if modified < cutoff:
            item.unlink(missing_ok=True)


def build_common_context(request: Request) -> dict:
    fields = load_label_config()
    required_keys = get_required_field_keys()
    return {
        "request": request,
        "result": None,
        "error": None,
        "fields": fields,
        "message": None,
        "required_keys": required_keys,
        "auth_enabled": AUTH_ENABLED,
        "missing_required_labels": [],
    }


@app.on_event("startup")
def startup() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_old_uploads()
    init_database()
    ensure_required_labels()


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    ensure_authorized(request)
    return templates.TemplateResponse(request, "index.html", build_common_context(request))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)) -> HTMLResponse:
    ensure_authorized(request)
    context = build_common_context(request)
    if not file.filename:
        context["error"] = "No file selected."
        return templates.TemplateResponse(request, "index.html", context, status_code=400)

    if not file.filename.lower().endswith((".xlsx", ".xlsm", ".xltx", ".xltm")):
        context["error"] = "Please upload an Excel file."
        return templates.TemplateResponse(request, "index.html", context, status_code=400)

    payload = await file.read()
    if len(payload) > MAX_UPLOAD_BYTES:
        max_mb = int(MAX_UPLOAD_BYTES / (1024 * 1024))
        context["error"] = f"File is too large. Maximum size is {max_mb} MB."
        return templates.TemplateResponse(request, "index.html", context, status_code=400)

    original_name = Path(file.filename).name
    unique_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(4)}_{original_name}"
    target_path = UPLOAD_DIR / unique_name
    target_path.write_bytes(payload)

    extracted = extract_fields_from_workbook(str(target_path))
    append_extraction(original_name, extracted)
    cleanup_old_uploads()

    required_label_map = {
        field["key"]: field["label"]
        for field in context["fields"]
        if field["key"] in context["required_keys"]
    }
    missing_required_keys = [
        key for key in sorted(context["required_keys"]) if not (extracted.get(key) or "").strip()
    ]
    context["result"] = extracted
    context["message"] = "Extraction saved in database."
    context["missing_required_labels"] = [
        required_label_map.get(key, key) for key in missing_required_keys
    ]
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/upload")
def upload_get_redirect() -> RedirectResponse:
    return RedirectResponse(url="/", status_code=307)


@app.post("/labels/add", response_class=HTMLResponse)
async def add_label_route(
    request: Request,
    label: str = Form(...),
    patterns: str = Form(""),
    value_type: str = Form("text"),
) -> HTMLResponse:
    ensure_authorized(request)
    ok, msg = add_label(label, patterns, value_type)
    context = build_common_context(request)
    context["error"] = None if ok else msg
    context["message"] = msg if ok else None
    return templates.TemplateResponse(
        request,
        "index.html",
        context,
        status_code=200 if ok else 400,
    )


@app.get("/login", response_class=HTMLResponse)
def login_redirect() -> RedirectResponse:
    return RedirectResponse(url="/", status_code=307)


@app.post("/logout")
def logout() -> RedirectResponse:
    return RedirectResponse(url="/", status_code=303)


@app.get("/export/extractions.xlsx")
def export_extractions(request: Request) -> StreamingResponse | RedirectResponse:
    ensure_authorized(request)
    fields = load_label_config()
    headers = ["extracted_at", "source_file"] + [field["key"] for field in fields]
    rows = list_extractions()

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "ExtractedData"
    sheet.append(headers)

    for row_data in rows:
        sheet.append([row_data.get(header, "") for header in headers])

    output = BytesIO()
    workbook.save(output)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=extractions.xlsx"},
    )