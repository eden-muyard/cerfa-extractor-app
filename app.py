from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
import os
import secrets

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from starlette.middleware.sessions import SessionMiddleware

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
SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_urlsafe(32))
APP_USERNAME = os.getenv("APP_USERNAME", "").strip()
APP_PASSWORD = os.getenv("APP_PASSWORD", "").strip()
AUTH_ENABLED = bool(APP_USERNAME and APP_PASSWORD)

app = FastAPI(title="CERFA 2069-A-SD Extractor")
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=60 * 60 * 12,
    same_site="lax",
    https_only=False,
)
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def is_authenticated(request: Request) -> bool:
    if not AUTH_ENABLED:
        return True
    session = request.scope.get("session")
    if not isinstance(session, dict):
        return False
    return session.get("authenticated") is True


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


def require_auth_redirect(request: Request) -> RedirectResponse | None:
    if AUTH_ENABLED and not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


@app.on_event("startup")
def startup() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_old_uploads()
    init_database()
    ensure_required_labels()


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    auth_redirect = require_auth_redirect(request)
    if auth_redirect:
        return auth_redirect
    return templates.TemplateResponse(request, "index.html", build_common_context(request))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)) -> HTMLResponse:
    auth_redirect = require_auth_redirect(request)
    if auth_redirect:
        return auth_redirect
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
    auth_redirect = require_auth_redirect(request)
    if auth_redirect:
        return auth_redirect
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
def login_page(request: Request) -> HTMLResponse:
    if not AUTH_ENABLED:
        return RedirectResponse(url="/", status_code=307)
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=307)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "error": None},
    )


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
) -> HTMLResponse:
    if not AUTH_ENABLED:
        return RedirectResponse(url="/", status_code=307)
    if username == APP_USERNAME and password == APP_PASSWORD:
        session = request.scope.get("session")
        if not isinstance(session, dict):
            return templates.TemplateResponse(
                request,
                "login.html",
                {"request": request, "error": "Session is unavailable. Please redeploy."},
                status_code=500,
            )
        session["authenticated"] = True
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "error": "Invalid username or password."},
        status_code=401,
    )


@app.post("/logout")
def logout(request: Request) -> RedirectResponse:
    session = request.scope.get("session")
    if isinstance(session, dict):
        session.clear()
    target = "/login" if AUTH_ENABLED else "/"
    return RedirectResponse(url=target, status_code=303)


@app.get("/export/extractions.xlsx")
def export_extractions(request: Request) -> StreamingResponse | RedirectResponse:
    auth_redirect = require_auth_redirect(request)
    if auth_redirect:
        return auth_redirect
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