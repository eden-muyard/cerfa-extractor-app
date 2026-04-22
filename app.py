from pathlib import Path
from io import BytesIO

from fastapi import FastAPI, File, Form, Request, UploadFile
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

app = FastAPI(title="CERFA 2069-A-SD Extractor")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.on_event("startup")
def startup() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    init_database()
    ensure_required_labels()


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    fields = load_label_config()
    required_keys = get_required_field_keys()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "result": None,
            "error": None,
            "fields": fields,
            "message": None,
            "required_keys": required_keys,
        },
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)) -> HTMLResponse:
    fields = load_label_config()
    required_keys = get_required_field_keys()
    if not file.filename:
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "result": None,
                "error": "No file selected.",
                "fields": fields,
                "message": None,
                "required_keys": required_keys,
            },
            status_code=400,
        )

    if not file.filename.lower().endswith((".xlsx", ".xlsm", ".xltx", ".xltm")):
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "request": request,
                "result": None,
                "error": "Please upload an Excel file.",
                "fields": fields,
                "message": None,
                "required_keys": required_keys,
            },
            status_code=400,
        )

    target_path = UPLOAD_DIR / Path(file.filename).name
    target_path.write_bytes(await file.read())

    extracted = extract_fields_from_workbook(str(target_path))
    append_extraction(file.filename, extracted)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "result": extracted,
            "error": None,
            "fields": fields,
            "message": "Extraction saved in database.",
            "required_keys": required_keys,
        },
    )


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
    ok, msg = add_label(label, patterns, value_type)
    fields = load_label_config()
    required_keys = get_required_field_keys()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "result": None,
            "error": None if ok else msg,
            "fields": fields,
            "message": msg if ok else None,
            "required_keys": required_keys,
        },
        status_code=200 if ok else 400,
    )


@app.get("/export/extractions.xlsx")
def export_extractions() -> StreamingResponse:
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