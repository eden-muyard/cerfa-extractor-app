import json
from datetime import datetime

from database import SessionLocal
from models import ExtractionModel


def append_extraction(source_file: str, extracted: dict[str, str]) -> str:
    payload = {
        "extracted_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": source_file,
        **extracted,
    }
    session = SessionLocal()
    try:
        session.add(
            ExtractionModel(
                source_file=source_file,
                extracted_json=json.dumps(payload, ensure_ascii=False),
            )
        )
        session.commit()
        return "saved"
    finally:
        session.close()


def list_extractions() -> list[dict[str, str]]:
    session = SessionLocal()
    try:
        rows = session.query(ExtractionModel).order_by(ExtractionModel.created_at.asc()).all()
        return [json.loads(row.extracted_json) for row in rows]
    finally:
        session.close()