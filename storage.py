import json
from datetime import datetime

from database import SessionLocal
from models import ExtractionModel


def append_extraction(source_file: str, extracted: dict[str, str]) -> str:
    normalized_extracted = {key: (value or "") for key, value in extracted.items()}
    signature_payload = {
        "source_file": source_file,
        **normalized_extracted,
    }
    signature = json.dumps(signature_payload, sort_keys=True, ensure_ascii=False)
    payload = {
        "extracted_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": source_file,
        **normalized_extracted,
    }
    session = SessionLocal()
    try:
        recent_rows = (
            session.query(ExtractionModel)
            .filter(ExtractionModel.source_file == source_file)
            .order_by(ExtractionModel.created_at.desc())
            .limit(30)
            .all()
        )
        for row in recent_rows:
            previous_payload = json.loads(row.extracted_json)
            previous_payload.pop("extracted_at", None)
            previous_signature = json.dumps(previous_payload, sort_keys=True, ensure_ascii=False)
            if previous_signature == signature:
                return "duplicate_skipped"
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