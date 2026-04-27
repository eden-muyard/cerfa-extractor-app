import re
import json

from database import SessionLocal, engine
from models import LabelModel
from database import Base

REQUIRED_FIELDS = [
    {"key": "annee_valorisation", "label": "Annee Valorisation", "patterns": [r"annee\s+valorisation", r"\b20\d{2}\b"], "value_type": "year", "source_tabs": ["parametres_chiffrage", "synthese", "2069_a"]},
    {"key": "nom", "label": "NOM", "patterns": [r"\bnom\b", r"raison\s+sociale"], "value_type": "text", "source_tabs": ["2069_a"]},
    {"key": "siren", "label": "SIREN", "patterns": [r"\bsiren\b"], "value_type": "siren", "source_tabs": ["2069_a"]},
    {"key": "nace", "label": "NACE", "patterns": [r"\bnace\b", r"naf"], "value_type": "text", "source_tabs": ["2069_a"]},
    {"key": "pme", "label": "PME", "patterns": [r"\bpme\b", r"petite.*moyenne"], "value_type": "yes_no", "source_tabs": ["2069_a"]},
    {"key": "jeune_docteur", "label": "Jeune Docteur / Nombre de JD", "patterns": [r"jeune\s+docteur", r"nombre\s+de\s+jd", r"\bjd\b"], "value_type": "number", "source_tabs": ["2069_a"]},
    {"key": "nombre_salaries_valorises", "label": "Nombre de salaries valorises", "patterns": [r"nombre\s+de\s+salaries?\s+valoris", r"salaries?\s+valoris"], "value_type": "number", "source_tabs": ["2069_a"]},
    {"key": "pole", "label": "POLE", "patterns": [r"\bpole\b", r"p[ôo]le"], "value_type": "pole", "source_tabs": ["parametres_chiffrage"]},
    {"key": "honoraires", "label": "Hono (annee)", "patterns": [r"hono", r"honoraires?\s*(20\d{2})?"], "value_type": "number", "source_tabs": ["parametres_chiffrage"]},
    {"key": "credit_parametres", "label": "Credit", "patterns": [r"\bcredit\b", r"credits?"], "value_type": "credit_choice", "source_tabs": ["parametres_chiffrage"]},
    {"key": "jei", "label": "JEI", "patterns": [r"\bjei\b", r"jeune\s+entreprise\s+innovante"], "value_type": "yes_no", "source_tabs": ["parametres_chiffrage"]},
    {"key": "cloture_decalee", "label": "Cloture decalee", "patterns": [r"cloture\s+decal", r"decalage\s+de\s+cloture"], "value_type": "yes_no", "source_tabs": ["parametres_chiffrage"]},
    {"key": "type_honoraires", "label": "Type d'honoraires", "patterns": [r"type\s+d['’]?\s*honoraires?", r"\bhonoraires?\b"], "value_type": "honoraires_type", "source_tabs": ["parametres_chiffrage"]},
    {"key": "honoraires_n_1", "label": "Honoraires n-1", "patterns": [r"hono", r"honoraires?"], "value_type": "number", "source_tabs": ["parametres_chiffrage"]},
    {"key": "montant_credit_impot_cir", "label": "Montant du credit d'impot CIR", "patterns": [r"montant.*credit\s+d'?impot.*\bcir\b", r"credit\s+d'?impot.*recherche"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "montant_credit_impot_cii", "label": "Montant du credit d'impot CII", "patterns": [r"montant.*credit\s+d'?impot.*\bcii\b", r"credit\s+d'?impot.*innovation"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "montant_cic", "label": "Montant du CIC", "patterns": [r"montant.*\bcic\b", r"\bcic\b"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "dotations_amortissements", "label": "Dotations aux amortissements", "patterns": [r"dotations?\s+aux?\s+amort"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "depenses_personnel", "label": "Depenses de personnel", "patterns": [r"depenses?\s+de\s+personnel(?!.*jeunes?\s+docteurs?)"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "depenses_veille", "label": "Depenses de veille", "patterns": [r"depenses?\s+de\s+veille"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "depenses_brevets", "label": "Depenses de brevets", "patterns": [r"depenses?.*brevets?"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "depenses_fonctionnement", "label": "Depenses de fonctionnement", "patterns": [r"depenses?\s+de\s+fonctionnement"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "depenses_prestataires_externes", "label": "Depenses de prestataires externes", "patterns": [r"depenses?.*prestataires?.*extern", r"sous[\s-]?traitance"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "subventions_avances_remboursables", "label": "Subventions et avances remboursables", "patterns": [r"subventions?\s+et\s+avances?\s+remboursables?", r"subventions?"], "value_type": "number", "source_tabs": ["synthese"]},
    {"key": "depenses_internes", "label": "Depenses internes", "patterns": [r"montant total des depenses de recherche realisees"], "value_type": "number", "source_tabs": ["2069_a"]},
    {"key": "depenses_externes", "label": "Depenses externes", "patterns": [r"depenses de recherche ouvrant droit au credit d'impot"], "value_type": "number", "source_tabs": ["2069_a"]},
    {"key": "nombre_projets_cir", "label": "Nombre de projets CIR", "patterns": [r"\bcir\b"], "value_type": "number", "source_tabs": ["rep_cout"]},
    {"key": "nombre_projets_cii", "label": "Nombre de projets CII", "patterns": [r"\bcii\b"], "value_type": "number", "source_tabs": ["rep_cout"]},
]


def init_database() -> None:
    Base.metadata.create_all(bind=engine)


def ensure_required_labels() -> None:
    session = SessionLocal()
    try:
        existing = {label.key: label for label in session.query(LabelModel).all()}
        for idx, required_field in enumerate(REQUIRED_FIELDS):
            source_tabs = required_field.get("source_tabs", [])
            if required_field["key"] in existing:
                record = existing[required_field["key"]]
                record.label = required_field["label"]
                record.patterns_json = json.dumps(required_field["patterns"], ensure_ascii=False)
                record.source_tabs_json = json.dumps(source_tabs, ensure_ascii=False)
                record.value_type = required_field["value_type"]
                record.required = True
                record.sort_order = idx
                continue
            session.add(
                LabelModel(
                    key=required_field["key"],
                    label=required_field["label"],
                    patterns_json=json.dumps(required_field["patterns"], ensure_ascii=False),
                    source_tabs_json=json.dumps(source_tabs, ensure_ascii=False),
                    value_type=required_field["value_type"],
                    required=True,
                    sort_order=idx,
                )
            )
        session.commit()
    finally:
        session.close()


def _row_to_field(row: LabelModel) -> dict:
    return {
        "key": row.key,
        "label": row.label,
        "patterns": json.loads(row.patterns_json or "[]"),
        "value_type": row.value_type,
        "source_tabs": json.loads(row.source_tabs_json or "[]"),
    }


def load_label_config() -> list[dict]:
    session = SessionLocal()
    try:
        rows = (
            session.query(LabelModel)
            .order_by(LabelModel.sort_order.asc(), LabelModel.id.asc())
            .all()
        )
        return [_row_to_field(row) for row in rows]
    finally:
        session.close()


def get_field_keys() -> list[str]:
    return [field["key"] for field in load_label_config()]


def get_required_field_keys() -> set[str]:
    return {field["key"] for field in REQUIRED_FIELDS}


def to_field_key(label: str) -> str:
    lowered = label.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return slug


def add_label(label: str, patterns_text: str, value_type: str) -> tuple[bool, str]:
    clean_label = label.strip()
    if not clean_label:
        return False, "Label name is required."

    key = to_field_key(clean_label)
    if not key:
        return False, "Label name must contain letters or numbers."

    if value_type not in {"text", "number", "siren", "yes_no", "pole", "credit_choice", "year", "honoraires_type"}:
        return False, "Invalid value type."

    patterns = [part.strip() for part in patterns_text.split(",") if part.strip()]
    if not patterns:
        patterns = [re.escape(clean_label)]

    for pattern in patterns:
        try:
            re.compile(pattern, re.IGNORECASE)
        except re.error:
            return False, f"Invalid pattern: {pattern}"

    session = SessionLocal()
    try:
        if session.query(LabelModel).filter(LabelModel.key == key).first():
            return False, f"Label already exists: {key}"
        max_sort = session.query(LabelModel.sort_order).order_by(LabelModel.sort_order.desc()).first()
        next_sort = (max_sort[0] + 1) if max_sort else len(REQUIRED_FIELDS)
        session.add(
            LabelModel(
                key=key,
                label=clean_label,
                patterns_json=json.dumps(patterns, ensure_ascii=False),
                source_tabs_json="[]",
                value_type=value_type,
                required=False,
                sort_order=next_sort,
            )
        )
        session.commit()
        return True, f"Label added: {clean_label}"
    finally:
        session.close()
