"""
Lightweight XLSX/XLSM cell reads via ZIP + XML (low RAM — Render free tier).
"""
from __future__ import annotations

import re
import zipfile
from xml.etree import ElementTree as ET
from typing import Any

MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL_NS = "{http://schemas.openxmlformats.org/package/2006/relationships}"
OFFICE_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


def column_letters_to_index(letters: str) -> int:
    idx = 0
    for ch in letters.upper():
        idx = idx * 26 + (ord(ch) - 64)
    return idx


def parse_cell_ref(ref: str) -> tuple[int, int]:
    match = re.match(r"^([A-Z]+)(\d+)$", ref.upper())
    if not match:
        return 0, 0
    return int(match.group(2)), column_letters_to_index(match.group(1))


def load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    out: list[str] = []
    for si in root.findall(f"{MAIN_NS}si"):
        parts: list[str] = []
        for node in si.iter():
            if node.tag == f"{MAIN_NS}t" and node.text:
                parts.append(node.text)
        out.append("".join(parts))
    return out


def cell_element_value(cell: ET.Element, shared_strings: list[str]) -> Any:
    cell_type = cell.get("t")
    if cell_type == "inlineStr":
        texts = [t.text or "" for t in cell.findall(f".//{MAIN_NS}t")]
        return "".join(texts) if texts else None
    value_node = cell.find(f"{MAIN_NS}v")
    if value_node is None or value_node.text is None:
        return None
    raw = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (IndexError, ValueError):
            return raw
    if cell_type == "b":
        return raw == "1"
    try:
        if "." in raw or "e" in raw.lower():
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def workbook_sheet_paths(zf: zipfile.ZipFile) -> list[tuple[str, str]]:
    """Return (sheet_name, path_in_zip) for each worksheet."""
    rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rid_to_path: dict[str, str] = {}
    for rel in rels_root.findall(f"{REL_NS}Relationship"):
        rid = rel.get("Id")
        target = rel.get("Target")
        if not rid or not target:
            continue
        if target.startswith("/"):
            path = target.lstrip("/")
        elif target.startswith("xl/"):
            path = target
        else:
            path = f"xl/{target}"
        rid_to_path[rid] = path

    wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
    sheets: list[tuple[str, str]] = []
    for sheet in wb_root.findall(f"{MAIN_NS}sheets/{MAIN_NS}sheet"):
        name = sheet.get("name") or ""
        rid = sheet.get(OFFICE_REL + "id")
        if rid and rid in rid_to_path:
            sheets.append((name, rid_to_path[rid]))
    return sheets


def read_cells_from_sheet(
    zf: zipfile.ZipFile,
    sheet_path: str,
    wanted: set[tuple[int, int]],
    shared_strings: list[str],
) -> dict[tuple[int, int], Any]:
    needed_rows = {row for row, _col in wanted}
    found: dict[tuple[int, int], Any] = {}
    with zf.open(sheet_path) as stream:
        for _event, elem in ET.iterparse(stream, events=("end",)):
            if elem.tag != f"{MAIN_NS}row":
                continue
            row_attr = elem.get("r")
            if not row_attr:
                elem.clear()
                continue
            try:
                row_num = int(row_attr)
            except ValueError:
                elem.clear()
                continue
            if row_num not in needed_rows:
                elem.clear()
                continue
            for cell in elem.findall(f"{MAIN_NS}c"):
                ref = cell.get("r")
                if not ref:
                    continue
                coords = parse_cell_ref(ref)
                if coords in wanted and coords not in found:
                    found[coords] = cell_element_value(cell, shared_strings)
            elem.clear()
            if len(found) >= len(wanted):
                break
    return found


def read_parametres_fixed_cells(
    file_path: str,
    sheet_matcher,
) -> dict[tuple[int, int], Any]:
    """
    Read C13, G13, D15, E15 from the first sheet where sheet_matcher(name) is truthy.
    sheet_matcher: callable(sheet_name) -> bool
    """
    wanted = {(13, 3), (13, 7), (15, 4), (15, 5)}
    with zipfile.ZipFile(file_path, "r") as zf:
        shared = load_shared_strings(zf)
        for sheet_name, sheet_path in workbook_sheet_paths(zf):
            if not sheet_matcher(sheet_name):
                continue
            return read_cells_from_sheet(zf, sheet_path, wanted, shared)
    return {}
