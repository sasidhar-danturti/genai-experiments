from __future__ import annotations

import ast
import json
import re
from typing import Any, Dict, List, Union

Json = Dict[str, Any]


def _safe_json_loads(s: str) -> Any:
    s = s.strip()
    try:
        return json.loads(s)
    except Exception:
        pass
    try:
        return ast.literal_eval(s)
    except Exception as exc:
        raise ValueError(f"Could not parse as JSON or Python literal: {exc}") from exc


def _coerce_to_dict(obj: Any) -> Json:
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        parsed = _safe_json_loads(obj)
        if not isinstance(parsed, dict):
            raise ValueError("Parsed string did not yield a dict.")
        return parsed
    if hasattr(obj, "asDict"):
        return obj.asDict(recursive=True)
    raise ValueError(f"Unsupported object type for dict coercion: {type(obj)}")


def _split_text_to_lines(text: str) -> List[str]:
    lines = [ln.rstrip() for ln in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return [ln for ln in lines if ln.strip() != ""]


def _tokenize_words(line: str) -> List[str]:
    return [t for t in re.split(r"\s+", line.strip()) if t]


def normalize_adi(adi_payload: Union[str, Json]) -> Json:
    adi = _coerce_to_dict(adi_payload)

    analyze = adi.get("analyzeResult") or {}
    full_text = analyze.get("content") or ""

    pages_in = analyze.get("pages") or []
    paragraphs_in = analyze.get("paragraphs") or []
    tables_in = analyze.get("tables") or []
    styles_in = analyze.get("styles") or []
    content_format = analyze.get("contentFormat")

    blocks_by_page: Dict[int, List[Json]] = {}
    for p in paragraphs_in:
        brs = p.get("boundingRegions") or []
        page_num = 1
        if brs and isinstance(brs, list) and isinstance(brs[0], dict) and brs[0].get("pageNumber"):
            page_num = int(brs[0]["pageNumber"])
        blocks_by_page.setdefault(page_num, []).append(
            {
                "kind": "paragraph",
                "content": p.get("content", ""),
                "bounding_regions": brs,
                "spans": p.get("spans"),
            }
        )

    pages_out: List[Json] = []
    for pg in pages_in:
        pnum = int(pg.get("pageNumber", len(pages_out) + 1))
        lines_in = pg.get("lines") or []
        words_in = pg.get("words") or []

        pages_out.append(
            {
                "page_number": pnum,
                "width": pg.get("width"),
                "height": pg.get("height"),
                "unit": pg.get("unit"),
                "angle": pg.get("angle"),
                "lines": [
                    {
                        "content": ln.get("content", ""),
                        "polygon": ln.get("polygon"),
                        "spans": ln.get("spans"),
                    }
                    for ln in lines_in
                ],
                "words": [
                    {
                        "content": w.get("content", ""),
                        "polygon": w.get("polygon"),
                        "confidence": w.get("confidence"),
                        "span": w.get("span"),
                    }
                    for w in words_in
                ],
                "blocks": blocks_by_page.get(pnum, []),
            }
        )

    if not pages_out:
        pages_out = [
            {
                "page_number": 1,
                "width": None,
                "height": None,
                "unit": None,
                "angle": None,
                "lines": [
                    {"content": ln, "polygon": None, "spans": None}
                    for ln in _split_text_to_lines(full_text)
                ],
                "words": [],
                "blocks": blocks_by_page.get(1, []),
            }
        ]

    return {
        "schema_version": "1.0",
        "source": "adi",
        "doc_kind": "document",
        "text": {"full": full_text, "pages": pages_out},
        "metadata": {
            "adi_status": adi.get("status"),
            "createdDateTime": adi.get("createdDateTime"),
            "lastUpdatedDateTime": adi.get("lastUpdatedDateTime"),
            "apiVersion": analyze.get("apiVersion"),
            "modelId": analyze.get("modelId"),
            "stringIndexType": analyze.get("stringIndexType"),
            "contentFormat": content_format,
            "has_tables": bool(tables_in),
            "styles": styles_in,
        },
        "raw": {"adi": adi},
    }


def normalize_adi_plus_llm(payload: Any) -> Json:
    payload_dict = _coerce_to_dict(payload)
    adi_norm = normalize_adi(payload_dict.get("adi_result", {}))
    llm_text = payload_dict.get("llm_result") or ""

    adi_norm["source"] = "adi+llm"
    adi_norm["metadata"]["llm_text_present"] = bool(llm_text.strip())
    adi_norm["metadata"]["llm_text"] = llm_text
    adi_norm["raw"]["llm_result"] = llm_text
    adi_norm["raw"]["original_payload"] = payload_dict
    return adi_norm


def normalize_llm_image(payload: Any) -> Json:
    payload_dict = _coerce_to_dict(payload)
    image_desc = payload_dict.get("image_description") or ""
    visible_text = payload_dict.get("text") or ""

    full = "\n".join(
        [
            p
            for p in [
                ("[IMAGE_DESCRIPTION]\n" + image_desc).strip()
                if image_desc.strip()
                else "",
                ("[VISIBLE_TEXT]\n" + visible_text).strip()
                if visible_text.strip()
                else "",
            ]
            if p
        ]
    )

    return {
        "schema_version": "1.0",
        "source": "llm_image",
        "doc_kind": "image",
        "text": {
            "full": full,
            "pages": [
                {
                    "page_number": 1,
                    "width": None,
                    "height": None,
                    "unit": None,
                    "angle": None,
                    "lines": [
                        {"content": ln, "polygon": None, "spans": None}
                        for ln in _split_text_to_lines(full)
                    ],
                    "words": [],
                    "blocks": [
                        {
                            "kind": "image_caption",
                            "content": image_desc,
                            "bounding_regions": None,
                            "spans": None,
                        }
                    ]
                    if image_desc
                    else [],
                }
            ],
        },
        "metadata": {
            "image_description_present": bool(image_desc.strip()),
            "visible_text_present": bool(visible_text.strip()),
        },
        "raw": {"original_payload": payload_dict},
    }


def normalize_pymupdf_text(text: str) -> Json:
    lines = _split_text_to_lines(text or "")
    words_out: List[Json] = []
    for ln in lines:
        for w in _tokenize_words(ln):
            words_out.append(
                {"content": w, "polygon": None, "confidence": None, "span": None}
            )

    return {
        "schema_version": "1.0",
        "source": "pymupdf",
        "doc_kind": "document",
        "text": {
            "full": "\n".join(lines),
            "pages": [
                {
                    "page_number": 1,
                    "width": None,
                    "height": None,
                    "unit": None,
                    "angle": None,
                    "lines": [
                        {"content": ln, "polygon": None, "spans": None}
                        for ln in lines
                    ],
                    "words": words_out,
                    "blocks": [],
                }
            ],
        },
        "metadata": {
            "note": "No confidence/polygons available from plain text input."
        },
        "raw": {"original_text": text},
    }


def normalize_by_type(parser_type: Any, parser_response: Any) -> str:
    try:
        ptype = (parser_type or "").strip().lower()
        resp = parser_response
        if hasattr(resp, "asDict"):
            resp = resp.asDict(recursive=True)

        if isinstance(resp, str) and resp.strip().startswith(("{", "[")):
            pass

        if ptype in {"adi+llm", "adi_llm", "adi-llm", "adi_and_llm"}:
            norm = normalize_adi_plus_llm(
                resp if not isinstance(resp, str) else _safe_json_loads(resp)
            )
        elif ptype in {"adi", "just_adi", "azure_di", "document_intelligence"}:
            norm = normalize_adi(resp)
        elif ptype in {"llm_image", "image_llm", "vision_llm"}:
            norm = normalize_llm_image(
                resp if not isinstance(resp, str) else _safe_json_loads(resp)
            )
        elif ptype in {"pymupdf", "fitz", "pdf_text"}:
            norm = normalize_pymupdf_text(
                resp if isinstance(resp, str) else json.dumps(resp, ensure_ascii=False)
            )
        else:
            if isinstance(resp, str):
                if resp.strip().startswith("{"):
                    maybe = _safe_json_loads(resp)
                    if isinstance(maybe, dict) and "analyzeResult" in maybe:
                        norm = normalize_adi(maybe)
                    elif (
                        isinstance(maybe, dict)
                        and "adi_result" in maybe
                        and "llm_result" in maybe
                    ):
                        norm = normalize_adi_plus_llm(maybe)
                    elif (
                        isinstance(maybe, dict)
                        and "image_description" in maybe
                        and "text" in maybe
                    ):
                        norm = normalize_llm_image(maybe)
                    else:
                        norm = normalize_pymupdf_text(resp)
                else:
                    norm = normalize_pymupdf_text(resp)
            elif isinstance(resp, dict) and "analyzeResult" in resp:
                norm = normalize_adi(resp)
            else:
                norm = normalize_pymupdf_text(str(resp))

        return json.dumps(norm, ensure_ascii=False)
    except Exception as exc:
        err = {
            "schema_version": "1.0",
            "source": "unknown",
            "doc_kind": "unknown",
            "text": {"full": ""},
            "metadata": {"normalization_error": str(exc)},
            "raw": {
                "parser_type": str(parser_type),
                "parser_response_preview": (
                    str(parser_response)[:2000] if parser_response is not None else None
                ),
            },
        }
        return json.dumps(err, ensure_ascii=False)


def standardize_text(text: str) -> str:
    if not text:
        return ""
    s = text.replace("\r\n", "\n").replace("\r", "\n")
    s = "\n".join([ln.rstrip() for ln in s.split("\n")])
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def extract_adi_structured(norm_json: str) -> str:
    try:
        d = json.loads(norm_json) if norm_json else {}
        source = d.get("source")
        raw_adi = ((d.get("raw") or {}).get("adi") or {})
        analyze = raw_adi.get("analyzeResult") or {}

        result = {
            "has_adi": source in ("adi", "adi+llm"),
            "modelId": analyze.get("modelId"),
            "apiVersion": analyze.get("apiVersion"),
            "key_value_pairs": [],
            "documents_fields_flat": {},
            "documents_raw": analyze.get("documents") or None,
        }

        kvps = analyze.get("keyValuePairs") or []
        for kv in kvps:
            key_obj = kv.get("key") or {}
            val_obj = kv.get("value") or {}
            result["key_value_pairs"].append(
                {
                    "key": key_obj.get("content"),
                    "value": val_obj.get("content"),
                    "confidence": kv.get("confidence"),
                    "key_boundingRegions": key_obj.get("boundingRegions"),
                    "value_boundingRegions": val_obj.get("boundingRegions"),
                }
            )

        docs = analyze.get("documents") or []
        flat = {}
        for di, doc in enumerate(docs):
            fields = doc.get("fields") or {}
            for fname, fval in fields.items():
                _flatten_fields(f"documents[{di}].fields.{fname}", fval, flat)
        result["documents_fields_flat"] = flat

        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return json.dumps({"error": str(exc), "has_adi": False}, ensure_ascii=False)


def _flatten_fields(prefix: str, obj: Any, out: Dict[str, Any]):
    if obj is None:
        out[prefix] = None
        return

    if isinstance(obj, dict):
        if any(k.startswith("value") for k in obj.keys()) and "confidence" in obj:
            val = None
            for k, v in obj.items():
                if k.startswith("value") and v is not None:
                    val = v
                    break
            out[prefix] = {
                "value": val,
                "confidence": obj.get("confidence"),
                "type": obj.get("type"),
            }
            return

        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            _flatten_fields(key, v, out)
        return

    if isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            _flatten_fields(key, v, out)
        return

    out[prefix] = obj
