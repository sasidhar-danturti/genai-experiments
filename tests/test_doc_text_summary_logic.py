def _first_non_null(values):
    for value in values:
        if value is not None:
            return value
    return None


def _aggregate_doc_pages(rows):
    per_page = {}
    for row in rows:
        key = (row["docuid"], row["page_number"])
        if key not in per_page:
            per_page[key] = {
                "docuid": row["docuid"],
                "page_number": row["page_number"],
                "extracted_page_text": row["extracted_page_text"],
                "standardized_page_text": row["standardized_page_text"],
                "normalized_response_json": row["normalized_response_json"],
                "page_avg_conf": row["page_avg_conf"],
                "page_low_conf_words": row["page_low_conf_words"],
                "page_total_words": row["page_total_words"],
                "source": row["source"],
                "parser_type": row["parser_type"],
                "doc_kind": row["doc_kind"],
            }
        else:
            existing = per_page[key]
            existing["extracted_page_text"] = _first_non_null(
                [existing["extracted_page_text"], row["extracted_page_text"]]
            )
            existing["standardized_page_text"] = _first_non_null(
                [existing["standardized_page_text"], row["standardized_page_text"]]
            )
            existing["normalized_response_json"] = _first_non_null(
                [existing["normalized_response_json"], row["normalized_response_json"]]
            )
            existing["page_avg_conf"] = _first_non_null(
                [existing["page_avg_conf"], row["page_avg_conf"]]
            )
            existing["page_low_conf_words"] = _first_non_null(
                [existing["page_low_conf_words"], row["page_low_conf_words"]]
            )
            existing["page_total_words"] = _first_non_null(
                [existing["page_total_words"], row["page_total_words"]]
            )
            existing["source"] = _first_non_null([existing["source"], row["source"]])
            existing["parser_type"] = _first_non_null(
                [existing["parser_type"], row["parser_type"]]
            )
            existing["doc_kind"] = _first_non_null(
                [existing["doc_kind"], row["doc_kind"]]
            )
    docs = {}
    for row in per_page.values():
        docuid = row["docuid"]
        docs.setdefault(docuid, {"pages": []})
        docs[docuid]["pages"].append(row)
    outputs = {}
    for docuid, payload in docs.items():
        pages = sorted(payload["pages"], key=lambda item: item["page_number"])
        outputs[docuid] = {
            "extracted_doc_text": {
                str(item["page_number"]): item["extracted_page_text"] for item in pages
            },
            "standardized_doc_text": {
                str(item["page_number"]): item["standardized_page_text"] for item in pages
            },
            "normalized_response_json": {
                str(item["page_number"]): item["normalized_response_json"] for item in pages
            },
        }
    return outputs


def test_doc_text_summary_dedupes_page_keys():
    rows = [
        {
            "docuid": "A",
            "page_number": 1,
            "extracted_page_text": "p1a",
            "standardized_page_text": "s1a",
            "normalized_response_json": '{"a":1}',
            "page_avg_conf": 0.9,
            "page_low_conf_words": 0,
            "page_total_words": 10,
            "source": "src",
            "parser_type": "parser",
            "doc_kind": "doc",
        },
        {
            "docuid": "A",
            "page_number": 1,
            "extracted_page_text": None,
            "standardized_page_text": "s1b",
            "normalized_response_json": '{"a":2}',
            "page_avg_conf": None,
            "page_low_conf_words": 1,
            "page_total_words": None,
            "source": None,
            "parser_type": None,
            "doc_kind": None,
        },
        {
            "docuid": "A",
            "page_number": 2,
            "extracted_page_text": "p2",
            "standardized_page_text": "s2",
            "normalized_response_json": '{"b":1}',
            "page_avg_conf": 0.95,
            "page_low_conf_words": 0,
            "page_total_words": 12,
            "source": "src",
            "parser_type": "parser",
            "doc_kind": "doc",
        },
    ]

    output = _aggregate_doc_pages(rows)

    assert set(output.keys()) == {"A"}
    assert output["A"]["extracted_doc_text"] == {"1": "p1a", "2": "p2"}
    assert output["A"]["standardized_doc_text"] == {"1": "s1a", "2": "s2"}
    assert output["A"]["normalized_response_json"] == {"1": '{"a":1}', "2": '{"b":1}'}
