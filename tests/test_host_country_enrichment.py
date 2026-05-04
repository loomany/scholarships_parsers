from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from country_eligibility import apply_country_eligibility
from normalize_scholarship import apply_normalization


def _base_record(**overrides):
    record = {
        "source": "unit_source",
        "source_id": "unit-1",
        "url": "https://example.com/scholarship",
        "title": "Unit Test Scholarship",
        "provider_name": "Unit Provider",
        "award_amount_text": "$1,000",
        "deadline_text": "December 31, 2099",
        "deadline_date": "2099-12-31",
        "description": "A source-backed test scholarship.",
        "eligibility_text": "Open to eligible students.",
        "requirements_text": "Submit an application.",
        "raw_data": {},
    }
    record.update(overrides)
    return record


def test_unknown_host_keeps_empty_array_and_evidence():
    record = _base_record()

    apply_country_eligibility(record)

    assert record["host_country_codes"] == []
    assert record["raw_data"]["host_enrichment"]["status"] == "unknown"
    assert record["raw_data"]["host_enrichment"]["method"] == "none"
    assert record["raw_data"]["host_enrichment"]["confidence"] == "none"


def test_state_text_infers_us():
    record = _base_record(state_territory_text="California residents preferred")

    apply_country_eligibility(record)

    assert record["host_country_codes"] == ["US"]
    assert record["raw_data"]["host_enrichment"]["method"] == "state_text"


def test_edu_and_gov_domains_infer_us():
    edu = _base_record(provider_url="https://financialaid.example.edu")
    gov = _base_record(apply_url="https://agency.example.gov/apply")

    apply_country_eligibility(edu)
    apply_country_eligibility(gov)

    assert edu["host_country_codes"] == ["US"]
    assert gov["host_country_codes"] == ["US"]
    assert edu["raw_data"]["host_enrichment"]["method"] == "domain_tld"
    assert gov["raw_data"]["host_enrichment"]["method"] == "domain_tld"


def test_clear_cctlds_infer_host_country():
    de = _base_record(provider_url="https://www.daad.de/en/")
    ca = _base_record(apply_url="https://scholarships.example.ca/apply")

    apply_country_eligibility(de)
    apply_country_eligibility(ca)

    assert de["host_country_codes"] == ["DE"]
    assert ca["host_country_codes"] == ["CA"]


def test_dot_com_does_not_infer_host_country():
    record = _base_record(provider_url="https://example.com")

    apply_country_eligibility(record)

    assert record["host_country_codes"] == []
    assert record["raw_data"]["host_enrichment"]["status"] == "unknown"


def test_host_applicant_overlap_cleanup():
    record = _base_record(
        host_country_codes=["US"],
        applicant_country_codes=["US", "CA"],
    )

    apply_country_eligibility(record)

    assert record["host_country_codes"] == ["US"]
    assert record["applicant_country_codes"] == ["CA"]


def test_apply_normalization_preserves_official_source_name():
    record = _base_record(official_source_name="Bold.org")

    apply_normalization(record)

    assert record["official_source_name"] == "Bold.org"
    assert record["raw_data"]["host_enrichment"]["status"] == "unknown"
