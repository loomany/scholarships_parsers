"""Country eligibility normalization for scholarship parser rows."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable


_UNRESTRICTED_RE = re.compile(
    r"\b(unrestricted|all nationalit(?:y|ies)|any nationalit(?:y|ies)|worldwide|global|any country)\b",
    re.I,
)

_COUNTRY_ALIAS_ROWS = """
US|United States|United States of America|U.S.|U.S.A.|USA|US citizen|American
CA|Canada|Canadian
GB|United Kingdom|UK|U.K.|Great Britain|Britain|British|England|English|Scotland|Scottish|Wales|Welsh|Northern Ireland
CN|China|People's Republic of China|PRC|Chinese
IN|India|Indian
TR|Turkey|Turkiye|Türkiye|Turkish
IT|Italy|Italian
FR|France|French
DE|Germany|German
ES|Spain|Spanish
AU|Australia|Australian
AT|Austria|Austrian
BE|Belgium|Belgian
BR|Brazil|Brazilian
CH|Switzerland|Swiss
CL|Chile|Chilean
CO|Colombia|Colombian
DK|Denmark|Danish
FI|Finland|Finnish
GR|Greece|Greek
HK|Hong Kong
IE|Ireland|Irish
IL|Israel|Israeli
JP|Japan|Japanese
KR|South Korea|Korea, Republic of|Republic of Korea|Korean
MX|Mexico|Mexican
NL|Netherlands|Dutch|Holland
NO|Norway|Norwegian
NZ|New Zealand
PL|Poland|Polish
PT|Portugal|Portuguese
RU|Russia|Russian Federation|Russian
SE|Sweden|Swedish
SG|Singapore|Singaporean
TW|Taiwan|Taiwanese
UA|Ukraine|Ukrainian
ZA|South Africa|South African
AF|Afghanistan|Afghan
AL|Albania|Albanian
DZ|Algeria|Algerian
AD|Andorra|Andorran
AO|Angola|Angolan
AR|Argentina|Argentine|Argentinian
AM|Armenia|Armenian
AZ|Azerbaijan|Azerbaijani
BD|Bangladesh|Bangladeshi
BY|Belarus|Belarusian
BZ|Belize|Belizean
BJ|Benin|Beninese
BT|Bhutan|Bhutanese
BO|Bolivia|Bolivian
BA|Bosnia and Herzegovina|Bosnian
BW|Botswana|Botswanan
BG|Bulgaria|Bulgarian
BF|Burkina Faso|Burkinabe
BI|Burundi|Burundian
KH|Cambodia|Cambodian
CM|Cameroon|Cameroonian
CV|Cape Verde|Cabo Verde
CF|Central African Republic
TD|Chad|Chadian
CR|Costa Rica|Costa Rican
CI|Cote d'Ivoire|Ivory Coast|Ivorian
HR|Croatia|Croatian
CU|Cuba|Cuban
CY|Cyprus|Cypriot
CZ|Czech Republic|Czechia|Czech
CD|Democratic Republic of the Congo|Congo, Democratic Republic of the|DR Congo
DO|Dominican Republic|Dominican
EC|Ecuador|Ecuadorian
EG|Egypt|Egyptian
SV|El Salvador|Salvadoran
EE|Estonia|Estonian
ET|Ethiopia|Ethiopian
GE|Georgia|Georgian
GH|Ghana|Ghanaian
GT|Guatemala|Guatemalan
GN|Guinea|Guinean
HT|Haiti|Haitian
HN|Honduras|Honduran
HU|Hungary|Hungarian
IS|Iceland|Icelandic
ID|Indonesia|Indonesian
IR|Iran|Iranian
IQ|Iraq|Iraqi
JM|Jamaica|Jamaican
JO|Jordan|Jordanian
KZ|Kazakhstan|Kazakhstani
KE|Kenya|Kenyan
KW|Kuwait|Kuwaiti
KG|Kyrgyzstan|Kyrgyz
LA|Laos|Lao People's Democratic Republic
LV|Latvia|Latvian
LB|Lebanon|Lebanese
LS|Lesotho
LR|Liberia|Liberian
LY|Libya|Libyan
LI|Liechtenstein
LT|Lithuania|Lithuanian
LU|Luxembourg|Luxembourgish
MY|Malaysia|Malaysian
MV|Maldives|Maldivian
ML|Mali|Malian
MT|Malta|Maltese
MA|Morocco|Moroccan
MZ|Mozambique|Mozambican
MM|Myanmar|Burma|Burmese
NA|Namibia|Namibian
NP|Nepal|Nepali
NI|Nicaragua|Nicaraguan
NE|Niger|Nigerien
NG|Nigeria|Nigerian
MK|North Macedonia|Macedonia|Macedonian
OM|Oman|Omani
PK|Pakistan|Pakistani
PA|Panama|Panamanian
PY|Paraguay|Paraguayan
PE|Peru|Peruvian
PH|Philippines|Filipino|Philippine
QA|Qatar|Qatari
RO|Romania|Romanian
RW|Rwanda|Rwandan
SA|Saudi Arabia|Saudi|Saudi Arabian
SN|Senegal|Senegalese
RS|Serbia|Serbian
SK|Slovakia|Slovak
SI|Slovenia|Slovenian
SO|Somalia|Somali
LK|Sri Lanka|Sri Lankan
SD|Sudan|Sudanese
SY|Syria|Syrian
TJ|Tajikistan|Tajik
TZ|Tanzania|Tanzanian
TH|Thailand|Thai
TN|Tunisia|Tunisian
UG|Uganda|Ugandan
AE|United Arab Emirates|UAE|Emirati
UY|Uruguay|Uruguayan
UZ|Uzbekistan|Uzbek
VE|Venezuela|Venezuelan
VN|Vietnam|Viet Nam|Vietnamese
YE|Yemen|Yemeni
ZM|Zambia|Zambian
ZW|Zimbabwe|Zimbabwean
""".strip()


def _fold(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text).strip().lower()
    return re.sub(r"\s+", " ", text)


def _unique_codes(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = str(value or "").strip().upper()
        if re.fullmatch(r"[A-Z]{2}", code) and code not in seen:
            seen.add(code)
            out.append(code)
    return out


_ALIAS_TO_CODE: dict[str, str] = {}
for _row in _COUNTRY_ALIAS_ROWS.splitlines():
    _parts = [p.strip() for p in _row.split("|") if p.strip()]
    if len(_parts) < 2:
        continue
    _code, _aliases = _parts[0].upper(), _parts[1:]
    _ALIAS_TO_CODE[_fold(_code)] = _code
    for _alias in _aliases:
        _ALIAS_TO_CODE[_fold(_alias)] = _code

_ALIASES_BY_LENGTH = sorted(
    (
        (alias, code)
        for alias, code in _ALIAS_TO_CODE.items()
        if len(alias) >= 3 or alias in {"us", "uk"}
    ),
    key=lambda item: len(item[0]),
    reverse=True,
)


def country_codes_from_labels(labels: Any) -> list[str]:
    if not isinstance(labels, list):
        return []
    codes: list[str] = []
    for label in labels:
        folded = _fold(label)
        code = _ALIAS_TO_CODE.get(folded)
        if code:
            codes.append(code)
    return _unique_codes(codes)


def _codes_in_text(text: str) -> list[str]:
    folded = f" {_fold(text)} "
    hits: list[tuple[int, str]] = []
    for alias, code in _ALIASES_BY_LENGTH:
        match = re.search(rf"(?<![a-z0-9]){re.escape(alias)}(?![a-z0-9])", folded)
        if match:
            hits.append((match.start(), code))
    return _unique_codes(code for _pos, code in sorted(hits, key=lambda item: item[0]))


def _bounded_match_codes(patterns: Iterable[str], text: str) -> list[str]:
    if not text or _UNRESTRICTED_RE.search(text):
        return []
    codes: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.I):
            codes.extend(_codes_in_text(match.group(1)))
    return _unique_codes(codes)


def applicant_codes_from_text(text: str) -> list[str]:
    patterns = (
        r"\b(?:citizens?|nationals?|residents?|applicants?|students?)\s+(?:of|from|in)\s+([^.;:\n]{2,140})",
        r"\b(?:nationality|citizenship|residency|home country)\s*(?:required)?\s*[:\-]\s*([^.;\n]{2,140})",
        r"\bfrom\s+([^.;:\n]{2,100})\s+(?:may apply|are eligible|students|citizens|residents|nationals)\b",
        r"\b([^.;:\n]{2,100})\s+(?:citizens?|nationals?|residents?|students?|applicants?)\b",
    )
    return _bounded_match_codes(patterns, text)


def host_codes_from_text(text: str) -> list[str]:
    patterns = (
        r"\bhost countr(?:y|ies)\s*[:\-]\s*([^.;\n]{2,160})",
        r"\b(?:study|studying|research|program|opportunity)\s+in\s+([^.;:\n]{2,120})",
        r"\bhosted\s+in\s+([^.;:\n]{2,120})",
        r"\b(?:study location|location)\s*[:\-]\s*([^.;\n]{2,160})",
    )
    return _bounded_match_codes(patterns, text)


def apply_country_eligibility(record: dict[str, Any]) -> None:
    """Populate ISO-2 country arrays and parser notes in a scholarship row."""
    applicant_codes = _unique_codes(record.get("applicant_country_codes") or [])
    host_codes = _unique_codes(record.get("host_country_codes") or [])
    notes: list[str] = [str(x).strip() for x in (record.get("country_eligibility_notes") or []) if str(x).strip()]

    source = str(record.get("source") or "").strip().lower()
    if source == "iefa":
        iefa_applicant = country_codes_from_labels(record.get("applicant_country_names"))
        iefa_host = country_codes_from_labels(record.get("host_country_names"))
        if iefa_applicant:
            applicant_codes.extend(iefa_applicant)
            notes.append("IEFA nationality")
        if iefa_host:
            host_codes.extend(iefa_host)
            notes.append("IEFA host countries")

    if source != "iefa":
        applicant_blob = "\n".join(
            str(record.get(k) or "")
            for k in ("eligibility_text", "requirements_text", "who_can_apply", "country_summary")
        )
        host_blob = "\n".join(
            str(record.get(k) or "")
            for k in ("title", "description", "eligibility_text", "requirements_text", "country_summary")
        )
        text_applicant = applicant_codes_from_text(applicant_blob)
        text_host = host_codes_from_text(host_blob)
        if text_applicant:
            applicant_codes.extend(text_applicant)
            notes.append("applicant country text")
        if text_host:
            host_codes.extend(text_host)
            notes.append("host country text")

    record["applicant_country_codes"] = _unique_codes(applicant_codes)
    record["host_country_codes"] = _unique_codes(host_codes)
    record["country_eligibility_notes"] = list(dict.fromkeys(notes))
