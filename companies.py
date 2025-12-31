import json
import string

# characters to remove ONLY from start & end
STRIP_CHARS = "-+_ "

def load_companies(path="companies_list.json"):
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    companies = {}

    for item in raw:
        parts = item.split("__")
        if len(parts) < 3:
            continue

        symbol = parts[0].strip()
        company = parts[1].strip()

        # 🔥 CLEAN SLUG (start/end only)
        slug = parts[2].strip(STRIP_CHARS)

        companies[symbol] = {
            "company": company,
            "slug": slug
        }

    return companies
