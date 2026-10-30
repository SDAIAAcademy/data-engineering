# %% [markdown]
# # Lab 4.4 · Find it, label it, protect it
#
# Day 4 · Session 4 · 25 min. Qafila holds Saudi customers' personal data, so the **Personal Data Protection Law (PDPL)**
# applies. You'll build the data engineer's half of that:
# - find personal data by what the values look like, not by column names;
# - give every column a classification level;
# - replace IDs with tokens, and give each role its own masked view.
#
# **Tasks**
# - [ ] 1 · Look at where personal data hides (3')
# - [ ] 2 · Find PII by content: a pattern plus a checksum (6')
# - [ ] 3 · Classify every column (4')
# - [ ] 4 · Pseudonymize with a keyed hash (6')
# - [ ] 5 · Build a masked view per role (6')
# - [ ] ✱ Stretch · Redact free-text notes before they leave the platform
#
# ⚖️ This is an engineering exercise, not legal advice. Levels and rules come from your organization's policies and its data
# protection officer.

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements-colab.txt"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # solutions/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import de_utils as U
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import hashlib
import hmac
import random
import re

import duckdb
import pandas as pd

pd.set_option("display.max_colwidth", 90)
con = duckdb.connect()
con.sql(f"ATTACH '{U.build_warehouse().as_posix()}' AS wh (READ_ONLY)")
con.sql("CREATE TABLE customers AS SELECT * EXCLUDE (_loaded_at) FROM wh.raw.customers")

# Customer-support notes: free text typed by agents.
rng = random.Random(7)
cust = con.sql("SELECT customer_id, email, mobile, national_id, iban FROM customers WHERE iban IS NOT NULL").fetchall()
templates = [
    "Customer called from {mobile} about a late order.",
    "Refund to {iban} requested; identity checked against ID {nid}.",
    "Asked us to change the account email to {email}.",
    "Missing items in the bag, customer was polite, no follow-up needed.",
    "Courier could not find the building; tracking no. {tracking}.",
    "Order {order} delivered to the wrong district.",
]
notes = []
for i in range(400):
    cid, email, mob, nid, iban = rng.choice(cust)
    notes.append((i + 1, cid, rng.choice(templates).format(
        mobile=mob, iban=iban, nid=nid, email=email, order=rng.randint(5_000_000, 5_045_000),
        tracking="".join(str(rng.randint(0, 9)) for _ in range(10)))))
con.sql("CREATE TABLE support_notes (note_id INT, customer_id BIGINT, note VARCHAR)")
con.executemany("INSERT INTO support_notes VALUES (?, ?, ?)", notes)
print(f"customers: {con.sql('SELECT count(*) FROM customers').fetchall()[0][0]:,} | support notes: {len(notes)}")

# %% [markdown]
# ## 1 · Look at where personal data hides (3')
#
# **Personal data** is anything that identifies a person. **PII** (personally identifiable information) is the everyday name
# for it. Here's Khalid's row, then a few support notes. Run the cell.

# %%
print(con.sql("SELECT customer_id, full_name, email, mobile, national_id, iban, city, district FROM customers WHERE customer_id = 107371").df().T.to_string(header=False))
con.sql("SELECT note FROM support_notes LIMIT 6").df()

# %% [markdown]
# 🔍 A column called `note` holds IDs, IBANs, mobiles and emails. A check of column names would mark it as harmless.
# So discovery tools read the **values**.
#
# ## 2 · Find PII by content: a pattern plus a checksum (6')
#
# Saudi national IDs (and Iqama numbers) have 10 digits, start with `1` or `2`, and end in a **check digit**: from the right, double
# every second digit, subtract 9 from anything above 9, add everything up, and the total must divide by 10 (the **Luhn** check).
# A pattern finds **candidates**, and the checksum **confirms** them. The check is written for you.
#
# ✏️ **Exercise 1** · Write `find_national_ids(text)`: every 10-digit candidate in `text` that passes `is_saudi_id`.
#
# *Hint:* `re.findall(ID_PATTERN, text)` gives the candidates. A list comprehension with `if is_saudi_id(c)` keeps the real ones. One line.

# %%
ID_PATTERN = r"\b[12]\d{9}\b"

def is_saudi_id(s: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(s)):
        d = int(ch) * (2 if i % 2 == 1 else 1)
        total += d - 9 if d > 9 else d
    return len(s) == 10 and s[0] in "12" and total % 10 == 0

def find_national_ids(text: str) -> list:
    # BEGIN SOLUTION
    return [c for c in re.findall(ID_PATTERN, text) if is_saudi_id(c)]
    # END SOLUTION

# %%
try:
    find_national_ids("x")
except NotImplementedError:
    print("⏭️ Exercise 1 not done yet. Expected: fewer real IDs than candidates. Using the reference.")
    def find_national_ids(text):
        return [c for c in re.findall(ID_PATTERN, text) if is_saudi_id(c)]

candidates = sum(len(re.findall(ID_PATTERN, n)) for _, _, n in notes)
real_ids = sum(len(find_national_ids(n)) for _, _, n in notes)
print(f"10-digit candidates in the notes: {candidates} | real national IDs: {real_ids} | false alarms removed: {candidates - real_ids}")
U.check(real_ids < candidates, "the check digit removed the tracking numbers")
U.check(find_national_ids("ID 1278681299, tracking 1234567890") == ["1278681299"], "Khalid's ID passes; a random number doesn't")

# %% [markdown]
# Now the same idea for four kinds of PII across every text column. IBANs have a checksum too (ISO 13616: rearrange, then the number
# mod 97 must be 1). Mobiles and emails only have patterns. Run the cell.

# %%
def is_saudi_iban(s: str) -> bool:
    return int("".join(str(int(c, 36)) for c in s[4:] + s[:4])) % 97 == 1

DETECTORS = {   # type -> (pattern, confirm)
    "national_id": (ID_PATTERN, is_saudi_id),
    "iban":        (r"\bSA\d{22}\b", is_saudi_iban),
    "mobile":      (r"(?<![\w+])(?:\+?966|0)\s?5\d(?:\s?\d){7}(?!\d)", lambda s: True),
    "email":       (r"[\w.+-]+@[\w-]+\.[\w.]+", lambda s: True),
}

def find_pii(text: str) -> dict:
    """{pii type: [matches]} for one value."""
    found = {}
    for kind, (pattern, confirm) in DETECTORS.items():
        hits = [m for m in re.findall(pattern, text or "") if confirm(m)]
        if hits:
            found[kind] = hits
    return found

def scan_table(table: str, sample: int = 2000) -> pd.DataFrame:
    """For each text column: the share of sampled values containing each type of PII."""
    cols = [r[0] for r in con.sql(f"DESCRIBE {table}").fetchall() if r[1] == "VARCHAR"]
    rows = []
    for col in cols:
        values = [v for (v,) in con.sql(f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL USING SAMPLE {sample} ROWS (reservoir, 1)").fetchall()]
        hits = pd.Series([k for v in values for k in find_pii(str(v))], dtype=str).value_counts()
        for kind, n in hits.items():
            rows.append({"table": table, "column": col, "pii": kind, "share": round(n / len(values), 3)})
    return pd.DataFrame(rows)

inventory = pd.concat([scan_table("customers"), scan_table("support_notes")], ignore_index=True)
U.check(set(inventory[inventory.table == "support_notes"].pii) == {"national_id", "iban", "mobile", "email"}, "all four types found in free text")
inventory

# %% [markdown]
# ## 3 · Classify every column (4')
#
# Saudi Arabia's **NDMO** (National Data Management Office) uses four levels: **Top Secret, Secret, Restricted, Public**. Below is an
# *illustrative* Qafila policy. A column takes the highest level among what it holds. A name has no pattern, so we declare it. A column
# with no PII of its own, in a table that holds personal data, is still Restricted: a `city` next to a name identifies someone. Run the cell.

# %%
LEVELS = ["Public", "Restricted", "Secret", "Top Secret"]
POLICY = {   # pii type -> (level, handling)
    "national_id": ("Secret", "pseudonymize; raw value only in a vault"),
    "iban":        ("Secret", "never leaves the payments domain"),
    "mobile":      ("Restricted", "mask for everyone except support"),
    "email":       ("Restricted", "mask for everyone except support"),
    "name":        ("Restricted", "mask outside support"),
}
DECLARED = {"full_name": "name"}   # some PII can't be found by pattern

rows = []
for table in ["customers", "support_notes"]:
    for (col,) in con.sql(f"SELECT column_name FROM (DESCRIBE {table})").fetchall():
        kinds = sorted(set(inventory[(inventory.table == table) & (inventory.column == col)].pii)
                       | ({DECLARED[col]} if col in DECLARED else set()))
        if kinds:
            top = max(kinds, key=lambda k: LEVELS.index(POLICY[k][0]))
            level, handling = POLICY[top]
        else:
            level, handling = "Restricted", "personal data by association"
        rows.append({"table": table, "column": col, "pii": ", ".join(kinds) or "-", "level": level, "handling": handling})
catalog = pd.DataFrame(rows)
lvl = catalog.set_index(["table", "column"]).level
U.check(lvl["customers", "national_id"] == "Secret", "national IDs are Secret")
U.check(lvl["support_notes", "note"] == "Secret", "free text takes its most sensitive content's level")
print(catalog.level.value_counts().to_string())
catalog

# %% [markdown]
# 🔍 Why is `support_notes.note` Secret, when its column name says nothing? What would change if you found an IBAN in `district`?
# At work these labels are **tags** in a data catalog, and masking policies read them.
#
# ## 4 · Pseudonymize with a keyed hash (6')
#
# Analysts need to count **people**, not accounts, without seeing a national ID. So replace each ID with a **token**: the same ID always
# gives the same token. **A plain hash isn't enough.** Anyone holding a list of IDs can hash them too and match. Run the attack.

# %%
plain = {hashlib.sha256(nid.encode()).hexdigest() for (nid,) in con.sql("SELECT national_id FROM customers").fetchall()}
leaked_list = [nid for (nid,) in con.sql("SELECT national_id FROM customers USING SAMPLE 500 ROWS (reservoir, 3)").fetchall()]
reidentified = sum(hashlib.sha256(n.encode()).hexdigest() in plain for n in leaked_list)
print(f"attacker holding {len(leaked_list)} leaked IDs matches {reidentified} of them in the 'anonymous' plain hashes")

# %% [markdown]
# ✏️ **Exercise 2** · Write `pseudonymize(value, key)`: the **HMAC-SHA256** of `value` under a secret `key`, first 20 hex characters. An HMAC is a hash
# that also needs the key, so nobody can recompute it without the key.
#
# *Hint:* `hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:20]`. One line.

# %%
PSEUDO_KEY = os.environ.get("QAFILA_PSEUDO_KEY", "lab-only-key-keep-real-ones-in-a-secret-manager").encode()

def pseudonymize(value: str, key: bytes = PSEUDO_KEY) -> str:
    # BEGIN SOLUTION
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:20]
    # END SOLUTION

# %%
try:
    pseudonymize("x")
except NotImplementedError:
    print("⏭️ Exercise 2 not done yet. Expected: 58 people with two accounts, 0 leaked IDs matched. Using the reference.")
    def pseudonymize(value, key=PSEUDO_KEY):
        return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:20]

con.create_function("pseudo", lambda v: pseudonymize(v), ["VARCHAR"], "VARCHAR")
accounts, people = con.sql("SELECT count(*), count(DISTINCT pseudo(national_id)) FROM customers").fetchall()[0]
twice = con.sql("SELECT count(*) FROM (SELECT pseudo(national_id) FROM customers GROUP BY 1 HAVING count(*) > 1)").fetchall()[0][0]
matched = sum(pseudonymize(n) in plain for n in leaked_list)
print(f"{accounts:,} accounts | {people:,} people | {twice} people with two accounts | leaked IDs matched without the key: {matched}")
print("Khalid's token:", pseudonymize("1278681299"))
U.check(twice == 58, "the 58 people with two accounts still show up, with no ID in sight")
U.check(matched == 0, "without the key, the leaked list matches nothing")

# %% [markdown]
# 🔍 Pseudonymized data is **still personal data** under PDPL: whoever holds the key can match it. It lowers the risk and it's the right
# default for analytics. The key belongs in a secret manager, never in the repo. What happens to every join if you change the key?
#
# ## 5 · Build a masked view per role (6')
#
# **Least privilege:** each role reads only what its job needs. DuckDB has no column security, so we model it the classic way: each role
# may read only its own **views**. Warehouses do the same with grants plus masking policies. Support's view is done.

# %%
con.sql("CREATE SCHEMA analyst; CREATE SCHEMA support")
con.sql("""CREATE MACRO mask_mobile(m) AS
           CASE WHEN m IS NULL THEN NULL ELSE '+9665•••••' || right(regexp_replace(m, '\\D', '', 'g'), 2) END""")
con.sql("""CREATE VIEW support.customers AS
           SELECT customer_id, full_name, email, mask_mobile(mobile) AS mobile, city, district FROM main.customers""")

def query_as(role: str, sql_text: str) -> pd.DataFrame:
    """Toy access control: a role may only reference its own schema."""
    referenced = set(re.findall(r"\b(\w+)\.\w+", sql_text))
    if referenced - {role}:
        raise PermissionError(f"{role} may not read {referenced - {role}}")
    return con.sql(sql_text).df()

query_as("support", "SELECT * FROM support.customers WHERE customer_id = 107371")

# %% [markdown]
# ✏️ **Exercise 3** · Write the `SELECT` for `analyst.customers`: `person_token` (`pseudo(national_id)`), `customer_id`, `city`, `district`,
# `email_domain` (the part after `@`) and `created_at`. No names, no mobiles, no IBANs, no raw IDs.
#
# *Hint:* `lower(split_part(email, '@', 2)) AS email_domain`. About four lines of SQL.

# %%
def analyst_view_sql():
    # return """SELECT pseudo(national_id) AS person_token, customer_id, ... FROM main.customers"""
    # BEGIN SOLUTION
    return """SELECT pseudo(national_id) AS person_token, customer_id, city, district,
                     lower(split_part(email, '@', 2)) AS email_domain, created_at
              FROM main.customers"""
    # END SOLUTION

# %%
try:
    select_sql = analyst_view_sql()
except NotImplementedError:
    print("⏭️ Exercise 3 not done yet. Expected: the scanner finds nothing in the view. Using the reference.")
    select_sql = """SELECT pseudo(national_id) AS person_token, customer_id, city, district,
                           lower(split_part(email, '@', 2)) AS email_domain, created_at FROM main.customers"""
con.sql(f"CREATE OR REPLACE VIEW analyst.customers AS {select_sql}")

sample = query_as("analyst", "SELECT * FROM analyst.customers")
leaks = int(sample.map(lambda v: bool(find_pii(str(v)))).sum().sum())
print(sample[sample.customer_id == 107371].T.to_string(header=False))
U.check(leaks == 0, "the PII scanner finds nothing in the analyst view")
try:
    query_as("analyst", "SELECT national_id FROM main.customers")
    U.check(False, "analysts must not read the base table")
except PermissionError as e:
    U.check(True, f"blocked: {e}")

# %% [markdown]
# 🔍 This view works only if every tool goes through it. A warehouse's masking policy sits on the column, so a new tool can't get around it.
# Real erasure requests go further: they must reach every copy, including old table snapshots and backups.
#
# ## ✱ Stretch · Redact free-text notes before they leave the platform
#
# Write `redact(text)` that replaces every detected value with a typed placeholder (`[NATIONAL_ID]`, `[IBAN]`, `[MOBILE]`, `[EMAIL]`).

# %%
def redact(text: str) -> str:
    # BEGIN SOLUTION
    for kind, values in find_pii(text).items():
        for v in values:
            text = text.replace(v, f"[{kind.upper()}]")
    return text
    # END SOLUTION

try:
    redacted = [redact(n) for _, _, n in notes]
    print("\n".join(redacted[:4]))
    U.check(not any(find_pii(t) for t in redacted), "no PII left in any note")
except NotImplementedError:
    print("✱ Optional: write `redact(text)` if you have time.")

# %% [markdown]
# ## Checkpoint
#
# 1. Why confirm with a checksum instead of using a pattern alone?
#
# <details><summary>Answer</summary>A pattern also matches tracking numbers and other digits. A check digit tells a real ID from a random number, so the inventory has fewer false alarms and people trust it.</details>
#
# 2. Is pseudonymized data still personal data under PDPL?
#
# <details><summary>Answer</summary>Yes. Whoever holds the key can match it back to people. It lowers the risk but doesn't remove the duty to protect it.</details>
#
# 3. Why is a plain SHA-256 of a national ID not enough?
#
# <details><summary>Answer</summary>Anyone with a list of IDs can hash them and match. In this lab that was 500 out of 500. A keyed hash needs a secret the attacker doesn't have.</details>
#
# ## Carry forward
#
# Your capstone needs a short governance note: the level of every column that holds personal data, and how you protect it. This lab's `catalog`
# table and `analyst.customers` view are the pattern to copy.
