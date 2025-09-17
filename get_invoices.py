from datetime import datetime as dt

import pandas as pd
import pandas_gbq
import requests
import os
import time
import json
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor, as_completed

SCOPES = ["https://www.googleapis.com/auth/bigquery"]
creds_dict = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)


# Constants
BQ_TABLE = "Pennylane.suppliers_invoices"
PROJECT_ID = "korner-datalake"
BASE_URL = "https://app.pennylane.com/api/external/v2/supplier_invoices"
filters = [{"field": "date", "operator": "gteq", "value": "2025-01-01"}]

# Pennylane tokens from GitHub secret (JSON string)
tokens = json.loads(os.environ["PENNYLANE_TOKENS"])

# Retry-safe GET request
def request_with_retries(url, headers, params, max_retries=5):
    retries = 0
    while retries < max_retries:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            wait = min(60, 2 ** retries)  # Cap wait to 60s max
            print(f"Rate limited. Waiting {wait}s before retry #{retries+1}...")
            time.sleep(wait)
            retries += 1
        else:
            print(f"[{response.status_code}] Error: {response.text}")
            return None
    print("Max retries exceeded.")
    return None


# Per-token fetching function
def fetch_invoices_for_token(token_name, token_value):
    print(f"🔄 Fetching invoices for {token_name}")
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token_value}"
    }

    rows = []
    cursor = None
    has_more = True
    params = {"filter": json.dumps(filters)}
    while has_more:
        if cursor:
            params["cursor"] = cursor

        response = request_with_retries(BASE_URL, headers, params)
        if not response:
            break

        try:
            data = response.json()
        except Exception as e:
            print(f"Failed to decode JSON for token {token_name}: {e}")
            break

        items = data.get("items")
        if items is None:
            print(f"No 'items' found in response for token {token_name}: {data}")
            break
        for invoice in data.get("items", []):
            amount = invoice.get("amount"),
            invoice_date = invoice.get("date")
            due_date = invoice.get("deadline")
            label = invoice.get("label")
            payment_status = invoice.get("payment_status")
            updated_at = invoice.get("updated_at")
            reconciled = invoice.get("reconciled")
            accounting_status = invoice.get("accounting_status")
            fournisseur = invoice.get("supplier")
            if fournisseur:
                fournisseur_id = fournisseur.get("id")
            else:
                fournisseur_id = None
            matched_transaction = invoice.get("matched_transactions")
            if matched_transaction:
                transaction = matched_transaction.get("url")
            else:
                transaction = None

            rows.append({
                'tag': token_name,
                'amount': amount,
                'invoice_date': invoice_date,
                'echeance': due_date,
                'label': label,
                'payment_status': payment_status,
                'updated_at': updated_at,
                'reconciled': reconciled,
                'accounting_status': accounting_status,
                'fournisseur_id': fournisseur_id,
                'matching_transaction': transaction
            })
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
        time.sleep(0.5)  # Politeness delay

    return rows

# Transaction fetch function
def fetch_transaction_date_with_retries(url, headers, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                items = response.json().get("items", [])
                return items[0].get("date") if items else None
            elif response.status_code == 429:
                print("⚠️ Rate limited. Backing off...(date fetching")
                time.sleep(2 ** retries)
            elif 500 <= response.status_code < 600:
                print(f"⚠️ Server error ({response.status_code}). Retrying...")
                time.sleep(2 ** retries)
            else:
                print(f"❌ Unexpected response [{response.status_code}]: {response.text[:100]}")
                return None
        except Exception:
            return None
    return None


# Parallel execution
all_rows = []

with ThreadPoolExecutor(max_workers=3) as executor:
    future_to_token = {
        executor.submit(fetch_invoices_for_token, name, value): name
        for name, value in tokens.items()
    }

    for future in as_completed(future_to_token):
        token_name = future_to_token[future]
        try:
            result = future.result()
            print(f"✅ Done: {token_name} ({len(result)} rows)")
            all_rows.extend(result)
        except Exception as e:
            print(f"❌ Error for {token_name}: {e}")

# Merge into DataFrame
df = pd.DataFrame(all_rows)
df = df[(df['reconciled'] == True) & (df['accounting_status'] != 'archived')]


transaction_dates = []

for idx, row in df.iterrows():
    url = row["matching_transaction"]
    tag = row["tag"]

    if not url or pd.isna(url) or not isinstance(url, str) or not url.strip():
        transaction_dates.append(None)
        continue

    token = tokens.get(tag)
    if not token:
        print(f"⚠️ No token found for tag '{tag}', skipping.")
        transaction_dates.append(None)
        continue

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}"
    }

    date = fetch_transaction_date_with_retries(url, headers)
    transaction_dates.append(date)

df["transaction_date"] = transaction_dates

df = df[df["transaction_date"].notna()]

pandas_gbq.to_gbq(
    df,
    destination_table=BQ_TABLE,
    project_id=PROJECT_ID,
    credentials=creds,
    if_exists="replace",
    chunksize=10000
)
