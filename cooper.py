# =========================
# Cooper / CMS quick outlier visuals (single-year) — FULL SCRIPT (heap tie-break fixed)
# =========================

import sys
import os
import zipfile
import subprocess
import importlib
import heapq
from itertools import count
from collections import defaultdict

# --------- USER CONFIG ----------
ZIP_PATH = "Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023.zip"

CHUNKSIZE = 500_000          # lower if you run out of RAM
SAMPLE_FRAC = 0.002          # fraction of rows sampled for scatter plots
MIN_BENES_FOR_OUTLIERS = 30  # small-denominator guardrail
MIN_SERVCS_FOR_OUTLIERS = 50
TOPK_KEEP = 300              # outlier rows to export
TOP_SPECIALTIES_FOR_PLOTS = 20
# --------------------------------
"""
CMS Medicare Provider & Service Outlier Explorer (2023)

Expects:
  Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023.zip
in the project folder (or update ZIP_PATH).

Outputs (generated locally; not committed to git):
  - outliers_intensity_top.csv
  - outliers_compression_top.csv
  - outliers_allowed_dollars_top.csv

Notes:
  - Repo does not include raw CMS data.
  - Scatter plots use sampling for speed; specialty and RUCA aggregates are computed across all rows streamed.
  - Avoid committing any files containing HCPCS descriptions (licensing).
"""

import sys
import os
import zipfile
import heapq
from itertools import count
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# =========================
# Helpers
# =========================
def detect_delimiter(file_like, nbytes=8192):
    pos = file_like.tell()
    sample = file_like.read(nbytes).decode("utf-8", errors="replace")
    file_like.seek(pos)
    counts = {",": sample.count(","), "\t": sample.count("\t"), "|": sample.count("|"), ";": sample.count(";")}
    return max(counts, key=counts.get)

def ruca_bucket(ruca_code):
    # best-effort coarse grouping by numeric RUCA
    try:
        if pd.isna(ruca_code):
            return "Unknown"
        x = int(float(ruca_code))
        if 1 <= x <= 3:
            return "Urban"
        if 4 <= x <= 6:
            return "Large Rural"
        if 7 <= x <= 9:
            return "Small Rural"
        if x >= 10:
            return "Isolated/Other"
    except Exception:
        pass
    return "Unknown"

def safe_div(a, b):
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    out = np.full_like(a, np.nan, dtype="float64")
    mask = np.isfinite(a) & np.isfinite(b) & (b != 0)
    out[mask] = a[mask] / b[mask]
    return out

# Tie-breaker counter to avoid heap comparing dicts when scores tie
_tiebreak = count()

def push_topk(heap, score, payload, k=TOPK_KEEP):
    """
    Keep top-k largest 'score' items in a min-heap.
    Heap items are (score, tie_breaker, payload_dict) so ties never compare dicts.
    """
    item = (float(score), next(_tiebreak), payload)

    if len(heap) < k:
        heapq.heappush(heap, item)
    else:
        if item[0] > heap[0][0]:
            heapq.heapreplace(heap, item)


# =========================
# Locate data file inside ZIP
# =========================
if not os.path.exists(ZIP_PATH):
    raise FileNotFoundError(f"ZIP not found: {ZIP_PATH}")

with zipfile.ZipFile(ZIP_PATH, "r") as z:
    members = [n for n in z.namelist() if n.lower().endswith((".csv", ".txt"))]
    if not members:
        raise RuntimeError("No .csv or .txt found inside the zip file.")

    members_sorted = sorted(members, key=lambda n: z.getinfo(n).file_size, reverse=True)
    DATA_MEMBER = members_sorted[0]
    print("Using file inside zip:", DATA_MEMBER)

    with z.open(DATA_MEMBER, "r") as f0:
        delim = detect_delimiter(f0)
        print("Detected delimiter:", repr(delim))


# =========================
# Columns (from your pasted dictionary)
# NOTE: avoid HCPCS_Desc (licensing) — not used here.
# =========================
USECOLS = [
    "Rndrng_NPI",
    "Rndrng_Prvdr_Type",
    "Rndrng_Prvdr_State_Abrvtn",
    "Rndrng_Prvdr_RUCA",
    "Place_Of_Srvc",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
    "HCPCS_Cd",
    "HCPCS_Drug_Ind",
    "Tot_Benes",
    "Tot_Srvcs",
    "Tot_Bene_Day_Srvcs",
    "Avg_Sbmtd_Chrg",
    "Avg_Mdcr_Alowd_Amt",
    "Avg_Mdcr_Pymt_Amt",
    "Avg_Mdcr_Stdzd_Amt",
]

DTYPES = {
    "Rndrng_NPI": "string",
    "Rndrng_Prvdr_Type": "string",
    "Rndrng_Prvdr_State_Abrvtn": "string",
    "Rndrng_Prvdr_RUCA": "string",
    "Place_Of_Srvc": "string",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "string",
    "HCPCS_Cd": "string",
    "HCPCS_Drug_Ind": "string",
}

NUM_COLS = [
    "Tot_Benes",
    "Tot_Srvcs",
    "Tot_Bene_Day_Srvcs",
    "Avg_Sbmtd_Chrg",
    "Avg_Mdcr_Alowd_Amt",
    "Avg_Mdcr_Pymt_Amt",
    "Avg_Mdcr_Stdzd_Amt",
]


# =========================
# Streaming accumulators
# =========================
rng = np.random.default_rng(42)

sample_allowed = []
sample_compress = []
sample_intensity = []
sample_benes = []
sample_pos = []        # F=1, O=0
sample_specialty = []  # optional use later

spec_sum_allowed = defaultdict(float)
spec_sum_paid = defaultdict(float)
spec_sum_srvcs = defaultdict(float)
spec_sum_benes = defaultdict(float)

ruca_spec_srvcs = defaultdict(float)
ruca_spec_benes = defaultdict(float)

spec_compress_samples = defaultdict(list)

heap_intensity = []
heap_compress = []
heap_allowed_dollars = []


# =========================
# Read in chunks and compute key metrics
# =========================
with zipfile.ZipFile(ZIP_PATH, "r") as z:
    with z.open(DATA_MEMBER, "r") as f:
        reader = pd.read_csv(
            f,
            sep=delim,
            usecols=lambda c: c in USECOLS,
            dtype=DTYPES,
            chunksize=CHUNKSIZE,
            low_memory=False,
            encoding="utf-8",
            encoding_errors="replace",
        )

        for chunk_i, chunk in enumerate(reader, start=1):
            # numeric coercion
            for c in NUM_COLS:
                if c in chunk.columns:
                    chunk[c] = pd.to_numeric(chunk[c], errors="coerce")

            # normalize categories
            chunk["Place_Of_Srvc"] = chunk["Place_Of_Srvc"].astype("string").str.strip().str.upper()
            chunk["Rndrng_Prvdr_Mdcr_Prtcptg_Ind"] = chunk["Rndrng_Prvdr_Mdcr_Prtcptg_Ind"].astype("string").str.strip().str.upper()
            chunk["HCPCS_Drug_Ind"] = chunk["HCPCS_Drug_Ind"].astype("string").str.strip().str.upper()
            chunk["Rndrng_Prvdr_Type"] = chunk["Rndrng_Prvdr_Type"].astype("string").str.strip()

            avg_allowed = chunk["Avg_Mdcr_Alowd_Amt"].to_numpy(dtype="float64", na_value=np.nan)
            avg_paid = chunk["Avg_Mdcr_Pymt_Amt"].to_numpy(dtype="float64", na_value=np.nan)
            avg_sub = chunk["Avg_Sbmtd_Chrg"].to_numpy(dtype="float64", na_value=np.nan)
            tot_srvcs = chunk["Tot_Srvcs"].to_numpy(dtype="float64", na_value=np.nan)
            tot_benes = chunk["Tot_Benes"].to_numpy(dtype="float64", na_value=np.nan)

            allowed_dollars = avg_allowed * tot_srvcs
            paid_dollars = avg_paid * tot_srvcs

            compress = safe_div(avg_sub, avg_allowed)      # charge-to-allowed
            intensity = safe_div(tot_srvcs, tot_benes)     # services per bene

            spec = chunk["Rndrng_Prvdr_Type"].fillna("Unknown").astype("string").to_numpy()

            # Specialty totals
            tmp = pd.DataFrame({
                "spec": spec,
                "allowed_$": allowed_dollars,
                "paid_$": paid_dollars,
                "srvcs": tot_srvcs,
                "benes": tot_benes,
            })
            g = tmp.groupby("spec", dropna=False).sum(numeric_only=True)
            for s, row in g.iterrows():
                s = str(s)
                spec_sum_allowed[s] += float(row["allowed_$"])
                spec_sum_paid[s] += float(row["paid_$"])
                spec_sum_srvcs[s] += float(row["srvcs"])
                spec_sum_benes[s] += float(row["benes"])

            # RUCA bucket × specialty aggregates
            ruca_codes = chunk["Rndrng_Prvdr_RUCA"].to_numpy()
            ruca_b = np.array([ruca_bucket(x) for x in ruca_codes], dtype=object)
            tmp2 = pd.DataFrame({
                "ruca": ruca_b,
                "spec": spec,
                "srvcs": tot_srvcs,
                "benes": tot_benes,
            })
            g2 = tmp2.groupby(["ruca", "spec"], dropna=False).sum(numeric_only=True)
            for (rb, s), row in g2.iterrows():
                key = (str(rb), str(s))
                ruca_spec_srvcs[key] += float(row["srvcs"])
                ruca_spec_benes[key] += float(row["benes"])

            # Plot sampling
            valid = (
                np.isfinite(avg_allowed) & (avg_allowed > 0) &
                np.isfinite(tot_benes) & (tot_benes > 0) &
                np.isfinite(tot_srvcs) & (tot_srvcs > 0) &
                np.isfinite(compress) & (compress > 0) &
                np.isfinite(intensity) & (intensity > 0)
            )
            if valid.any():
                idx = np.where(valid)[0]
                take = rng.random(len(idx)) < SAMPLE_FRAC
                idx = idx[take]
                if len(idx) > 0:
                    sample_allowed.extend(avg_allowed[idx].tolist())
                    sample_compress.extend(compress[idx].tolist())
                    sample_intensity.extend(intensity[idx].tolist())
                    sample_benes.extend(tot_benes[idx].tolist())

                    pos = chunk["Place_Of_Srvc"].to_numpy()
                    pos_flag = np.where(pos[idx] == "F", 1.0, np.where(pos[idx] == "O", 0.0, np.nan))
                    sample_pos.extend(pos_flag.tolist())
                    sample_specialty.extend(spec[idx].tolist())

            # Compression samples per specialty for boxplots (stricter denom)
            denom_ok = (
                np.isfinite(compress) & (compress > 0) &
                np.isfinite(tot_benes) & (tot_benes >= MIN_BENES_FOR_OUTLIERS) &
                np.isfinite(avg_allowed) & (avg_allowed > 0)
            )
            if denom_ok.any():
                idx2 = np.where(denom_ok)[0]
                take2 = rng.random(len(idx2)) < max(0.002, SAMPLE_FRAC)
                idx2 = idx2[take2]
                for j in idx2:
                    s = str(spec[j])
                    spec_compress_samples[s].append(float(compress[j]))

            # Outliers: intensity
            out_ok = (
                np.isfinite(intensity) &
                np.isfinite(tot_benes) & (tot_benes >= MIN_BENES_FOR_OUTLIERS) &
                np.isfinite(tot_srvcs) & (tot_srvcs >= MIN_SERVCS_FOR_OUTLIERS)
            )
            if out_ok.any():
                idx3 = np.where(out_ok)[0]
                for j in idx3:
                    payload = {
                        "Rndrng_NPI": str(chunk.iloc[j]["Rndrng_NPI"]),
                        "Rndrng_Prvdr_Type": str(chunk.iloc[j]["Rndrng_Prvdr_Type"]),
                        "State": str(chunk.iloc[j]["Rndrng_Prvdr_State_Abrvtn"]),
                        "RUCA": str(chunk.iloc[j]["Rndrng_Prvdr_RUCA"]),
                        "Place_Of_Srvc": str(chunk.iloc[j]["Place_Of_Srvc"]),
                        "Mdcr_Prtcptg_Ind": str(chunk.iloc[j]["Rndrng_Prvdr_Mdcr_Prtcptg_Ind"]),
                        "HCPCS_Cd": str(chunk.iloc[j]["HCPCS_Cd"]),
                        "Drug_Ind": str(chunk.iloc[j]["HCPCS_Drug_Ind"]),
                        "Tot_Benes": float(tot_benes[j]),
                        "Tot_Srvcs": float(tot_srvcs[j]),
                        "Avg_Mdcr_Alowd_Amt": float(avg_allowed[j]),
                        "Avg_Mdcr_Pymt_Amt": float(avg_paid[j]) if np.isfinite(avg_paid[j]) else np.nan,
                        "Intensity_Srvcs_per_Bene": float(intensity[j]),
                        "Charge_to_Allowed": float(compress[j]) if np.isfinite(compress[j]) else np.nan,
                        "Allowed_$": float(allowed_dollars[j]) if np.isfinite(allowed_dollars[j]) else np.nan,
                    }
                    push_topk(heap_intensity, intensity[j], payload)

            # Outliers: compression
            comp_ok = (
                np.isfinite(compress) & (compress > 0) &
                np.isfinite(tot_benes) & (tot_benes >= MIN_BENES_FOR_OUTLIERS) &
                np.isfinite(avg_allowed) & (avg_allowed > 0)
            )
            if comp_ok.any():
                idx4 = np.where(comp_ok)[0]
                for j in idx4:
                    payload = {
                        "Rndrng_NPI": str(chunk.iloc[j]["Rndrng_NPI"]),
                        "Rndrng_Prvdr_Type": str(chunk.iloc[j]["Rndrng_Prvdr_Type"]),
                        "State": str(chunk.iloc[j]["Rndrng_Prvdr_State_Abrvtn"]),
                        "RUCA": str(chunk.iloc[j]["Rndrng_Prvdr_RUCA"]),
                        "Place_Of_Srvc": str(chunk.iloc[j]["Place_Of_Srvc"]),
                        "Mdcr_Prtcptg_Ind": str(chunk.iloc[j]["Rndrng_Prvdr_Mdcr_Prtcptg_Ind"]),
                        "HCPCS_Cd": str(chunk.iloc[j]["HCPCS_Cd"]),
                        "Drug_Ind": str(chunk.iloc[j]["HCPCS_Drug_Ind"]),
                        "Tot_Benes": float(tot_benes[j]) if np.isfinite(tot_benes[j]) else np.nan,
                        "Tot_Srvcs": float(tot_srvcs[j]) if np.isfinite(tot_srvcs[j]) else np.nan,
                        "Avg_Sbmtd_Chrg": float(avg_sub[j]) if np.isfinite(avg_sub[j]) else np.nan,
                        "Avg_Mdcr_Alowd_Amt": float(avg_allowed[j]),
                        "Charge_to_Allowed": float(compress[j]),
                        "Allowed_$": float(allowed_dollars[j]) if np.isfinite(allowed_dollars[j]) else np.nan,
                    }
                    push_topk(heap_compress, compress[j], payload)

            # Outliers: dollars
            dollars_ok = np.isfinite(allowed_dollars) & (allowed_dollars > 0)
            if dollars_ok.any():
                idx5 = np.where(dollars_ok)[0]
                for j in idx5:
                    payload = {
                        "Rndrng_NPI": str(chunk.iloc[j]["Rndrng_NPI"]),
                        "Rndrng_Prvdr_Type": str(chunk.iloc[j]["Rndrng_Prvdr_Type"]),
                        "State": str(chunk.iloc[j]["Rndrng_Prvdr_State_Abrvtn"]),
                        "RUCA": str(chunk.iloc[j]["Rndrng_Prvdr_RUCA"]),
                        "Place_Of_Srvc": str(chunk.iloc[j]["Place_Of_Srvc"]),
                        "HCPCS_Cd": str(chunk.iloc[j]["HCPCS_Cd"]),
                        "Drug_Ind": str(chunk.iloc[j]["HCPCS_Drug_Ind"]),
                        "Tot_Benes": float(tot_benes[j]) if np.isfinite(tot_benes[j]) else np.nan,
                        "Tot_Srvcs": float(tot_srvcs[j]) if np.isfinite(tot_srvcs[j]) else np.nan,
                        "Avg_Mdcr_Alowd_Amt": float(avg_allowed[j]) if np.isfinite(avg_allowed[j]) else np.nan,
                        "Allowed_$": float(allowed_dollars[j]),
                    }
                    push_topk(heap_allowed_dollars, allowed_dollars[j], payload)

            if chunk_i % 5 == 0:
                print(f"Processed chunks: {chunk_i:,} | plot samples: {len(sample_allowed):,}")

print("Finished reading ZIP.")


# =========================
# Build summaries + plots
# =========================
spec_df = pd.DataFrame({
    "Specialty": list(spec_sum_allowed.keys()),
    "Allowed_$": list(spec_sum_allowed.values()),
    "Paid_$": list(spec_sum_paid.values()),
    "Tot_Srvcs": list(spec_sum_srvcs.values()),
    "Tot_Benes": list(spec_sum_benes.values()),
}).sort_values("Allowed_$", ascending=False).reset_index(drop=True)

print("\nTop specialties by Allowed_$:")
print(spec_df.head(10).to_string(index=False))

top_specs = spec_df.head(TOP_SPECIALTIES_FOR_PLOTS)["Specialty"].tolist()

comp_arr = np.array(sample_compress, dtype="float64")
if len(comp_arr) > 0:
    cap = float(np.nanpercentile(comp_arr, 99.5))
    cap = float(np.clip(cap, 10.0, 500.0))
else:
    cap = 50.0
print(f"\nCharge-to-allowed plot cap: {cap:.2f}")


# 1) Avg Allowed distribution (log scale)
plt.figure()
x = np.array(sample_allowed, dtype="float64")
x = x[np.isfinite(x) & (x > 0)]
plt.hist(x, bins=80)
plt.xscale("log")
plt.title("Distribution: Avg Medicare Allowed Amount (sample, log x)")
plt.xlabel("Avg_Mdcr_Alowd_Amt (log)")
plt.ylabel("Count (sample)")
plt.tight_layout()
plt.show()


# 2) Pareto: Allowed_$ by specialty (top 15) + cumulative share
top15 = spec_df.head(15).copy()
top15["CumShare"] = top15["Allowed_$"].cumsum() / max(spec_df["Allowed_$"].sum(), 1.0)

fig = plt.figure(figsize=(10, 6))
ax = fig.add_subplot(111)
ax.bar(range(len(top15)), top15["Allowed_$"].values)
ax.set_yscale("log")
ax.set_title("Pareto: Allowed Dollars by Specialty (Top 15)")
ax.set_xlabel("Specialty")
ax.set_ylabel("Allowed_$ (log)")
ax.set_xticks(range(len(top15)))
ax.set_xticklabels(top15["Specialty"].tolist(), rotation=60, ha="right")

#ax2 = ax.twinx()
#ax2.plot(range(len(top15)), top15["CumShare"].values, linewidth=2)
#ax2.set_ylim(0, 1.05)
#ax2.set_ylabel("Cumulative share of Allowed_$")
plt.tight_layout()
plt.show()


# 3) Boxplot: charge-to-allowed by top specialties (sampled)
box_data, labels = [], []
for s in top_specs:
    vals = np.array(spec_compress_samples.get(s, []), dtype="float64")
    vals = vals[np.isfinite(vals) & (vals > 0) & (vals <= cap)]
    if len(vals) >= 50:
        box_data.append(vals)
        labels.append(s)

if box_data:
    plt.figure(figsize=(12, 6))
    plt.boxplot(box_data, showfliers=True)
    plt.yscale("log")
    plt.title("Charge-to-Allowed Ratio by Specialty (sampled, log y)")
    plt.ylabel("Avg_Sbmtd_Chrg / Avg_Mdcr_Alowd_Amt (log)")
    plt.xticks(range(1, len(labels) + 1), labels, rotation=60, ha="right")
    plt.tight_layout()
    plt.show()
else:
    print("Not enough boxplot samples. Increase SAMPLE_FRAC or lower the 50-minimum cutoff.")


# 4) Scatter “outlier map”: intensity vs compression (filter low-denominator noise)
si = np.array(sample_intensity, dtype="float64")
sc = np.array(sample_compress, dtype="float64")
sb = np.array(sample_benes, dtype="float64")

mask = (
    np.isfinite(si) & (si > 0) &
    np.isfinite(sc) & (sc > 0) & (sc <= cap) &
    np.isfinite(sb) & (sb >= MIN_BENES_FOR_OUTLIERS)
)

plt.figure(figsize=(10, 6))
plt.scatter(si[mask], sc[mask], s=8, alpha=0.25)
plt.xscale("log")
plt.yscale("log")
plt.title(f"Outlier Map: Services/Bene vs Charge-to-Allowed (Tot_Benes ≥ {MIN_BENES_FOR_OUTLIERS})")
plt.xlabel("Intensity = Tot_Srvcs / Tot_Benes (log)")
plt.ylabel("Charge-to-Allowed = Avg_Sbmtd_Chrg / Avg_Mdcr_Alowd_Amt (log)")
plt.tight_layout()
plt.show()


# 5) Funnel-ish view: intensity variability vs beneficiary count (sample-based percentile bands)
mask2 = np.isfinite(si) & (si > 0) & np.isfinite(sb) & (sb > 0)
si2 = si[mask2]
sb2 = sb[mask2]

log_b = np.log10(sb2)
bins = np.linspace(np.nanmin(log_b), np.nanmax(log_b), 14)
bin_ids = np.digitize(log_b, bins)

centers, p10, p50, p90 = [], [], [], []
for b in range(1, len(bins) + 1):
    vals = si2[bin_ids == b]
    if len(vals) >= 80:
        centers.append(10 ** (np.nanmean(log_b[bin_ids == b])))
        p10.append(np.nanpercentile(vals, 10))
        p50.append(np.nanpercentile(vals, 50))
        p90.append(np.nanpercentile(vals, 90))

plt.figure(figsize=(10, 6))
plt.scatter(sb2, si2, s=6, alpha=0.15)
if centers:
    plt.plot(centers, p10, linewidth=2)
    plt.plot(centers, p50, linewidth=2)
    plt.plot(centers, p90, linewidth=2)
plt.xscale("log")
plt.yscale("log")
plt.title("Funnel View: Intensity varies more at low beneficiary counts (sample)")
plt.xlabel("Tot_Benes (log)")
plt.ylabel("Intensity = Tot_Srvcs / Tot_Benes (log)")
plt.tight_layout()
plt.show()


# 6) Heatmap: RUCA bucket × specialty (system intensity = sum_srvcs / sum_benes)
rows = ["Urban", "Large Rural", "Small Rural", "Isolated/Other", "Unknown"]
cols = top_specs

mat = np.full((len(rows), len(cols)), np.nan, dtype="float64")
for ri, r in enumerate(rows):
    for ci, s in enumerate(cols):
        ben = ruca_spec_benes.get((r, s), 0.0)
        srv = ruca_spec_srvcs.get((r, s), 0.0)
        if ben > 0:
            mat[ri, ci] = srv / ben

plt.figure(figsize=(12, 5))
plt.imshow(mat, aspect="auto")
plt.title("Heatmap: Services per Beneficiary by RUCA Bucket × Specialty")
plt.yticks(range(len(rows)), rows)
plt.xticks(range(len(cols)), cols, rotation=60, ha="right")
plt.colorbar(label="Services per Beneficiary (sum Tot_Srvcs / sum Tot_Benes)")
plt.tight_layout()
plt.show()


# =========================
# Export outlier worklists (local files)
# =========================
def heap_to_df(heap, score_name):
    # heap items are (score, tie_breaker, payload)
    items = sorted(heap, key=lambda x: x[0], reverse=True)
    rows = []
    for score, _, payload in items:
        payload = dict(payload)
        payload[score_name] = float(score)
        rows.append(payload)
    return pd.DataFrame(rows)

df_int = heap_to_df(heap_intensity, "Score_Intensity")
df_cmp = heap_to_df(heap_compress, "Score_Charge_to_Allowed")
df_big = heap_to_df(heap_allowed_dollars, "Score_Allowed_$")

df_int.to_csv("outliers_intensity_top.csv", index=False)
df_cmp.to_csv("outliers_compression_top.csv", index=False)
df_big.to_csv("outliers_allowed_dollars_top.csv", index=False)

print("\nSaved CSVs:")
print(" - outliers_intensity_top.csv")
print(" - outliers_compression_top.csv")
print(" - outliers_allowed_dollars_top.csv")