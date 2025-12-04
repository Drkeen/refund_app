from __future__ import annotations

from datetime import date
from io import BytesIO
from typing import Iterable, List, Tuple
import re

import pandas as pd


# ---------- Helpers ----------

def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + underscores for column names."""
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _file_to_bytes(file) -> bytes:
    """Safely get raw bytes from a Streamlit UploadedFile or file-like object."""
    if hasattr(file, "getvalue"):
        return file.getvalue()
    return file.read()


# ---------- Loaders for your specific spreadsheets ----------

def load_study_plan_excel(file) -> pd.DataFrame:
    """
    Load TQ Study Plan export.

    Assumes there is a header row that contains 'Enrolment Activity Start Date',
    with data rows beneath it.
    """
    raw = pd.read_excel(file, header=None)

    # Find the header row by looking for 'Enrolment Activity Start Date'
    header_row_idx = None
    for i in range(min(15, len(raw))):  # scan first few rows
        row = raw.iloc[i].astype(str)
        if row.str.contains("Enrolment Activity Start Date", case=False, na=False).any():
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError("Could not find 'Enrolment Activity Start Date' header in study plan file.")

    header = raw.iloc[header_row_idx]
    df = raw.iloc[header_row_idx + 1:].copy()
    df.columns = header

    df = _normalise_columns(df)
    # Expect: 'spk_cd', 'title', 'enrolment_activity_start_date'

    required = ["spk_cd", "title", "enrolment_activity_start_date"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Study plan file missing column: {col}")

    df["enrolment_activity_start_date"] = pd.to_datetime(
        df["enrolment_activity_start_date"], errors="coerce"
    )

    df_units = df.rename(
        columns={
            "spk_cd": "unit_code",
            "title": "unit_name",
            "enrolment_activity_start_date": "easd",
        }
    )[["unit_code", "unit_name", "easd"]]

    # Normalise unit codes for safer joins
    df_units["unit_code"] = (
        df_units["unit_code"].astype(str).str.strip().str.upper()
    )

    return df_units


def load_engagement_excel(file) -> pd.DataFrame:
    """
    Load TQ Unit Engagement export.

    We dynamically detect the header row by looking for the 'Recorded Hours' column,
    then normalise the columns.

    Expected (after normalisation):
      - 'curriculum_item' (unit code)
      - 'recorded_hours' -> engagement hours (e.g. '32.97 hours')
      - 'unit_start_date' (optional, for aligning with EASD)
      - a 'pass/fail' style column for unit status.
    """
    raw = pd.read_excel(file, header=None)

    # Find the header row by looking for 'Recorded Hours'
    header_row_idx = None
    for i in range(min(15, len(raw))):  # scan first few rows
        row = raw.iloc[i].astype(str)
        if row.str.contains("Recorded Hours", case=False, na=False).any():
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError("Could not find a 'Recorded Hours' header in the engagement file.")

    header = raw.iloc[header_row_idx]
    df = raw.iloc[header_row_idx + 1:].copy()
    df.columns = header

    df = _normalise_columns(df)

    if "recorded_hours" not in df.columns:
        raise ValueError("Engagement file must contain a 'Recorded Hours' column.")

    # Treat 'Curriculum Item' as the unit code if present
    unit_col = None
    for cand in ["curriculum_item", "unitspkstudypackagecode", "ssp_spk_cd", "spk_cd", "unit_code"]:
        if cand in df.columns:
            unit_col = cand
            break

    if unit_col is None:
        raise ValueError(
            "Engagement file must contain a unit code column "
            "(e.g. 'Curriculum Item')."
        )

    # Optional name column
    name_col = None
    for cand in ["full_title", "unitspkfulltitle", "title", "unit_name"]:
        if cand in df.columns:
            name_col = cand
            break

    # Optional unit start date column
    unit_start_col = "unit_start_date" if "unit_start_date" in df.columns else None

    # Optional pass/fail status column – look for anything containing both 'pass' and 'fail'
    status_col = None
    for col in df.columns:
        if "pass" in col and "fail" in col:
            status_col = col
            break

    # Recorded hours are strings like "32.97 hours" or "49,47 hours"
    df["recorded_hours"] = (
        df["recorded_hours"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.extract(r"([\d\.]+)", expand=False)
    )
    df["recorded_hours"] = pd.to_numeric(df["recorded_hours"], errors="coerce").fillna(0.0)

    # Build the output frame
    rename_map = {unit_col: "unit_code"}
    if name_col:
        rename_map[name_col] = "unit_name"
    if unit_start_col:
        rename_map[unit_start_col] = "unit_start_date"
    if status_col:
        rename_map[status_col] = "unit_status_raw"

    df = df.rename(columns=rename_map)

    cols = ["unit_code", "recorded_hours"]
    if "unit_name" in df.columns:
        cols.append("unit_name")
    else:
        df["unit_name"] = ""
        cols.append("unit_name")

    if "unit_start_date" in df.columns:
        cols.append("unit_start_date")
    if "unit_status_raw" in df.columns:
        cols.append("unit_status_raw")

    df_units = df[cols].copy()

    # Normalise unit codes
    df_units["unit_code"] = (
        df_units["unit_code"].astype(str).str.strip().str.upper()
    )

    # Normalise unit start date
    if "unit_start_date" in df_units.columns:
        df_units["unit_start_date"] = pd.to_datetime(
            df_units["unit_start_date"], errors="coerce"
        )

    # Map raw pass/fail/planned/enrolled status to a clean enum
    def map_status(s) -> str:
        if not isinstance(s, str):
            return "UNKNOWN"
        s_clean = s.strip().lower()
        if not s_clean:
            return "UNKNOWN"
        if "pass" in s_clean and "fail" not in s_clean:
            return "PASSED"
        if "fail" in s_clean:
            return "FAILED"
        if "withdraw" in s_clean:
            return "WITHDRAWN"
        if "enrol" in s_clean:
            return "ENROLLED"
        if "plan" in s_clean:
            return "PLANNED"
        return s_clean.upper()

    if "unit_status_raw" in df_units.columns:
        df_units["unit_status"] = df_units["unit_status_raw"].apply(map_status)
    else:
        df_units["unit_status"] = "UNKNOWN"

    df_units.drop(columns=[c for c in ["unit_status_raw"] if c in df_units.columns], inplace=True)

    return df_units


ddef load_student_account_excel(file) -> Tuple[pd.DataFrame, float]:
    """
    Load TQ Student Account export.

    We detect the header row by looking for 'Txn Amt', then normalise.

    We expect:
        'SSP Spk Cd' (unit code)  -> 'ssp_spk_cd'
        'Txn Amt' (transaction amount; +ve = charge, -ve = payment/reversal) -> 'txn_amt'

    Returns:
        unit_prices: DataFrame[unit_code, unit_price]
            - unit_price is the NET fee per unit after reversals (and any unit-level credits),
              based on all transactions that carry that unit code.
        account_balance: float (sum of all txn_amt across the whole account)
    """
    raw = pd.read_excel(file, header=None)

    # Find header row via 'Txn Amt'
    header_row_idx = None
    for i in range(min(10, len(raw))):
        row = raw.iloc[i].astype(str)
        if row.str.contains("Txn Amt", case=False, na=False).any():
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError("Could not find 'Txn Amt' header in student account file.")

    header = raw.iloc[header_row_idx]
    df = raw.iloc[header_row_idx + 1 :].copy()
    df.columns = header

    df = _normalise_columns(df)
    # Now we expect 'ssp_spk_cd' and 'txn_amt'

    required = ["ssp_spk_cd", "txn_amt"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Student account file missing column: {col}")

    df["txn_amt"] = pd.to_numeric(df["txn_amt"], errors="coerce").fillna(0.0)

    # Overall account balance: ALL transactions (charges, reversals, payments)
    account_balance = float(df["txn_amt"].sum())

    # --- NEW LOGIC ---
    # For unit prices, we want the *net* fee per unit, after reversals.
    # So we include both positive and negative txn_amt for each unit code.
    # This handles:
    #   +156 (charge), -156 (fee reversal), +156 (new charge) -> net 156.
    per_unit = df.dropna(subset=["ssp_spk_cd"]).copy()

    unit_totals = (
        per_unit.groupby("ssp_spk_cd", as_index=False)["txn_amt"].sum()
    )

    # If a unit ends up with a negative net (e.g. fully reversed, or over-credited),
    # treat its price as 0 for the purposes of our financial impact report.
    unit_totals["txn_amt"] = unit_totals["txn_amt"].clip(lower=0.0)

    unit_prices = unit_totals.rename(
        columns={"ssp_spk_cd": "unit_code", "txn_amt": "unit_price"}
    )

    return unit_prices, account_balance


# ---------- Metadata extraction (student number + course info) ----------

def extract_case_metadata(study_plan_file, engagement_file=None) -> dict:
    """
    Detect case metadata using:
      - Student number: prefer Unit Engagement (more consistent)
      - Course code + course name: from Study Plan
    """
    # 1. Student number from Engagement (if provided)
    student_number: str | None = None

    if engagement_file is not None:
        eng_bytes = _file_to_bytes(engagement_file)
        raw_eng = pd.read_excel(BytesIO(eng_bytes), header=None)

        for i in range(len(raw_eng)):
            row = raw_eng.iloc[i].astype(str)
            tokens = " ".join(row).split()
            for tok in tokens:
                digits = re.sub(r"\D", "", tok)  # strip non-digits
                if len(digits) == 9 and digits[0] == "4":
                    student_number = digits
                    break
            if student_number:
                break

    # 2. Course code + course name from Study Plan
    course_code: str | None = None
    course_name: str | None = None

    sp_bytes = _file_to_bytes(study_plan_file)
    raw_sp = pd.read_excel(BytesIO(sp_bytes), header=None)

    course_pattern = re.compile(r"\b[A-Z]{2,4}\d{5}\b")

    def looks_like_date_or_number(tok: str) -> bool:
        t = tok.replace("/", "").replace("-", "")
        return t.isdigit() and len(t) >= 4

    for i in range(len(raw_sp)):
        row = raw_sp.iloc[i].astype(str)
        parts = [p for p in row if p not in ("nan", "", " ")]
        if not parts:
            continue
        text = " ".join(parts)
        tokens = text.split()

        # Course code
        if course_code is None:
            m2 = course_pattern.search(text)
            if m2:
                course_code = m2.group()

        # Course name (from same line as course code)
        if course_code and course_name is None and course_code in tokens:
            code_idx = tokens.index(course_code)

            level_keywords = {
                "CII", "CIII", "CIV", "CV",
                "CERT", "CERTIFICATE",
                "DIPLOMA", "ADVANCED",
                "BACHELOR", "GRADUATE",
            }
            level_idx = None
            for idx in range(code_idx + 1, len(tokens)):
                t_upper = tokens[idx].upper()
                if t_upper in level_keywords:
                    level_idx = idx
                    break

            start_idx = level_idx if level_idx is not None else code_idx + 1

            name_tokens: list[str] = []
            for tok in tokens[start_idx:]:
                # Stop at something that looks like a date/number
                if looks_like_date_or_number(tok):
                    break

                # Stop if this looks like a surname / staff code from the next column
                upper_tok = tok.upper()
                if tok.isalpha() and tok == upper_tok and len(tok) >= 5:
                    break

                name_tokens.append(tok)

                # Hard cap so we don't eat the next column's text
                if len(name_tokens) >= 8:
                    break

            if len(name_tokens) >= 2:
                course_name = " ".join(name_tokens).strip()

        if course_code and course_name:
            break

    return {
        "student_number": student_number,
        "course_code": course_code,
        "course_name": course_name,
    }


def extract_case_metadata_from_study_plan(file) -> dict:
    """
    Backwards-compatible wrapper: keeps the old name if something else calls it.
    """
    return extract_case_metadata(file, engagement_file=None)


# ---------- Core financial logic ----------

def compute_financials(
    study_plan_file,
    engagement_file,
    student_account_file,
    request_date: date,
    selected_units: Iterable[str] | None = None,
) -> Tuple[pd.DataFrame, float, float, int, float, int, float]:
    """
    Using the three spreadsheets, compute per-unit decisions and totals.

    Returns:
        units_df: columns
          - unit_code
          - unit_name
          - unit_status
          - engagement_status
          - engagement_hours
          - engagement_summary
          - action
          - unit_price
          - easd
          - days_from_easd
        account_balance
        total_fee_waiver, n_fee_waiver
        total_ewid,       n_ewid
        estimated_balance_after_changes
    """
    sp_bytes = _file_to_bytes(study_plan_file)
    eng_bytes = _file_to_bytes(engagement_file)
    acc_bytes = _file_to_bytes(student_account_file)

    sp_df = load_study_plan_excel(BytesIO(sp_bytes))
    eng_df = load_engagement_excel(BytesIO(eng_bytes))
    prices_df, account_balance = load_student_account_excel(BytesIO(acc_bytes))

    # Merge: Study Plan + Engagement + Unit prices
    if "unit_start_date" in eng_df.columns:
        df = sp_df.merge(
            eng_df[["unit_code", "recorded_hours", "unit_start_date", "unit_status"]],
            left_on=["unit_code", "easd"],
            right_on=["unit_code", "unit_start_date"],
            how="left",
        )
        df.drop(columns=["unit_start_date"], inplace=True)
    else:
        df = sp_df.merge(
            eng_df[["unit_code", "recorded_hours", "unit_status"]],
            on="unit_code",
            how="left",
        )

    df = df.merge(prices_df, on="unit_code", how="left")

    # Filter to the units the student is withdrawing from (if provided)
    if selected_units:
        selected_units = [u.strip().upper() for u in selected_units if u.strip()]
        df = df[df["unit_code"].isin(selected_units)].copy()

    # Clean status
    df["unit_status"] = df["unit_status"].fillna("UNKNOWN")

    # Filter out units that cannot / need not be altered
    df = df[~df["unit_status"].isin(["PASSED", "FAILED", "PLANNED"])].copy()

    # Filter out cluster "units" – codes starting with 'CLS'
    df = df[~df["unit_code"].str.upper().str.startswith("CLS")].copy()

    # Filter out the course code itself (e.g. ACM30321 line)
    if "unit_code" in sp_df.columns and not sp_df.empty:
        course_code_main = str(sp_df["unit_code"].iloc[0]).strip().upper()
        df = df[df["unit_code"].str.upper() != course_code_main]

    # Engagement: hours + ENG/NATT
    df["recorded_hours"] = df["recorded_hours"].fillna(0.0)
    df["engagement_hours"] = df["recorded_hours"]
    df["engagement_status"] = df["engagement_hours"].apply(
        lambda h: "ENG" if h > 0 else "NATT"
    )

    # Ensure EASD is datetime & compute days_from_easd
    df["easd"] = pd.to_datetime(df["easd"], errors="coerce")
    req_ts = pd.to_datetime(request_date)
    df["days_from_easd"] = (req_ts - df["easd"]).dt.days

    def classify_action(row) -> str:
        days = row["days_from_easd"]
        status = row["engagement_status"]

        if pd.isna(days):
            return "UNKNOWN"

        if status == "ENG":
            if days > 14:
                return "Fee Waiver"
            return "EWID (remove engagement)"

        # NATT
        if days > 0:
            return "Post EASD EWID"
        else:
            return "Pre EASD EWID"

    df["action"] = df.apply(classify_action, axis=1)
    df["unit_price"] = df["unit_price"].fillna(0.0)

    # Drop units with UNKNOWN action (planned/old/irrelevant) and zero-dollar units
    df = df[df["action"] != "UNKNOWN"].copy()
    df = df[df["unit_price"] > 0].copy()

    # Engagement summary string
    def format_eng(row) -> str:
        if row["engagement_status"] == "ENG":
            return f"ENG: {row['engagement_hours']:.2f}"
        return "NATT"

    df["engagement_summary"] = df.apply(format_eng, axis=1)

    # Totals
    def action_group(action: str) -> str:
        if "Fee Waiver" in action:
            return "Fee Waiver"
        if "EWID" in action:
            return "EWID"
        return "Other"

    df["action_group"] = df["action"].apply(action_group)

    totals = (
        df.groupby("action_group")["unit_price"]
        .agg(["sum", "count"])
        .reset_index()
    )

    def _get_total(group: str) -> Tuple[float, int]:
        if group not in totals["action_group"].values:
            return 0.0, 0
        row = totals[totals["action_group"] == group].iloc[0]
        return float(row["sum"]), int(row["count"])

    total_fee_waiver, n_fee_waiver = _get_total("Fee Waiver")
    total_ewid, n_ewid = _get_total("EWID")

    total_reversal = total_fee_waiver + total_ewid
    estimated_balance = account_balance - total_reversal

    units_df = df[
        [
            "unit_code",
            "unit_name",
            "unit_status",
            "engagement_status",
            "engagement_hours",
            "engagement_summary",
            "action",
            "unit_price",
            "easd",
            "days_from_easd",
        ]
    ].copy()

    return (
        units_df,
        float(account_balance),
        total_fee_waiver,
        n_fee_waiver,
        total_ewid,
        n_ewid,
        float(estimated_balance),
    )


# ---------- Recommendation builder ----------

def build_recommendation_text(
    units_df: pd.DataFrame,
    account_balance: float,
    total_fee_waiver: float,
    total_ewid: float,
    estimated_balance: float,
) -> str:
    """
    Build the delegate-facing recommendation text.
    """
    total_reversal = total_fee_waiver + total_ewid
    if total_reversal <= 0:
        return ""

    lines: list[str] = []

    fw_units = units_df[units_df["action"] == "Fee Waiver"].copy()
    ewid_units = units_df[units_df["action"].str.contains("EWID")].copy()

    def build_group_chunk(group_df: pd.DataFrame, header: str, label: str) -> str:
        if group_df.empty:
            return ""

        n_units = len(group_df)
        group_total = float(group_df["unit_price"].sum())

        price_counts = (
            group_df.groupby("unit_price")
            .size()
            .reset_index(name="count")
        )

        if len(price_counts) == 1:
            row = price_counts.iloc[0]
            count = int(row["count"])
            price = float(row["unit_price"])
            breakdown = f"({count} @ ${price:,.2f})"
        else:
            parts = []
            for _, row in price_counts.iterrows():
                count = int(row["count"])
                price = float(row["unit_price"])
                parts.append(f"{count} @ ${price:,.2f}")
            breakdown = "(" + " + ".join(parts) + ")"

        return (
            f"{header} {n_units} units and apply {label} of "
            f"${group_total:,.2f} {breakdown}"
        )

    if not fw_units.empty:
        lines.append(
            build_group_chunk(
                fw_units,
                header="Post EASD WID FREF",
                label="fee waiver",
            )
        )

    if not ewid_units.empty:
        pre = ewid_units[ewid_units["action"].str.contains("Pre")].copy()
        post = ewid_units[ewid_units["action"].str.contains("Post")].copy()
        mid = ewid_units[ewid_units["action"] == "EWID (remove engagement)"].copy()

        if not pre.empty:
            lines.append(
                build_group_chunk(
                    pre,
                    header="Pre EASD EWID FREF",
                    label="EWID",
                )
            )
        if not post.empty:
            lines.append(
                build_group_chunk(
                    post,
                    header="Post EASD EWID FREF",
                    label="EWID",
                )
            )
        if not mid.empty:
            lines.append(
                build_group_chunk(
                    mid,
                    header="EWID FREF",
                    label="EWID",
                )
            )

    lines = [ln for ln in lines if ln]
    if not lines:
        return ""

    if estimated_balance >= 0:
        tail = (
            f" = ${total_reversal:,.2f} credited to student account. "
            f"${estimated_balance:,.2f} ODT."
        )
    else:
        credit_after_fref = -estimated_balance
        refund_amount = max(credit_after_fref - 100.0, 0.0)
        tail = (
            f" = ${total_reversal:,.2f} credited to student account. "
            f"${credit_after_fref:,.2f} credit - $100.00 Administration fee "
            f"= ${refund_amount:,.2f} refund to student."
        )

    lines[-1] = lines[-1] + tail

    return "\n".join(lines)


# ---------- Report text builder ----------

def build_report_text(
    student_number: str,
    request_type: str,
    request_date: date,
    submitted_by: str,
    course_code: str,
    course_name: str,
    specific_units: List[str],
    units_df: pd.DataFrame,
    account_balance: float,
    total_fee_waiver: float,
    n_fee_waiver: int,
    total_ewid: float,
    n_ewid: int,
    estimated_balance: float,
) -> str:
    lines: List[str] = []

    # Header
    lines.append(f"{student_number}")
    lines.append("")
    lines.append(request_type)
    lines.append("")
    lines.append(request_date.strftime("%d/%m/%Y"))
    lines.append("")
    lines.append(submitted_by)
    lines.append("")
    lines.append(f"{course_code} - {course_name}")
    lines.append("")

    if specific_units:
        lines.append("Specific units (if not full course):")
        for u in specific_units:
            lines.append(f"- {u}")
        lines.append("")

    lines.append("Student current financials:")
    lines.append("")

    for _, row in units_df.iterrows():
        status_str = row["unit_status"]
        eng_str = row["engagement_summary"]

        if status_str == "ENROLLED":
            bracket = f"[{eng_str}]"
        else:
            bracket = f"[{status_str}, {eng_str}]"

        lines.append(
            f"{row['unit_code']} {bracket} {row['action']} ${row['unit_price']:,.2f}"
        )

    lines.append("")
    lines.append("Totals:")
    lines.append(
        f"Total Fee Waiver: ${total_fee_waiver:,.2f} ({n_fee_waiver} unit(s))"
    )
    lines.append(
        f"Total EWID: ${total_ewid:,.2f} ({n_ewid} unit(s))"
    )

    total_reversal = total_fee_waiver + total_ewid
    lines.append("")
    lines.append(f"Account Balance: ${account_balance:,.2f}")
    lines.append(
        f"Total of Fee Waiver and EWID: ${total_reversal:,.2f}"
    )
    lines.append(
        f"Estimated remaining balance: ${estimated_balance:,.2f}"
    )

    if estimated_balance < 0:
        lines.append(f"=> Refund to student: ${-estimated_balance:,.2f}")
    elif estimated_balance > 0:
        lines.append(f"=> Student still has a debt: ${estimated_balance:,.2f}")
    else:
        lines.append("=> Remaining balance: $0.00 (no debt / refund).")

    # Recommendation
    recommendation = build_recommendation_text(
        units_df=units_df,
        account_balance=account_balance,
        total_fee_waiver=total_fee_waiver,
        total_ewid=total_ewid,
        estimated_balance=estimated_balance,
    )
    if recommendation:
        lines.append("")
        lines.append("Recommendation:")
        lines.append(recommendation)

    return "\n".join(lines)
