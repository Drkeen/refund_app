import streamlit as st
from datetime import date

from logic import (
    compute_financials,
    build_report_text,
    extract_case_metadata,
)

from special_circ import (
    extract_text_from_file,
    generate_special_circ_summary,
)

st.set_page_config(
    page_title="Withdrawal Financials Prototype",
    layout="wide",
)

st.title("Withdrawal Financials – Prototype (Facts Only)")

st.markdown(
    "Upload Study Plan, Unit Engagement, and Student Account exports, "
    "then generate a financial impact report and delegate-ready recommendation."
)

# ---------- 1. Upload spreadsheets ----------

st.header("1. Upload spreadsheets")

study_plan_file = st.file_uploader(
    "Study Plan (Excel)", type=["xlsx", "xls"], key="study_plan"
)
engagement_file = st.file_uploader(
    "Unit Engagement (Excel)", type=["xlsx", "xls"], key="engagement"
)
student_account_file = st.file_uploader(
    "Student Account (Excel)", type=["xlsx", "xls"], key="student_account"
)

# ---------- 2. Case details (with auto-suggest) ----------

st.header("2. Case details")

student_default = ""
course_code_default = ""
course_name_default = ""

if study_plan_file is not None:
    try:
        meta = extract_case_metadata(
            study_plan_file=study_plan_file,
            engagement_file=engagement_file if engagement_file is not None else None,
        )
        student_default = meta.get("student_number") or ""
        course_code_default = meta.get("course_code") or ""
        course_name_default = meta.get("course_name") or ""
    except Exception as e:
        st.error(f"Could not extract case details from uploaded files: {e}")

col1, col2 = st.columns(2)

with col1:
    student_number = st.text_input("Student Number", value=student_default)

with col2:
    course_code = st.text_input("Course Code", value=course_code_default)
    course_name = st.text_input("Course Name", value=course_name_default)

st.caption(
    "Student number is suggested from Unit Engagement; course code and name from Study Plan."
)

# ---------- 3. Request details & specific units ----------

st.header("3. Request details")

col3, col4 = st.columns(2)

with col3:
    request_type = st.text_input("Request Type", value="Withdrawal & Refund")
    submitted_by = st.selectbox("Submitted by", ["Student", "Faculty"])

with col4:
    request_date = st.date_input("Date requested", value=date.today())

full_course = st.radio(
    "Is this a full course withdrawal?",
    ["Yes (all units)", "No (specific units only)"],
    index=1,
)
is_full_course = full_course.startswith("Yes")

specific_units: list[str] = []
if not is_full_course:
    units_text = st.text_area(
        "Specific units (one unit code per line)",
        placeholder="e.g.\nACMBEH302\nACMGEN303",
    )
    specific_units = [u.strip() for u in units_text.splitlines() if u.strip()]

# ---------- 4. Run calculation ----------

st.header("4. Calculate financial impact")

if st.button("Generate financial report"):
    if not (study_plan_file and engagement_file and student_account_file):
        st.error("Please upload all three spreadsheets.")
    elif not all([student_number, course_code, course_name]):
        st.error("Please fill in student number and course details.")
    elif not is_full_course and not specific_units:
        st.error("Please enter at least one unit code, or select full course.")
    else:
        try:
            selected_units = None if is_full_course else specific_units

            (
                units_df,
                account_balance,
                total_fee_waiver,
                n_fee_waiver,
                total_ewid,
                n_ewid,
                estimated_balance,
            ) = compute_financials(
                study_plan_file=study_plan_file,
                engagement_file=engagement_file,
                student_account_file=student_account_file,
                request_date=request_date,
                selected_units=selected_units,
            )
        except Exception as e:
            st.error(f"Error processing spreadsheets: {e}")
        else:
            st.subheader("Per-unit decisions")

            if units_df.empty:
                st.warning("No matching units found for the selected criteria.")
            else:
                st.dataframe(units_df)

                st.subheader("Generated report text")

                report_text = build_report_text(
                    student_number=student_number,
                    request_type=request_type,
                    request_date=request_date,
                    submitted_by=submitted_by,
                    course_code=course_code,
                    course_name=course_name,
                    specific_units=specific_units if not is_full_course else [],
                    units_df=units_df,
                    account_balance=account_balance,
                    total_fee_waiver=total_fee_waiver,
                    n_fee_waiver=n_fee_waiver,
                    total_ewid=total_ewid,
                    n_ewid=n_ewid,
                    estimated_balance=estimated_balance,
                )

                st.code(report_text, language="text")

# ---------- 5. Special circumstances (optional, AI-assisted) ----------

st.header("5. Special circumstances (optional)")

special_flag = st.checkbox("This case includes Special Circumstances documentation")

if special_flag:
    st.markdown(
        "Upload any supporting documents (medical certificates, statements, emails, etc.). "
        "The app will send them to an AI model to build a timeline and summary."
    )

    sc_files = st.file_uploader(
        "Supporting documents (PDF, DOCX, TXT, images)",
        type=["pdf", "docx", "txt", "png", "jpg", "jpeg"],
        accept_multiple_files=True,
        key="support_docs",
    )


    if st.button("Generate Special Circumstances timeline and summary with AI"):
        if not sc_files:
            st.error("Please upload at least one supporting document.")
        else:
            api_key = st.secrets.get("OPENAI_API_KEY", None)
            if not api_key:
                st.error(
                    "No OpenAI API key configured. "
                    "Add OPENAI_API_KEY to your Streamlit secrets."
                )
            else:
                combined_text_parts = []
                for f in sc_files:
                    text = extract_text_from_file(f)
                    if text.strip():
                        combined_text_parts.append(
                            f"===== DOCUMENT: {f.name} =====\n\n{text}"
                        )

                combined_text = "\n\n\n".join(combined_text_parts).strip()

                if not combined_text:
                    st.error(
                        "Could not extract any text from the uploaded documents. "
                        "Please check file types and try again."
                    )
                else:
                    # Get earliest financially impactful EASD from session (if available)
                    earliest_easd = st.session_state.get("earliest_easd", None)

                    with st.spinner("Calling AI to analyse documents..."):
                        try:
                            sc_summary = generate_special_circ_summary(
                                api_key=api_key,
                                student_number=student_number,
                                course_code=course_code,
                                course_name=course_name,
                                request_date=request_date,
                                request_type=request_type,
                                submitted_by=submitted_by,
                                raw_docs_text=combined_text,
                                earliest_easd=earliest_easd,
                                image_files=sc_files,
                            )


                        except Exception as e:
                            st.error(f"Error generating AI summary: {e}")
                        else:
                            st.subheader(
                                "AI-generated Special Circumstances timeline & summary"
                            )
                            st.markdown(sc_summary)
                            st.caption(
                                "Review this carefully against the original documents. "
                                "Edit or trim as needed before including in your recommendation."
                            )
