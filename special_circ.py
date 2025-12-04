from __future__ import annotations

from datetime import date
from io import BytesIO
from typing import List, Iterable
import base64

from openai import OpenAI
from pypdf import PdfReader
import docx


def extract_text_from_file(file) -> str:
    """
    Extract plain text from an uploaded file.

    Supports:
    - .txt
    - .pdf
    - .docx

    Image files (.png, .jpg, .jpeg) are handled separately via OpenAI Vision
    in generate_special_circ_summary, so this function returns "" for them.
    """
    name = getattr(file, "name", "") or ""
    ext = name.split(".")[-1].lower() if "." in name else ""

    # Images: we don't OCR locally; handled via Vision later
    if ext in ("png", "jpg", "jpeg"):
        return ""

    # Get raw bytes
    if hasattr(file, "getvalue"):
        data = file.getvalue()
    else:
        data = file.read()

    if not data:
        return ""

    # TXT
    if ext == "txt":
        try:
            return data.decode("utf-8", errors="ignore")
        except Exception:
            return ""

    # PDF
    if ext == "pdf":
        try:
            reader = PdfReader(BytesIO(data))
            parts: List[str] = []
            for page in reader.pages:
                text = page.extract_text() or ""
                parts.append(text)
            return "\n\n".join(parts)
        except Exception:
            return ""

    # DOCX
    if ext == "docx":
        try:
            doc = docx.Document(BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs)
        except Exception:
            return ""

    # Fallback – best effort treat as text
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _image_files_to_content_blocks(files: Iterable) -> List[dict]:
    """
    Convert uploaded image files to OpenAI 'input_image' content blocks
    using data URLs.
    """
    blocks: List[dict] = []
    for f in files or []:
        name = getattr(f, "name", "") or ""
        ext = name.split(".")[-1].lower() if "." in name else ""
        if ext not in ("png", "jpg", "jpeg"):
            continue

        if hasattr(f, "getvalue"):
            data = f.getvalue()
        else:
            data = f.read()

        if not data:
            continue

        if ext == "png":
            mime = "image/png"
        else:
            mime = "image/jpeg"

        b64 = base64.b64encode(data).decode("ascii")
        data_url = f"data:{mime};base64,{b64}"

        blocks.append(
            {
                "type": "input_image",
                "image_url": {"url": data_url},
            }
        )

    return blocks


def build_special_circ_input_items(
    student_number: str,
    course_code: str,
    course_name: str,
    request_date: date,
    request_type: str,
    submitted_by: str,
    raw_docs_text: str,
    image_files: Iterable | None = None,
) -> list[dict]:
    """
    Build the input items for the OpenAI Responses API, using TAFE QLD
    special circumstances wording and structure, including optional images.
    """
    header_context = f"""You are assisting with a TAFE Queensland fee review for a Special Circumstances case.

Case metadata:
- Student number: {student_number or "N/A"}
- Course: {course_code} - {course_name}
- Request type: {request_type}
- Date requested: {request_date.strftime("%d/%m/%Y")}
- Submitted by: {submitted_by}

You are given supporting documentation (medical certificates, statements, emails, etc.).
Your job is to assess the case against TAFE Queensland's Special Circumstances guidelines and produce structured notes suitable for an internal recommendation.
"""

    instructions = """
TAFE Queensland defines special circumstances as those that:
- Are beyond the student's control;
- Do not make their full impact upon the student until after the start of study or census date/s for a course or specific module/s; and 
- Make it unreasonable for the student to complete the study requirements for the course or module/s during their allotted timeframe.

Examples:

Medical reasons:
- The medical condition only becomes apparent after the start of study or census date/s and the effects are sufficiently serious that it is unrealistic for the student to continue with their studies.
- The student contracts an illness prior to the start of study or census date/s. The illness continues past the start of study or census date/s and deteriorates to the extent that they are unable to continue.

Family/personal reasons:
- A member of the student's family suffers from a severe medical condition that requires the student to provide full-time care; as a result they are unable to continue their studies.
- The student or their family's financial circumstances change unexpectedly to the extent that they are unable to continue their studies.

Employment related reasons:
- The student is transferred to a new location for their job and it is now unreasonable for them to travel, or TAFE Queensland doesn’t offer the program in the new area.
- The employer unexpectedly increases the student's hours of employment in circumstances where they are unable to object. As a result they are unable to continue their studies.

Course related reasons:
- The student has been disadvantaged by changed arrangements to their unit of study, and it was impossible to undertake alternative units.

QTAC or higher education:
- The student accepts a place offered through QTAC or a higher education institution for the current semester.

Supporting documentation should be independent (e.g. doctor, counsellor, employer, faculty) rather than family/friends, and should ideally address:
- when the circumstances began or changed;
- how they affected the student's ability to study;
- when it became apparent the student could not continue.

Your tasks:

1. Reason for COE
   - Decide on the most appropriate 'Reason for COE' by selecting ONE of the following:
       * Medical reasons
       * Family/personal reasons
       * Employment related reasons
       * Course related reasons
       * QTAC or higher education
   - If no category clearly fits, choose the closest one and note the uncertainty.

2. Special Circumstances eligibility assessment
   - Briefly assess whether the circumstances:
       * were beyond the student's control;
       * did not make full impact until after the start of study / census date for the relevant period;
       * made it unreasonable for them to complete the study requirements during the timeframe.
   - State clearly if each criterion appears MET, PARTIALLY MET, or NOT CLEAR from the documents.

3. Documentation assessment
   - Comment on whether the documentation appears:
       * independent (e.g. doctor, counsellor, employer, faculty vs. family/friends)
       * sufficiently detailed (dates, impact on ability to study, when it became apparent they could not continue).
   - Mention any gaps (e.g. missing dates, vague impact description, no link to study).

4. Timeline of events
   - Build a chronological timeline of key events, focused on the relevant study period.
   - Wherever possible, anchor your timeline relative to the EASD / teaching period (e.g. "Shortly after unit start in 08/2024").
   - Use the format:
       dd/mm/yyyy – brief event description
     If only month/year or approximate timing is known, indicate this (e.g. "Approx. 08/2024 – ...").

5. Impact on study (summary)
   - Generate a brief summary (3–6 bullet points) of how the circumstances affected the student's ability to:
       * attend classes;
       * engage with learning;
       * submit assessment;
       * meet timelines for the relevant study period.

Formatting:

Return your answer in the following sections, in order:

1. Reason for COE:
   - <one of: Medical reasons / Family/personal reasons / Employment related reasons / Course related reasons / QTAC or higher education>
   - Brief rationale (1–3 sentences).

2. Special Circumstances eligibility:
   - Beyond control: <MET / PARTIALLY MET / NOT CLEAR> – short explanation.
   - After start of study/census: <MET / PARTIALLY MET / NOT CLEAR> – short explanation.
   - Unreasonable to complete: <MET / PARTIALLY MET / NOT CLEAR> – short explanation.

3. Documentation assessment:
   - Independence of documentation: <brief comment>.
   - Adequacy of detail (dates, impact, when it became apparent they could not continue): <brief comment>.
   - Noted gaps/limitations: <brief comment or 'None obvious'>.

4. Timeline of events:
   - dd/mm/yyyy – ...
   - dd/mm/yyyy – ...
   (Use 'Approx.' where exact dates are unclear.)

5. Impact on study (summary):
   - • ...
   - • ...
   - • ...

Important:
- DO NOT invent specific dates or facts not supported by the documents.
- If information is missing or unclear, say so explicitly.
- Keep wording neutral and professional, suitable for internal recommendation notes.
"""

    # Text block (policy + raw text)
    text_block = {
        "type": "input_text",
        "text": header_context + "\n" + instructions + "\n\n"
        + "---\nSUPPORTING DOCUMENTS (raw text):\n\n"
        + (raw_docs_text or "").strip()
    }

    image_blocks = _image_files_to_content_blocks(image_files)

    # Single user message with text + images
    return [
        {
            "role": "user",
            "content": [text_block, *image_blocks],
        }
    ]


def generate_special_circ_summary(
    api_key: str,
    student_number: str,
    course_code: str,
    course_name: str,
    request_date: date,
    request_type: str,
    submitted_by: str,
    raw_docs_text: str,
    image_files: Iterable | None = None,
    model: str = "gpt-4.1-mini",
) -> str:
    """
    Call the OpenAI Responses API to generate:
    - Reason for COE
    - Special Circumstances eligibility assessment
    - Documentation assessment
    - Timeline of events
    - Impact on study

    Includes both text documents and images (screenshots) as inputs.
    """
    if not api_key:
        raise ValueError("Missing OpenAI API key.")

    client = OpenAI(api_key=api_key)

    input_items = build_special_circ_input_items(
        student_number=student_number,
        course_code=course_code,
        course_name=course_name,
        request_date=request_date,
        request_type=request_type,
        submitted_by=submitted_by,
        raw_docs_text=raw_docs_text,
        image_files=image_files,
    )

    response = client.responses.create(
        model=model,
        input=input_items,
        temperature=0.2,
    )

    try:
        return response.output[0].content[0].text
    except Exception:
        return str(response)
