from __future__ import annotations

import base64
from typing import List

import fitz

from idp.db_manager.spark_models import (
    AttachmentExtractionOutput,
    PageImageExtractionOutput,
)
from idp.runners.record_worker import RecordWorker


class PageImageExtractionWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def process(
        self, record: AttachmentExtractionOutput
    ) -> List[PageImageExtractionOutput]:
        outputs: List[PageImageExtractionOutput] = []
        path = record.downloaded_attachment_path

        if not path:
            outputs.append(
                PageImageExtractionOutput(
                    docuid=record.docuid,
                    final_docuid=record.final_docuid,
                    downloaded_attachment_path=record.downloaded_attachment_path,
                    page_image=None,
                    page_number=None,
                    page_metadata=None,
                    status="ERRORED",
                )
            )
            return outputs

        lower_path = path.lower()
        try:
            if lower_path.endswith(".pdf"):
                doc = fitz.open(path)
                for page_num in range(doc.page_count):
                    page = doc.load_page(page_num)
                    page_rect = page.rect
                    page_width = page_rect.width
                    page_height = page_rect.height
                    page_area = page_width * page_height
                    fonts = page.get_fonts()
                    has_fonts = len(fonts) > 0

                    image_info = page.get_image_info()
                    image_count = len(image_info)
                    has_full_page_image = False
                    total_image_area = 0.0
                    for img in image_info:
                        x0, y0, x1, y1 = img["bbox"]
                        image_area = (x1 - x0) * (y1 - y0)
                        total_image_area += image_area

                    if page_area > 0 and (total_image_area / page_area >= 0.95):
                        has_full_page_image = True

                    content = page.read_contents() if page.read_contents() else b""
                    has_text_ops = b"Tj" in content or b"Td" in content
                    has_image_ops = b"Do" in content
                    extracted_text = page.get_text("text").strip()
                    text_length = len(extracted_text)

                    is_scanned = (
                        (has_full_page_image and image_count >= 1 and text_length < 10)
                        or (not has_fonts and not has_text_ops)
                        or (image_count >= 1 and has_image_ops and text_length < 10)
                    )
                    page_type = (
                        "Scanned (image-based)"
                        if is_scanned
                        else "Digital (text-based)"
                    )

                    page_meta_data = {
                        "page": page_num + 1,
                        "type": page_type,
                        "has_fonts": has_fonts,
                        "font_count": len(fonts),
                        "image_count": image_count,
                        "has_full_page_image": has_full_page_image,
                        "has_text_ops": has_text_ops,
                        "has_image_ops": has_image_ops,
                        "text_length": text_length,
                        "page_area": page_area,
                        "total_image_area": total_image_area,
                    }

                    pix = page.get_pixmap(dpi=150)
                    img_bytes = pix.tobytes("png")
                    b64_img = base64.b64encode(img_bytes).decode("utf-8")
                    image_base64 = f"data:image/png;base64,{b64_img}"
                    outputs.append(
                        PageImageExtractionOutput(
                            docuid=record.docuid,
                            final_docuid=record.final_docuid,
                            downloaded_attachment_path=record.downloaded_attachment_path,
                            page_image=image_base64,
                            page_number=page_num + 1,
                            page_metadata=page_meta_data,
                            status="COMPLETED",
                        )
                    )
                doc.close()
            elif lower_path.endswith((".jpg", ".jpeg", ".png", ".gif")):
                with open(path, "rb") as img_file:
                    img_bytes = img_file.read()
                b64_img = base64.b64encode(img_bytes).decode("utf-8")
                ext = lower_path.split(".")[-1]
                image_base64 = f"data:image/{ext};base64,{b64_img}"
                outputs.append(
                    PageImageExtractionOutput(
                        docuid=record.docuid,
                        final_docuid=record.final_docuid,
                        downloaded_attachment_path=record.downloaded_attachment_path,
                        page_image=image_base64,
                        page_number=1,
                        page_metadata={},
                        status="COMPLETED",
                    )
                )
            else:
                outputs.append(
                    PageImageExtractionOutput(
                        docuid=record.docuid,
                        final_docuid=record.final_docuid,
                        downloaded_attachment_path=record.downloaded_attachment_path,
                        page_image=None,
                        page_number=None,
                        page_metadata=None,
                        status="ERRORED",
                    )
                )
        except Exception:
            outputs.append(
                PageImageExtractionOutput(
                    docuid=record.docuid,
                    final_docuid=record.final_docuid,
                    downloaded_attachment_path=record.downloaded_attachment_path,
                    page_image=None,
                    page_number=None,
                    page_metadata=None,
                    status="ERRORED",
                )
            )

        return outputs
