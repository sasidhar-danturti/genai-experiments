from __future__ import annotations

import os
from email import policy
from email import message_from_binary_file
from typing import Optional

from idp.db_manager.attachment_models import AttachmentExtractionInput, AttachmentExtractionOutput
from idp.runners.record_worker import RecordWorker


class AttachmentExtractionWorker(RecordWorker):
    is_spark_serializable = False
    is_threadsafe = True

    def _download_embedded_files(
        self,
        output_path: str,
        filepath: Optional[str] = None,
        file_type: str = "all",
        prefix: Optional[str] = None,
        replace: bool = False,
    ) -> None:
        if filepath is None:
            raise ValueError("filepath is required to download embedded files.")

        allowed_type = ["attachment", "inline", "all"]
        if file_type not in allowed_type:
            raise ValueError(
                f"Can only use these file_type values: {allowed_type} got {file_type}"
            )
        extracted_type = allowed_type if file_type == "all" else [file_type]

        with open(filepath, "rb") as handle:
            message = message_from_binary_file(handle, policy=policy.default)

        os.makedirs(output_path, exist_ok=True)

        for idx, part in enumerate(message.walk()):
            if part.get_filename():
                if (part.get_content_disposition() in extracted_type) or (
                    part.get_content_disposition() is None
                ):
                    filename = (
                        f"{prefix}.{part.get_filename()}"
                        if prefix
                        else part.get_filename()
                    )
                    base, ext = os.path.splitext(filename)
                    filename = f"{base}_{idx}{ext}"
                    filename = os.path.join(output_path, filename)
                    if (not os.path.exists(filename)) or replace:
                        with open(filename, "wb") as handle:
                            handle.write(part.get_payload(decode=True))

    def process(self, record: AttachmentExtractionInput) -> AttachmentExtractionOutput:
        file_extension = (record.file_extension or "").lower()
        downloaded_path = ""

        if file_extension in {"eml", "msg"} and record.file_path:
            root_dir = os.path.dirname(record.file_path)
            filename = os.path.basename(record.file_path)
            folder_name = os.path.splitext(filename)[0]
            output_path = os.path.join(root_dir, folder_name)
            prefix = record.attachment_name or None
            self._download_embedded_files(
                output_path=output_path,
                filepath=record.file_path,
                file_type="all",
                prefix=prefix,
            )
            if prefix:
                matching_files = [
                    os.path.join(output_path, f)
                    for f in os.listdir(output_path)
                    if os.path.isfile(os.path.join(output_path, f)) and prefix in f
                ]
                downloaded_path = matching_files[0] if matching_files else ""
            else:
                downloaded_path = record.file_path
        else:
            downloaded_path = record.file_path or ""

        status = "COMPLETED" if downloaded_path else "ERRORED"

        return AttachmentExtractionOutput(
            docuid=record.docuid,
            final_docuid=record.final_docuid,
            file_path=record.file_path,
            file_extension=record.file_extension,
            file_name=record.file_name,
            attachment_index=record.attachment_index,
            attachment_name=record.attachment_name,
            is_inline=record.is_inline,
            downloaded_attachment_path=downloaded_path,
            status=status,
        )
