from __future__ import annotations

import os
import email
import importlib.util
from email import policy
from email import message_from_binary_file
from typing import Optional

from idp.db_manager.spark_models import AttachmentExtractionOutput, DownloadProcessOutput
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

    def _extract_email_attachments(self, file_path: str, file_extension: str):
        try:
            if not file_extension or file_extension.lower() not in ("eml", "email", "msg"):
                return [{"attachment_name": None, "is_inline": False}]
            if not file_path or not os.path.exists(file_path):
                return [{"attachment_name": None, "is_inline": False}]
            with open(file_path, "rb") as handle:
                content_bytes = handle.read()
            message = email.message_from_bytes(content_bytes)
            attachments = [{"attachment_name": None, "is_inline": False}]
            for part in message.walk():
                content_disposition = part.get("Content-Disposition", "")
                if part.get_content_maintype() == "multipart":
                    continue
                if "attachment" in content_disposition or "inline" in content_disposition:
                    name = part.get_filename() or "unknown"
                    attachments.append(
                        {
                            "attachment_name": name,
                            "is_inline": "inline" in content_disposition,
                        }
                    )
            return attachments
        except Exception:
            return [{"attachment_name": None, "is_inline": False}]

    def _extract_email_content_eml(self, file_path: str) -> tuple[str, list[str]]:
        with open(file_path, "rb") as handle:
            message = message_from_binary_file(handle, policy=policy.default)

        from_ = message.get("From", "")
        to_ = message.get("To", "")
        cc_ = message.get("Cc", "")

        body = ""
        if message.is_multipart():
            for part in message.walk():
                if part.get_content_type() == "text/plain" and not part.get_filename():
                    try:
                        body = part.get_content()
                        break
                    except Exception:
                        body = ""
        else:
            try:
                body = message.get_content()
            except Exception:
                body = ""

        inline_names = []
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            content_disposition = (part.get("Content-Disposition") or "").lower()
            if "inline" in content_disposition:
                inline_names.append(part.get_filename() or "inline")

        content = f"From: {from_}\nTo: {to_}\nCc: {cc_}\n\n{body}"
        return content, inline_names

    def _extract_email_content_msg(self, file_path: str) -> tuple[str, list[str]]:
        inline_names: list[str] = []
        if importlib.util.find_spec("extract_msg") is None:
            return "", inline_names
        import extract_msg

        msg = extract_msg.openMsg(file_path)
        try:
            from_ = msg.sender or ""
            to_ = msg.to or ""
            cc_ = msg.cc or ""
            body = msg.body or ""
            for att in msg.attachments:
                if getattr(att, "cid", None):
                    inline_names.append(att.longFilename or att.shortFilename or "inline")
            content = f"From: {from_}\nTo: {to_}\nCc: {cc_}\n\n{body}"
            return content, inline_names
        finally:
            msg.close()

    def _build_email_content(self, file_path: str, file_extension: str) -> tuple[str, list[str]]:
        if not file_path or not file_extension:
            return "", []
        ext = file_extension.lower()
        if ext in ("eml", "email"):
            return self._extract_email_content_eml(file_path)
        if ext == "msg":
            return self._extract_email_content_msg(file_path)
        return "", []

    def _add_inline_markers(self, content: str, inline_names: list[str]) -> str:
        if not inline_names:
            return content
        markers = "\n".join(f"[INLINE_ATTACHMENT: {name}]" for name in inline_names)
        return f"{content}\n\n{markers}"

    def process(self, record: DownloadProcessOutput) -> list[AttachmentExtractionOutput]:
        file_extension = ""
        if record.file_path:
            _, ext = os.path.splitext(record.file_path)
            file_extension = ext.lstrip(".")
        file_name = os.path.basename(record.file_path or "")
        attachments = self._extract_email_attachments(record.file_path, file_extension)
        email_content, inline_names = self._build_email_content(record.file_path, file_extension)
        email_content = self._add_inline_markers(email_content, inline_names)
        inline_names_str = ", ".join(inline_names) if inline_names else None
        has_inline_attachments = bool(inline_names)

        outputs: list[AttachmentExtractionOutput] = []
        for idx, attachment in enumerate(attachments):
            attachment_name = attachment.get("attachment_name")
            is_inline = bool(attachment.get("is_inline"))
            if idx == 0:
                final_docuid = record.docuid
            else:
                final_docuid = f"{record.docuid}_{idx}"

            downloaded_path = ""
            if file_extension.lower() in {"eml", "msg"} and record.file_path:
                root_dir = os.path.dirname(record.file_path)
                folder_name = os.path.splitext(file_name)[0]
                output_path = os.path.join(root_dir, folder_name)
                prefix = attachment_name or None
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

            outputs.append(
                AttachmentExtractionOutput(
                    docuid=record.docuid,
                    final_docuid=final_docuid,
                    file_path=record.file_path,
                    file_extension=file_extension,
                    file_name=file_name,
                    attachment_index=idx,
                    attachment_name=attachment_name,
                    is_inline=is_inline,
                    downloaded_attachment_path=downloaded_path,
                    email_content=email_content or None,
                    has_inline_attachments=has_inline_attachments,
                    inline_attachment_names=inline_names_str,
                    status=status,
                )
            )

        return outputs
