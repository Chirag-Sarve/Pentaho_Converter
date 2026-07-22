"""Mail-category job entry helpers (SMTP send, POP3/IMAP get, address validation).

Pure functions used by ``handlers.handle_mail``, ``handle_get_pop``, and
``handle_mail_validator``. Intended for Databricks driver-side execution
(standard library only — no Spark executor SMTP/IMAP).
"""

from __future__ import annotations

import email
import imaplib
import logging
import poplib
import re
import smtplib
import ssl
from dataclasses import dataclass, field
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_EMAIL_STRICT = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_EMAIL_RELAXED = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def yn_true(raw: Any, default: bool = False) -> bool:
    """Parse Pentaho Y/N (and common boolean strings)."""
    if raw is None or raw == "":
        return default
    return str(raw).strip().upper() in {"Y", "YES", "TRUE", "1"}


def split_addresses(raw: str | None) -> list[str]:
    """Split a PDI recipient string on commas / semicolons / whitespace."""
    if not raw:
        return []
    parts = re.split(r"[,;\s]+", str(raw).strip())
    return [p for p in parts if p]


def resolve_password(raw: str | None) -> tuple[str, list[str]]:
    """Return password text and warnings for PDI ``Encrypted …`` values."""
    warnings: list[str] = []
    text = "" if raw is None else str(raw)
    if text.startswith("Encrypted "):
        warnings.append(
            "Password is PDI-encrypted; decryption is not supported — "
            "use a plain password or ${VAR} substitution"
        )
        # Leave encrypted blob as-is so auth fails loudly rather than silently
    return text, warnings


def _attr(attrs: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return str(attrs[key])
    return default


# ---------------------------------------------------------------------------
# Mail Validator
# ---------------------------------------------------------------------------


@dataclass
class MailValidationOutcome:
    valid: bool
    address: str
    error: str = ""
    warnings: list[str] = field(default_factory=list)


def validate_email_address(
    address: str,
    *,
    smtp_check: bool = False,
    sender: str = "",
    default_smtp: str = "",
    timeout: int = 0,
) -> MailValidationOutcome:
    """Validate one address (syntax; optional SMTP check warned, not executed)."""
    warnings: list[str] = []
    text = (address or "").strip()
    if not text:
        return MailValidationOutcome(False, text, "Email address is empty")

    _name, parsed = parseaddr(text)
    candidate = (parsed or text).strip()
    pattern = _EMAIL_STRICT if smtp_check else _EMAIL_RELAXED
    if not pattern.match(candidate):
        return MailValidationOutcome(False, candidate, "Invalid email address format")

    if smtp_check:
        warnings.append(
            "smtpCheck=Y is unsupported on Databricks — "
            f"used structural validation only "
            f"(sender={sender!r}, defaultSMTP={default_smtp!r}, timeout={timeout})"
        )
    return MailValidationOutcome(True, candidate, "", warnings)


def validate_email_addresses(
    addresses: str,
    *,
    smtp_check: bool = False,
    sender: str = "",
    default_smtp: str = "",
    timeout: int = 0,
) -> MailValidationOutcome:
    """Validate space-separated addresses; fail fast on the first invalid one (PDI)."""
    parts = [p for p in (addresses or "").split() if p]
    if not parts:
        return MailValidationOutcome(False, "", "Email address is empty")

    all_warnings: list[str] = []
    last_ok = ""
    for part in parts:
        outcome = validate_email_address(
            part,
            smtp_check=smtp_check,
            sender=sender,
            default_smtp=default_smtp,
            timeout=timeout,
        )
        all_warnings.extend(outcome.warnings)
        if not outcome.valid:
            outcome.warnings = all_warnings
            return outcome
        last_ok = outcome.address
    return MailValidationOutcome(True, last_ok, "", all_warnings)


# ---------------------------------------------------------------------------
# Mail (SMTP)
# ---------------------------------------------------------------------------


@dataclass
class MailSendConfig:
    server: str
    port: int
    to: list[str]
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    subject: str = ""
    body: str = ""
    html: bool = False
    encoding: str = "UTF-8"
    from_address: str = ""
    from_name: str = ""
    reply_to: list[str] = field(default_factory=list)
    use_auth: bool = False
    auth_user: str = ""
    auth_password: str = ""
    use_secure: bool = False
    secure_type: str = "TLS"  # TLS | SSL
    priority: str | None = None
    importance: str | None = None
    sensitivity: str | None = None
    use_priority: bool = False
    attachments: list[str] = field(default_factory=list)
    embedded_images: list[dict[str, str]] = field(default_factory=list)
    contact_person: str = ""
    contact_phone: str = ""
    include_date: bool = False
    only_comment: bool = False
    zip_files: bool = False
    zip_name: str = ""
    include_files: bool = False
    filetypes: list[str] = field(default_factory=list)


@dataclass
class MailSendResult:
    sent: bool
    recipients: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    message_id: str = ""


def mail_config_from_attributes(attrs: Mapping[str, Any]) -> MailSendConfig:
    """Build a send config from parsed MAIL job-entry attributes."""
    from_addr = _attr(attrs, "sender_address", "replyto", "replyAddress")
    from_name = _attr(attrs, "sender_name", "replytoname", "replyName")
    reply_to = split_addresses(
        _attr(attrs, "replyToAddresses", "reply_to", "replyAddress")
    )
    # When replyToAddresses is empty, do not treat From (replyto) as Reply-To
    if reply_to == split_addresses(from_addr) and not _attr(
        attrs, "replyToAddresses", "reply_to"
    ):
        reply_to = []

    embedded: list[dict[str, str]] = []
    raw_images = attrs.get("embeddedimages") or attrs.get("embedded_images") or []
    if isinstance(raw_images, list):
        for item in raw_images:
            if isinstance(item, Mapping):
                embedded.append(
                    {
                        "image_name": str(item.get("image_name") or ""),
                        "content_id": str(item.get("content_id") or ""),
                    }
                )

    filetypes = attrs.get("filetypes") or []
    if isinstance(filetypes, Mapping):
        filetypes = list(filetypes.values())
    elif not isinstance(filetypes, list):
        filetypes = [str(filetypes)] if filetypes else []

    port_raw = _attr(attrs, "port", default="25") or "25"
    try:
        port = int(port_raw)
    except ValueError:
        port = 25

    return MailSendConfig(
        server=_attr(attrs, "server", "smtpServer"),
        port=port,
        to=split_addresses(_attr(attrs, "destination", "destinationAddress", "to")),
        cc=split_addresses(
            _attr(attrs, "destinationCc", "destinationcc", "destination_cc", "cc")
        ),
        bcc=split_addresses(
            _attr(
                attrs,
                "destinationBCc",
                "destinationbcc",
                "destination_bcc",
                "bcc",
            )
        ),
        subject=_attr(attrs, "subject"),
        body=_attr(attrs, "comment", "body", "message"),
        html=yn_true(attrs.get("use_HTML") or attrs.get("useHTML")),
        encoding=_attr(attrs, "encoding", default="UTF-8") or "UTF-8",
        from_address=from_addr,
        from_name=from_name,
        reply_to=reply_to,
        use_auth=yn_true(attrs.get("use_auth") or attrs.get("useauthentication")),
        auth_user=_attr(attrs, "auth_user", "authUser"),
        auth_password=_attr(attrs, "auth_password", "authPassword"),
        use_secure=yn_true(
            attrs.get("use_secure_auth")
            or attrs.get("use_secureAuth")
            or attrs.get("secureauth")
        ),
        secure_type=(
            _attr(attrs, "secureconnectiontype", "secureConnectionType", default="TLS")
            or "TLS"
        ).upper(),
        priority=_attr(attrs, "priority") or None,
        importance=_attr(attrs, "importance") or None,
        sensitivity=_attr(attrs, "sensitivity") or None,
        use_priority=yn_true(attrs.get("use_Priority") or attrs.get("usePriority")),
        contact_person=_attr(attrs, "contact_person", "contactPerson"),
        contact_phone=_attr(attrs, "contact_phone", "contactPhone"),
        include_date=yn_true(attrs.get("include_date") or attrs.get("includeDate")),
        only_comment=yn_true(attrs.get("only_comment") or attrs.get("onlyComment")),
        zip_files=yn_true(attrs.get("zip_files") or attrs.get("zipFiles")),
        zip_name=_attr(attrs, "zip_name", "zip_filename", "zipFilename"),
        include_files=yn_true(attrs.get("include_files") or attrs.get("includeFiles")),
        filetypes=[str(x) for x in filetypes if x],
        embedded_images=embedded,
    )


def _build_email_message(cfg: MailSendConfig) -> EmailMessage:
    msg = EmailMessage()
    if cfg.from_name and cfg.from_address:
        msg["From"] = formataddr((cfg.from_name, cfg.from_address))
    elif cfg.from_address:
        msg["From"] = cfg.from_address
    if cfg.to:
        msg["To"] = ", ".join(cfg.to)
    if cfg.cc:
        msg["Cc"] = ", ".join(cfg.cc)
    if cfg.bcc:
        # Bcc is still listed for transport; clients typically omit display
        msg["Bcc"] = ", ".join(cfg.bcc)
    if cfg.reply_to:
        msg["Reply-To"] = ", ".join(cfg.reply_to)
    msg["Subject"] = cfg.subject or ""

    body = cfg.body or ""
    if not cfg.only_comment:
        extras: list[str] = []
        if cfg.contact_person:
            extras.append(f"Contact: {cfg.contact_person}")
        if cfg.contact_phone:
            extras.append(f"Phone: {cfg.contact_phone}")
        if extras:
            body = (body + "\n\n" + "\n".join(extras)).strip()

    subtype = "html" if cfg.html else "plain"
    msg.set_content(body, subtype=subtype, charset=cfg.encoding or "utf-8")

    if cfg.use_priority:
        # PDI priority values: high / normal / low (also numeric-ish)
        prio = (cfg.priority or "normal").lower()
        if prio in {"high", "1", "highest"}:
            msg["X-Priority"] = "1"
            msg["Priority"] = "urgent"
        elif prio in {"low", "5", "lowest"}:
            msg["X-Priority"] = "5"
            msg["Priority"] = "non-urgent"
        else:
            msg["X-Priority"] = "3"
            msg["Priority"] = "normal"
        if cfg.importance:
            msg["Importance"] = cfg.importance
        if cfg.sensitivity:
            msg["Sensitivity"] = cfg.sensitivity

    return msg


def _attach_files(msg: EmailMessage, paths: Sequence[str], warnings: list[str]) -> None:
    for raw in paths:
        path = Path(raw)
        if not path.is_file():
            warnings.append(f"Attachment not found, skipped: {raw}")
            continue
        data = path.read_bytes()
        maintype, subtype = "application", "octet-stream"
        msg.add_attachment(
            data,
            maintype=maintype,
            subtype=subtype,
            filename=path.name,
        )


def send_smtp_mail(
    cfg: MailSendConfig,
    *,
    smtp_factory: Callable[..., Any] | None = None,
    smtp_ssl_factory: Callable[..., Any] | None = None,
) -> MailSendResult:
    """Send one message via SMTP. Raises on connection/auth/send failures."""
    warnings: list[str] = []
    if not cfg.server:
        raise ValueError("MAIL requires SMTP server")
    recipients = list(dict.fromkeys([*cfg.to, *cfg.cc, *cfg.bcc]))
    if not recipients:
        raise ValueError("MAIL requires at least one recipient (destination/Cc/Bcc)")
    if not cfg.from_address:
        raise ValueError("MAIL requires a sender (replyto / sender_address)")

    if cfg.include_files:
        warnings.append(
            "include_files=Y (attach previous-result files) is not fully supported — "
            "only explicit attachment paths are used"
        )
    if cfg.zip_files:
        warnings.append(
            f"zip_files=Y (zip_name={cfg.zip_name!r}) is unsupported — "
            "attachments are sent uncompressed when present"
        )
    if cfg.include_date:
        warnings.append("include_date=Y is preserved but not injected into the body")
    if cfg.filetypes:
        warnings.append(
            f"Result-file filetype filter {cfg.filetypes!r} is unsupported without "
            "a PDI result-file list"
        )
    if cfg.embedded_images:
        warnings.append(
            "embeddedimages are not inlined; listed as unsupported HTML CIDs"
        )
        for img in cfg.embedded_images:
            name = img.get("image_name") or ""
            if name and Path(name).is_file():
                cfg.attachments = [*cfg.attachments, name]
            elif name:
                warnings.append(f"Embedded image not found: {name}")

    password, pw_warnings = resolve_password(cfg.auth_password)
    warnings.extend(pw_warnings)

    msg = _build_email_message(cfg)
    if cfg.attachments:
        _attach_files(msg, cfg.attachments, warnings)

    use_ssl = cfg.use_secure and cfg.secure_type == "SSL"
    use_tls = cfg.use_secure and cfg.secure_type != "SSL"

    smtp_cls = smtp_factory or smtplib.SMTP
    smtp_ssl_cls = smtp_ssl_factory or smtplib.SMTP_SSL

    if use_ssl:
        context = ssl.create_default_context()
        client = smtp_ssl_cls(cfg.server, cfg.port, context=context, timeout=60)
    else:
        client = smtp_cls(cfg.server, cfg.port, timeout=60)

    try:
        client.ehlo()
        if use_tls:
            client.starttls(context=ssl.create_default_context())
            client.ehlo()
        if cfg.use_auth:
            if not cfg.auth_user:
                raise ValueError("MAIL use_auth=Y but auth_user is empty")
            client.login(cfg.auth_user, password)
        refused = client.send_message(msg)
        # smtplib returns {} on full success; treat only non-empty mappings as failure
        if isinstance(refused, Mapping) and refused:
            raise smtplib.SMTPRecipientsRefused(refused)
    finally:
        try:
            client.quit()
        except Exception:  # noqa: BLE001
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass

    return MailSendResult(
        sent=True,
        recipients=recipients,
        warnings=warnings,
        message_id=str(msg.get("Message-ID") or ""),
    )


# ---------------------------------------------------------------------------
# Get Mails (POP3 / IMAP)
# ---------------------------------------------------------------------------


@dataclass
class GetMailsConfig:
    protocol: str = "POP3"  # POP3 | IMAP
    server: str = ""
    username: str = ""
    password: str = ""
    use_ssl: bool = False
    ssl_port: int | None = None
    output_directory: str = ""
    filename_pattern: str = ""
    retrieve_mails: int = 0  # 0=all, 1=unread, 2=first N
    first_mails: int = 0
    delete: bool = False
    save_message: bool = True
    save_attachment: bool = True
    use_different_attachment_folder: bool = False
    attachment_folder: str = ""
    attachment_wildcard: str = ""
    imap_folder: str = "INBOX"
    imap_list: str = "all"
    imap_first_mails: int = 0
    action_type: str = "get"  # get | move | delete
    after_get_imap: str = "nothing"  # nothing | delete | move
    move_to_imap_folder: str = ""
    create_move_to_folder: bool = False
    create_local_folder: bool = False
    include_subfolders: bool = False
    use_proxy: bool = False
    proxy_username: str = ""
    sender_search: str = ""
    not_sender_search: bool = False
    recipient_search: str = ""
    not_recipient_search: bool = False
    subject_search: str = ""
    not_subject_search: bool = False
    body_search: str = ""
    not_body_search: bool = False
    condition_received_date: str = ""
    received_date1: str = ""
    received_date2: str = ""
    not_received_date: bool = False


@dataclass
class GetMailsResult:
    retrieved: int = 0
    saved_messages: list[str] = field(default_factory=list)
    saved_attachments: list[str] = field(default_factory=list)
    deleted: int = 0
    warnings: list[str] = field(default_factory=list)


def get_mails_config_from_attributes(attrs: Mapping[str, Any]) -> GetMailsConfig:
    """Build Get Mails config from GET_POP job-entry attributes."""

    def _int(raw: Any, default: int = 0) -> int:
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            return default

    ssl_port_raw = _attr(attrs, "sslport", "ssl_port", "port")
    ssl_port = _int(ssl_port_raw, 0) or None

    return GetMailsConfig(
        protocol=(_attr(attrs, "protocol", default="POP3") or "POP3").upper(),
        server=_attr(attrs, "servername", "server", "host"),
        username=_attr(attrs, "username", "user"),
        password=_attr(attrs, "password"),
        use_ssl=yn_true(attrs.get("usessl") or attrs.get("use_ssl")),
        ssl_port=ssl_port,
        output_directory=_attr(attrs, "outputdirectory", "output_directory"),
        filename_pattern=_attr(attrs, "filenamepattern", "filename_pattern"),
        retrieve_mails=_int(attrs.get("retrievemails"), 0),
        first_mails=_int(attrs.get("firstmails"), 0),
        delete=yn_true(attrs.get("delete")),
        save_message=yn_true(attrs.get("savemessage"), default=True),
        save_attachment=yn_true(attrs.get("saveattachment"), default=True),
        use_different_attachment_folder=yn_true(
            attrs.get("usedifferentfolderforattachment")
        ),
        attachment_folder=_attr(attrs, "attachmentfolder", "attachment_folder"),
        attachment_wildcard=_attr(attrs, "attachmentwildcard", "attachment_wildcard"),
        imap_folder=_attr(attrs, "imapfolder", "imap_folder", default="INBOX")
        or "INBOX",
        imap_list=(_attr(attrs, "valueimaplist", default="all") or "all").lower(),
        imap_first_mails=_int(attrs.get("imapfirstmails"), 0),
        action_type=(_attr(attrs, "actiontype", default="get") or "get").lower(),
        after_get_imap=(
            _attr(attrs, "aftergetimap", default="nothing") or "nothing"
        ).lower(),
        move_to_imap_folder=_attr(attrs, "movetoimapfolder", "move_to_imap_folder"),
        create_move_to_folder=yn_true(attrs.get("createmovetofolder")),
        create_local_folder=yn_true(attrs.get("createlocalfolder")),
        include_subfolders=yn_true(
            attrs.get("includesubfolders") or attrs.get("include_subfolders")
        ),
        use_proxy=yn_true(attrs.get("useproxy") or attrs.get("use_proxy")),
        proxy_username=_attr(attrs, "proxyusername", "proxy_username"),
        sender_search=_attr(attrs, "sendersearch", "sender_search"),
        not_sender_search=yn_true(attrs.get("nottermsendersearch")),
        recipient_search=_attr(attrs, "receipientsearch", "recipientsearch"),
        not_recipient_search=yn_true(attrs.get("nottermreceipientsearch")),
        subject_search=_attr(attrs, "subjectsearch", "subject_search"),
        not_subject_search=yn_true(attrs.get("nottermsubjectsearch")),
        body_search=_attr(attrs, "bodysearch", "body_search"),
        not_body_search=yn_true(attrs.get("nottermbodysearch")),
        condition_received_date=_attr(
            attrs, "conditionreceiveddate", "condition_received_date"
        ),
        received_date1=_attr(attrs, "receiveddate1", "receivedDate1"),
        received_date2=_attr(attrs, "receiveddate2", "receivedDate2"),
        not_received_date=yn_true(attrs.get("nottermreceiveddatesearch")),
    )


def _default_mail_port(protocol: str, use_ssl: bool, ssl_port: int | None) -> int:
    if ssl_port:
        return ssl_port
    proto = protocol.upper()
    if proto == "IMAP":
        return 993 if use_ssl else 143
    return 995 if use_ssl else 110


def _safe_filename(raw: str, fallback: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", raw or "").strip() or fallback
    return cleaned[:180]


def _message_matches_filters(msg: email.message.Message, cfg: GetMailsConfig) -> bool:
    def _check(value: str, needle: str, invert: bool) -> bool:
        if not needle:
            return True
        found = needle.lower() in (value or "").lower()
        return (not found) if invert else found

    sender = msg.get("From", "") or ""
    to_hdr = " ".join(
        filter(None, [msg.get("To", ""), msg.get("Cc", ""), msg.get("Delivered-To", "")])
    )
    subject = msg.get("Subject", "") or ""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    body = part.get_payload(decode=True).decode(  # type: ignore[union-attr]
                        errors="replace"
                    )
                except Exception:  # noqa: BLE001
                    body = str(part.get_payload())
                break
    else:
        payload = msg.get_payload(decode=True)
        if isinstance(payload, bytes):
            body = payload.decode(errors="replace")
        else:
            body = str(payload or "")

    if not _check(sender, cfg.sender_search, cfg.not_sender_search):
        return False
    if not _check(to_hdr, cfg.recipient_search, cfg.not_recipient_search):
        return False
    if not _check(subject, cfg.subject_search, cfg.not_subject_search):
        return False
    if not _check(body, cfg.body_search, cfg.not_body_search):
        return False
    return True


def _save_message_parts(
    msg: email.message.Message,
    *,
    index: int,
    cfg: GetMailsConfig,
    out_dir: Path,
    attach_dir: Path,
    result: GetMailsResult,
) -> None:
    subject = msg.get("Subject") or f"message_{index}"
    base = _safe_filename(subject, f"message_{index}")
    if cfg.filename_pattern:
        # Minimal PDI-like tokens
        name = (
            cfg.filename_pattern.replace("${subject}", subject)
            .replace("${nr}", str(index))
            .replace("${date}", "")
        )
        base = _safe_filename(name, base)

    if cfg.save_message:
        path = out_dir / f"{base}_{index}.eml"
        path.write_bytes(msg.as_bytes())
        result.saved_messages.append(str(path))

    if cfg.save_attachment:
        for part in msg.walk():
            filename = part.get_filename()
            if not filename:
                continue
            if cfg.attachment_wildcard:
                # Simple glob: * and ?
                pattern = (
                    re.escape(cfg.attachment_wildcard)
                    .replace(r"\*", ".*")
                    .replace(r"\?", ".")
                )
                if not re.fullmatch(pattern, filename, flags=re.IGNORECASE):
                    continue
            payload = part.get_payload(decode=True)
            if not isinstance(payload, bytes):
                continue
            dest = attach_dir / _safe_filename(filename, f"attach_{index}")
            dest.write_bytes(payload)
            result.saved_attachments.append(str(dest))


def _retrieve_via_pop3(
    cfg: GetMailsConfig,
    *,
    out_dir: Path,
    attach_dir: Path,
    password: str,
    result: GetMailsResult,
    pop_factory: Callable[..., Any] | None = None,
    pop_ssl_factory: Callable[..., Any] | None = None,
) -> None:
    port = _default_mail_port(cfg.protocol, cfg.use_ssl, cfg.ssl_port)
    if cfg.use_ssl:
        cls = pop_ssl_factory or poplib.POP3_SSL
        conn = cls(cfg.server, port, timeout=60)
    else:
        cls = pop_factory or poplib.POP3
        conn = cls(cfg.server, port, timeout=60)
    try:
        conn.user(cfg.username)
        conn.pass_(password)
        count = len(conn.list()[1])
        indices = list(range(1, count + 1))
        if cfg.retrieve_mails == 2 and cfg.first_mails > 0:
            indices = indices[: cfg.first_mails]
        # retrieve_mails==1 (unread) is not meaningful on POP3 — warn
        if cfg.retrieve_mails == 1:
            result.warnings.append(
                "retrievemails=1 (unread) is not supported on POP3 — retrieving all"
            )

        retrieved = 0
        for idx in indices:
            resp, lines, _octets = conn.retr(idx)
            raw = b"\r\n".join(lines)
            msg = email.message_from_bytes(raw)
            if not _message_matches_filters(msg, cfg):
                continue
            retrieved += 1
            _save_message_parts(
                msg,
                index=retrieved,
                cfg=cfg,
                out_dir=out_dir,
                attach_dir=attach_dir,
                result=result,
            )
            if cfg.delete or cfg.action_type == "delete":
                conn.dele(idx)
                result.deleted += 1
            if cfg.retrieve_mails == 2 and retrieved >= cfg.first_mails > 0:
                break
        result.retrieved = retrieved
    finally:
        try:
            conn.quit()
        except Exception:  # noqa: BLE001
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def _imap_search_criteria(cfg: GetMailsConfig) -> str:
    """Build a basic IMAP SEARCH string from list mode + simple filters."""
    parts: list[str] = []
    mode = (cfg.imap_list or "all").lower()
    if mode in {"unread", "new"}:
        parts.append("UNSEEN")
    elif mode in {"read", "old"}:
        parts.append("SEEN")
    elif mode == "flagged":
        parts.append("FLAGGED")
    elif mode == "not_flagged":
        parts.append("UNFLAGGED")
    elif mode == "draft":
        parts.append("DRAFT")
    elif mode == "not_draft":
        parts.append("UNDRAFT")
    else:
        parts.append("ALL")

    if cfg.sender_search and not cfg.not_sender_search:
        parts.append(f'FROM "{cfg.sender_search}"')
    if cfg.subject_search and not cfg.not_subject_search:
        parts.append(f'SUBJECT "{cfg.subject_search}"')
    return "(" + " ".join(parts) + ")"


def _retrieve_via_imap(
    cfg: GetMailsConfig,
    *,
    out_dir: Path,
    attach_dir: Path,
    password: str,
    result: GetMailsResult,
    imap_factory: Callable[..., Any] | None = None,
    imap_ssl_factory: Callable[..., Any] | None = None,
) -> None:
    port = _default_mail_port(cfg.protocol, cfg.use_ssl, cfg.ssl_port)
    if cfg.use_ssl:
        cls = imap_ssl_factory or imaplib.IMAP4_SSL
        conn = cls(cfg.server, port)
    else:
        cls = imap_factory or imaplib.IMAP4
        conn = cls(cfg.server, port)
    try:
        conn.login(cfg.username, password)
        folder = cfg.imap_folder or "INBOX"
        typ, _ = conn.select(folder)
        if typ != "OK":
            raise RuntimeError(f"Cannot select IMAP folder {folder!r}")

        criteria = _imap_search_criteria(cfg)
        typ, data = conn.search(None, criteria)
        if typ != "OK":
            raise RuntimeError(f"IMAP SEARCH failed: {criteria}")
        ids = (data[0] or b"").split()
        if cfg.retrieve_mails == 2 and cfg.first_mails > 0:
            ids = ids[: cfg.first_mails]
        elif cfg.imap_first_mails > 0:
            ids = ids[: cfg.imap_first_mails]

        retrieved = 0
        for msg_id in ids:
            typ, msg_data = conn.fetch(msg_id, "(RFC822)")
            if typ != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            if not isinstance(raw, (bytes, bytearray)):
                continue
            msg = email.message_from_bytes(bytes(raw))
            # Client-side filters for NOT terms / body / recipient
            if not _message_matches_filters(msg, cfg):
                continue
            retrieved += 1

            if cfg.action_type == "delete":
                conn.store(msg_id, "+FLAGS", "\\Deleted")
                result.deleted += 1
                continue

            if cfg.action_type == "move":
                result.warnings.append(
                    "actiontype=move requires a target folder copy; "
                    "falling back to get+optional delete flags"
                )

            _save_message_parts(
                msg,
                index=retrieved,
                cfg=cfg,
                out_dir=out_dir,
                attach_dir=attach_dir,
                result=result,
            )

            if cfg.delete or cfg.after_get_imap == "delete":
                conn.store(msg_id, "+FLAGS", "\\Deleted")
                result.deleted += 1
            elif cfg.after_get_imap == "move":
                if cfg.move_to_imap_folder:
                    try:
                        if cfg.create_move_to_folder:
                            conn.create(cfg.move_to_imap_folder)
                    except Exception:  # noqa: BLE001
                        pass
                    typ_c, _ = conn.copy(msg_id, cfg.move_to_imap_folder)
                    if typ_c == "OK":
                        conn.store(msg_id, "+FLAGS", "\\Deleted")
                        result.deleted += 1
                    else:
                        result.warnings.append(
                            f"IMAP MOVE/COPY to {cfg.move_to_imap_folder!r} failed"
                        )
                else:
                    result.warnings.append(
                        "aftergetimap=move but movetoimapfolder is empty"
                    )

        if result.deleted:
            conn.expunge()
        result.retrieved = retrieved
    finally:
        try:
            conn.logout()
        except Exception:  # noqa: BLE001
            pass


def get_mails(
    cfg: GetMailsConfig,
    *,
    pop_factory: Callable[..., Any] | None = None,
    pop_ssl_factory: Callable[..., Any] | None = None,
    imap_factory: Callable[..., Any] | None = None,
    imap_ssl_factory: Callable[..., Any] | None = None,
) -> GetMailsResult:
    """Retrieve messages via POP3 or IMAP and optionally save locally."""
    result = GetMailsResult()
    if not cfg.server:
        raise ValueError("GET_POP requires servername")
    if not cfg.username:
        raise ValueError("GET_POP requires username")
    if not cfg.output_directory and (
        cfg.save_message or cfg.save_attachment
    ) and cfg.action_type == "get":
        raise ValueError("GET_POP requires outputdirectory when saving messages")

    password, pw_warnings = resolve_password(cfg.password)
    result.warnings.extend(pw_warnings)

    if cfg.use_proxy:
        result.warnings.append(
            f"useproxy=Y (proxyusername={cfg.proxy_username!r}) is unsupported — "
            "connecting directly"
        )
    if cfg.include_subfolders:
        result.warnings.append(
            "includesubfolders=Y is unsupported — only the selected IMAP folder is used"
        )
    if cfg.condition_received_date or cfg.received_date1 or cfg.received_date2:
        result.warnings.append(
            "Received-date search filters are not applied "
            f"(condition={cfg.condition_received_date!r}, "
            f"from={cfg.received_date1!r}, to={cfg.received_date2!r})"
        )

    out_dir = Path(cfg.output_directory) if cfg.output_directory else Path(".")
    if cfg.create_local_folder or cfg.action_type == "get":
        out_dir.mkdir(parents=True, exist_ok=True)

    if cfg.use_different_attachment_folder and cfg.attachment_folder:
        attach_dir = Path(cfg.attachment_folder)
        attach_dir.mkdir(parents=True, exist_ok=True)
    else:
        attach_dir = out_dir

    proto = cfg.protocol.upper()
    if proto == "IMAP":
        _retrieve_via_imap(
            cfg,
            out_dir=out_dir,
            attach_dir=attach_dir,
            password=password,
            result=result,
            imap_factory=imap_factory,
            imap_ssl_factory=imap_ssl_factory,
        )
    elif proto == "POP3":
        _retrieve_via_pop3(
            cfg,
            out_dir=out_dir,
            attach_dir=attach_dir,
            password=password,
            result=result,
            pop_factory=pop_factory,
            pop_ssl_factory=pop_ssl_factory,
        )
    else:
        raise ValueError(f"Unsupported mail protocol: {cfg.protocol!r}")

    return result


def iter_warning_logs(prefix: str, warnings: Iterable[str]) -> None:
    for warning in warnings:
        logger.warning("%s | %s", prefix, warning)
