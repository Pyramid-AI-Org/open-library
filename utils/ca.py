"""Trust store assembled from certifi plus the PEMs in ``certs/``.

Some government servers send an incomplete certificate chain - the leaf
without the intermediate that signs it. Browsers and curl recover by fetching
the missing certificate from the leaf's ``authorityInfoAccess`` URL; requests
does not, so verification fails with "unable to get local issuer certificate".

Rather than disable verification, or thread a ``verify=`` argument through the
hundred-odd modules that build their own session, we assemble one bundle at
startup and publish it through ``REQUESTS_CA_BUNDLE``. requests reads that
variable per request, so every session picks it up, in CI and locally alike.

See ``certs/README.md`` for what is in there and why.
"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile

CERT_DIR = Path(__file__).resolve().parent.parent / "certs"


def build_ca_bundle() -> str | None:
    """Write certifi + ``certs/*.pem`` to a temp file and return its path.

    Returns ``None`` when there is nothing to add, so the caller leaves the
    default trust store alone.
    """
    extra = sorted(CERT_DIR.glob("*.pem")) if CERT_DIR.is_dir() else []
    if not extra:
        return None

    try:
        import certifi
    except ImportError:  # pragma: no cover - certifi ships with requests
        return None

    parts = [Path(certifi.where()).read_text(encoding="utf-8")]
    for pem in extra:
        parts.append(pem.read_text(encoding="utf-8"))

    fd, path = tempfile.mkstemp(prefix="open-library-ca-", suffix=".pem")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write("\n".join(parts))
    return path


def install_ca_bundle(*, verbose: bool = False) -> str | None:
    """Point requests at the assembled bundle for the rest of the process.

    An operator who has already set ``REQUESTS_CA_BUNDLE`` wins: their bundle
    is deliberate and may be a corporate trust store we must not replace.
    """
    existing = (os.environ.get("REQUESTS_CA_BUNDLE") or "").strip()
    if existing:
        if verbose:
            print(f"CA bundle: using REQUESTS_CA_BUNDLE from the environment ({existing})")
        return existing

    path = build_ca_bundle()
    if not path:
        return None

    os.environ["REQUESTS_CA_BUNDLE"] = path
    if verbose:
        names = ", ".join(p.name for p in sorted(CERT_DIR.glob("*.pem")))
        print(f"CA bundle: certifi + certs/ ({names})")
    return path
