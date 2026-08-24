"""The assembled trust store, offline.

The bug this guards against was silent: LegCo's seven crawlers reported
successful runs for months while collecting nothing, because verification
failed and each one caught the error and carried on. The bundle is the fix, so
it is worth asserting that it actually contains the extra certificate and that
it never quietly overrides an operator's own.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import ca


def test_the_bundle_contains_certifi_and_every_extra_pem() -> None:
    path = ca.build_ca_bundle()
    assert path, "certs/ has PEMs, so a bundle must be built"
    text = Path(path).read_text(encoding="utf-8")

    import certifi

    certifi_text = Path(certifi.where()).read_text(encoding="utf-8")
    assert certifi_text.strip() in text, "the public roots must still be trusted"

    for pem in sorted(ca.CERT_DIR.glob("*.pem")):
        body = pem.read_text(encoding="utf-8").strip()
        assert body in text, f"{pem.name} is missing from the bundle"

    os.unlink(path)


def test_every_shipped_pem_is_a_parseable_certificate() -> None:
    # A truncated or DER-encoded file would be accepted here and then break
    # verification for every site at once.
    for pem in sorted(ca.CERT_DIR.glob("*.pem")):
        body = pem.read_text(encoding="utf-8")
        assert "-----BEGIN CERTIFICATE-----" in body, pem.name
        assert "-----END CERTIFICATE-----" in body, pem.name


def test_an_operator_bundle_is_not_overridden() -> None:
    # A corporate trust store is a deliberate choice and may be the only way
    # out to the internet; replacing it would break every request.
    before = os.environ.get("REQUESTS_CA_BUNDLE")
    os.environ["REQUESTS_CA_BUNDLE"] = "/somewhere/corporate.pem"
    try:
        assert ca.install_ca_bundle() == "/somewhere/corporate.pem"
        assert os.environ["REQUESTS_CA_BUNDLE"] == "/somewhere/corporate.pem"
    finally:
        if before is None:
            del os.environ["REQUESTS_CA_BUNDLE"]
        else:
            os.environ["REQUESTS_CA_BUNDLE"] = before


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"PASS {name}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {name}: {e}")
        except Exception as e:  # noqa: BLE001
            failures += 1
            print(f"ERROR {name}: {type(e).__name__}: {e}")
    print(f"\n{failures} failure(s)")
    raise SystemExit(1 if failures else 0)
