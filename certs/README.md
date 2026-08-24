# Extra CA certificates

PEM files here are appended to the `certifi` trust store at startup by
`utils/ca.py`, which every crawler picks up through `REQUESTS_CA_BUNDLE`.

## Why this directory exists

`www.legco.gov.hk` serves an incomplete certificate chain: the leaf only,
without the Hongkong Post intermediate that signs it. Browsers and curl paper
over this by fetching the missing certificate from the `authorityInfoAccess`
URL in the leaf. Python's `requests` does not do that, so verification failed
with `unable to get local issuer certificate` - and because the LegCo crawlers
caught the error per year and carried on, all seven of them reported successful
runs while collecting nothing. 5,605 PDFs were missing from the library for as
long as that went unnoticed.

Supplying the intermediate ourselves fixes it without weakening verification:
the chain still has to terminate in a root that `certifi` already trusts
(Hongkong Post Root CA 3, in this case).

## Files

| File | Subject | Expires | Needed by |
| --- | --- | --- | --- |
| `hongkong-post-ecert-ssl-ca-3-17.pem` | Hongkong Post e-Cert SSL CA 3 - 17 | 2032-06-03 | www.legco.gov.hk |

Fetched from the leaf's own AIA URL, `http://www1.eCert.gov.hk/root/ecert_ssl_ca_3-17.crt`.

## Maintenance

When one of these expires the affected crawlers start failing verification
again. Re-fetch the current intermediate and replace the file:

```bash
echo | openssl s_client -connect www.legco.gov.hk:443 -servername www.legco.gov.hk 2>/dev/null \
  | openssl x509 -noout -issuer -ext authorityInfoAccess
curl -s -o new.crt "<the CA Issuers URI printed above>"
openssl x509 -inform DER -in new.crt -out certs/hongkong-post-ecert-ssl-ca-3-17.pem
```

Add a certificate here only to complete a chain a server should have sent
itself. It is not a place to trust something `certifi` deliberately does not.
