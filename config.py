import os

CS_HOST = "chatsurfer.nro.mil"
CHATKEY = os.environ.get("CHATKEY", "")
TEST = os.environ.get("TEST_LOCAL", "False")
API_KEY = os.environ.get("API_KEY", "")
GEOFENCE_CONFIG = os.environ.get("GEOFENCE_CONFIG", "")

# Certificate paths - prioritize environment variables if injected via Rancher Secret
if TEST == "True":
    CERT_PATH = os.environ.get(
        "CCTV_CERT_PATH", "/Users/samueltownsend/dev/certs/justcert.pem"
    )
    KEY_PATH = os.environ.get(
        "CCTV_KEY_PATH", "/Users/samueltownsend/dev/certs/decrypted.key"
    )
    CA_BUNDLE_PATH = os.environ.get(
        "CCTV_CA_BUNDLE_PATH", "/Users/samueltownsend/dev/certs/dod_CAs.pem"
    )
else:
    # Use mounted TLS secret by default in production
    CERT_PATH = os.environ.get("CCTV_CERT_PATH", "/certs/tls.crt")
    KEY_PATH = os.environ.get("CCTV_KEY_PATH", "/certs/tls.key")
    CA_BUNDLE_PATH = os.environ.get("CCTV_CA_BUNDLE_PATH", "/ca-certs/ca")


def validate_certificates():
    """Validates certificate files on startup to catch PEM errors early."""
    import os

    # Check CA Bundle
    if os.path.exists(CA_BUNDLE_PATH):
        try:
            with open(CA_BUNDLE_PATH, "r") as f:
                content = f.read()
                if "BEGIN CERTIFICATE" not in content:
                    print(
                        f"CRITICAL WARNING: CA_BUNDLE_PATH ({CA_BUNDLE_PATH}) does not appear to contain a valid PEM certificate."
                    )
        except Exception as e:
            print(f"CRITICAL WARNING: Could not read CA_BUNDLE_PATH: {e}")
    else:
        print(f"CRITICAL WARNING: CA_BUNDLE_PATH ({CA_BUNDLE_PATH}) does not exist.")

    # Check TLS Cert
    if os.path.exists(CERT_PATH):
        try:
            with open(CERT_PATH, "r") as f:
                content = f.read()
                if "BEGIN CERTIFICATE" not in content:
                    print(
                        f"CRITICAL WARNING: CERT_PATH ({CERT_PATH}) does not appear to contain a valid PEM certificate."
                    )
        except Exception as e:
            print(f"CRITICAL WARNING: Could not read CERT_PATH: {e}")
    else:
        print(f"CRITICAL WARNING: CERT_PATH ({CERT_PATH}) does not exist.")

    # Check TLS Key
    if os.path.exists(KEY_PATH):
        try:
            with open(KEY_PATH, "r") as f:
                content = f.read()
                if (
                    "BEGIN PRIVATE KEY" not in content
                    and "BEGIN RSA PRIVATE KEY" not in content
                ):
                    print(
                        f"CRITICAL WARNING: KEY_PATH ({KEY_PATH}) does not appear to contain a valid PEM private key."
                    )
                if "ENCRYPTED" in content:
                    print(
                        f"CRITICAL WARNING: KEY_PATH ({KEY_PATH}) appears to be an encrypted private key! python requests library will throw an SSLError."
                    )
        except Exception as e:
            print(f"CRITICAL WARNING: Could not read KEY_PATH: {e}")
    else:
        print(f"CRITICAL WARNING: KEY_PATH ({KEY_PATH}) does not exist.")


if TEST != "True":
    validate_certificates()
