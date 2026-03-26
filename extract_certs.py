import ssl
import base64
import os
import certifi

ca_certs = ""

# Append default certifi bundle first
with open(certifi.where(), "r", encoding="utf-8") as f:
    ca_certs += f.read()
    ca_certs += "\n"

# Append all Windows OS trust store certificates (including corporate proxies)
for store in ['CA', 'ROOT']:
    try:
        for cert, encoding, trust in ssl.enum_certificates(store):
            if encoding == "x509_asn":
                pem = "-----BEGIN CERTIFICATE-----\n" 
                pem += base64.encodebytes(cert).decode('ascii') 
                pem += "-----END CERTIFICATE-----\n"
                ca_certs += pem
    except Exception as e:
        print(f"Failed extracting {store}: {e}")

output_file = os.path.join(os.path.dirname(__file__), "windows_certs.pem")
with open(output_file, "w", encoding="utf-8") as f:
    f.write(ca_certs)

print(f"Saved {len(ca_certs)} bytes of CA certs to {output_file}")
