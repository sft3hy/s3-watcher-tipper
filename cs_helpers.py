import requests
import time
import random
from config import *


def send_public_message(
    message_text: str,
    roomName: str,
    classification: str = "UNCLASSIFIED//FOUO",
    domainId: str = "chatsurferxmppunclass",
    nickName: str = "",
):
    headers = {
        "Content-type": "application/json",
    }
    message = {
        "classification": classification,
        "message": message_text,
        "domainId": domainId,
        "nickName": nickName,
        "roomName": roomName,
    }

    url = f"https://{CS_HOST}/api/chatserver/message?api-key={CHATKEY}"

    max_attempts = 3
    for attempt in range(max_attempts):
        send = requests.post(
            url,
            cert=(CERT_PATH, KEY_PATH),
            verify=CA_BUNDLE_PATH,
            headers=headers,
            json=message,
        )
        if send.status_code == 429 and attempt < max_attempts - 1:
            sleep_time = random.uniform(1.0, 3.0) * (2**attempt)
            print(
                f"ChatSurfer rate limit reached (429). Retrying in {sleep_time:.2f} seconds... (Attempt {attempt + 1}/{max_attempts})"
            )
            time.sleep(sleep_time)
            continue
        break

    print(f"Response from ChatSurfer send public message: {send}")
    if send.status_code >= 400:
        print(f"Error details: {send.text}")
