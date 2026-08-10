import hashlib


def stable_random_key(provider: str, resource_id: str) -> float:
    digest = hashlib.sha256(f"{provider}:{resource_id}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big")
    return value / 2**64
