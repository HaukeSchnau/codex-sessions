use sha2::{Digest, Sha256};

pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    let digest = Sha256::digest(bytes.as_ref());
    hex::encode(digest)
}

pub fn stable_json_hash(value: &serde_json::Value) -> String {
    sha256_hex(serde_json::to_vec(value).unwrap_or_default())
}
