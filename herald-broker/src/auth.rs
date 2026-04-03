use std::fmt::Write;
use std::fs;
use std::io;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Generates a random 32-byte hex token.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    let read_urandom = fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .is_ok();

    if !read_urandom {
        // Fallback: derive from current time and pid for uniqueness.
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            ^ (std::process::id() as u128);
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = ((seed >> (i % 16 * 8)) & 0xff) as u8 ^ (i as u8).wrapping_mul(37);
        }
    }

    hex_encode(&bytes)
}

/// Loads or creates the token file. Returns the token string.
pub fn load_or_create_token(data_dir: &Path) -> io::Result<String> {
    let token_path = data_dir.join("token");
    if token_path.exists() {
        let token = fs::read_to_string(&token_path)?.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }

    fs::create_dir_all(data_dir)?;
    let token = generate_token();
    fs::write(&token_path, &token)?;

    // Restrict permissions on Unix.
    #[cfg(unix)]
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600))?;

    Ok(token)
}

/// Constant-time comparison to avoid timing attacks.
pub fn verify_token(expected: &str, provided: &str) -> bool {
    if expected.len() != provided.len() {
        return false;
    }
    expected
        .bytes()
        .zip(provided.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_token_is_64_hex_chars() {
        let token = generate_token();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generated_tokens_are_unique() {
        let a = generate_token();
        let b = generate_token();
        assert_ne!(a, b);
    }

    #[test]
    fn verify_token_accepts_matching() {
        let token = "abc123def456";
        assert!(verify_token(token, token));
    }

    #[test]
    fn verify_token_rejects_different() {
        assert!(!verify_token("abc123", "abc124"));
        assert!(!verify_token("abc123", "abc12"));
        assert!(!verify_token("short", "longer-token"));
    }

    #[test]
    fn load_or_create_token_creates_and_reloads() {
        let dir = std::env::temp_dir().join(format!("herald-test-auth-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let token = load_or_create_token(&dir).unwrap();
        assert_eq!(token.len(), 64);

        let reloaded = load_or_create_token(&dir).unwrap();
        assert_eq!(token, reloaded);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
