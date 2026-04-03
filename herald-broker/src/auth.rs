use std::fmt::Write;
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Generates a random 32-byte hex token using the OS secure RNG.
pub fn generate_token() -> io::Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(hex_encode(&bytes))
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
    let token = generate_token()?;
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
        let token = generate_token().unwrap();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generated_tokens_are_unique() {
        let a = generate_token().unwrap();
        let b = generate_token().unwrap();
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
