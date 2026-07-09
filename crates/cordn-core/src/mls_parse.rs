//! Minimal MLS key-package parser for admission checks at publish time.
//!
//! The coordinator core stays MLS-free; this module is used by the **adapter**
//! (`cordn-server`) to read the credential identity (the caller's stable pubkey)
//! and detect the last-resort marker, then hand both to
//! [`crate::coordinator::Coordinator::publish_key_package`]. It is intentionally
//! a one-way walker over the exact wire layout that `ts-mls` produces — it never
//! re-encodes and does not validate signatures (the publication event is
//! signature-bound upstream).
//!
//! Layout reference (every length prefix is `ts-mls`'s variable-length integer:
//! the top 2 bits of the first byte give the field size — 1, 2, or 4 bytes):
//!
//! ```text
//! KeyPackage  = version(u16) cipher_suite(u16) init_key(var)
//!               LeafNode kp_extensions(var-list) signature(var)
//! LeafNode    = hpke_pub(var) sig_pub(var)
//!               cred_type(u16) identity(var) capabilities(5×var-list)
//!               source(u8=1) lifetime(16B) ln_extensions(var-list) signature(var)
//! Extension   = type(u16) data(var)
//! last-resort = Extension{ type=0x0006,
//!               data = var( u16(0x0004) var(empty) ) }
//! ```
//!
//! Cross-checked against captured `ts-mls` fixtures in `tests/mls_parse.rs`.

use thiserror::Error;

/// The app-data-dictionary custom extension type that carries the last-resort
/// marker.
pub const APP_DATA_DICTIONARY_EXTENSION_TYPE: u16 = 0x0006;
/// Component id within the app-data dictionary that marks a last-resort key
/// package.
pub const LAST_RESORT_KEY_PACKAGE_COMPONENT_ID: u16 = 0x0004;

#[derive(Debug, Error)]
pub enum MlsParseError {
    #[error("unexpected end of key package")]
    UnexpectedEof,
    #[error("unsupported MLS protocol version: {0}")]
    UnsupportedVersion(u16),
    #[error("only BasicCredential key packages are supported (got credential type {0})")]
    UnsupportedCredential(u16),
    #[error("expected a key-package leaf node source (got {0})")]
    UnexpectedLeafNodeSource(u8),
    #[error("8-byte length prefixes are not supported by ts-mls")]
    UnsupportedLengthPrefix,
    #[error("invalid last-resort app-data dictionary")]
    InvalidAppDataDictionary,
    #[error("key package credential identity is not valid UTF-8")]
    InvalidIdentity,
    #[error("trailing data after key package")]
    TrailingData,
}

/// The fields a publish-time admission check needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedKeyPackage {
    /// BasicCredential identity decoded as UTF-8 (the caller's stable pubkey).
    pub credential_identity: String,
    pub cipher_suite: u16,
    pub is_last_resort: bool,
}

struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }
    fn u8(&mut self) -> Result<u8, MlsParseError> {
        let v = *self.b.get(self.i).ok_or(MlsParseError::UnexpectedEof)?;
        self.i += 1;
        Ok(v)
    }
    fn u16(&mut self) -> Result<u16, MlsParseError> {
        let v = self
            .b
            .get(self.i..self.i + 2)
            .ok_or(MlsParseError::UnexpectedEof)?;
        self.i += 2;
        Ok(u16::from_be_bytes([v[0], v[1]]))
    }
    /// Read a `ts-mls` variable-length prefix and return the following data,
    /// advancing past both prefix and data.
    fn var_bytes(&mut self) -> Result<&'a [u8], MlsParseError> {
        let (data, next) = read_var_bytes(self.b, self.i)?;
        self.i = next;
        Ok(data)
    }
    fn skip(&mut self, n: usize) -> Result<(), MlsParseError> {
        if self.i + n > self.b.len() {
            return Err(MlsParseError::UnexpectedEof);
        }
        self.i += n;
        Ok(())
    }
    fn remaining(&self) -> &'a [u8] {
        &self.b[self.i..]
    }
}

/// Decode a `ts-mls` variable-length field at `offset`. Returns the data slice
/// and the absolute offset just past the field (i.e. `offset + prefix + data`).
/// Ports `determineLength` + `varLenDataDecoder` from `ts-mls`, whose
/// `decodeVarBytes` likewise returns the new offset rather than a length.
fn read_var_bytes(buf: &[u8], offset: usize) -> Result<(&[u8], usize), MlsParseError> {
    let first = *buf.get(offset).ok_or(MlsParseError::UnexpectedEof)?;
    let prefix = first >> 6;
    // ts-mls throws on the 0b11 (8-byte) prefix; match that.
    if prefix == 0b11 {
        return Err(MlsParseError::UnsupportedLengthPrefix);
    }
    let length_field_size = 1usize << prefix;
    if offset + length_field_size > buf.len() {
        return Err(MlsParseError::UnexpectedEof);
    }
    let mut length = (first & 0x3f) as usize;
    for k in 1..length_field_size {
        length = (length << 8) | buf[offset + k] as usize;
    }
    let start = offset + length_field_size;
    let end = start + length;
    if end > buf.len() {
        return Err(MlsParseError::UnexpectedEof);
    }
    Ok((&buf[start..end], end))
}

/// Parse a key package for the two facts the adapter needs at publish time:
/// the credential identity and whether it is a last-resort key package.
///
/// This validates structure (version, leaf-node source, credential type) the
/// same way the TS `keyPackageDecoder` + `readStablePubkeyFromCredential` do,
/// so malformed packages are rejected before they reach storage.
pub fn parse_key_package(bytes: &[u8]) -> Result<ParsedKeyPackage, MlsParseError> {
    let mut c = Cursor::new(bytes);

    let version = c.u16()?;
    if version != 1 {
        return Err(MlsParseError::UnsupportedVersion(version));
    }
    let cipher_suite = c.u16()?;
    let _init_key = c.var_bytes()?;

    // ── leaf node ──
    let _hpke_pub = c.var_bytes()?;
    let _sig_pub = c.var_bytes()?;
    let cred_type = c.u16()?;
    if cred_type != 1 {
        return Err(MlsParseError::UnsupportedCredential(cred_type));
    }
    let identity_bytes = c.var_bytes()?;
    let credential_identity = std::str::from_utf8(identity_bytes)
        .map_err(|_| MlsParseError::InvalidIdentity)?
        .to_owned();
    // capabilities: five variable-length lists (versions, ciphersuites,
    // extensions, proposals, credentials) — skip each whole list.
    for _ in 0..5 {
        let _ = c.var_bytes()?;
    }
    let source = c.u8()?;
    if source != 1 {
        return Err(MlsParseError::UnexpectedLeafNodeSource(source));
    }
    c.skip(16)?; // lifetime: not_before(u64) + not_after(u64)
    let _ln_extensions = c.var_bytes()?;
    let _ln_signature = c.var_bytes()?;

    // ── key-package extensions (last-resort lives here) ──
    let kp_extensions = c.var_bytes()?;
    let is_last_resort = detect_last_resort(kp_extensions)?;

    // ── trailing key-package signature ──
    let _signature = c.var_bytes()?;
    if !c.remaining().is_empty() {
        return Err(MlsParseError::TrailingData);
    }

    Ok(ParsedKeyPackage {
        credential_identity,
        cipher_suite,
        is_last_resort,
    })
}

/// Walk the key-package extensions looking for the last-resort marker. Ports
/// `isLastResortKeyPackageExtension` from `references/cordn/src/lastResortKeyPackage.ts`.
fn detect_last_resort(extensions: &[u8]) -> Result<bool, MlsParseError> {
    let mut i = 0;
    while i < extensions.len() {
        let ext_type = u16::from_be_bytes([
            *extensions.get(i).ok_or(MlsParseError::UnexpectedEof)?,
            *extensions.get(i + 1).ok_or(MlsParseError::UnexpectedEof)?,
        ]);
        i += 2;
        let (ext_data, next) = read_var_bytes(extensions, i)?;
        i = next;
        if ext_type == APP_DATA_DICTIONARY_EXTENSION_TYPE && is_last_resort_extension(ext_data)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Inspect one app-data-dictionary extension payload for the last-resort
/// component (id 0x0004 with empty data). Errors on trailing garbage, matching
/// the TS reference.
fn is_last_resort_extension(extension_data: &[u8]) -> Result<bool, MlsParseError> {
    let (dictionary, end) = read_var_bytes(extension_data, 0)?;
    if end != extension_data.len() {
        return Err(MlsParseError::InvalidAppDataDictionary);
    }
    let mut offset = 0;
    while offset < dictionary.len() {
        let component_id = u16::from_be_bytes([
            *dictionary.get(offset).ok_or(MlsParseError::UnexpectedEof)?,
            *dictionary
                .get(offset + 1)
                .ok_or(MlsParseError::UnexpectedEof)?,
        ]);
        offset += 2;
        let (component_data, next) = read_var_bytes(dictionary, offset)?;
        offset = next;
        if component_id == LAST_RESORT_KEY_PACKAGE_COMPONENT_ID && component_data.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_bytes_one_byte_prefix() {
        // length 5 → 0x05, then 5 data bytes
        let buf = [0x05, 1, 2, 3, 4, 5];
        let (data, consumed) = read_var_bytes(&buf, 0).unwrap();
        assert_eq!(data, &[1, 2, 3, 4, 5]);
        assert_eq!(consumed, 6);
    }

    #[test]
    fn var_bytes_two_byte_prefix() {
        // length 64 → 0x40 0x40, then 64 data bytes
        let mut buf = vec![0x40, 0x40];
        buf.extend(std::iter::repeat_n(0xaa, 64));
        let (data, consumed) = read_var_bytes(&buf, 0).unwrap();
        assert_eq!(data.len(), 64);
        assert_eq!(consumed, 66);
    }

    #[test]
    fn var_bytes_rejects_eight_byte_prefix() {
        let buf = [0xff, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(matches!(
            read_var_bytes(&buf, 0),
            Err(MlsParseError::UnsupportedLengthPrefix)
        ));
    }
}
