use std::{
    collections::HashMap,
    fmt,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::permissions::Capabilities;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    #[must_use]
    pub fn expose_for_cookie(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionId(<redacted>)")
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&str> for SessionId {
    type Error = uuid::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed_uuid = Uuid::parse_str(value)?;

        Ok(Self(parsed_uuid.to_string()))
    }
}

/// The per-session secret that a mutation request must carry.
///
/// The session cookie alone does not prove that the operator asked for the
/// mutation: a browser sends the cookie with a cross-origin form post too. Each
/// rendered mutation form carries this token in a hidden field, and each JSON
/// mutation carries it in the `x-krabka-csrf` header. A page on another origin
/// cannot read the token, so it cannot forge the request.
#[derive(Clone, PartialEq, Eq)]
pub struct CsrfToken(String);

impl CsrfToken {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// The token value to write into a rendered form or to compare a header
    /// against.
    #[must_use]
    pub fn expose_for_form(&self) -> &str {
        &self.0
    }

    /// Compares the token with a request-supplied value in constant time for
    /// the value length, so a wrong token leaks no position information.
    #[must_use]
    pub fn matches(&self, candidate: &str) -> bool {
        let expected = self.0.as_bytes();
        let supplied = candidate.as_bytes();

        if expected.len() != supplied.len() {
            return false;
        }

        expected
            .iter()
            .zip(supplied)
            .fold(0_u8, |difference, (expected, supplied)| {
                difference | (expected ^ supplied)
            })
            == 0
    }
}

impl Default for CsrfToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CsrfToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CsrfToken(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUser {
    pub username: String,
    pub principal: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SessionCredentials {
    password: String,
}

impl SessionCredentials {
    #[must_use]
    pub fn scram_sha512(password: String) -> Self {
        Self { password }
    }

    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for SessionCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionCredentials(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub user: SessionUser,
    pub credentials: Option<SessionCredentials>,
    /// What the operator's ACLs permit, derived once at login.
    pub capabilities: Capabilities,
    pub csrf_token: CsrfToken,
    pub expires_at: Instant,
}

impl SessionRecord {
    #[must_use]
    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

pub struct SessionStore {
    ttl: Duration,
    sessions: RwLock<HashMap<SessionId, SessionRecord>>,
}

impl fmt::Debug for SessionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionStore")
            .field("ttl", &self.ttl)
            .field("session_count", &self.sessions.read().len())
            .finish()
    }
}

impl SessionStore {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    #[must_use]
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Creates a session that holds no broker credentials.
    ///
    /// Such a session cannot open a broker seam, so it reaches no broker data.
    /// It carries every capability: no ACL read stands behind it, and the
    /// broker refuses any operation it would reach anyway.
    pub fn create(&self, user: SessionUser) -> SessionId {
        self.create_record(user, None, Capabilities::all())
    }

    pub fn create_user(&self, username: &str, principal: &str) -> SessionId {
        self.create(SessionUser {
            username: username.to_string(),
            principal: principal.to_string(),
        })
    }

    pub fn create_authenticated(
        &self,
        user: SessionUser,
        credentials: SessionCredentials,
        capabilities: Capabilities,
    ) -> SessionId {
        self.create_record(user, Some(credentials), capabilities)
    }

    fn create_record(
        &self,
        user: SessionUser,
        credentials: Option<SessionCredentials>,
        capabilities: Capabilities,
    ) -> SessionId {
        let session_id = SessionId::new();
        let now = Instant::now();
        let session_record = SessionRecord {
            user,
            credentials,
            capabilities,
            csrf_token: CsrfToken::new(),
            expires_at: now.checked_add(self.ttl).unwrap_or(now),
        };

        let mut sessions = self.sessions.write();
        // An operator who signs in again leaves the older record behind. Drop
        // every expired record here so that no abandoned session holds its
        // password past the TTL.
        sessions.retain(|_, record| !record.is_expired(now));
        sessions.insert(session_id.clone(), session_record);

        session_id
    }

    #[must_use]
    pub fn get(&self, id: &SessionId) -> Option<SessionRecord> {
        let now = Instant::now();
        let record = self.sessions.read().get(id).cloned()?;

        if !record.is_expired(now) {
            return Some(record);
        }

        self.sessions.write().remove(id);
        None
    }

    /// The number of records the store holds, expired ones included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.read().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remove(&self, id: &SessionId) -> bool {
        self.sessions.write().remove(id).is_some()
    }
}
