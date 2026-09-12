use std::time::Duration;

use assert2::assert;
use krabka_admin_ui::{
    permissions::Capabilities,
    session::{SessionCredentials, SessionId, SessionStore, SessionUser},
};

#[test]
fn session_store_creates_and_retrieves_user() {
    let store = SessionStore::new(Duration::from_mins(1));

    let id = store.create(SessionUser {
        username: "alice".to_string(),
        principal: "User:alice".to_string(),
    });

    let record = store.get(&id).expect("session exists");
    assert!(record.user.username == "alice");
    assert!(record.user.principal == "User:alice");
}

#[test]
fn logout_removes_session() {
    let store = SessionStore::new(Duration::from_mins(1));
    let id = store.create(SessionUser {
        username: "bob".to_string(),
        principal: "User:bob".to_string(),
    });

    assert!(store.remove(&id));
    assert!(store.get(&id).is_none());
}

#[test]
fn expired_session_returns_none_without_panicking() {
    let store = SessionStore::new(Duration::ZERO);
    let id = store.create(SessionUser {
        username: "carol".to_string(),
        principal: "User:carol".to_string(),
    });

    assert!(store.get(&id).is_none());
}

#[test]
fn session_id_try_from_accepts_cookie_uuid_and_rejects_invalid_value() {
    let id = SessionId::new();
    let cookie_value = id.expose_for_cookie();

    let parsed = SessionId::try_from(cookie_value).expect("uuid cookie value is valid");

    assert!(parsed == id);
    assert!(SessionId::try_from("not-a-uuid").is_err());
}

#[test]
fn session_id_debug_redacts_cookie_value() {
    let id = SessionId::new();

    let debug_output = format!("{id:?}");

    assert!(!debug_output.contains(id.expose_for_cookie()));
}

#[test]
fn oversized_ttl_session_creation_does_not_panic() {
    let result = std::panic::catch_unwind(|| {
        let store = SessionStore::new(Duration::MAX);
        store.create(SessionUser {
            username: "dave".to_string(),
            principal: "User:dave".to_string(),
        })
    });

    assert!(result.is_ok());
}

#[test]
fn session_store_debug_does_not_include_session_storage() {
    let store = SessionStore::new(Duration::from_mins(1));
    store.create(SessionUser {
        username: "erin".to_string(),
        principal: "User:erin".to_string(),
    });

    let debug_output = format!("{store:?}");

    assert!(!debug_output.contains("sessions"));
    assert!(!debug_output.contains("erin"));
}

fn user(username: &str) -> SessionUser {
    SessionUser {
        username: username.to_string(),
        principal: format!("User:{username}"),
    }
}

#[test]
fn creating_a_session_drops_the_expired_records_of_earlier_logins() {
    let store = SessionStore::new(Duration::ZERO);

    let first = store.create_authenticated(
        user("alice"),
        SessionCredentials::scram_sha512("first-password".to_string()),
        Capabilities::all(),
    );
    let second = store.create_authenticated(
        user("alice"),
        SessionCredentials::scram_sha512("second-password".to_string()),
        Capabilities::all(),
    );

    // The first record held a password past its TTL until the second login
    // dropped it.
    assert!(store.len() == 1);
    assert!(store.get(&first).is_none());
    assert!(store.get(&second).is_none());
}

#[test]
fn creating_a_session_keeps_the_records_that_are_still_live() {
    let store = SessionStore::new(Duration::from_mins(1));

    let first = store.create(user("alice"));
    let second = store.create(user("bob"));

    assert!(store.len() == 2);
    assert!(store.get(&first).is_some());
    assert!(store.get(&second).is_some());
}

#[test]
fn each_session_carries_its_own_csrf_token() {
    let store = SessionStore::new(Duration::from_mins(1));
    let first = store.create(user("alice"));
    let second = store.create(user("bob"));

    let first_record = store.get(&first).expect("first session exists");
    let second_record = store.get(&second).expect("second session exists");
    let first_token = first_record.csrf_token.expose_for_form().to_string();

    assert!(first_record.csrf_token.matches(&first_token));
    assert!(!second_record.csrf_token.matches(&first_token));
    assert!(!first_record.csrf_token.matches(""));
    assert!(!first_record.csrf_token.matches(&format!("{first_token}x")));
}

#[test]
fn csrf_token_debug_redacts_its_value() {
    let store = SessionStore::new(Duration::from_mins(1));
    let id = store.create(user("alice"));
    let record = store.get(&id).expect("session exists");

    let debug_output = format!("{:?}", record.csrf_token);

    assert!(!debug_output.contains(record.csrf_token.expose_for_form()));
}

#[test]
fn an_authenticated_session_keeps_the_capabilities_it_was_created_with() {
    let store = SessionStore::new(Duration::from_mins(1));

    let id = store.create_authenticated(
        user("alice"),
        SessionCredentials::scram_sha512("password".to_string()),
        Capabilities::none(),
    );

    let record = store.get(&id).expect("session exists");
    assert!(record.capabilities == Capabilities::none());
}
