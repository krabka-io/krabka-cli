use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::assert;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use krabka_admin_ui::{
    auth::LoginBroker,
    config::AdminUiConfig,
    dto::{
        AclRequestDto, AlterConfigRequestDto, CreatePartitionsRequestDto, CreateTopicRequestDto,
        DeleteTopicRequestDto, GroupRow, LogDirMoveRequestDto, LogDirRow, QuotaDeleteDto,
        QuotaUpsertDto, ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto, TopicRow,
    },
    error::UiError,
    permissions::Capabilities,
    server::{AppState, SESSION_COOKIE_NAME, router, router_with_factory},
    server_fns::{AclRow, AdminMutationSeam, AdminReadSeam, AdminSeamFactory, QuotaRow, UserRow},
    session::{SessionCredentials, SessionId, SessionRecord, SessionStore, SessionUser},
    views::{
        OperatorView, ReadRouteState, Route, RoutePage, render_page, render_page_for_operator,
        render_route_html,
    },
};
use krabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use krabka_units::bytes;
use tower::ServiceExt as _;

fn smoke_app() -> axum::Router {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        ..AdminUiConfig::default()
    };

    router(AppState::new(cfg))
}

async fn get(path: &str) -> axum::response::Response {
    smoke_app()
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds")
}

async fn get_from(
    app: axum::Router,
    path: &str,
    cookie: Option<String>,
) -> axum::response::Response {
    let mut request = Request::builder().uri(path);
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }

    app.oneshot(request.body(Body::empty()).expect("request builds"))
        .await
        .expect("router responds")
}

async fn post_form_from(
    app: axum::Router,
    path: &str,
    form_body: &'static str,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(Method::POST)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(form_body))
            .expect("request builds"),
    )
    .await
    .expect("router responds")
}

async fn post_json_from(
    app: axum::Router,
    path: &str,
    json_body: impl Into<Body>,
    cookie: Option<String>,
) -> axum::response::Response {
    post_mutation(app, path, "application/json", json_body, cookie, None).await
}

async fn post_json_with_csrf(
    app: axum::Router,
    path: &str,
    json_body: impl Into<Body>,
    cookie: Option<String>,
    csrf_token: &str,
) -> axum::response::Response {
    post_mutation(
        app,
        path,
        "application/json",
        json_body,
        cookie,
        Some(csrf_token.to_string()),
    )
    .await
}

async fn post_form_mutation(
    app: axum::Router,
    path: &str,
    form_body: String,
    cookie: Option<String>,
) -> axum::response::Response {
    post_mutation(
        app,
        path,
        "application/x-www-form-urlencoded",
        form_body,
        cookie,
        None,
    )
    .await
}

async fn post_mutation(
    app: axum::Router,
    path: &str,
    content_type: &str,
    body: impl Into<Body>,
    cookie: Option<String>,
    csrf_token: Option<String>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(header::CONTENT_TYPE, content_type);
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    if let Some(csrf_token) = csrf_token {
        request = request.header("x-krabka-csrf", csrf_token);
    }

    app.oneshot(request.body(body.into()).expect("request builds"))
        .await
        .expect("router responds")
}

fn csrf_token(sessions: &SessionStore, session_id: &SessionId) -> String {
    sessions
        .get(session_id)
        .expect("session exists")
        .csrf_token
        .expose_for_form()
        .to_string()
}

fn operator_for(sessions: &SessionStore, session_id: &SessionId) -> OperatorView {
    let record = sessions.get(session_id).expect("session exists");

    OperatorView::new(
        record.capabilities,
        record.csrf_token.expose_for_form().to_string(),
    )
}

/// A session whose only ACL is describe on topics.
fn topic_reader_session(sessions: &SessionStore) -> SessionId {
    sessions.create_authenticated(
        SessionUser {
            username: "alice".to_string(),
            principal: "User:alice".to_string(),
        },
        SessionCredentials::scram_sha512("password".to_string()),
        krabka_admin_ui::permissions::derive_capabilities(
            "User:alice",
            &[AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: "*".to_string(),
                pattern_type: PatternType::Literal,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Describe,
                permission_type: PermissionType::Allow,
            }],
        ),
    )
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("body can be collected");

    String::from_utf8(bytes.to_vec()).expect("HTML body is UTF-8")
}

fn sample_topic_row() -> TopicRow {
    TopicRow {
        name: "orders".to_string(),
        topic_id: None,
        partition_count: 3,
        replication_factor: 1,
        error: None,
    }
}

fn sample_acl_row() -> AclRow {
    AclRow {
        resource: "Topic:orders".to_string(),
        pattern_type: "Literal".to_string(),
        principal: "User:alice".to_string(),
        host: "*".to_string(),
        operation: "Read".to_string(),
        permission: "Allow".to_string(),
    }
}

#[tokio::test]
async fn healthz_returns_ok() {
    let response = get("/healthz").await;

    assert!(response.status() == StatusCode::OK);
}

#[tokio::test]
async fn root_without_cookie_renders_login_instead_of_operations_shell() {
    let response = get("/").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        response.headers().get(header::CONTENT_TYPE)
            == Some(&header::HeaderValue::from_static(
                "text/html; charset=utf-8"
            ))
    );

    let body = response_text(response).await;
    assert!(body.contains("<!doctype html>"));
    assert!(body.contains("Sign in to Krabka Admin"));
    assert!(!body.contains("operations-shell"));
    assert!(!body.contains("Krabka Operations"));
}

#[tokio::test]
async fn root_with_valid_cookie_renders_overview_shell() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let operator = operator_for(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    let response = get_from(app, "/", cookie).await;

    assert!(response.status() == StatusCode::OK);
    let body = response_text(response).await;
    assert!(body == render_page_for_operator(&RoutePage::overview(), &operator));
    assert!(factory.read_seam_calls.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn login_returns_html_with_login_prompt() {
    let response = get("/login").await;

    assert!(response.status() == StatusCode::OK);
    assert!(
        response.headers().get(header::CONTENT_TYPE)
            == Some(&header::HeaderValue::from_static(
                "text/html; charset=utf-8"
            ))
    );

    let body = response_text(response).await;
    assert!(body.contains("Sign in to Krabka"));
    assert!(body.contains("method=\"post\""));
    assert!(body.contains("name=\"username\""));
    assert!(body.contains("name=\"password\""));
}

#[tokio::test]
async fn posting_login_sets_session_cookie_and_cookie_authenticates_protected_route() {
    let state = AppState::new(AdminUiConfig::default());
    let factory = RecordingAdminSeamFactory::default();
    let login_broker = RecordingLoginBroker::default();
    let app = krabka_admin_ui::server::router_with_factory_and_login_broker(
        state,
        factory.clone(),
        login_broker.clone(),
    );
    let password_sentinel = "login-route-password-sentinel";

    let login_response = post_form_from(
        app.clone(),
        "/login",
        "username=alice&password=login-route-password-sentinel",
    )
    .await;

    assert!(login_response.status() == StatusCode::OK);
    let set_cookie = login_response
        .headers()
        .get(header::SET_COOKIE)
        .expect("login sets a session cookie")
        .to_str()
        .expect("cookie is ASCII")
        .to_string();
    assert!(set_cookie.starts_with(&format!("{SESSION_COOKIE_NAME}=")));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("SameSite=Lax"));
    assert!(set_cookie.contains("Path=/"));
    assert!(login_broker.calls.load(Ordering::SeqCst) == 1);

    let login_body = response_text(login_response).await;
    assert!(!login_body.contains(password_sentinel));

    let protected_response = get_from(app, "/topics", Some(set_cookie)).await;

    assert!(protected_response.status() == StatusCode::OK);
    let protected_body = response_text(protected_response).await;
    assert!(protected_body.contains("orders"));
}

#[tokio::test]
async fn authenticated_post_mutation_routes_call_admin_mutation_seam() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let token = csrf_token(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    for (path, body, expected_resource) in [
        (
            "/topics/create",
            r#"{"name":"orders","partitions":3,"replicas":1,"configs":[]}"#,
            "orders",
        ),
        ("/topics/delete", r#"{"name":"orders"}"#, "orders"),
        (
            "/topics/partitions",
            r#"{"topic":"orders","total_count":6}"#,
            "orders",
        ),
        (
            "/topics/configs",
            r#"{"resource_type":"topic","resource_name":"orders","configs":[{"name":"cleanup.policy","value":"compact"}]}"#,
            "orders",
        ),
        (
            "/acls/create",
            r#"{"resource_type":"topic","resource_name":"orders","pattern_type":"literal","principal":"User:alice","operation":"Read","permission":"Allow","host":"*"}"#,
            "User:alice",
        ),
        (
            "/acls/delete",
            r#"{"resource_type":"topic","resource_name":"orders","pattern_type":"prefixed","principal":"User:alice","operation":"Read","permission":"Allow","host":"*"}"#,
            "User:alice",
        ),
        (
            "/users/scram/upsert",
            r#"{"username":"alice","password":"secret","iterations":4096}"#,
            "alice",
        ),
        ("/users/scram/delete", r#"{"username":"alice"}"#, "alice"),
        (
            "/quotas/upsert",
            r#"{"entity":"user=alice","quota_type":"producer_byte_rate","value":1024.0}"#,
            "user=alice",
        ),
        (
            "/quotas/delete",
            r#"{"entity":"user=alice","quota_type":"producer_byte_rate"}"#,
            "user=alice",
        ),
        (
            "/log-dirs/move",
            r#"{"topic":"orders","partition":0,"destination_log_dir":"/var/lib/krabka-1"}"#,
            "orders",
        ),
    ] {
        let response =
            post_json_with_csrf(app.clone(), path, body, Some(cookie.clone()), &token).await;

        assert!(response.status() == StatusCode::OK, "{path} should succeed");
        let text = response_text(response).await;
        assert!(text.contains("status=ok"), "{path} returned {text}");
        assert!(text.contains(expected_resource), "{path} returned {text}");
    }

    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 11);
    assert!(factory.create_topic.load(Ordering::SeqCst) == 1);
    assert!(factory.delete_topic.load(Ordering::SeqCst) == 1);
    assert!(factory.create_partitions.load(Ordering::SeqCst) == 1);
    assert!(factory.alter_configs.load(Ordering::SeqCst) == 1);
    assert!(factory.create_acl.load(Ordering::SeqCst) == 1);
    assert!(factory.delete_acl.load(Ordering::SeqCst) == 1);
    assert!(factory.upsert_scram.load(Ordering::SeqCst) == 1);
    assert!(factory.delete_scram.load(Ordering::SeqCst) == 1);
    assert!(factory.upsert_quota.load(Ordering::SeqCst) == 1);
    assert!(factory.delete_quota.load(Ordering::SeqCst) == 1);
    assert!(factory.move_log_dir.load(Ordering::SeqCst) == 1);
}

#[tokio::test]
async fn post_mutation_routes_authenticate_before_decoding_request_body() {
    let state = AppState::new(AdminUiConfig::default());
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());

    let response = post_json_from(app, "/topics/create", "not-json", None).await;

    assert!(response.status() == StatusCode::UNAUTHORIZED);
    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
    assert!(factory.total_mutation_calls() == 0);
    let text = response_text(response).await;
    assert!(text.contains("not authenticated"));
}

#[tokio::test]
async fn post_mutation_routes_reject_stale_cookie_before_decoding_request_body() {
    let state = AppState::new(AdminUiConfig::default());
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}=stale-session");

    let response = post_json_from(app, "/topics/create", "not-json", Some(cookie)).await;

    assert!(response.status() == StatusCode::UNAUTHORIZED);
    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
    assert!(factory.total_mutation_calls() == 0);
    let text = response_text(response).await;
    assert!(text.contains("not authenticated"));
}

#[tokio::test]
async fn post_mutation_routes_return_bad_request_for_authenticated_malformed_json() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let token = csrf_token(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let response =
        post_json_with_csrf(app, "/topics/create", "not-json", Some(cookie), &token).await;

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
    assert!(factory.total_mutation_calls() == 0);
    let text = response_text(response).await;
    assert!(text.contains("invalid JSON request"));
}

#[tokio::test]
async fn authenticated_mutation_routes_share_the_configured_body_limit() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(
        Arc::new(AdminUiConfig {
            mutation_json_body_limit: bytes(16),
            ..AdminUiConfig::default()
        }),
        sessions,
    );
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    for path in [
        "/topics/create",
        "/topics/delete",
        "/topics/partitions",
        "/topics/configs",
        "/acls/create",
        "/acls/delete",
        "/users/scram/upsert",
        "/users/scram/delete",
        "/quotas/upsert",
        "/quotas/delete",
        "/log-dirs/move",
    ] {
        let response =
            post_json_from(app.clone(), path, "x".repeat(17), Some(cookie.clone())).await;

        assert!(response.status() == StatusCode::PAYLOAD_TOO_LARGE, "{path}");
        let text = response_text(response).await;
        assert!(text.contains("request body too large"), "{path}: {text}");
    }

    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
    assert!(factory.total_mutation_calls() == 0);
}

#[tokio::test]
async fn protected_http_routes_without_cookie_render_guarded_login_page() {
    for (path, route) in [
        ("/topics", Route::Topics),
        ("/groups", Route::Groups),
        ("/acls", Route::Acls),
        ("/users", Route::Users),
        ("/quotas", Route::Quotas),
        ("/log-dirs", Route::LogDirs),
    ] {
        let response = get(path).await;

        assert!(response.status() == StatusCode::OK, "{path} status");
        let body = response_text(response).await;
        assert!(
            body == render_route_html(route),
            "{path} guarded route HTML"
        );
        assert!(
            body == render_page(&RoutePage::login()),
            "{path} login HTML"
        );
        assert!(
            !body.contains("operations-shell"),
            "{path} operations shell"
        );
        assert!(
            !body.contains("Authentication required."),
            "{path} auth shell copy"
        );
    }
}

#[tokio::test]
async fn authenticated_read_routes_call_injected_seams_and_render_rows() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    for (path, expected) in [
        ("/topics", "orders"),
        ("/groups", "consumer-a"),
        ("/acls", "User:alice"),
        ("/users", "scram-alice"),
        ("/quotas", "producer_byte_rate"),
        ("/log-dirs", "/var/lib/krabka"),
    ] {
        let response = get_from(app.clone(), path, cookie.clone()).await;

        assert!(response.status() == StatusCode::OK, "{path} status");
        let body = response_text(response).await;
        assert!(body.contains(expected), "{path} row content");
    }

    assert!(factory.topics.load(Ordering::SeqCst) == 1);
    assert!(factory.groups.load(Ordering::SeqCst) == 1);
    assert!(factory.acls.load(Ordering::SeqCst) == 1);
    assert!(factory.users.load(Ordering::SeqCst) == 1);
    assert!(factory.quotas.load(Ordering::SeqCst) == 1);
    assert!(factory.log_dirs.load(Ordering::SeqCst) == 1);
}

#[tokio::test]
async fn dynamic_read_routes_match_shared_page_renderer() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let operator = operator_for(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory);
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    let cases = [
        (
            "/topics",
            render_page_for_operator(
                &RoutePage::topics(ReadRouteState::Rows(vec![sample_topic_row()])),
                &operator,
            ),
        ),
        (
            "/groups",
            render_page_for_operator(
                &RoutePage::groups(ReadRouteState::Rows(vec![GroupRow {
                    group_id: "consumer-a".to_string(),
                }])),
                &operator,
            ),
        ),
        (
            "/acls",
            render_page_for_operator(
                &RoutePage::acls(ReadRouteState::Rows(vec![sample_acl_row()])),
                &operator,
            ),
        ),
        (
            "/users",
            render_page_for_operator(
                &RoutePage::users(ReadRouteState::Rows(vec![UserRow {
                    username: "scram-alice".to_string(),
                    principal: "User:scram-alice".to_string(),
                }])),
                &operator,
            ),
        ),
        (
            "/quotas",
            render_page_for_operator(
                &RoutePage::quotas(ReadRouteState::Rows(vec![QuotaRow {
                    entity: "User:alice".to_string(),
                    quota_type: "producer_byte_rate".to_string(),
                    value: "1024".to_string(),
                }])),
                &operator,
            ),
        ),
        (
            "/log-dirs",
            render_page_for_operator(
                &RoutePage::log_dirs(ReadRouteState::Rows(vec![LogDirRow {
                    log_dir: "/var/lib/krabka".to_string(),
                    topic: "orders".to_string(),
                    partition: 0,
                    partition_size: 10,
                    offset_lag: 0,
                    is_future_key: false,
                    error: None,
                }])),
                &operator,
            ),
        ),
    ];

    for (path, expected_body) in cases {
        let body = response_text(get_from(app.clone(), path, cookie.clone()).await).await;

        assert!(body == expected_body, "{path} shared renderer output");
    }
}

#[tokio::test]
async fn missing_or_invalid_cookie_does_not_call_injected_seams() {
    for cookie in [None, Some(format!("{SESSION_COOKIE_NAME}=not-a-session"))] {
        let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
        let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
        let factory = RecordingAdminSeamFactory::default();
        let app = router_with_factory(state, factory.clone());

        for path in [
            "/topics",
            "/groups",
            "/log-dirs",
            "/acls",
            "/users",
            "/quotas",
        ] {
            let response = get_from(app.clone(), path, cookie.clone()).await;

            assert!(response.status() == StatusCode::OK, "{path} status");
            let body = response_text(response).await;
            assert!(
                body == render_page(&RoutePage::login()),
                "{path} login page"
            );
        }

        assert!(factory.read_seam_calls.load(Ordering::SeqCst) == 0);
        assert!(factory.topics.load(Ordering::SeqCst) == 0);
        assert!(factory.groups.load(Ordering::SeqCst) == 0);
        assert!(factory.acls.load(Ordering::SeqCst) == 0);
        assert!(factory.users.load(Ordering::SeqCst) == 0);
        assert!(factory.quotas.load(Ordering::SeqCst) == 0);
        assert!(factory.log_dirs.load(Ordering::SeqCst) == 0);
    }
}

#[derive(Clone, Default)]
struct RecordingLoginBroker {
    calls: Arc<AtomicUsize>,
}

impl LoginBroker for RecordingLoginBroker {
    fn authenticate<'a>(
        &'a self,
        _cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Capabilities, UiError>> + Send + 'a>> {
        Box::pin(async move {
            assert!(username == "alice");
            assert!(password == "login-route-password-sentinel");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Capabilities::all())
        })
    }
}

/// A broker that reports the failure it is built with.
#[derive(Clone)]
struct FailingLoginBroker {
    error: UiError,
}

impl LoginBroker for FailingLoginBroker {
    fn authenticate<'a>(
        &'a self,
        _cfg: &'a AdminUiConfig,
        _username: &'a str,
        _password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Capabilities, UiError>> + Send + 'a>> {
        let error = self.error.clone();

        Box::pin(async move { Err(error) })
    }
}

#[derive(Clone, Default)]
struct RecordingAdminSeamFactory {
    read_seam_calls: Arc<AtomicUsize>,
    topics: Arc<AtomicUsize>,
    groups: Arc<AtomicUsize>,
    acls: Arc<AtomicUsize>,
    users: Arc<AtomicUsize>,
    quotas: Arc<AtomicUsize>,
    log_dirs: Arc<AtomicUsize>,
    mutation_seam_calls: Arc<AtomicUsize>,
    create_topic: Arc<AtomicUsize>,
    delete_topic: Arc<AtomicUsize>,
    create_partitions: Arc<AtomicUsize>,
    alter_configs: Arc<AtomicUsize>,
    create_acl: Arc<AtomicUsize>,
    delete_acl: Arc<AtomicUsize>,
    upsert_scram: Arc<AtomicUsize>,
    delete_scram: Arc<AtomicUsize>,
    upsert_quota: Arc<AtomicUsize>,
    delete_quota: Arc<AtomicUsize>,
    move_log_dir: Arc<AtomicUsize>,
}

impl RecordingAdminSeamFactory {
    fn total_mutation_calls(&self) -> usize {
        self.create_topic.load(Ordering::SeqCst)
            + self.delete_topic.load(Ordering::SeqCst)
            + self.create_partitions.load(Ordering::SeqCst)
            + self.alter_configs.load(Ordering::SeqCst)
            + self.create_acl.load(Ordering::SeqCst)
            + self.delete_acl.load(Ordering::SeqCst)
            + self.upsert_scram.load(Ordering::SeqCst)
            + self.delete_scram.load(Ordering::SeqCst)
            + self.upsert_quota.load(Ordering::SeqCst)
            + self.delete_quota.load(Ordering::SeqCst)
            + self.move_log_dir.load(Ordering::SeqCst)
    }
}

impl AdminSeamFactory for RecordingAdminSeamFactory {
    type Reader<'a> = Self;
    type Mutations<'a> = Self;

    fn read_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Reader<'a>, UiError> {
        assert!(record.user.username == "alice");
        self.read_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.clone())
    }

    fn mutation_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Mutations<'a>, UiError> {
        assert!(record.user.username == "alice");
        self.mutation_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.clone())
    }
}

impl AdminReadSeam for RecordingAdminSeamFactory {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.topics.fetch_add(1, Ordering::SeqCst);
            Ok(vec![sample_topic_row()])
        })
    }

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.groups.fetch_add(1, Ordering::SeqCst);
            Ok(vec![GroupRow {
                group_id: "consumer-a".to_string(),
            }])
        })
    }

    fn acls<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AclRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.acls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![sample_acl_row()])
        })
    }

    fn users<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<UserRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.users.fetch_add(1, Ordering::SeqCst);
            Ok(vec![UserRow {
                username: "scram-alice".to_string(),
                principal: "User:scram-alice".to_string(),
            }])
        })
    }

    fn quotas<'a>(
        &'a self,
        entity: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QuotaRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.quotas.fetch_add(1, Ordering::SeqCst);
            Ok(vec![QuotaRow {
                entity: entity.unwrap_or_else(|| "User:alice".to_string()),
                quota_type: "producer_byte_rate".to_string(),
                value: "1024".to_string(),
            }])
        })
    }

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.log_dirs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![LogDirRow {
                log_dir: "/var/lib/krabka".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                partition_size: 10,
                offset_lag: 0,
                is_future_key: false,
                error: None,
            }])
        })
    }
}

impl AdminMutationSeam for RecordingAdminSeamFactory {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_topic.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.name)])
        })
    }

    fn delete_topic<'a>(
        &'a self,
        request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_topic.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.name)])
        })
    }

    fn create_partitions<'a>(
        &'a self,
        request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_partitions.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.topic)])
        })
    }

    fn alter_configs<'a>(
        &'a self,
        request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.alter_configs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.resource_name)])
        })
    }

    fn create_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_acl.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.principal)])
        })
    }

    fn delete_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_acl.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.principal)])
        })
    }

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert_scram.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.username)])
        })
    }

    fn delete_scram_user<'a>(
        &'a self,
        request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_scram.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.username)])
        })
    }

    fn upsert_quota<'a>(
        &'a self,
        request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert_quota.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.entity)])
        })
    }

    fn delete_quota<'a>(
        &'a self,
        request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_quota.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.entity)])
        })
    }

    fn move_log_dir<'a>(
        &'a self,
        request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.move_log_dir.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.topic)])
        })
    }
}

#[tokio::test]
async fn a_json_mutation_without_the_csrf_token_is_refused() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let response = post_json_from(
        app,
        "/topics/create",
        r#"{"name":"orders","partitions":3,"replicas":1,"configs":[]}"#,
        Some(cookie),
    )
    .await;

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert!(factory.total_mutation_calls() == 0);
    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn a_json_mutation_with_another_sessions_csrf_token_is_refused() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let other_id = sessions.create_user("alice", "User:alice");
    let other_token = csrf_token(&sessions, &other_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let response = post_json_with_csrf(
        app,
        "/topics/create",
        r#"{"name":"orders","partitions":3,"replicas":1,"configs":[]}"#,
        Some(cookie),
        &other_token,
    )
    .await;

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert!(factory.total_mutation_calls() == 0);
}

#[tokio::test]
async fn a_cross_site_simple_post_cannot_reach_a_mutation() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    // A form on another origin can send text/plain with the session cookie
    // attached, and it can send no CSRF token. Both doors are shut.
    for content_type in ["text/plain", "multipart/form-data"] {
        let response = post_mutation(
            app.clone(),
            "/topics/delete",
            content_type,
            r#"{"name":"orders"}"#,
            Some(cookie.clone()),
            None,
        )
        .await;

        assert!(
            response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{content_type} was accepted"
        );
    }

    let form_without_token = post_form_mutation(
        app,
        "/topics/delete",
        "name=orders".to_string(),
        Some(cookie),
    )
    .await;

    assert!(form_without_token.status() == StatusCode::FORBIDDEN);
    assert!(factory.total_mutation_calls() == 0);
    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn a_rendered_form_post_reaches_the_mutation_seam() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let token = csrf_token(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    for (path, body, expected_resource) in [
        (
            "/topics/create",
            format!(
                "csrf_token={token}&name=orders&partitions=3&replicas=1&configs=cleanup.policy%3Dcompact"
            ),
            "orders",
        ),
        (
            "/topics/configs",
            format!(
                "csrf_token={token}&resource_type=topic&resource_name=orders&configs=retention.ms%3D60000"
            ),
            "orders",
        ),
        (
            "/acls/delete",
            format!(
                "csrf_token={token}&resource_type=topic&resource_name=orders&pattern_type=prefixed&principal=User%3Aalice&host=*&operation=read&permission=allow"
            ),
            "User:alice",
        ),
        (
            "/users/scram/upsert",
            format!("csrf_token={token}&username=bob&password=secret&iterations=4096"),
            "bob",
        ),
        (
            "/quotas/upsert",
            format!("csrf_token={token}&entity=bob&quota_type=producer_byte_rate&value=1024"),
            "bob",
        ),
        (
            "/log-dirs/move",
            format!(
                "csrf_token={token}&topic=orders&partition=0&destination_log_dir=%2Fvar%2Flib%2Fkrabka-1"
            ),
            "orders",
        ),
    ] {
        let response = post_form_mutation(app.clone(), path, body, Some(cookie.clone())).await;

        assert!(response.status() == StatusCode::OK, "{path} status");
        let text = response_text(response).await;
        assert!(text.contains("status=ok"), "{path} returned {text}");
        assert!(text.contains(expected_resource), "{path} returned {text}");
    }

    assert!(factory.create_topic.load(Ordering::SeqCst) == 1);
    assert!(factory.alter_configs.load(Ordering::SeqCst) == 1);
    assert!(factory.delete_acl.load(Ordering::SeqCst) == 1);
    assert!(factory.upsert_scram.load(Ordering::SeqCst) == 1);
    assert!(factory.upsert_quota.load(Ordering::SeqCst) == 1);
    assert!(factory.move_log_dir.load(Ordering::SeqCst) == 1);
}

#[tokio::test]
async fn a_semantic_validation_failure_is_a_client_error() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let token = csrf_token(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    for body in [
        r#"{"name":"","partitions":3,"replicas":1,"configs":[]}"#,
        r#"{"name":"orders","partitions":-1,"replicas":1,"configs":[]}"#,
    ] {
        let response = post_json_with_csrf(
            app.clone(),
            "/topics/create",
            body,
            Some(cookie.clone()),
            &token,
        )
        .await;

        assert!(response.status() == StatusCode::BAD_REQUEST, "{body}");
    }

    assert!(factory.total_mutation_calls() == 0);
}

#[tokio::test]
async fn logout_removes_the_session_and_expires_the_cookie() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let token = csrf_token(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions.clone());
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory);
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let response = post_form_mutation(
        app.clone(),
        "/logout",
        format!("csrf_token={token}"),
        Some(cookie.clone()),
    )
    .await;

    assert!(response.status() == StatusCode::OK);
    let set_cookie = response
        .headers()
        .get(header::SET_COOKIE)
        .expect("logout clears the cookie")
        .to_str()
        .expect("cookie is ASCII")
        .to_string();
    assert!(set_cookie.contains("Max-Age=0"));
    assert!(sessions.get(&session_id).is_none());

    let after = get_from(app, "/topics", Some(cookie)).await;
    let body = response_text(after).await;
    assert!(body == render_page(&RoutePage::login()));
}

#[tokio::test]
async fn logout_without_the_csrf_token_keeps_the_session() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions.clone());
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory);
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let response =
        post_form_mutation(app, "/logout", "csrf_token=wrong".to_string(), Some(cookie)).await;

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert!(sessions.get(&session_id).is_some());
}

#[tokio::test]
async fn a_restricted_operator_reaches_neither_the_page_nor_the_mutation() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = topic_reader_session(&sessions);
    let token = csrf_token(&sessions, &session_id);
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let page = get_from(app.clone(), "/acls", Some(cookie.clone())).await;
    let body = response_text(page).await;

    assert!(body.contains("Your ACLs do not permit ACL access."));
    assert!(!body.contains("href=\"/acls\""));
    assert!(factory.acls.load(Ordering::SeqCst) == 0);

    let mutation = post_json_with_csrf(
        app,
        "/acls/create",
        r#"{"resource_type":"topic","resource_name":"orders","pattern_type":"literal","principal":"User:alice","operation":"Read","permission":"Allow","host":"*"}"#,
        Some(cookie),
        &token,
    )
    .await;

    assert!(mutation.status() == StatusCode::FORBIDDEN);
    assert!(factory.total_mutation_calls() == 0);
    assert!(factory.mutation_seam_calls.load(Ordering::SeqCst) == 0);
}

#[tokio::test]
async fn the_quota_page_reads_the_entity_the_operator_asked_for() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    let body = response_text(get_from(app, "/quotas?entity=bob", cookie).await).await;

    assert!(body.contains("bob producer_byte_rate 1024"));
    assert!(factory.quotas.load(Ordering::SeqCst) == 1);
}

#[tokio::test]
async fn a_broker_outage_at_login_is_not_reported_as_a_bad_password() {
    for (error, expected_status) in [
        (
            UiError::BrokerConnection("no bootstrap address was reachable: tried 1".to_string()),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (UiError::NotAuthenticated, StatusCode::UNAUTHORIZED),
    ] {
        let state = AppState::new(AdminUiConfig::default());
        let app = krabka_admin_ui::server::router_with_factory_and_login_broker(
            state,
            RecordingAdminSeamFactory::default(),
            FailingLoginBroker { error },
        );

        let response = post_form_from(app, "/login", "username=alice&password=secret").await;

        assert!(response.status() == expected_status);
    }
}
