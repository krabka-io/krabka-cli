//! HTTP server helpers for the admin UI.

use std::sync::Arc;

use axum::{
    Form, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Query, State},
    http::{HeaderMap, Request, StatusCode, header},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use krabka_units::prelude::*;
use serde::{Deserialize, de::DeserializeOwned};

use crate::{
    auth::{AdminClientLoginBroker, LoginBroker, LoginRequest},
    config::AdminUiConfig,
    dto::{
        AclRequestDto, AlterConfigFormDto, CreatePartitionsRequestDto, CreateTopicFormDto,
        DeleteTopicRequestDto, LogDirMoveRequestDto, MutationForm, QuotaDeleteDto, QuotaUpsertDto,
        ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto,
    },
    error::UiError,
    permissions::Capabilities,
    server_fns::{self, AdminSeamFactory, BrokerAdminSeamFactory, ServerFunctionContext},
    session::{SessionRecord, SessionStore},
    views::{
        OperatorView, ReadRouteState, Route, RoutePage, render_page, render_page_for_operator,
    },
};

pub const SESSION_COOKIE_NAME: &str = "krabka_admin_session";

/// The header a JSON mutation carries its CSRF token in.
///
/// A browser cannot set this header on a cross-origin request without a
/// preflight that this server never answers, and a page on another origin
/// cannot read the token, so only the UI itself can send it.
pub const CSRF_HEADER_NAME: &str = "x-krabka-csrf";

/// The form field a rendered mutation form carries its CSRF token in.
pub const CSRF_FIELD_NAME: &str = "csrf_token";

#[derive(Debug, Clone)]
pub struct AppState {
    pub cfg: Arc<AdminUiConfig>,
    pub sessions: Arc<SessionStore>,
}

#[derive(Clone)]
pub struct AdminRouterState<F, B = AdminClientLoginBroker> {
    app: AppState,
    seam_factory: F,
    login_broker: B,
}

impl AppState {
    #[must_use]
    pub fn new(cfg: AdminUiConfig) -> Self {
        let session_ttl = cfg.session_ttl.to_std();

        Self {
            cfg: Arc::new(cfg),
            sessions: Arc::new(SessionStore::new(session_ttl)),
        }
    }

    #[must_use]
    pub const fn from_parts(cfg: Arc<AdminUiConfig>, sessions: Arc<SessionStore>) -> Self {
        Self { cfg, sessions }
    }

    #[must_use]
    pub fn sessions_ttl_seconds(&self) -> u64 {
        self.sessions.ttl().as_secs()
    }
}

/// The body of a mutation request, and the form it arrived in.
enum MutationBody {
    /// A JSON body, as the UI's own API takes it.
    Json(Bytes),
    /// An `application/x-www-form-urlencoded` body, as a rendered form posts
    /// it. The CSRF field is already removed.
    Form(String),
}

/// What a quota page shows: the named user, or the signed-in operator.
#[derive(Debug, Clone, Deserialize)]
pub struct QuotaLookup {
    pub entity: Option<String>,
}

impl QuotaLookup {
    fn requested_entity(self) -> Option<String> {
        self.entity
            .map(|entity| entity.trim().to_string())
            .filter(|entity| !entity.is_empty())
    }
}

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(healthz))
}

pub fn router(state: AppState) -> Router {
    router_with_factory_and_login_broker(state, BrokerAdminSeamFactory, AdminClientLoginBroker)
}

pub fn router_with_factory<F>(state: AppState, seam_factory: F) -> Router
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
{
    router_with_factory_and_login_broker(state, seam_factory, AdminClientLoginBroker)
}

pub fn router_with_factory_and_login_broker<F, B>(
    state: AppState,
    seam_factory: F,
    login_broker: B,
) -> Router
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let router_state = AdminRouterState {
        app: state,
        seam_factory,
        login_broker,
    };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/", get(root::<F, B>))
        .route("/login", get(login).post(post_login::<F, B>))
        .route("/logout", post(post_logout::<F, B>))
        .route("/topics", get(topics))
        .route("/groups", get(groups))
        .route("/acls", get(acls))
        .route("/users", get(users))
        .route("/quotas", get(quotas))
        .route("/log-dirs", get(log_dirs))
        .route("/topics/create", post(post_create_topic::<F, B>))
        .route("/topics/delete", post(post_delete_topic::<F, B>))
        .route("/topics/partitions", post(post_create_partitions::<F, B>))
        .route("/topics/configs", post(post_alter_configs::<F, B>))
        .route("/acls/create", post(post_create_acl::<F, B>))
        .route("/acls/delete", post(post_delete_acl::<F, B>))
        .route("/users/scram/upsert", post(post_upsert_scram::<F, B>))
        .route("/users/scram/delete", post(post_delete_scram::<F, B>))
        .route("/quotas/upsert", post(post_upsert_quota::<F, B>))
        .route("/quotas/delete", post(post_delete_quota::<F, B>))
        .route("/log-dirs/move", post(post_move_log_dir::<F, B>))
        .with_state(router_state)
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn root<F, B>(State(state): State<AdminRouterState<F, B>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((_, operator)) = authenticated_page(&state, &headers) else {
        return Html(render_page(&RoutePage::for_unauthenticated_route(
            Route::Overview,
        )));
    };

    Html(render_page_for_operator(&RoutePage::overview(), &operator))
}

async fn login() -> Html<String> {
    login_page()
}

async fn post_login<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    Form(request): Form<LoginRequest>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let result = server_fns::login_with_app_state(&state.app, &state.login_broker, request).await;

    let success = match result {
        Ok(success) => success,
        Err(error) => {
            let (status, page) = login_failure_response(&error);

            return (status, Html(render_page(&page))).into_response();
        }
    };

    let cookie = format!(
        "{SESSION_COOKIE_NAME}={}; HttpOnly; SameSite=Lax; Path=/",
        success.session_id
    );

    (
        [(header::SET_COOKIE, cookie)],
        Html(render_page(&RoutePage::signed_in())),
    )
        .into_response()
}

/// Separates a rejected sign-in from a broker that did not answer.
///
/// Monitoring reads the status code, so a cluster outage must not arrive as
/// `401`: an operator who reads "authentication failed" retypes a password that
/// was never the problem.
fn login_failure_response(error: &UiError) -> (StatusCode, RoutePage) {
    match error {
        UiError::NotAuthenticated | UiError::SessionExpired => {
            (StatusCode::UNAUTHORIZED, RoutePage::login_failed())
        }
        UiError::BrokerConnection(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            RoutePage::login_broker_unavailable(),
        ),
        _ => (
            StatusCode::BAD_GATEWAY,
            RoutePage::login_broker_unavailable(),
        ),
    }
}

async fn post_logout<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, _) = match authenticated_mutation_body(&state, any_capability, request).await {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    if server_fns::logout(&context).is_err() {
        return mutation_error_response(StatusCode::UNAUTHORIZED, "not authenticated");
    }

    let expired_cookie =
        format!("{SESSION_COOKIE_NAME}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0");

    (
        StatusCode::OK,
        [(header::SET_COOKIE, expired_cookie)],
        Html(render_page(&RoutePage::login())),
    )
        .into_response()
}

async fn topics<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((context, operator)) = authenticated_page(&state, &headers) else {
        return login_page();
    };

    if !Route::Topics.is_permitted(operator.capabilities()) {
        return read_page(&RoutePage::topics(ReadRouteState::NotPermitted), &operator);
    }

    Html(match server_fns::list_topics_with_context(&context).await {
        Ok(rows) => {
            render_page_for_operator(&RoutePage::topics(ReadRouteState::Rows(rows)), &operator)
        }
        Err(UiError::NotAuthenticated) => render_login_page(),
        Err(_) => {
            render_page_for_operator(&RoutePage::topics(ReadRouteState::LoadFailed), &operator)
        }
    })
}

async fn groups<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((context, operator)) = authenticated_page(&state, &headers) else {
        return login_page();
    };

    if !Route::Groups.is_permitted(operator.capabilities()) {
        return read_page(&RoutePage::groups(ReadRouteState::NotPermitted), &operator);
    }

    Html(match server_fns::list_groups_with_context(&context).await {
        Ok(rows) => {
            render_page_for_operator(&RoutePage::groups(ReadRouteState::Rows(rows)), &operator)
        }
        Err(UiError::NotAuthenticated) => render_login_page(),
        Err(_) => {
            render_page_for_operator(&RoutePage::groups(ReadRouteState::LoadFailed), &operator)
        }
    })
}

async fn acls<F, B>(State(state): State<AdminRouterState<F, B>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((context, operator)) = authenticated_page(&state, &headers) else {
        return login_page();
    };

    if !Route::Acls.is_permitted(operator.capabilities()) {
        return read_page(&RoutePage::acls(ReadRouteState::NotPermitted), &operator);
    }

    Html(match server_fns::list_acls(&context).await {
        Ok(rows) => {
            render_page_for_operator(&RoutePage::acls(ReadRouteState::Rows(rows)), &operator)
        }
        Err(UiError::NotAuthenticated) => render_login_page(),
        Err(_) => render_page_for_operator(&RoutePage::acls(ReadRouteState::LoadFailed), &operator),
    })
}

async fn users<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((context, operator)) = authenticated_page(&state, &headers) else {
        return login_page();
    };

    if !Route::Users.is_permitted(operator.capabilities()) {
        return read_page(&RoutePage::users(ReadRouteState::NotPermitted), &operator);
    }

    Html(match server_fns::list_users(&context).await {
        Ok(rows) => {
            render_page_for_operator(&RoutePage::users(ReadRouteState::Rows(rows)), &operator)
        }
        Err(UiError::NotAuthenticated) => render_login_page(),
        Err(_) => {
            render_page_for_operator(&RoutePage::users(ReadRouteState::LoadFailed), &operator)
        }
    })
}

async fn quotas<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    Query(lookup): Query<QuotaLookup>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((context, operator)) = authenticated_page(&state, &headers) else {
        return login_page();
    };
    let entity = lookup.requested_entity();

    if !Route::Quotas.is_permitted(operator.capabilities()) {
        return read_page(
            &RoutePage::quotas_for_entity(ReadRouteState::NotPermitted, entity),
            &operator,
        );
    }

    Html(
        match server_fns::list_quotas(&context, entity.clone()).await {
            Ok(rows) => render_page_for_operator(
                &RoutePage::quotas_for_entity(ReadRouteState::Rows(rows), entity),
                &operator,
            ),
            Err(UiError::NotAuthenticated) => render_login_page(),
            Err(_) => render_page_for_operator(
                &RoutePage::quotas_for_entity(ReadRouteState::LoadFailed, entity),
                &operator,
            ),
        },
    )
}

async fn log_dirs<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some((context, operator)) = authenticated_page(&state, &headers) else {
        return login_page();
    };

    if !Route::LogDirs.is_permitted(operator.capabilities()) {
        return read_page(
            &RoutePage::log_dirs(ReadRouteState::NotPermitted),
            &operator,
        );
    }

    Html(
        match server_fns::list_log_dirs_with_context(&context).await {
            Ok(rows) => render_page_for_operator(
                &RoutePage::log_dirs(ReadRouteState::Rows(rows)),
                &operator,
            ),
            Err(UiError::NotAuthenticated) => render_login_page(),
            Err(_) => render_page_for_operator(
                &RoutePage::log_dirs(ReadRouteState::LoadFailed),
                &operator,
            ),
        },
    )
}

async fn post_create_topic<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<CreateTopicFormDto, _, _>(
        &state,
        Capabilities::can_create_topics,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::create_topic(&context, request).await)
}

async fn post_delete_topic<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) =
        match parse_authenticated_mutation_request::<DeleteTopicRequestDto, _, _>(
            &state,
            Capabilities::can_delete_topics,
            request,
        )
        .await
        {
            Ok(parsed) => parsed,
            Err(response) => return response,
        };

    mutation_result_response(server_fns::delete_topic(&context, request).await)
}

async fn post_create_partitions<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<
        CreatePartitionsRequestDto,
        _,
        _,
    >(&state, Capabilities::can_alter_topics, request)
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::create_partitions(&context, request).await)
}

async fn post_alter_configs<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<AlterConfigFormDto, _, _>(
        &state,
        Capabilities::can_alter_topics,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::alter_configs(&context, request).await)
}

async fn post_create_acl<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<AclRequestDto, _, _>(
        &state,
        Capabilities::can_alter_acls,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::create_acl(&context, request).await)
}

async fn post_delete_acl<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<AclRequestDto, _, _>(
        &state,
        Capabilities::can_alter_acls,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::delete_acl(&context, request).await)
}

async fn post_upsert_scram<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<ScramUserUpsertDto, _, _>(
        &state,
        Capabilities::can_alter_users,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::upsert_scram_sha512_user(&context, request).await)
}

async fn post_delete_scram<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<ScramUserDeleteDto, _, _>(
        &state,
        Capabilities::can_alter_users,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::delete_scram_user(&context, request).await)
}

async fn post_upsert_quota<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<QuotaUpsertDto, _, _>(
        &state,
        Capabilities::can_alter_quotas,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::upsert_quota(&context, request).await)
}

async fn post_delete_quota<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) = match parse_authenticated_mutation_request::<QuotaDeleteDto, _, _>(
        &state,
        Capabilities::can_alter_quotas,
        request,
    )
    .await
    {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };

    mutation_result_response(server_fns::delete_quota(&context, request).await)
}

async fn post_move_log_dir<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    request: Request<Body>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    for<'a> F::Mutations<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let (context, request) =
        match parse_authenticated_mutation_request::<LogDirMoveRequestDto, _, _>(
            &state,
            Capabilities::can_alter_log_dirs,
            request,
        )
        .await
        {
            Ok(parsed) => parsed,
            Err(response) => return response,
        };

    mutation_result_response(server_fns::move_log_dir(&context, request).await)
}

/// Authenticates a mutation, proves the operator asked for it, and returns the
/// body.
///
/// The session cookie alone does not prove intent: a browser attaches it to a
/// cross-origin form post as well. So a mutation must arrive with an accepted
/// content type and must repeat the session's CSRF token, which only a page
/// this server rendered can know.
async fn authenticated_mutation_body<F, B>(
    state: &AdminRouterState<F, B>,
    required_capability: fn(Capabilities) -> bool,
    request: Request<Body>,
) -> Result<(ServerFunctionContext<'_, F>, MutationBody), axum::response::Response> {
    let Some((context, record)) = authenticated_session(state, request.headers()) else {
        return Err(mutation_error_response(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
        ));
    };

    if !required_capability(record.capabilities) {
        return Err(mutation_error_response(
            StatusCode::FORBIDDEN,
            "not permitted",
        ));
    }

    let content_type = request_content_type(request.headers());
    let header_token = request
        .headers()
        .get(CSRF_HEADER_NAME)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();

    let body = to_bytes(
        request.into_body(),
        state.app.cfg.mutation_json_body_limit.bytes_usize(),
    )
    .await
    .map_err(|_| {
        mutation_error_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
    })?;

    let body = match content_type.as_deref() {
        Some("application/json") => {
            if !record.csrf_token.matches(&header_token) {
                return Err(csrf_error_response());
            }

            MutationBody::Json(body)
        }
        Some("application/x-www-form-urlencoded") => {
            let Ok(fields) = serde_urlencoded::from_bytes::<Vec<(String, String)>>(&body) else {
                return Err(mutation_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid form request",
                ));
            };
            let supplied = fields
                .iter()
                .find(|(name, _)| name == CSRF_FIELD_NAME)
                .map_or("", |(_, value)| value.as_str());

            if !record.csrf_token.matches(supplied) {
                return Err(csrf_error_response());
            }

            let remaining: Vec<(String, String)> = fields
                .into_iter()
                .filter(|(name, _)| name != CSRF_FIELD_NAME)
                .collect();
            let Ok(encoded) = serde_urlencoded::to_string(&remaining) else {
                return Err(mutation_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid form request",
                ));
            };

            MutationBody::Form(encoded)
        }
        _ => {
            return Err(mutation_error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "mutations take application/json or application/x-www-form-urlencoded",
            ));
        }
    };

    Ok((context, body))
}

async fn parse_authenticated_mutation_request<T, F, B>(
    state: &AdminRouterState<F, B>,
    required_capability: fn(Capabilities) -> bool,
    request: Request<Body>,
) -> Result<(ServerFunctionContext<'_, F>, T::Request), axum::response::Response>
where
    T: MutationForm,
{
    let (context, body) = authenticated_mutation_body(state, required_capability, request).await?;

    let request = match body {
        MutationBody::Json(bytes) => parse_json_request::<T::Request>(&bytes).map_err(|_| {
            mutation_error_response(StatusCode::BAD_REQUEST, "invalid JSON request")
        })?,
        MutationBody::Form(encoded) => {
            let form = serde_urlencoded::from_str::<T>(&encoded).map_err(|_| {
                mutation_error_response(StatusCode::BAD_REQUEST, "invalid form request")
            })?;

            form.into_request()
                .map_err(|reason| mutation_error_response(StatusCode::BAD_REQUEST, &reason))?
        }
    };

    Ok((context, request))
}

/// Every signed-in operator may sign out, whatever their ACLs say.
const fn any_capability(_: Capabilities) -> bool {
    true
}

/// The media type of the request, without its parameters.
fn request_content_type(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    let media_type = value.split(';').next()?.trim().to_ascii_lowercase();

    Some(media_type)
}

fn parse_json_request<T: DeserializeOwned>(body: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(body)
}

fn csrf_error_response() -> axum::response::Response {
    mutation_error_response(StatusCode::FORBIDDEN, "missing or stale CSRF token")
}

fn mutation_result_response(
    result: Result<Vec<ResourceOutcome>, UiError>,
) -> axum::response::Response {
    match result {
        Ok(outcomes) => (StatusCode::OK, mutation_outcome_text(&outcomes)).into_response(),
        Err(error) => mutation_error_response(mutation_error_status(&error), &error.to_string()),
    }
}

/// The status that matches what went wrong.
///
/// A request the client can correct is a client error; a broker that did not
/// answer is an upstream failure; only an unclassified admin failure is this
/// server's own.
fn mutation_error_status(error: &UiError) -> StatusCode {
    match error {
        UiError::NotAuthenticated | UiError::SessionExpired => StatusCode::UNAUTHORIZED,
        UiError::NotPermitted => StatusCode::FORBIDDEN,
        UiError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        UiError::BrokerConnection(_) | UiError::Broker { .. } => StatusCode::BAD_GATEWAY,
        UiError::Admin(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn mutation_error_response(status: StatusCode, message: &str) -> axum::response::Response {
    (status, format!("status=error\nmessage={message}\n")).into_response()
}

fn mutation_outcome_text(outcomes: &[ResourceOutcome]) -> String {
    let mut text = String::from("status=ok\n");

    for outcome in outcomes {
        text.push_str("resource=");
        text.push_str(&outcome.resource);
        if let Some(error) = &outcome.error {
            text.push_str("\tstatus=error\terror_code=");
            text.push_str(&error.code.to_string());
            text.push_str("\terror_name=");
            text.push_str(&error.name);
        } else {
            text.push_str("\tstatus=ok");
        }
        text.push('\n');
    }

    text
}

fn login_page() -> Html<String> {
    Html(render_login_page())
}

fn read_page(page: &RoutePage, operator: &OperatorView) -> Html<String> {
    Html(render_page_for_operator(page, operator))
}

fn render_login_page() -> String {
    render_page(&RoutePage::login())
}

/// The session behind the request's cookie, with the context that reaches the
/// broker.
fn authenticated_session<'a, F, B>(
    state: &'a AdminRouterState<F, B>,
    headers: &HeaderMap,
) -> Option<(ServerFunctionContext<'a, F>, SessionRecord)> {
    let raw_session_id = session_cookie(headers)?;
    let record =
        server_fns::session_record_with_store(&state.app.sessions, Some(raw_session_id)).ok()?;

    Some((
        ServerFunctionContext::new(
            &state.app.cfg,
            &state.app.sessions,
            Some(raw_session_id),
            &state.seam_factory,
        ),
        record,
    ))
}

/// The same, with the record turned into what the shell renders from.
fn authenticated_page<'a, F, B>(
    state: &'a AdminRouterState<F, B>,
    headers: &HeaderMap,
) -> Option<(ServerFunctionContext<'a, F>, OperatorView)> {
    let (context, record) = authenticated_session(state, headers)?;
    let operator = OperatorView::new(
        record.capabilities,
        record.csrf_token.expose_for_form().to_string(),
    );

    Some((context, operator))
}

fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;

    cookie_header.split(';').find_map(|cookie| {
        let (name, value) = cookie.trim().split_once('=')?;
        (name == SESSION_COOKIE_NAME).then_some(value)
    })
}
