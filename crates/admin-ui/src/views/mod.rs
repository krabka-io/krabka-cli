pub mod acls;
pub mod groups;
pub mod layout;
pub mod log_dirs;
pub mod login;
pub mod overview;
pub mod page;
pub mod quotas;
pub mod topics;
pub mod users;

use dioxus::prelude::*;
use layout::sidebar_links_for;
use page::LoginRouteState;
pub use page::{
    OperatorView, ReadRouteState, RoutePage, render_page, render_page_body_html,
    render_page_body_html_for_operator, render_page_for_operator,
};

use crate::{dto::LogDirRow, permissions::Capabilities, session::CSRF_FIELD_NAME};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    Overview,
    Login,
    Topics,
    Groups,
    Acls,
    Users,
    Quotas,
    LogDirs,
}

impl Route {
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::Overview => "/",
            Self::Login => "/login",
            Self::Topics => "/topics",
            Self::Groups => "/groups",
            Self::Acls => "/acls",
            Self::Users => "/users",
            Self::Quotas => "/quotas",
            Self::LogDirs => "/log-dirs",
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Login => "Login",
            Self::Topics => "Topics",
            Self::Groups => "Groups",
            Self::Acls => "ACLs",
            Self::Users => "Users",
            Self::Quotas => "Quotas",
            Self::LogDirs => "Log Dirs",
        }
    }

    #[must_use]
    pub const fn guard_for_authentication(self, is_authenticated: bool) -> Self {
        if is_authenticated || matches!(self, Self::Login) {
            return self;
        }

        Self::Login
    }

    /// Whether the operator's ACLs permit the page the route names.
    #[must_use]
    pub const fn is_permitted(self, capabilities: Capabilities) -> bool {
        match self {
            Self::Overview | Self::Login => true,
            Self::Topics => capabilities.can_view_topics(),
            Self::Groups => capabilities.can_view_groups(),
            Self::Acls => capabilities.can_view_acls(),
            Self::Users => capabilities.can_view_users(),
            Self::Quotas => capabilities.can_view_quotas(),
            Self::LogDirs => capabilities.can_view_log_dirs(),
        }
    }
}

#[must_use]
pub fn render_route_html(route: Route) -> String {
    render_page(&RoutePage::for_unauthenticated_route(route))
}

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn render_route(route: Route) -> Element {
    render_page_element(&RoutePage::for_unauthenticated_route(route))
}

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn render_page_element(page: &RoutePage) -> Element {
    render_page_element_for_operator(page, &OperatorView::anonymous())
}

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn render_page_element_for_operator(page: &RoutePage, operator: &OperatorView) -> Element {
    match page.clone() {
        RoutePage::Login { state } => render_login_element(state),
        page => render_operations_shell_element(page, operator),
    }
}

/// One input of a rendered mutation form.
#[derive(Clone, PartialEq, Eq)]
struct FormField {
    name: &'static str,
    label: &'static str,
    kind: &'static str,
    value: String,
}

impl FormField {
    fn text(name: &'static str, label: &'static str, value: &str) -> Self {
        Self {
            name,
            label,
            kind: "text",
            value: value.to_string(),
        }
    }

    fn number(name: &'static str, label: &'static str, value: &str) -> Self {
        Self {
            name,
            label,
            kind: "number",
            value: value.to_string(),
        }
    }

    fn password(name: &'static str, label: &'static str) -> Self {
        Self {
            name,
            label,
            kind: "password",
            value: String::new(),
        }
    }

    fn fixed(name: &'static str, value: &str) -> Self {
        Self {
            name,
            label: "",
            kind: "hidden",
            value: value.to_string(),
        }
    }
}

/// Renders one mutation as a plain HTML form.
///
/// The server ships no JavaScript, so a control that mutates the cluster has to
/// be a form that the browser itself submits. The hidden token proves that the
/// signed-in operator asked for the mutation; the server rejects a body without
/// it.
fn mutation_form(action: &str, submit_label: &str, token: &str, fields: Vec<FormField>) -> Element {
    rsx! {
        form { class: "mutation-form", method: "post", action: "{action}",
            h3 { "{submit_label}" }
            input { r#type: "hidden", name: "{CSRF_FIELD_NAME}", value: "{token}" }
            for field in fields {
                if field.kind == "hidden" {
                    input { r#type: "hidden", name: "{field.name}", value: "{field.value}" }
                } else {
                    label {
                        "{field.label} "
                        input {
                            r#type: "{field.kind}",
                            name: "{field.name}",
                            value: "{field.value}",
                        }
                    }
                }
            }
            button { r#type: "submit", "{submit_label}" }
        }
    }
}

/// The forms a page shows, already filtered by what the operator may do.
fn mutation_forms(page: &RoutePage, operator: &OperatorView) -> Vec<Element> {
    let Some(token) = operator.csrf_token() else {
        return Vec::new();
    };
    let capabilities = operator.capabilities();

    match page {
        RoutePage::Topics { .. } => topics_forms(token, capabilities),
        RoutePage::Acls { .. } => acls_forms(token, capabilities),
        RoutePage::Users { .. } => users_forms(token, capabilities),
        RoutePage::Quotas { .. } => quotas_forms(token, capabilities),
        RoutePage::LogDirs { .. } => log_dirs_forms(token, capabilities),
        RoutePage::Overview { .. } | RoutePage::Groups { .. } | RoutePage::Login { .. } => {
            Vec::new()
        }
    }
}

fn topics_forms(token: &str, capabilities: Capabilities) -> Vec<Element> {
    let mut forms = Vec::new();

    if capabilities.can_create_topics() {
        forms.push(mutation_form(
            "/topics/create",
            "Create Topic",
            token,
            vec![
                FormField::text("name", "Name", ""),
                FormField::number("partitions", "Partitions", "1"),
                FormField::number("replicas", "Replicas", "1"),
                FormField::text("configs", "Configs (name=value,name=value)", ""),
            ],
        ));
    }

    if capabilities.can_alter_topics() {
        forms.push(mutation_form(
            "/topics/partitions",
            "Add Partitions",
            token,
            vec![
                FormField::text("topic", "Topic", ""),
                FormField::number("total_count", "Total partitions", "1"),
            ],
        ));
        forms.push(mutation_form(
            "/topics/configs",
            "Alter Topic Configs",
            token,
            vec![
                FormField::fixed("resource_type", "topic"),
                FormField::text("resource_name", "Topic", ""),
                FormField::text("configs", "Configs (name=value,name=value)", ""),
            ],
        ));
    }

    if capabilities.can_delete_topics() {
        forms.push(mutation_form(
            "/topics/delete",
            "Delete Topic",
            token,
            vec![FormField::text("name", "Name", "")],
        ));
    }

    forms
}

fn acl_fields() -> Vec<FormField> {
    vec![
        FormField::text("resource_type", "Resource type", "topic"),
        FormField::text("resource_name", "Resource name", ""),
        FormField::text(
            "pattern_type",
            "Pattern type (literal or prefixed)",
            "literal",
        ),
        FormField::text("principal", "Principal", "User:"),
        FormField::text("host", "Host", "*"),
        FormField::text("operation", "Operation", "read"),
        FormField::text("permission", "Permission", "allow"),
    ]
}

fn acls_forms(token: &str, capabilities: Capabilities) -> Vec<Element> {
    if !capabilities.can_alter_acls() {
        return Vec::new();
    }

    vec![
        mutation_form("/acls/create", "Create ACL", token, acl_fields()),
        mutation_form("/acls/delete", "Delete ACL", token, acl_fields()),
    ]
}

fn users_forms(token: &str, capabilities: Capabilities) -> Vec<Element> {
    if !capabilities.can_alter_users() {
        return Vec::new();
    }

    vec![
        mutation_form(
            "/users/scram/upsert",
            "Upsert SCRAM-SHA-512",
            token,
            vec![
                FormField::text("username", "Username", ""),
                FormField::password("password", "Password"),
                FormField::number("iterations", "Iterations", "4096"),
            ],
        ),
        mutation_form(
            "/users/scram/delete",
            "Delete SCRAM User",
            token,
            vec![FormField::text("username", "Username", "")],
        ),
    ]
}

fn quotas_forms(token: &str, capabilities: Capabilities) -> Vec<Element> {
    if !capabilities.can_alter_quotas() {
        return Vec::new();
    }

    vec![
        mutation_form(
            "/quotas/upsert",
            "Set Quota",
            token,
            vec![
                FormField::text("entity", "User", ""),
                FormField::text("quota_type", "Quota", "producer_byte_rate"),
                FormField::text("value", "Value", ""),
            ],
        ),
        mutation_form(
            "/quotas/delete",
            "Remove Quota",
            token,
            vec![
                FormField::text("entity", "User", ""),
                FormField::text("quota_type", "Quota", "producer_byte_rate"),
            ],
        ),
    ]
}

fn log_dirs_forms(token: &str, capabilities: Capabilities) -> Vec<Element> {
    if !capabilities.can_alter_log_dirs() {
        return Vec::new();
    }

    vec![mutation_form(
        "/log-dirs/move",
        "Move Replica",
        token,
        vec![
            FormField::text("topic", "Topic", ""),
            FormField::number("partition", "Partition", "0"),
            FormField::text("destination_log_dir", "Destination log dir", ""),
        ],
    )]
}

fn render_login_element(state: LoginRouteState) -> Element {
    let message = match state {
        LoginRouteState::Form => None,
        LoginRouteState::AuthenticationFailed => Some("Authentication failed."),
        LoginRouteState::BrokerUnavailable => {
            Some("No broker answered. The cluster may be down; sign in again when it is back.")
        }
    };

    rsx! {
        div {
            section { class: "login-shell",
                h1 { "Sign in to Krabka Admin" }
                p { "Authentication is required before broker operations are shown." }
                if let Some(message) = message {
                    p { class: "login-message", "{message}" }
                } else {
                    form { method: "post", action: "/login",
                        label {
                            "Username "
                            input { name: "username", autocomplete: "username" }
                        }
                        label {
                            "Password "
                            input { name: "password", r#type: "password", autocomplete: "current-password" }
                        }
                        button { r#type: "submit", "Sign in" }
                    }
                }
            }
        }
    }
}

fn render_operations_shell_element(page: RoutePage, operator: &OperatorView) -> Element {
    let forms = mutation_forms(&page, operator);
    let links = sidebar_links_for(operator.capabilities());
    let sign_out_token = operator.csrf_token().map(str::to_string);
    let content = match page {
        RoutePage::Overview { message } => render_overview_element(message),
        RoutePage::Topics { state } => render_topics_element(state),
        RoutePage::Groups { state } => render_groups_element(state),
        RoutePage::Acls { state } => render_acls_element(state),
        RoutePage::Users { state } => render_users_element(state),
        RoutePage::Quotas { state, entity } => render_quotas_element(state, entity),
        RoutePage::LogDirs { state } => render_log_dirs_element(state),
        RoutePage::Login { .. } => unreachable!("login pages render outside the operations shell"),
    }?;

    rsx! {
        div {
            section { class: "operations-shell",
                aside {
                    h1 { "Krabka Operations" }
                    nav {
                        for link in links {
                            a { class: "nav-link", href: "{link.path}", "{link.label}" }
                        }
                    }
                    if let Some(token) = sign_out_token {
                        form { class: "sign-out-form", method: "post", action: "/logout",
                            input { r#type: "hidden", name: "{CSRF_FIELD_NAME}", value: "{token}" }
                            button { r#type: "submit", "Sign out" }
                        }
                    }
                }
                main {
                    {content}
                    for form in forms {
                        {form}
                    }
                }
            }
        }
    }
}

fn render_overview_element(message: Option<&'static str>) -> Element {
    rsx! {
        section {
            h2 { "Cluster Overview" }
            p { "Broker administration shell is ready." }
            if let Some(message) = message {
                p { "{message}" }
            }
        }
    }
}

fn render_topics_element(state: ReadRouteState<crate::dto::TopicRow>) -> Element {
    match state {
        ReadRouteState::Rows(rows) if !rows.is_empty() => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                ul {
                    for row in rows {
                        li { "{row.name}" }
                    }
                }
            }
        },
        state => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                p { "{topics_message(&state)}" }
            }
        },
    }
}

fn topics_message(state: &ReadRouteState<crate::dto::TopicRow>) -> &'static str {
    match state {
        ReadRouteState::Loading => "Loading topics…",
        ReadRouteState::AuthenticationRequired => "Authentication required.",
        ReadRouteState::NotPermitted => "Your ACLs do not permit topic access.",
        ReadRouteState::LoadFailed => "Unable to load topics.",
        ReadRouteState::Rows(_) => "No topics loaded yet.",
    }
}

fn render_groups_element(state: ReadRouteState<crate::dto::GroupRow>) -> Element {
    match state {
        ReadRouteState::Rows(rows) if !rows.is_empty() => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                ul {
                    for row in rows {
                        li { "{row.group_id}" }
                    }
                }
            }
        },
        state => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                p { "{groups_message(&state)}" }
            }
        },
    }
}

fn groups_message(state: &ReadRouteState<crate::dto::GroupRow>) -> &'static str {
    match state {
        ReadRouteState::Loading => "Loading consumer groups…",
        ReadRouteState::AuthenticationRequired => "Authentication required.",
        ReadRouteState::NotPermitted => "Your ACLs do not permit consumer group access.",
        ReadRouteState::LoadFailed => "Unable to load consumer groups.",
        ReadRouteState::Rows(_) => "No consumer groups loaded yet.",
    }
}

fn render_acls_element(state: ReadRouteState<crate::dto::AclRow>) -> Element {
    match state {
        ReadRouteState::Rows(rows) if !rows.is_empty() => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                ul {
                    for row in rows {
                        li {
                            "{row.resource} {row.pattern_type} {row.principal} host={row.host} {row.operation} {row.permission}"
                        }
                    }
                }
            }
        },
        state => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                p { "{acls_message(&state)}" }
            }
        },
    }
}

fn acls_message(state: &ReadRouteState<crate::dto::AclRow>) -> &'static str {
    match state {
        ReadRouteState::Loading => "Loading ACLs…",
        ReadRouteState::AuthenticationRequired => "Authentication required.",
        ReadRouteState::NotPermitted => "Your ACLs do not permit ACL access.",
        ReadRouteState::LoadFailed => "Unable to load ACLs.",
        ReadRouteState::Rows(_) => "No ACLs loaded yet.",
    }
}

fn render_users_element(state: ReadRouteState<crate::dto::UserRow>) -> Element {
    match state {
        ReadRouteState::Rows(rows) if !rows.is_empty() => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                ul {
                    for row in rows {
                        li { "{row.username} {row.principal}" }
                    }
                }
            }
        },
        state => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                p { "{users_message(&state)}" }
            }
        },
    }
}

fn users_message(state: &ReadRouteState<crate::dto::UserRow>) -> &'static str {
    match state {
        ReadRouteState::Loading => "Loading SCRAM users…",
        ReadRouteState::AuthenticationRequired => "Authentication required.",
        ReadRouteState::NotPermitted => "Your ACLs do not permit SCRAM user access.",
        ReadRouteState::LoadFailed => "Unable to load SCRAM users.",
        ReadRouteState::Rows(_) => "No SCRAM users loaded yet.",
    }
}

fn render_quotas_element(
    state: ReadRouteState<crate::dto::QuotaRow>,
    entity: Option<String>,
) -> Element {
    let lookup_value = entity.unwrap_or_default();

    match state {
        ReadRouteState::Rows(rows) if !rows.is_empty() => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                {quota_lookup_form(&lookup_value)}
                ul {
                    for row in rows {
                        li { "{row.entity} {row.quota_type} {row.value}" }
                    }
                }
            }
        },
        state => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                {quota_lookup_form(&lookup_value)}
                p { "{quotas_message(&state)}" }
            }
        },
    }
}

/// The form that asks for another user's quotas.
///
/// It is a `GET`, so it needs no token: it changes nothing.
fn quota_lookup_form(entity: &str) -> Element {
    rsx! {
        form { class: "quota-lookup-form", method: "get", action: "/quotas",
            label {
                "User "
                input { r#type: "text", name: "entity", value: "{entity}" }
            }
            button { r#type: "submit", "Show quotas" }
        }
    }
}

fn quotas_message(state: &ReadRouteState<crate::dto::QuotaRow>) -> &'static str {
    match state {
        ReadRouteState::Loading => "Loading quotas…",
        ReadRouteState::AuthenticationRequired => "Authentication required.",
        ReadRouteState::NotPermitted => "Your ACLs do not permit quota access.",
        ReadRouteState::LoadFailed => "Unable to load quotas.",
        ReadRouteState::Rows(_) => "No quotas loaded yet.",
    }
}

fn render_log_dirs_element(state: ReadRouteState<LogDirRow>) -> Element {
    match state {
        ReadRouteState::Rows(rows) if !rows.is_empty() => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                ul {
                    for row in rows {
                        li { "{log_dir_row_text(&row)}" }
                    }
                }
            }
        },
        state => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                p { "{log_dirs_message(&state)}" }
            }
        },
    }
}

/// One log-dir line.
///
/// A directory the broker could not read carries no partition data, so the row
/// says what went wrong instead of showing the empty topic and the `-1`
/// partition that stand for "no data".
fn log_dir_row_text(row: &LogDirRow) -> String {
    let location = if row.partition < 0 {
        row.log_dir.clone()
    } else {
        format!(
            "{} {}/{}-{}",
            row.log_dir, row.topic, row.partition, row.partition_size
        )
    };

    let Some(error) = &row.error else {
        return location;
    };

    match &error.message {
        Some(message) => format!(
            "{location} error={} ({}): {message}",
            error.name, error.code
        ),
        None => format!("{location} error={} ({})", error.name, error.code),
    }
}

fn log_dirs_message(state: &ReadRouteState<LogDirRow>) -> &'static str {
    match state {
        ReadRouteState::Loading => "Loading log-dir data…",
        ReadRouteState::AuthenticationRequired => "Authentication required.",
        ReadRouteState::NotPermitted => "Your ACLs do not permit log-dir access.",
        ReadRouteState::LoadFailed => "Unable to load log-dir data.",
        ReadRouteState::Rows(_) => "No log-dir data loaded yet.",
    }
}
