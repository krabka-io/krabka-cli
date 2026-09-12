use assert2::assert;
use krabka_admin_ui::{
    dto::{KafkaErrorDto, LogDirRow, TopicRow},
    permissions::{Capabilities, derive_capabilities},
    server_fns::AclRow,
    views::{
        OperatorView, ReadRouteState, Route, RoutePage, acls, groups,
        layout::{sidebar_links, sidebar_links_for},
        log_dirs, quotas, render_page, render_page_body_html, render_page_for_operator,
        render_route, render_route_html, topics, users,
    },
};
use krabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};

const TOKEN: &str = "csrf-token-sentinel";

fn operator(capabilities: Capabilities) -> OperatorView {
    OperatorView::new(capabilities, TOKEN.to_string())
}

/// An operator whose only ACL is describe on topics.
fn topic_reader() -> Capabilities {
    derive_capabilities(
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
    )
}

fn ssr_route_body(page: &RoutePage) -> String {
    render_page_body_html(page)
}

fn ssr_full_page(page: &RoutePage) -> String {
    format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title></head><body>{}</body></html>",
        expected_title(page),
        dioxus_ssr::render_element(krabka_admin_ui::views::render_page_element(page))
    )
}

fn expected_title(page: &RoutePage) -> &'static str {
    match page {
        RoutePage::Overview { .. } => "Krabka Admin",
        RoutePage::Login { .. } => "Sign in to Krabka",
        RoutePage::Topics { .. } => "Topics",
        RoutePage::Groups { .. } => "Consumer Groups",
        RoutePage::Acls { .. } => "ACLs",
        RoutePage::Users { .. } => "SCRAM Users",
        RoutePage::Quotas { .. } => "Quotas",
        RoutePage::LogDirs { .. } => "Log Dirs",
    }
}

#[test]
fn route_exposes_first_slice_paths_and_labels() {
    let routes = [
        (Route::Overview, "/", "Overview"),
        (Route::Login, "/login", "Login"),
        (Route::Topics, "/topics", "Topics"),
        (Route::Groups, "/groups", "Groups"),
        (Route::Acls, "/acls", "ACLs"),
        (Route::Users, "/users", "Users"),
        (Route::Quotas, "/quotas", "Quotas"),
        (Route::LogDirs, "/log-dirs", "Log Dirs"),
    ];

    for (route, expected_path, expected_label) in routes {
        assert!(route.path() == expected_path);
        assert!(route.label() == expected_label);
    }
}

#[test]
fn sidebar_links_include_operations_routes_in_order() {
    let links = sidebar_links();

    let labels: Vec<_> = links.iter().map(|link| link.label).collect();
    let paths: Vec<_> = links.iter().map(|link| link.path).collect();

    assert!(
        labels
            == vec![
                "Overview", "Topics", "Groups", "ACLs", "Users", "Quotas", "Log Dirs"
            ]
    );
    assert!(
        paths
            == vec![
                "/",
                "/topics",
                "/groups",
                "/acls",
                "/users",
                "/quotas",
                "/log-dirs"
            ]
    );
}

#[test]
fn login_route_is_outside_operations_sidebar() {
    assert!(Route::Login.path() == "/login");
    assert!(Route::Login.label() == "Login");
    assert!(
        sidebar_links()
            .iter()
            .all(|link| link.route != Route::Login)
    );
}

#[test]
fn unauthenticated_route_guard_selects_login() {
    assert!(Route::Overview.guard_for_authentication(false) == Route::Login);
    assert!(Route::Topics.guard_for_authentication(false) == Route::Login);
    assert!(Route::Login.guard_for_authentication(false) == Route::Login);
    assert!(Route::Overview.guard_for_authentication(true) == Route::Overview);
}

#[test]
fn app_remains_callable() {
    assert!(krabka_admin_ui::app().is_ok());
}

#[test]
fn protected_view_modules_use_shared_unauthenticated_page_by_default() {
    let expected_body = ssr_route_body(&RoutePage::login());

    for view in [
        topics::topics_view,
        groups::groups_view,
        acls::acls_view,
        users::users_view,
        quotas::quotas_view,
        log_dirs::log_dirs_view,
    ] {
        assert!(dioxus_ssr::render_element(view()) == expected_body);
    }
}

#[test]
fn every_route_renders_a_page() {
    let routes = [
        Route::Overview,
        Route::Login,
        Route::Topics,
        Route::Groups,
        Route::Acls,
        Route::Users,
        Route::Quotas,
        Route::LogDirs,
    ];

    for route in routes {
        assert!(
            render_route(route).is_ok(),
            "route {route:?} should render a page"
        );
    }
}

#[test]
fn protected_render_routes_use_shared_unauthenticated_page_by_default() {
    let expected_html = render_page(&RoutePage::login());
    let expected_body = ssr_route_body(&RoutePage::login());

    for route in [
        Route::Overview,
        Route::Topics,
        Route::Groups,
        Route::Acls,
        Route::Users,
        Route::Quotas,
        Route::LogDirs,
    ] {
        let route_html = render_route_html(route);

        assert!(route_html == expected_html, "{route:?} shared HTML");
        assert!(dioxus_ssr::render_element(render_route(route)) == expected_body);
        assert!(route_html.contains("Sign in to Krabka Admin"));
        assert!(!route_html.contains("operations-shell"));
    }
}

#[test]
fn route_element_ssr_embeds_shared_page_body_html() {
    let page = RoutePage::for_unauthenticated_route(Route::Acls);

    assert!(dioxus_ssr::render_element(render_route(Route::Acls)) == ssr_route_body(&page));
}

#[test]
fn shared_page_renderer_renders_dynamic_topics_and_acls() {
    let topics_html = render_page(&RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
        name: "orders<east>".to_string(),
        topic_id: None,
        partition_count: 3,
        replication_factor: 1,
        error: None,
    }])));
    let acls_html = render_page(&RoutePage::acls(ReadRouteState::Rows(vec![AclRow {
        resource: "Topic:orders".to_string(),
        pattern_type: "Literal".to_string(),
        principal: "User:alice".to_string(),
        host: "10.0.0.7".to_string(),
        operation: "Read".to_string(),
        permission: "Allow".to_string(),
    }])));

    assert!(topics_html.contains("admin-section topics-section"));
    assert!(topics_html.contains("orders&#60;east&#62;"));
    assert!(!topics_html.contains("orders<east>"));
    assert!(acls_html.contains("admin-section acls-section"));
    assert!(acls_html.contains("Topic:orders Literal User:alice host=10.0.0.7 Read Allow"));
}

#[test]
fn full_page_renderer_uses_dioxus_ssr_for_dynamic_and_protected_pages() {
    let dynamic_topic_page = RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
        name: "orders<east>".to_string(),
        topic_id: None,
        partition_count: 3,
        replication_factor: 1,
        error: None,
    }]));
    let protected_page = RoutePage::for_unauthenticated_route(Route::Topics);

    for page in [&dynamic_topic_page, &protected_page] {
        let rendered_page = render_page(page);

        assert!(rendered_page == ssr_full_page(page));
        assert!(rendered_page.contains("<body><div>"));
    }
}

#[test]
fn shared_page_renderer_renders_empty_table_states() {
    let cases = [
        (
            RoutePage::topics(ReadRouteState::Rows(Vec::new())),
            "No topics loaded yet.",
        ),
        (
            RoutePage::groups(ReadRouteState::Rows(Vec::new())),
            "No consumer groups loaded yet.",
        ),
        (
            RoutePage::acls(ReadRouteState::Rows(Vec::new())),
            "No ACLs loaded yet.",
        ),
        (
            RoutePage::users(ReadRouteState::Rows(Vec::new())),
            "No SCRAM users loaded yet.",
        ),
        (
            RoutePage::quotas(ReadRouteState::Rows(Vec::new())),
            "No quotas loaded yet.",
        ),
        (
            RoutePage::log_dirs(ReadRouteState::Rows(Vec::new())),
            "No log-dir data loaded yet.",
        ),
    ];

    for (page, empty_message) in cases {
        let rendered = render_page(&page);

        assert!(
            rendered.contains(empty_message),
            "missing empty message {empty_message}"
        );
        assert!(
            !rendered.contains("<ul>"),
            "empty state should not render a list for {empty_message}"
        );
    }
}

#[test]
fn shared_page_renderer_escapes_all_html_metacharacters() {
    let rendered = render_page(&RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
        name: "<&>\"'".to_string(),
        topic_id: None,
        partition_count: 1,
        replication_factor: 1,
        error: None,
    }])));

    assert!(rendered.contains("&#38;"));
    assert!(rendered.contains("&#60;"));
    assert!(rendered.contains("&#62;"));
    assert!(rendered.contains("&#34;"));
    assert!(rendered.contains("&#39;"));
    assert!(!rendered.contains("<&>\"'"));
}

#[test]
fn operations_navigation_renders_links_to_every_permitted_route() {
    let rendered = render_page_for_operator(&RoutePage::overview(), &operator(Capabilities::all()));

    for link in sidebar_links() {
        assert!(
            rendered.contains(&format!("href=\"{}\"", link.path)),
            "{} has no link",
            link.label
        );
    }
}

#[test]
fn navigation_drops_the_routes_the_operator_may_not_see() {
    let capabilities = topic_reader();

    let links = sidebar_links_for(capabilities);
    let rendered = render_page_for_operator(&RoutePage::overview(), &operator(capabilities));

    let paths: Vec<_> = links.iter().map(|link| link.path).collect();
    assert!(paths == vec!["/", "/topics"]);
    assert!(rendered.contains("href=\"/topics\""));
    assert!(!rendered.contains("href=\"/acls\""));
    assert!(!rendered.contains("href=\"/log-dirs\""));
}

#[test]
fn topic_page_renders_a_create_form_that_posts_to_the_create_endpoint() {
    let rendered = render_page_for_operator(
        &RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
            name: "orders".to_string(),
            topic_id: None,
            partition_count: 3,
            replication_factor: 1,
            error: None,
        }])),
        &operator(Capabilities::all()),
    );

    assert!(rendered.contains("action=\"/topics/create\""));
    assert!(rendered.contains("method=\"post\""));
    assert!(rendered.contains("name=\"partitions\""));
    assert!(rendered.contains("name=\"csrf_token\""));
    assert!(rendered.contains(&format!("value=\"{TOKEN}\"")));
}

#[test]
fn every_mutation_endpoint_has_a_form_on_its_page() {
    let pages = [
        (
            RoutePage::topics(ReadRouteState::Rows(Vec::new())),
            vec![
                "/topics/create",
                "/topics/partitions",
                "/topics/configs",
                "/topics/delete",
            ],
        ),
        (
            RoutePage::acls(ReadRouteState::Rows(Vec::new())),
            vec!["/acls/create", "/acls/delete"],
        ),
        (
            RoutePage::users(ReadRouteState::Rows(Vec::new())),
            vec!["/users/scram/upsert", "/users/scram/delete"],
        ),
        (
            RoutePage::quotas(ReadRouteState::Rows(Vec::new())),
            vec!["/quotas/upsert", "/quotas/delete"],
        ),
        (
            RoutePage::log_dirs(ReadRouteState::Rows(Vec::new())),
            vec!["/log-dirs/move"],
        ),
    ];

    for (page, actions) in pages {
        let rendered = render_page_for_operator(&page, &operator(Capabilities::all()));

        for action in actions {
            assert!(
                rendered.contains(&format!("action=\"{action}\"")),
                "{action} has no form"
            );
        }
    }
}

#[test]
fn a_page_without_a_session_renders_no_mutation_form() {
    let rendered = render_page(&RoutePage::topics(ReadRouteState::Rows(Vec::new())));

    assert!(!rendered.contains("action=\"/topics/create\""));
    assert!(!rendered.contains("csrf_token"));
}

#[test]
fn a_topic_reader_sees_no_topic_mutation_form() {
    let rendered = render_page_for_operator(
        &RoutePage::topics(ReadRouteState::Rows(Vec::new())),
        &operator(topic_reader()),
    );

    assert!(!rendered.contains("action=\"/topics/create\""));
    assert!(!rendered.contains("action=\"/topics/delete\""));
}

#[test]
fn the_shell_renders_a_logout_form_only_for_a_signed_in_operator() {
    let signed_in =
        render_page_for_operator(&RoutePage::overview(), &operator(Capabilities::all()));
    let anonymous = render_page(&RoutePage::overview());

    assert!(signed_in.contains("action=\"/logout\""));
    assert!(!anonymous.contains("action=\"/logout\""));
}

#[test]
fn quota_page_renders_the_lookup_form_with_the_requested_entity() {
    let rendered = render_page_for_operator(
        &RoutePage::quotas_for_entity(ReadRouteState::Rows(Vec::new()), Some("bob".to_string())),
        &operator(Capabilities::all()),
    );

    assert!(rendered.contains("action=\"/quotas\""));
    assert!(rendered.contains("method=\"get\""));
    assert!(rendered.contains("name=\"entity\""));
    assert!(rendered.contains("value=\"bob\""));
}

#[test]
fn log_dir_rows_show_the_broker_error_instead_of_sentinel_values() {
    let rendered = render_page(&RoutePage::log_dirs(ReadRouteState::Rows(vec![
        LogDirRow {
            log_dir: "/var/lib/krabka".to_string(),
            topic: String::new(),
            partition: -1,
            partition_size: 0,
            offset_lag: 0,
            is_future_key: false,
            error: Some(KafkaErrorDto {
                code: 57,
                name: "KAFKA_STORAGE_ERROR".to_string(),
                message: Some("disk offline".to_string()),
            }),
        },
    ])));

    assert!(rendered.contains("/var/lib/krabka error=KAFKA_STORAGE_ERROR (57): disk offline"));
    assert!(!rendered.contains("/-1-0"));
}

#[test]
fn a_readable_log_dir_row_still_shows_its_partition() {
    let rendered = render_page(&RoutePage::log_dirs(ReadRouteState::Rows(vec![
        LogDirRow {
            log_dir: "/var/lib/krabka".to_string(),
            topic: "orders".to_string(),
            partition: 0,
            partition_size: 10,
            offset_lag: 0,
            is_future_key: false,
            error: None,
        },
    ])));

    assert!(rendered.contains("/var/lib/krabka orders/0-10"));
}

#[test]
fn a_page_the_operator_may_not_read_says_so() {
    let rendered = render_page_for_operator(
        &RoutePage::topics(ReadRouteState::NotPermitted),
        &operator(topic_reader()),
    );

    assert!(rendered.contains("Your ACLs do not permit topic access."));
}
