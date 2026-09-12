//! Standalone Dioxus administration UI for one Krabka cluster.

use dioxus::dioxus_core::Element;

pub mod admin;
pub mod auth;
pub mod config;
pub mod dto;
pub mod error;
pub mod permissions;
pub mod server;
pub mod server_fns;
pub mod session;
pub mod views;

/// Renders the root route of the UI.
///
/// # Errors
/// Returns a render error when the overview route cannot be built.
pub fn app() -> Element {
    views::overview::overview_view()
}
