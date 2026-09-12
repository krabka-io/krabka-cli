use super::Route;
use crate::permissions::Capabilities;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidebarLink {
    pub route: Route,
    pub path: &'static str,
    pub label: &'static str,
}

impl SidebarLink {
    const fn new(route: Route) -> Self {
        Self {
            route,
            path: route.path(),
            label: route.label(),
        }
    }
}

const SIDEBAR_LINKS: &[SidebarLink] = &[
    SidebarLink::new(Route::Overview),
    SidebarLink::new(Route::Topics),
    SidebarLink::new(Route::Groups),
    SidebarLink::new(Route::Acls),
    SidebarLink::new(Route::Users),
    SidebarLink::new(Route::Quotas),
    SidebarLink::new(Route::LogDirs),
];

#[must_use]
pub const fn sidebar_links() -> &'static [SidebarLink] {
    SIDEBAR_LINKS
}

/// The sidebar links the operator's ACLs permit, in the order above.
#[must_use]
pub fn sidebar_links_for(capabilities: Capabilities) -> Vec<SidebarLink> {
    SIDEBAR_LINKS
        .iter()
        .copied()
        .filter(|link| link.route.is_permitted(capabilities))
        .collect()
}
