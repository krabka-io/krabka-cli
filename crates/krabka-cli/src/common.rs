//! Failures that more than one admin command uses.

use crate::output::CommandError;

/// The failure of a sub-feature that needs an `AdminClient` call that the
/// pinned `krabka-client-admin` does not provide.
///
/// The command exits [`crate::exit::Exit::Failure`] and names the call, so
/// the operator knows that the build, not the command line, is the problem.
#[must_use]
pub fn unsupported(feature: &str, call: &str) -> CommandError {
    CommandError::Other(format!(
        "{feature} is not supported by this build: it needs {call}, which the pinned \
         krabka-client-admin does not provide"
    ))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn an_unsupported_feature_names_the_missing_call_and_exits_1() {
        let error = unsupported("cluster-id", "AdminClient::describe_cluster");
        assert!(error.exit() == crate::exit::Exit::Failure);
        assert!(
            error.to_string()
                == "cluster-id is not supported by this build: it needs \
                    AdminClient::describe_cluster, which the pinned krabka-client-admin does \
                    not provide"
        );
    }
}
