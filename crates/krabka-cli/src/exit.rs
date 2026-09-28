//! The process exit codes that `krabka` owns.
//!
//! Every exit of the binary goes through [`Exit::code`], so a script that reads
//! `$?` and a script that reads the JSON error envelope see the same number.
//!
//! Two sources decide a code that this module does not name. A delegated
//! plugin's own exit code reaches the process unchanged, and so does the code
//! of the built-in formatter, which `krabka-format` owns. Both travel as
//! [`Exit::Passthrough`].

/// How the `krabka` process ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// 0: the command did what it was asked to do.
    Success,
    /// 1: the operation failed, or at least one row of a multi-row operation
    /// failed.
    Failure,
    /// 2: the command line is not valid. clap exits with this code for a parse
    /// error. A command uses it only for a refusal that the operator corrects
    /// by changing the command line, such as a missing `--yes`.
    Usage,
    /// 126: a `krabka-<name>` plugin was found on `PATH` but could not be run.
    /// A shell uses the same code for the same case.
    CannotExecute,
    /// 127: no built-in command has the name and no `krabka-<name>` is on
    /// `PATH`. A shell uses the same code for the same case.
    NotFound,
    /// 128 + n: the delegated plugin died from signal n, as a shell reports it.
    Signal(i32),
    /// 130: the operator cancelled the command, with Ctrl-C or by declining a
    /// confirmation prompt.
    Cancelled,
    /// A code that a delegated plugin or the built-in formatter decided.
    Passthrough(i32),
}

impl Exit {
    /// The numeric process exit code.
    #[must_use]
    pub const fn code(self) -> i32 {
        match self {
            Self::Success => 0,
            Self::Failure => 1,
            Self::Usage => 2,
            Self::CannotExecute => 126,
            Self::NotFound => 127,
            Self::Signal(signal) => 128 + signal,
            Self::Cancelled => 130,
            Self::Passthrough(code) => code,
        }
    }

    /// [`Exit::Failure`] when any row failed, else [`Exit::Success`].
    #[must_use]
    pub const fn from_failed(failed: bool) -> Self {
        if failed { Self::Failure } else { Self::Success }
    }

    /// Ends the process with this code.
    pub fn exit(self) -> ! {
        std::process::exit(self.code())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn every_case_has_its_documented_code() {
        let cases = [
            (Exit::Success, 0),
            (Exit::Failure, 1),
            (Exit::Usage, 2),
            (Exit::CannotExecute, 126),
            (Exit::NotFound, 127),
            (Exit::Signal(9), 137),
            (Exit::Signal(15), 143),
            (Exit::Cancelled, 130),
            (Exit::Passthrough(42), 42),
            (Exit::from_failed(false), 0),
            (Exit::from_failed(true), 1),
        ];
        for (exit, code) in cases {
            assert!(exit.code() == code, "{exit:?}");
        }
    }
}
