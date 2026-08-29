//! Opt-in OS autostart ("run at login") registration for the `autostart`
//! subcommand — one platform-specific login-item mechanism per OS, each
//! split into a pure file-writing half (unit-testable) and a best-effort
//! activation half (shells out to the OS service manager).
use std::path::{Path, PathBuf};

use buzz_node::model::NodeError;

/// Install an OS login item that runs `buzz-node up` at login, so the node
/// survives a reboot without the user remembering to start it by hand. Only
/// ever invoked from the explicit `buzz-node autostart` command — `up` and
/// `enroll` never register this as a side effect.
pub(super) fn cmd_autostart() -> Result<(), NodeError> {
    let exe = std::env::current_exe()
        .map_err(|e| NodeError::Config(format!("resolve current exe: {e}")))?;
    let path = install_autostart(&exe)?;
    eprintln!("buzz-node: autostart registered at {}", path.display());
    Ok(())
}

fn install_autostart(exe: &Path) -> Result<PathBuf, NodeError> {
    autostart_impl::install(exe)
}

#[cfg(target_os = "macos")]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    const LABEL: &str = "com.buzz.node";

    fn agents_dir() -> Result<PathBuf, NodeError> {
        let home = dirs::home_dir().ok_or_else(|| NodeError::Config("no home directory".into()))?;
        Ok(home.join("Library").join("LaunchAgents"))
    }

    fn plist_contents(exe: &Path) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
    <key>Label</key><string>{LABEL}</string>\n\
    <key>ProgramArguments</key>\n\
    <array>\n\
        <string>{exe}</string>\n\
        <string>up</string>\n\
    </array>\n\
    <key>RunAtLoad</key><true/>\n\
    <key>KeepAlive</key><false/>\n\
</dict>\n\
</plist>\n",
            exe = exe.display(),
        )
    }

    /// The pure, testable half: write the plist to an arbitrary `dir`.
    pub(super) fn install_to(dir: &Path, exe: &Path) -> Result<PathBuf, NodeError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| NodeError::Config(format!("create LaunchAgents dir: {e}")))?;
        let path = dir.join(format!("{LABEL}.plist"));
        std::fs::write(&path, plist_contents(exe))
            .map_err(|e| NodeError::Config(format!("write launch agent plist: {e}")))?;
        Ok(path)
    }

    pub(super) fn install(exe: &Path) -> Result<PathBuf, NodeError> {
        let path = install_to(&agents_dir()?, exe)?;
        // Best-effort immediate activation. The file alone still guarantees
        // autostart on the NEXT login even if `launchctl` is unavailable
        // (e.g. a sandboxed/headless environment) — that failure is not
        // surfaced as an error for this reason.
        let _ = std::process::Command::new("launchctl")
            .args(["load", "-w"])
            .arg(&path)
            .status();
        Ok(path)
    }
}

#[cfg(target_os = "linux")]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    fn unit_dir() -> Result<PathBuf, NodeError> {
        let home = dirs::home_dir().ok_or_else(|| NodeError::Config("no home directory".into()))?;
        Ok(home.join(".config").join("systemd").join("user"))
    }

    fn unit_contents(exe: &Path) -> String {
        format!(
            "[Unit]\nDescription=Buzz execution node\n\n\
[Service]\nExecStart={} up --foreground\nRestart=on-failure\n\n\
[Install]\nWantedBy=default.target\n",
            exe.display(),
        )
    }

    /// The pure, testable half: write the unit file to an arbitrary `dir`.
    pub(super) fn install_to(dir: &Path, exe: &Path) -> Result<PathBuf, NodeError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| NodeError::Config(format!("create systemd user dir: {e}")))?;
        let path = dir.join("buzz-node.service");
        std::fs::write(&path, unit_contents(exe))
            .map_err(|e| NodeError::Config(format!("write systemd unit: {e}")))?;
        Ok(path)
    }

    pub(super) fn install(exe: &Path) -> Result<PathBuf, NodeError> {
        let path = install_to(&unit_dir()?, exe)?;
        // Best-effort immediate activation; see the macOS arm's comment on
        // why a failure here is not surfaced as an error.
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", "buzz-node.service"])
            .status();
        Ok(path)
    }
}

#[cfg(target_os = "windows")]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    fn startup_dir() -> Result<PathBuf, NodeError> {
        let appdata = std::env::var_os("APPDATA")
            .ok_or_else(|| NodeError::Config("APPDATA is not set".into()))?;
        Ok(PathBuf::from(appdata)
            .join("Microsoft")
            .join("Windows")
            .join("Start Menu")
            .join("Programs")
            .join("Startup"))
    }

    fn script_contents(exe: &Path) -> String {
        format!("@echo off\r\nstart \"\" \"{}\" up\r\n", exe.display())
    }

    /// The pure, testable half: write the startup script to an arbitrary `dir`.
    pub(super) fn install_to(dir: &Path, exe: &Path) -> Result<PathBuf, NodeError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| NodeError::Config(format!("create Startup dir: {e}")))?;
        let path = dir.join("buzz-node.bat");
        std::fs::write(&path, script_contents(exe))
            .map_err(|e| NodeError::Config(format!("write startup script: {e}")))?;
        Ok(path)
    }

    pub(super) fn install(exe: &Path) -> Result<PathBuf, NodeError> {
        // Placing the file in the Startup folder is itself sufficient —
        // nothing further to activate; it takes effect at the next login.
        install_to(&startup_dir()?, exe)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
mod autostart_impl {
    use super::{NodeError, Path, PathBuf};

    pub(super) fn install(_exe: &Path) -> Result<PathBuf, NodeError> {
        Err(NodeError::Config(
            "autostart is not supported on this platform".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn autostart_plist_written_and_references_the_given_exe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let exe = PathBuf::from("/usr/local/bin/buzz-node");
        let path = autostart_impl::install_to(dir.path(), &exe).expect("install_to");
        let contents = std::fs::read_to_string(&path).expect("read plist");
        assert!(contents.contains("/usr/local/bin/buzz-node"));
        assert!(contents.contains("RunAtLoad"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn autostart_unit_written_and_references_the_given_exe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let exe = PathBuf::from("/usr/local/bin/buzz-node");
        let path = autostart_impl::install_to(dir.path(), &exe).expect("install_to");
        let contents = std::fs::read_to_string(&path).expect("read unit");
        assert!(contents.contains("/usr/local/bin/buzz-node"));
        assert!(contents.contains("[Service]"));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn autostart_script_written_and_references_the_given_exe() {
        let dir = tempfile::tempdir().expect("tempdir");
        let exe = PathBuf::from(r"C:\buzz-node.exe");
        let path = autostart_impl::install_to(dir.path(), &exe).expect("install_to");
        let contents = std::fs::read_to_string(&path).expect("read script");
        assert!(contents.contains("buzz-node.exe"));
    }
}
