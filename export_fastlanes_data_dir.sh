#!/usr/bin/env bash
# install_fastlanes_env.sh
# ─────────────────────────────────────────────────────────────
# Sets FASTLANES_DATA_DIR for:
#   • this shell
#   • every future shell (bash/zsh)
#   • all GUI apps (via LaunchAgent) – e.g. CLion
# ─────────────────────────────────────────────────────────────

set -euo pipefail

# 0) Hard-coded canonical path to your FastLanes_Data clone
DATA_DIR="$HOME/CLionProjects/FastLanes_Data"
if [[ ! -d "$DATA_DIR" ]]; then
  echo "❌  Expected FastLanes_Data at: $DATA_DIR" >&2
  echo "    Clone or move the repo there, then rerun this installer." >&2
  exit 1
fi

# 1) Export for **this** shell so you can build immediately
export FASTLANES_DATA_DIR="$DATA_DIR"
echo "FASTLANES_DATA_DIR set for current shell → $FASTLANES_DATA_DIR"

# 2) Install a silent helper into ~/.local/bin and source it from rc files
HELPER_DIR="$HOME/.local/bin"
HELPER_PATH="$HELPER_DIR/export_fastlanes_data_dir.sh"
mkdir -p "$HELPER_DIR"

cat >"$HELPER_PATH" <<'EOSH'
#!/usr/bin/env bash
# silent helper – just export the var if not already
FASTLANES_DATA_DIR_DEFAULT="$HOME/CLionProjects/FastLanes_Data"
export FASTLANES_DATA_DIR="${FASTLANES_DATA_DIR:-$FASTLANES_DATA_DIR_DEFAULT}"
EOSH
chmod +x "$HELPER_PATH"

add_source_line() {
  local rcfile="$1"
  local marker="# >>> FastLanes_Data export >>>"
  local line="source \"$HELPER_PATH\" >/dev/null 2>&1"
  if ! grep -Fq "$marker" "$rcfile" 2>/dev/null; then
    printf "\n%s\n%s\n# <<< FastLanes_Data export <<<\n" "$marker" "$line" >>"$rcfile"
    echo "✓ Added helper source to $rcfile"
  fi
}

# Pick the correct rc file
case "${SHELL##*/}" in
  zsh)  RC_FILE="$HOME/.zshrc" ;;
  bash) RC_FILE="${HOME}/.bash_profile"; [[ -f "$HOME/.bashrc" ]] && RC_FILE="$HOME/.bashrc" ;;
  *)    RC_FILE="$HOME/.profile" ;;
esac
add_source_line "$RC_FILE"

# 3) Create a LaunchAgent so GUI apps inherit the var at login
PLIST="$HOME/Library/LaunchAgents/com.fastlanes.setenv.plist"
cat >"$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>            <string>com.fastlanes.setenv</string>
  <key>ProgramArguments</key> <array>
                                <string>launchctl</string>
                                <string>setenv</string>
                                <string>FASTLANES_DATA_DIR</string>
                                <string>$DATA_DIR</string>
                              </array>
  <key>RunAtLoad</key>        <true/>
</dict>
</plist>
EOF

# Load (or reload) the agent right away
launchctl unload "$PLIST" 2>/dev/null || true
launchctl load  "$PLIST"
echo "✓ LaunchAgent installed & loaded (GUI apps will now see FASTLANES_DATA_DIR)"

echo -e "\n✅  Done.  Log out and back in once (or reboot) so CLion started from the Dock inherits the variable."
