#!/usr/bin/env python3
"""Synchronize non-sensitive environment variables from .env.prod to Helm values.yaml."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

SENSITIVE_KEYWORDS = ("PASSWORD", "SECRET", "TOKEN", "KEY")
REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PROD_PATH = REPO_ROOT / ".env.prod"
HELM_VALUES_PATH = REPO_ROOT / "deployments" / "helm" / "rag-telemetry-audit-daemon" / "values.yaml"


def configure_utf8_stdio() -> None:
    """Ensure UTF-8 stdout/stderr on Windows consoles."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(keyword in upper for keyword in SENSITIVE_KEYWORDS)


def parse_env_file(path: Path) -> Dict[str, str]:
    """Parse KEY=VALUE pairs from a .env file, excluding sensitive keys."""
    parsed: Dict[str, str] = {}
    for line_num, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            raise ValueError(f"{path}:{line_num}: expected KEY=VALUE assignment")
        key, raw_val = line.split("=", 1)
        key = key.strip()
        val = raw_val.strip()

        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        else:
            # Strip inline comments preceded by whitespace for unquoted values.
            val = re.sub(r"\s+#.*$", "", val).strip()

        if is_sensitive_key(key):
            continue
        parsed[key] = val
    return parsed


def load_helm_env(values_path: Path) -> Tuple[Dict[str, str], str | None]:
    """Load current env map from Helm values.yaml, returning (env_without_secretName, secretName)."""
    data = yaml.safe_load(values_path.read_text(encoding="utf-8")) or {}
    raw_env = data.get("env") or {}
    secret_name = raw_env.get("secretName")
    env_map: Dict[str, str] = {}
    for k, v in raw_env.items():
        if k == "secretName" or v is None:
            continue
        env_map[str(k)] = str(v)
    return env_map, (str(secret_name) if secret_name is not None else None)


def render_env_block(env_map: Dict[str, str], secret_name: str | None) -> str:
    """Render the `env:` YAML block with double-quoted string values."""
    lines: List[str] = ["env:"]
    if secret_name is not None:
        escaped_secret = secret_name.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'  secretName: "{escaped_secret}"')
    for key, val in env_map.items():
        escaped_val = val.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'  {key}: "{escaped_val}"')
    return "\n".join(lines) + "\n"


def update_values_yaml(values_path: Path, env_map: Dict[str, str], secret_name: str | None) -> None:
    """Replace only the top-level `env:` block in values.yaml, preserving all other blocks."""
    original = values_path.read_text(encoding="utf-8")
    new_block = render_env_block(env_map, secret_name)

    pattern = re.compile(r"^env:\s*\r?\n(?:(?:[ \t]+.*|\s*)\r?\n)*", re.MULTILINE)
    if pattern.search(original):
        updated = pattern.sub(lambda _: new_block, original, count=1)
    else:
        sep = "" if original.endswith("\n") else "\n"
        updated = original + sep + "\n" + new_block

    values_path.write_text(updated, encoding="utf-8", newline="\n")


def compute_drift(expected: Dict[str, str], actual: Dict[str, str]) -> List[str]:
    """Return human-readable drift descriptions between .env.prod and values.yaml."""
    diffs: List[str] = []
    for key in sorted(set(expected) | set(actual)):
        if key not in actual:
            diffs.append(f"  + missing in values.yaml: {key}={expected[key]!r}")
        elif key not in expected:
            diffs.append(f"  - extra in values.yaml:   {key}={actual[key]!r}")
        elif expected[key] != actual[key]:
            diffs.append(f"  ~ value mismatch for {key}: .env.prod={expected[key]!r} vs values.yaml={actual[key]!r}")
    return diffs


def main() -> int:
    configure_utf8_stdio()
    parser = argparse.ArgumentParser(description="Sync or check .env.prod against Helm values.yaml.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sync", action="store_true", help="Update Helm values.yaml from .env.prod")
    group.add_argument("--check-only", action="store_true", help="Check for drift without modifying files")
    args = parser.parse_args()

    if not ENV_PROD_PATH.exists():
        if args.check_only:
            print(f"[env-sync] {ENV_PROD_PATH.name} not found (gitignored); skipping drift check.")
            return 0
        print(f"[env-sync] ERROR: {ENV_PROD_PATH} does not exist.", file=sys.stderr)
        return 1

    if not HELM_VALUES_PATH.exists():
        print(f"[env-sync] ERROR: {HELM_VALUES_PATH} does not exist.", file=sys.stderr)
        return 1

    expected_env = parse_env_file(ENV_PROD_PATH)
    actual_env, secret_name = load_helm_env(HELM_VALUES_PATH)
    diffs = compute_drift(expected_env, actual_env)

    if args.check_only:
        if diffs:
            print("[env-sync] DRIFT DETECTED between .env.prod and deployments/helm/rag-telemetry-audit-daemon/values.yaml:")
            for line in diffs:
                print(line)
            print("\nRun `python scripts/sync_env_to_deployments.py --sync` to synchronize.", file=sys.stderr)
            return 1
        print("[env-sync] OK: zero drift between .env.prod and deployments/helm/rag-telemetry-audit-daemon/values.yaml.")
        return 0

    if not diffs:
        print("[env-sync] Already in sync; no changes written.")
        return 0

    update_values_yaml(HELM_VALUES_PATH, expected_env, secret_name)
    print(f"[env-sync] Synced {len(expected_env)} non-sensitive variables to {HELM_VALUES_PATH.relative_to(REPO_ROOT)}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
