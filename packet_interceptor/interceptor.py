import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIM = ROOT / "physics_engine" / "simulate_pipeline.py"
# Support both debug and release Rust builds; also handle .exe on Windows
_RUST_BASE = ROOT / "decision_engine" / "target"
RUST_BIN = (
    _RUST_BASE / "release" / "decision_engine.exe"
    if (_RUST_BASE / "release" / "decision_engine.exe").exists()
    else _RUST_BASE / "release" / "decision_engine"
    if (_RUST_BASE / "release" / "decision_engine").exists()
    else None
)
LOG = Path(__file__).with_name("sample_log.jsonl")
OUT = ROOT / "output" / "decisions.jsonl"


def simulate(value: float) -> dict:
    """Call the physics engine subprocess and return the simulation result."""
    out = subprocess.check_output(
        [sys.executable, str(SIM), str(value)], text=True
    )
    return json.loads(out.strip())


def decide(sim: dict) -> dict:
    """Return an ALLOW/DROP decision based on the simulation result.

    Prefers the compiled Rust decision engine when available; falls back to
    inline Python that mirrors the same threshold logic.
    """
    if RUST_BIN is not None and Path(RUST_BIN).exists():
        p = subprocess.run(
            [str(RUST_BIN)],
            input=json.dumps(sim),
            text=True,
            capture_output=True,
            check=True,
        )
        return json.loads(p.stdout.strip())

    # Python fallback — mirrors the Rust thresholds exactly
    blocked = (
        sim["state"] == "Catastrophic Failure"
        or sim["pressure_bar"] > 12.0
        or sim["flow_rate"] > 5.0
    )
    if blocked:
        return {"action": "DROP", "reason": "Physics threshold exceeded"}
    return {"action": "ALLOW", "reason": "Within safe bounds"}


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    if not LOG.exists() or LOG.stat().st_size == 0:
        print(f"[interceptor] {LOG} is missing or empty — running generator.py first …")
        subprocess.check_call([sys.executable, str(Path(__file__).with_name("generator.py"))])

    print(f"[interceptor] Reading {LOG}")
    print(f"[interceptor] Writing results to {OUT}")
    print(f"[interceptor] Rust decision engine: {'found at ' + str(RUST_BIN) if RUST_BIN else 'not found — using Python fallback'}")
    print()

    with LOG.open("r", encoding="utf-8") as f, OUT.open("w", encoding="utf-8") as o:
        for line in f:
            line = line.strip()
            if not line:
                continue
            cmd = json.loads(line)
            sim = simulate(cmd["value"])
            decision = decide(sim)
            record = {**cmd, **sim, **decision}
            o.write(json.dumps(record) + "\n")
            status = "[DROP]" if decision["action"] == "DROP" else "[ALLOW]"
            print(f"{status} | value={cmd['value']:5.1f} | pressure={sim['pressure_bar']:.3f} bar"
                  f" | flow={sim['flow_rate']:.3f} | state={sim['state']}")

    print(f"\n[interceptor] Done — {OUT}")


if __name__ == "__main__":
    main()