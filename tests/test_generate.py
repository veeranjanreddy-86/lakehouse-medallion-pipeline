from __future__ import annotations

import json

from lakehouse.generate import GeneratorConfig, generate

CFG = GeneratorConfig(seed=11, customers=20, days=6, batches=2)


def _read_all(root):
    return {p.relative_to(root).as_posix(): p.read_text() for p in sorted(root.rglob("*.jsonl"))}


def test_generator_is_deterministic(tmp_path):
    generate(tmp_path / "a", CFG)
    generate(tmp_path / "b", CFG)
    generate(tmp_path / "c", GeneratorConfig(seed=12, customers=20, days=6, batches=2))
    assert _read_all(tmp_path / "a") == _read_all(tmp_path / "b")
    assert _read_all(tmp_path / "a") != _read_all(tmp_path / "c")


def test_generator_injects_messy_delivery(tmp_path):
    manifest = generate(tmp_path, CFG)["entities"]
    files = _read_all(tmp_path)
    assert len(files) == 3 * CFG.batches
    for entity in ("customers", "accounts", "transactions"):
        assert manifest[entity]["malformed"] == CFG.batches * CFG.malformed_per_file
    assert manifest["transactions"]["duplicates"] > 0
    assert manifest["transactions"]["late"] > 0

    customers = [
        json.loads(line)
        for name, text in files.items() if name.startswith("customers/")
        for line in text.splitlines() if line.startswith("{") and line.endswith("}")
    ]  # fmt: skip
    ops = {c.get("op") for c in customers}
    assert {"I", "U"} <= ops
