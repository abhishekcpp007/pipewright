"""Model graph: parse the SQL files, read their config headers, resolve refs, sort them.

A model is a .sql file with a config comment on top. The graph comes from the refs inside the
query, so the dependency list cannot drift away from the code that depends on it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from .config import MODEL_DIR

REF_RE = re.compile(r"\{\{\s*ref\('([a-z0-9_]+)'\)\s*\}\}")
CONFIG_RE = re.compile(r"^--\s*config:\s*(.+)$", re.MULTILINE)
VALID_MATERIALIZATIONS = {"view", "table", "incremental", "scd2"}


@dataclass
class Model:
    name: str
    path: Path
    sql: str
    materialized: str = "view"
    unique_key: str | None = None
    watermark: str | None = None
    check_cols: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    refs: list[str] = field(default_factory=list)

    @property
    def layer(self) -> str:
        return self.tags[0] if self.tags else "other"


def parse_config(sql: str) -> dict:
    m = CONFIG_RE.search(sql)
    if not m:
        return {}
    out = {}
    for part in m.group(1).split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def load_model(path: Path) -> Model:
    sql = path.read_text()
    cfg = parse_config(sql)
    mat = cfg.get("materialized", "view")
    if mat not in VALID_MATERIALIZATIONS:
        raise ValueError(f"{path.name}: unknown materialization {mat!r}")
    model = Model(
        name=path.stem,
        path=path,
        sql=sql,
        materialized=mat,
        unique_key=cfg.get("unique_key"),
        watermark=cfg.get("watermark"),
        check_cols=[c for c in cfg.get("check_cols", "").split("|") if c],
        tags=[t for t in cfg.get("tags", "").split("|") if t],
        refs=sorted(set(REF_RE.findall(sql))),
    )
    if mat in ("incremental", "scd2") and not model.unique_key:
        raise ValueError(f"{path.name}: {mat} needs a unique_key")
    if mat == "scd2" and not model.check_cols:
        raise ValueError(f"{path.name}: scd2 needs check_cols")
    return model


class Graph:
    def __init__(self, models: dict[str, Model]) -> None:
        self.models = models
        missing = {r for m in models.values() for r in m.refs} - set(models)
        if missing:
            raise ValueError(f"refs to models that do not exist: {sorted(missing)}")
        self.order = self._toposort()

    @classmethod
    def load(cls, model_dir: Path | None = None) -> Graph:
        d = model_dir or MODEL_DIR
        models = {}
        for p in sorted(d.glob("*.sql")):
            m = load_model(p)
            models[m.name] = m
        return cls(models)

    def _toposort(self) -> list[str]:
        indeg = {n: 0 for n in self.models}
        children: dict[str, list[str]] = {n: [] for n in self.models}
        for n, m in self.models.items():
            for r in m.refs:
                children[r].append(n)
                indeg[n] += 1
        ready = sorted(n for n, d in indeg.items() if d == 0)
        out: list[str] = []
        while ready:
            n = ready.pop(0)
            out.append(n)
            for c in sorted(children[n]):
                indeg[c] -= 1
                if indeg[c] == 0:
                    ready.append(c)
            ready.sort()
        if len(out) != len(self.models):
            stuck = sorted(set(self.models) - set(out))
            raise ValueError(f"the model graph has a cycle through {stuck}")
        return out

    def layers(self, selected: list[str] | None = None) -> list[list[str]]:
        """Nodes grouped so everything in a group can run at the same time."""
        keep = set(selected or self.order)
        depth: dict[str, int] = {}
        for n in self.order:
            parents = [r for r in self.models[n].refs if r in keep]
            depth[n] = 1 + max([depth[p] for p in parents], default=-1) if n in keep else 0
        groups: dict[int, list[str]] = {}
        for n in self.order:
            if n in keep:
                groups.setdefault(depth[n], []).append(n)
        return [sorted(groups[k]) for k in sorted(groups)]

    def ancestors(self, name: str) -> set[str]:
        out: set[str] = set()
        stack = list(self.models[name].refs)
        while stack:
            n = stack.pop()
            if n not in out:
                out.add(n)
                stack.extend(self.models[n].refs)
        return out

    def descendants(self, name: str) -> set[str]:
        out: set[str] = set()
        stack = [n for n, m in self.models.items() if name in m.refs]
        while stack:
            n = stack.pop()
            if n not in out:
                out.add(n)
                stack.extend(c for c, m in self.models.items() if n in m.refs)
        return out

    def select(self, selector: str | None) -> list[str]:
        """dbt style selection: name, +name for upstream, name+ for downstream, tag:gold."""
        if not selector:
            return list(self.order)
        picked: set[str] = set()
        for token in selector.split():
            if token.startswith("tag:"):
                tag = token[4:]
                picked |= {n for n, m in self.models.items() if tag in m.tags}
                continue
            up = token.startswith("+")
            down = token.endswith("+")
            name = token.strip("+")
            if name not in self.models:
                raise KeyError(f"no model named {name!r}")
            picked.add(name)
            if up:
                picked |= self.ancestors(name)
            if down:
                picked |= self.descendants(name)
        return [n for n in self.order if n in picked]

    def to_dot(self) -> str:
        colors = {"bronze": "#b45309", "silver": "#475569", "dim": "#7c3aed", "gold": "#15803d"}
        lines = ["digraph pipewright {", '  rankdir=LR;',
                 '  node [shape=box style=rounded fontname="Helvetica" fontsize=10];']
        for n, m in sorted(self.models.items()):
            c = colors.get(m.layer, "#334155")
            lines.append(f'  "{n}" [color="{c}" fontcolor="{c}" '
                         f'label="{n}\\n{m.materialized}"];')
        for n, m in sorted(self.models.items()):
            for r in m.refs:
                lines.append(f'  "{r}" -> "{n}";')
        lines.append("}")
        return "\n".join(lines)

    def to_mermaid(self) -> str:
        lines = ["flowchart LR"]
        for n, m in sorted(self.models.items()):
            lines.append(f"  {n}[{n}<br/>{m.materialized}]")
        for n, m in sorted(self.models.items()):
            for r in m.refs:
                lines.append(f"  {r} --> {n}")
        return "\n".join(lines)
