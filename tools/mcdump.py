#!/usr/bin/env python3
"""Dump named method/field descriptors from a Mojang ProGuard mappings file.

Usage:
    python mcdump.py <mappings.txt> class <fully.qualified.Name> [--members]
    python mcdump.py <mappings.txt> find <fully.qualified.Name> <methodName>
    python mcdump.py <mappings.txt> classes <substring>
"""

import re
import sys

PRIMITIVES = {
    "void": "V", "boolean": "Z", "byte": "B", "char": "C",
    "short": "S", "int": "I", "long": "J", "float": "F", "double": "D",
}


def type_to_desc(t: str) -> str:
    t = t.strip()
    if t.endswith("..."):
        t = t[:-3] + "[]"
    dims = 0
    while t.endswith("[]"):
        dims += 1
        t = t[:-2]
    if t in PRIMITIVES:
        base = PRIMITIVES[t]
    else:
        base = "L" + t.replace(".", "/") + ";"
    return "[" * dims + base


class Mappings:
    def __init__(self, path):
        # named -> obf
        self.class_n2o = {}
        self.class_o2n = {}
        # named class -> list of (name, desc, obf_name, kind)
        self.members = {}
        self._parse(path)

    def _parse(self, path):
        cur_named = None
        method_re = re.compile(
            r"^(?:\d+:\d+:)?(\S.*?)\s+([\w$<>.]+)\s*\((.*?)\)\s*->\s*(\S+)$"
        )
        field_re = re.compile(r"^(\S.*?)\s+([\w$]+)\s*->\s*(\S+)$")
        class_re = re.compile(r"^(\S+)\s*->\s*(\S+):$")

        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.rstrip("\n")
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                if not line[0].isspace():
                    m = class_re.match(line.strip())
                    if m:
                        cur_named = m.group(1)
                        self.class_n2o[cur_named] = m.group(2)
                        self.class_o2n[m.group(2)] = cur_named
                        self.members.setdefault(cur_named, [])
                    continue
                if cur_named is None:
                    continue
                s = line.strip()
                if s.startswith("#"):
                    continue
                m = method_re.match(s)
                if m:
                    ret, name, args, obf = m.groups()
                    desc = "(" + "".join(
                        type_to_desc(a) for a in args.split(",") if a.strip()
                    ) + ")" + type_to_desc(ret)
                    self.members[cur_named].append((name, desc, obf, "method"))
                    continue
                m = field_re.match(s)
                if m:
                    typ, name, obf = m.groups()
                    self.members[cur_named].append(
                        (name, type_to_desc(typ), obf, "field")
                    )


def dump_class(mp, name, show_obf=False):
    if name not in mp.members:
        # try to resolve an inner-class style name
        print(f"!! class not in mappings: {name}")
        return
    obf = mp.class_n2o.get(name, "?")
    print(f"=== {name}  ->  {obf} ===")
    for mname, desc, obfname, kind in mp.members[name]:
        extra = f"   [obf {obfname}]" if show_obf else ""
        print(f"  {kind:6} {mname}{desc}{extra}")


def find_method(mp, cls, mname):
    print(f"=== {cls} :: {mname} ===")
    hits = [
        (n, d, o, k)
        for (n, d, o, k) in mp.members.get(cls, [])
        if n == mname
    ]
    if not hits:
        print("  (no match)")
    for n, d, o, k in hits:
        print(f"  {k:6} {n}{d}")

    # also search owner hierarchy names for constructors
    sub = [c for c in mp.members if c.startswith(cls + "$")]
    for c in sub:
        for n, d, o, k in mp.members[c]:
            if n == mname:
                print(f"  [{c}] {k} {n}{d}")


def find_classes(mp, needle):
    for c in sorted(mp.members):
        if needle in c:
            print(c)


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 1
    mp = Mappings(sys.argv[1])
    cmd = sys.argv[2]
    if cmd == "class":
        dump_class(mp, sys.argv[3], "--obf" in sys.argv)
    elif cmd == "find":
        find_method(mp, sys.argv[3], sys.argv[4])
    elif cmd == "classes":
        find_classes(mp, sys.argv[3])
    return 0


if __name__ == "__main__":
    sys.exit(main())
