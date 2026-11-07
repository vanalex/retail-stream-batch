"""Print every cell of a Jupyter notebook, untruncated, with cell numbers.

Usage: python3 -I dump_notebook.py <notebook.ipynb> [--outputs]
"""
import json
import sys


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    show_outputs = "--outputs" in sys.argv
    if len(args) != 1:
        print(__doc__.strip(), file=sys.stderr)
        return 2

    with open(args[0], encoding="utf-8") as f:
        nb = json.load(f)

    for i, cell in enumerate(nb.get("cells", [])):
        source = "".join(cell.get("source", [])).rstrip()
        if not source:
            continue
        kind = cell.get("cell_type", "?")
        print(f"===== cell {i} [{kind}] =====")
        print(source)
        if show_outputs and kind == "code":
            for out in cell.get("outputs", []):
                text = "".join(out.get("text", []) or out.get("data", {}).get("text/plain", []))
                if text:
                    print(f"----- output -----\n{text.rstrip()}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
