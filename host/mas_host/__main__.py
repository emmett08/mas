"""Run the mixed-agent demo or administer its private deployment."""
from __future__ import annotations
import sys


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "admin":
        from .admin import main as admin_main
        return admin_main(arguments[1:])
    from .demo import main as demo_main
    return demo_main(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
