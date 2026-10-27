"""Explicit local-only depth-maker paper entry point; no server/live execution."""

from .polymarket_pair import main


if __name__ == "__main__":
    main(default_strategy="depth-maker")
