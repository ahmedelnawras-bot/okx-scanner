from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical replay CLI placeholder")
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--symbols", type=int, default=200)
    args = parser.parse_args()
    print(f"Historical replay CLI scaffold ready: days={args.days}, symbols={args.symbols}")
    print("Heavy candle replay worker will be implemented in the next patch inside historical_replay/.")


if __name__ == "__main__":
    main()
