# OKX Long Bot Clean Rebuild v118

Near-final clean rebuild for the OKX Long Bot project.

## Included
- modular project tree
- orchestration-only `main.py`
- balanced pair selection with mixed ranked universe
- market modes: NORMAL / STRONG / BLOCK / RECOVERY
- normal signal first, execution later separation
- 40/40/20 lifecycle-aware tracking
- unified report routing and period ordering
- unified Telegram/menu/mode message helpers
- mode-color-aware normal signals, execution messages, status, transitions, and reminders

## Current status
This build is intended as the near-final baseline for continued refinement.

## Known limitations
- exact legacy parity is very close but not guaranteed line-for-line
- live exchange integration remains scaffolded rather than production-complete
- final end-to-end validation against full legacy datasets is still recommended
