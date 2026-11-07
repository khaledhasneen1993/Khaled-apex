"""Local Paper accounts; never use runtime/testnet or Demo state."""
import os
from pathlib import Path
ASSETS = Path(__file__).resolve().parent
ROOT = ASSETS.parent
BASE = ROOT/'runtime/paper-dashboard'
RUNTIME = Path(os.environ.get('KHALED_PAPER_SESSION', str(BASE/'unselected'))).expanduser().resolve()
FINAL_SOURCE = Path(os.environ.get('CASCADE_FINAL_DIR', str(ROOT/'.runtime/repos/Final'))).expanduser().resolve()
SOURCES = {'final': FINAL_SOURCE, 'khaled': ROOT}
STATE_PATHS = {name: RUNTIME/'accounts'/name/'state.json' for name in SOURCES}
