"""Scan tracked files and reachable Git history without printing matched values."""
import re
import subprocess
from pathlib import Path

PATTERNS = (
    re.compile(rb'''(?i)(?:api[_-]?(?:key|secret)|X-MBX-APIKEY|BINANCE_API_(?:KEY|SECRET))['"]?\s*[:=]\s*['"][A-Za-z0-9+/=_-]{32,}['"]'''),
    re.compile(rb'\b\d{8,12}:[A-Za-z0-9_-]{30,}\b'),
)


def main():
    objects = subprocess.check_output(['git', 'rev-list', '--objects', '--all'], text=True).splitlines()
    findings = set()
    for row in objects:
        sha, _, name = row.partition(' ')
        if not name: continue
        if subprocess.check_output(['git', 'cat-file', '-t', sha], text=True).strip() != 'blob': continue
        value = subprocess.check_output(['git', 'cat-file', '-p', sha])
        if any(pattern.search(value) for pattern in PATTERNS): findings.add('history:' + name)
    for name in subprocess.check_output(['git', 'ls-files'], text=True).splitlines():
        value = Path(name).read_bytes()
        if any(pattern.search(value) for pattern in PATTERNS): findings.add('working_tree:' + name)
    print({'potential_secret_files': sorted(findings), 'reachable_objects_checked': len(objects),
           'note': 'Pattern scan; not a proof that no credential has ever existed.'})
    raise SystemExit(bool(findings))


if __name__ == '__main__':
    main()
