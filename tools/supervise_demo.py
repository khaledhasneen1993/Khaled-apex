"""Portable restart supervisor for Termux; explicit launch only."""
import signal
import subprocess
import sys
import time
from pathlib import Path

def main():
    root = Path(__file__).resolve().parents[1]
    stopping = False
    child = None
    def stop(sig, frame):
        nonlocal stopping
        stopping = True
        if child and child.poll() is None: child.send_signal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while not stopping:
            child = subprocess.Popen([sys.executable, '-u', str(root / 'run_demo.py'), *sys.argv[1:]], cwd=root)
            while child.poll() is None:
                try: child.wait(timeout=1)
                except subprocess.TimeoutExpired: pass
                if stopping:
                    try: child.wait(timeout=30)
                    except subprocess.TimeoutExpired: child.kill(); child.wait()
            if not stopping:
                deadline = time.monotonic() + 5
                while not stopping and time.monotonic() < deadline: time.sleep(.2)
    finally:
        if child and child.poll() is None: child.terminate(); child.wait(timeout=30)

if __name__ == '__main__': main()
