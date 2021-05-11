import sys
sys.path.append('./../../cluster')

import time
import os

from tools import fetch_stats
from analyze import StatAnalyzer

def refresh():
    sa = StatAnalyzer()

    while True:
        fetch_stats()
        sa.load()
        os.rmdir('stats')
        time.sleep(20)
