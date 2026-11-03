"""A job that keeps its state in a fixed temp path, as a shared checkpoint directory would."""

import os
import tempfile
import time

import ray

STATE_DIR = os.path.join(tempfile.gettempdir(), "isolation-check-demo")

ray.init()
os.makedirs(STATE_DIR, exist_ok=True)
with open(os.path.join(STATE_DIR, "state.lock"), "a"):
    time.sleep(5)
