"""The same job with the controller in the job's own namespace (the Ray default)."""

import time

import ray


@ray.remote(num_cpus=0)
class Controller:
    def ping(self) -> int:
        return 1


ray.init()
controller = Controller.options(name="Controller").remote()
ray.get(controller.ping.remote())
time.sleep(5)
