"""A job that finds its controller by name in a namespace it picks itself, as TransferQueue did before #206."""

import os
import time

import ray

NAMESPACE = os.environ.get("QUEUE_NAMESPACE") or "shared_queue"


@ray.remote(num_cpus=0)
class Controller:
    def ping(self) -> int:
        return 1


def get_or_create_controller():
    try:
        return ray.get_actor("Controller", namespace=NAMESPACE)
    except ValueError:
        pass
    try:
        return Controller.options(name="Controller", namespace=NAMESPACE).remote()
    except ValueError:
        return ray.get_actor("Controller", namespace=NAMESPACE)


ray.init()
controller = get_or_create_controller()
ray.get(controller.ping.remote())
time.sleep(5)
