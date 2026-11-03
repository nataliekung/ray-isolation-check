"""A minimal TransferQueue job: init, put one key, wait, close."""

import time

import ray
import torch
import transfer_queue as tq
from omegaconf import OmegaConf

ray.init()
tq.init(OmegaConf.create({"backend": {"SimpleStorage": {"num_data_storage_units": 1}}}))
tq.kv_put("k", "p", fields={"v": torch.tensor([1])})
time.sleep(8)
tq.close()
