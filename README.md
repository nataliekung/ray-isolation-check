# ray-isolation-check

Runs one job command twice at the same time against one Ray cluster and reports named actors that the two runs share.

Libraries that create Ray named actors in a namespace they pick themselves make concurrent jobs attach to each other's actors. A test that runs one job at a time does not see it. [TransferQueue#206](https://github.com/Ascend/TransferQueue/pull/206) fixed one such case: two training jobs on one cluster shared one queue controller, read each other's data, and the first job's shutdown removed the second job's queue.

## How it decides

Both runs execute the same command, so each should create the same named actors. While both are alive, the tool polls the Ray state API and records every named actor with the job that created it.

- **SHARED**: an actor name created by only one of the two runs. The other run attached to it, or failed before creating it; the exit codes tell which.
- **COLLISION**: both runs created the same name in the same namespace. Ray normally refuses this, so it points at a race or a detached actor left from earlier.

Each run gets `RAY_ADDRESS` and `ISOLATION_CHECK_RUN=a` or `b`, so a job can derive a per-run setting from it. The second run starts one second after the first, to avoid a race in `ray.init()` when two drivers start at the same instant; the jobs still overlap.

## Usage

```bash
pip install -e .
ray start --head
ray-isolation-check --address 127.0.0.1:6379 --dashboard http://127.0.0.1:8265 -- python my_job.py
```

The exit status is 1 when a finding is reported or either run fails.

## Example: TransferQueue

`examples/transferqueue_job.py` initializes TransferQueue, puts one key, waits, and closes. On TransferQueue main at 12af06d, Ray 2.57.0, one local head:

```
$ ray-isolation-check --address 127.0.0.1:6393 --dashboard http://127.0.0.1:8267 -- python examples/transferqueue_job.py
run a: exit 0, named actors: ['abec29f9-.../TransferQueueStorageUnit#0', 'transfer_queue/TransferQueueController']
run b: exit 0, named actors: none
SHARED: named actor 'TransferQueueController' in namespace transfer_queue was created only by run a; run b either attached to it or never created it
SHARED: named actor 'TransferQueueStorageUnit#0' in namespace abec29f9-... was created only by run a; run b either attached to it or never created it

$ ray-isolation-check ... -- sh -c 'TQ_RAY_NAMESPACE=queue-$ISOLATION_CHECK_RUN exec python examples/transferqueue_job.py'
run a: exit 0, named actors: ['0de1b8ad-.../TransferQueueStorageUnit#0', 'queue-a/TransferQueueController']
run b: exit 0, named actors: ['99656c8e-.../TransferQueueStorageUnit#0', 'queue-b/TransferQueueController']
no shared named actors found
```

Both runs exit 0 with the default, which is why the problem is easy to miss: run b silently uses run a's controller and storage. With a namespace per run, each creates its own.

The two smaller examples, `fixed_namespace_job.py` and `job_namespace_job.py`, show the same pattern without TransferQueue and are what the tests run.

When the cluster's workers import the library from a source checkout, start `ray start` with the same `PYTHONPATH` as the jobs, or the workers load a different version than the drivers.

## Tests

```bash
pip install -e ".[test]"
pytest tests
```

The tests start their own Ray head on free ports.

## Limits

- Only named actors. Shared temporary paths, fixed ports and checkpoint directories are the same class of problem and are not checked yet.
- Actors that live shorter than the poll interval (0.5 s) can be missed.
- A job whose actor set depends on timing or input can show a difference that is not sharing; the report says what was seen and leaves that judgment to the reader.

## License

Apache-2.0
