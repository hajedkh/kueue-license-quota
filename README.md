# kueue-license-quota

A small controller that keeps a Kueue ClusterQueue quota in sync with a license server.

![Kueue Prometheus Pattern](./the-pattern-kueue-prometheus%20%282%29%20%283%29.jpg)

Every 30 seconds it reads how many license tokens are free (from Prometheus), adds back
the tokens already held by admitted jobs, subtracts a safety margin, and writes the result
into `nominalQuota` of one resource on one ClusterQueue.

    nominal = max(0, free + held - headroom)

Jobs that need more tokens than that wait in the queue. They do not start and fail.

## What you need

- Kueue installed (v1beta1 or v1beta2 API)
- Prometheus with two series:
  - `license_tokens_free{feature="fep"}` – free tokens on the license server
  - `license_tokens_used_external{feature="fep"}` – tokens held by machines outside the cluster
  (any FlexNet/lmstat exporter can produce these)
- A ClusterQueue that lists the license resource, for example `licenses.example.com/fep`

## Run it

    # edit the flags in deploy/rbac.yaml (Prometheus URL, ClusterQueue, resource)
    kubectl apply -f deploy/rbac.yaml

Or locally against a kubeconfig:

    go build -o reconciler .
    ./reconciler --kubeconfig ~/.kube/config --prometheus-url http://localhost:9090

## Flags

| flag | default | meaning |
|---|---|---|
| `--prometheus-url` | `http://prometheus.monitoring:9090` | where to query |
| `--cluster-queue` | `licensed-compute` | the ClusterQueue to manage |
| `--flavor` | `default` | ResourceFlavor inside the ClusterQueue |
| `--resource` | `licenses.example.com/fep` | the only resource this controller writes |
| `--free-query` | `license_tokens_free{feature="fep"}` | PromQL for free tokens |
| `--headroom-query` | `quantile_over_time(0.99, license_tokens_used_external{feature="fep"}[30d])` | PromQL for the safety margin |
| `--kueue-api-version` | `v1beta1` | `v1beta2` on newer Kueue |
| `--interval` | `30s` | reconcile interval |
| `--stale-after` | `2m` | if Prometheus data is older than this, quota is set to 0 |

## Behaviour

- Only `nominalQuota` of the configured resource is touched. GPUs, CPU and memory stay yours.
- If Prometheus has no fresh data, the quota goes to 0. New jobs wait, nothing is evicted.
- Lowering the quota never evicts running jobs; new admissions stop until usage falls.
- Run one replica. There is no leader election.

## Pair it with

Kueue's quota is 30 seconds old at worst. If you need a check at the moment of admission,
add an AdmissionCheck controller that asks the license server once more. This repo is only
the quota half of that pattern.

Licensed under Apache-2.0.
