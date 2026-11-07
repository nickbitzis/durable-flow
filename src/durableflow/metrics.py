"""Prometheus text format, computed from the database on each scrape so every worker
process contributes without a shared in-memory registry."""

from __future__ import annotations

from durableflow.engine import Engine

BUCKETS = [0.01, 0.05, 0.1, 0.5, 1, 5, 30, 120, 600]
STATUSES = ["RUNNING", "COMPLETED", "COMPENSATED", "FAILED", "TERMINATED"]


def _histogram(name: str, labels: str, values: list[float]) -> list[str]:
    out = []
    for b in BUCKETS:
        out.append(f'{name}_bucket{{{labels},le="{b}"}} {sum(1 for v in values if v <= b)}')
    out.append(f'{name}_bucket{{{labels},le="+Inf"}} {len(values)}')
    out.append(f"{name}_sum{{{labels}}} {sum(values):.6f}")
    out.append(f"{name}_count{{{labels}}} {len(values)}")
    return out


def render(engine: Engine) -> str:
    lines = [
        "# HELP durableflow_task_queue_depth Tasks by queue and state.",
        "# TYPE durableflow_task_queue_depth gauge",
    ]
    for key, v in engine.queue_depths().items():
        queue, state = key.split("_", 1)
        lines.append(f'durableflow_task_queue_depth{{queue="{queue}",state="{state}"}} {v}')
    lines += ["# HELP durableflow_workflows Workflows by status of their latest run.",
              "# TYPE durableflow_workflows gauge"]
    counts = engine.status_counts()
    for s in STATUSES:
        lines.append(f'durableflow_workflows{{status="{s.lower()}"}} {counts.get(s, 0)}')
    counters = [
        ("activity_retries", "Activity attempts that failed and were rescheduled."),
        ("activities_completed", "Activities that completed."),
        ("activities_failed", "Activities that failed for good."),
        ("activity_lease_takeovers", "Activity leases that expired and were taken over."),
        ("wf_lease_takeovers", "Workflow task leases that expired and were taken over."),
        ("fenced_completions", "Late reports rejected because the lease had moved on."),
        ("workflow_tasks", "Workflow tasks completed."),
        ("commands_replayed", "Commands answered from history during replay."),
        ("timers_fired", "Durable timers fired."),
        ("nondeterminism_errors", "Workflow tasks that failed replay."),
        ("side_effects_deduped", "Repeat external calls absorbed by idempotency keys."),
        ("continued_as_new", "Runs that continued as new."),
    ]
    for name, help_ in counters:
        lines += [f"# HELP durableflow_{name}_total {help_}", f"# TYPE durableflow_{name}_total counter",
                  f"durableflow_{name}_total {engine.counter(name)}"]
    lat = engine.task_latencies()
    lines += ["# HELP durableflow_task_schedule_to_start_seconds Time from ready to picked up.",
              "# TYPE durableflow_task_schedule_to_start_seconds histogram"]
    for queue, values in lat.items():
        lines += _histogram("durableflow_task_schedule_to_start_seconds", f'queue="{queue}"', values)
    return "\n".join(lines) + "\n"
