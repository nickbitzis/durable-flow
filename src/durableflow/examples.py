"""Example workflows for Kestrel Supply Co., a made-up outdoor gear retailer.

1. employee_onboarding: accounts in 4 systems in parallel, up to 7 days for a manager
   approval signal, a laptop ordered through a child workflow, a welcome email. Accounts
   are deleted again if a later step fails for good.
2. order_fulfillment: a saga. Reserve inventory, charge payment, ship. On a permanent
   failure the completed steps are undone in reverse order.
3. subscription_renewal: sleeps 30 days between renewals and continues as new every few
   renewals so no single history grows without bound.

Every activity talks to a fake external system through ctx.effect(), which honours the
engine's idempotency key. Compensations are registered before the step they undo, so a
step that applied its effect and then failed to report (worker crash) is still undone.
Compensations are no-ops when there is nothing to undo.
"""

from __future__ import annotations

import hashlib
from typing import Any

from durableflow.clock import DAY
from durableflow.errors import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    ParallelError,
    SignalTimeout,
)
from durableflow.worker import ActivityContext, Registry
from durableflow.workflow import (
    DEFAULT_VERSION,
    Compensated,
    RetryPolicy,
    WorkflowContext,
    activity,
    child_workflow,
    continue_as_new,
    parallel,
    sleep,
    wait_signal,
)

examples = Registry()

BUSINESS_ERRORS = ["CardDeclined", "OutOfStock", "AddressInvalid", "CardExpired", "SeatLimit"]
FORWARD = RetryPolicy(max_attempts=8, initial_interval=1.0, backoff=2.0, max_interval=60.0,
                      non_retryable=BUSINESS_ERRORS)
UNDO = RetryPolicy(max_attempts=25, initial_interval=1.0, backoff=2.0, max_interval=30.0)
SYSTEMS = ["directory", "email", "payroll", "chat"]
UNDO_OF = {"create_account": "delete_account", "reserve": "release", "charge": "refund"}


def _id(prefix: str, *parts: Any) -> str:
    return prefix + "_" + hashlib.sha1(":".join(map(str, parts)).encode()).hexdigest()[:10]


# ------------------------------------------------------------------ activities


@examples.activity()
def create_account(ctx: ActivityContext, system: str, employee: str) -> dict:
    if employee.endswith("-seatlimit") and system == "chat":
        raise ApplicationError("chat workspace has no free seats", type="SeatLimit")
    row = ctx.effect(system, "create_account", employee, {"login": employee})
    return {"system": system, "account_id": _id(system[:3], employee), "effect_id": row["id"]}


@examples.activity()
def delete_account(ctx: ActivityContext, system: str, employee: str) -> dict:
    if ctx.find_effect(system, "create_account", employee) is None:
        return {"system": system, "deleted": False, "reason": "no account to delete"}
    ctx.effect(system, "delete_account", employee)
    return {"system": system, "deleted": True}


@examples.activity()
def send_welcome(ctx: ActivityContext, employee: str, manager: str) -> dict:
    ctx.effect("email", "send_welcome", employee, {"cc": manager})
    return {"sent_to": employee}


@examples.activity()
def ship_welcome_kit(ctx: ActivityContext, employee: str) -> dict:
    ctx.effect("shipping", "ship_welcome_kit", employee)
    return {"kit": "standard", "employee": employee}


@examples.activity()
def reserve_inventory(ctx: ActivityContext, order_id: str, sku: str, qty: int) -> dict:
    if sku.startswith("OOS-"):
        raise ApplicationError(f"{sku} is out of stock", type="OutOfStock")
    ctx.effect("inventory", "reserve", order_id, {"sku": sku, "qty": qty})
    return {"reservation_id": _id("res", order_id), "sku": sku, "qty": qty}


@examples.activity()
def release_inventory(ctx: ActivityContext, order_id: str) -> dict:
    if ctx.find_effect("inventory", "reserve", order_id) is None:
        return {"released": False}
    ctx.effect("inventory", "release", order_id)
    return {"released": True}


@examples.activity()
def charge_payment(ctx: ActivityContext, order_id: str, amount_cents: int, card: str) -> dict:
    if card == "declined":
        raise ApplicationError("card declined by issuer", type="CardDeclined")
    ctx.effect("payments", "charge", order_id, {"amount_cents": amount_cents})
    return {"charge_id": _id("ch", order_id), "amount_cents": amount_cents}


@examples.activity()
def refund_payment(ctx: ActivityContext, order_id: str) -> dict:
    charge = ctx.find_effect("payments", "charge", order_id)
    if charge is None:
        return {"refunded": False}
    ctx.effect("payments", "refund", order_id)
    return {"refunded": True}


@examples.activity()
def ship_order(ctx: ActivityContext, order_id: str, address: str) -> dict:
    if address == "invalid":
        raise ApplicationError("carrier rejected the address", type="AddressInvalid")
    ctx.heartbeat({"stage": "label printed"})
    ctx.effect("shipping", "ship", order_id, {"address": address})
    return {"tracking": _id("trk", order_id)}


@examples.activity()
def charge_renewal(ctx: ActivityContext, customer: str, period: int, card: str,
                   amount_cents: int) -> dict:
    if card == "expires-after-2" and period > 2:
        raise ApplicationError("card on file has expired", type="CardExpired")
    ctx.effect("billing", "renew", f"{customer}:{period}", {"amount_cents": amount_cents})
    return {"invoice": _id("inv", customer, period), "period": period}


@examples.activity()
def cancel_subscription(ctx: ActivityContext, customer: str) -> dict:
    ctx.effect("billing", "cancel", customer)
    return {"canceled": customer}


# ------------------------------------------------------------------ workflows


def _undo(steps: list) -> Any:
    """Run compensations newest first. Used with `yield from`."""
    results = []
    for cmd in reversed(steps):
        results.append((yield cmd))
    return results


@examples.workflow()
def order_fulfillment(ctx: WorkflowContext, inp: dict) -> Any:
    order = inp["order_id"]
    steps = [
        ("reserve_inventory", (order, inp["sku"], inp.get("qty", 1)), "release_inventory"),
        ("charge_payment", (order, inp["amount_cents"], inp.get("card", "ok")), "refund_payment"),
        ("ship_order", (order, inp.get("address", "ok")), None),
    ]
    undo: list = []
    results: dict[str, Any] = {}
    for name, args, undo_name in steps:
        ctx.set_stage(name.replace("_", " "))
        if undo_name:
            undo.append(activity(undo_name, order, retry=UNDO))
        try:
            results[name] = yield activity(name, *args, retry=FORWARD, start_to_close=30)
        except ActivityError as e:
            ctx.set_stage(f"compensating after {name}")
            done = yield from _undo(undo)
            return Compensated(f"{name} failed: {e.error_type}",
                               {"order_id": order, "compensations": len(done)})
    ctx.set_stage("done")
    return {"order_id": order, **results}


def _undo_accounts(employee: str) -> list:
    return [activity("delete_account", s, employee, retry=UNDO) for s in SYSTEMS]


@examples.workflow()
def employee_onboarding(ctx: WorkflowContext, inp: dict) -> Any:
    emp, manager = inp["employee"], inp["manager"]
    ctx.set_stage("creating accounts in 4 systems")
    try:
        accounts = yield parallel(*[activity("create_account", s, emp, retry=FORWARD)
                                    for s in SYSTEMS])
    except ParallelError as e:
        ctx.set_stage("compensating: deleting accounts")
        failed = [SYSTEMS[i] for i, err in enumerate(e.errors) if err is not None]
        yield from _undo(_undo_accounts(emp))
        return Compensated(f"account creation failed in {', '.join(failed)}")

    ctx.set_stage("awaiting manager approval")
    try:
        approval = yield wait_signal("manager_approval", timeout=7 * DAY)
    except SignalTimeout:
        ctx.set_stage("compensating: approval timed out")
        yield from _undo(_undo_accounts(emp))
        return Compensated("manager approval timed out after 7 days")
    if not (approval or {}).get("approved"):
        ctx.set_stage("compensating: rejected")
        yield from _undo(_undo_accounts(emp))
        return Compensated("rejected by manager", approval)

    ctx.set_stage("provisioning laptop")
    try:
        laptop = yield child_workflow("order_fulfillment", {
            "order_id": f"laptop-{emp}", "sku": inp.get("laptop_sku", "LAPTOP-14"), "qty": 1,
            "amount_cents": 0, "card": "internal", "address": inp.get("address", "ok")})
    except ChildWorkflowError as e:
        ctx.set_stage("compensating: laptop failed")
        yield from _undo(_undo_accounts(emp))
        return Compensated("laptop could not be provisioned", {"child": e.status})

    # Added later: new runs also ship a welcome kit. Runs that were already past this point
    # when the change shipped replay as version 0 and skip it.
    if ctx.get_version("welcome-kit", DEFAULT_VERSION, 1) >= 1:
        yield activity("ship_welcome_kit", emp, retry=UNDO)
    ctx.set_stage("sending welcome")
    yield activity("send_welcome", emp, manager, retry=UNDO)
    ctx.set_stage("done")
    return {"employee": emp, "approved_by": approval.get("by", manager),
            "accounts": [a["account_id"] for a in accounts], "laptop": laptop["ship_order"]}


@examples.workflow()
def subscription_renewal(ctx: WorkflowContext, inp: dict) -> Any:
    customer = inp["customer"]
    done = inp.get("renewals_done", 0)
    total = inp.get("max_renewals", 12)
    per_run = inp.get("renewals_per_run", 6)
    in_run = 0
    while done < total:
        ctx.set_stage(f"sleeping until renewal {done + 1} of {total}")
        yield sleep(inp.get("period_days", 30) * DAY)
        ctx.set_stage(f"charging renewal {done + 1}")
        try:
            yield activity("charge_renewal", customer, done + 1, inp.get("card", "ok"),
                           inp.get("amount_cents", 1900), retry=FORWARD)
        except ActivityError as e:
            yield activity("cancel_subscription", customer, retry=UNDO)
            return Compensated(f"renewal {done + 1} failed: {e.error_type}",
                               {"renewals_done": done})
        done += 1
        in_run += 1
        if in_run >= per_run and done < total:
            # Cap history: start a fresh run that carries only the counter forward.
            return continue_as_new({**inp, "renewals_done": done})
    ctx.set_stage("done")
    return {"customer": customer, "renewals": done}
