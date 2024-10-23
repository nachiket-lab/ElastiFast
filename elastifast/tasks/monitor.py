from elastifast.tasks import celery_app


def get_celery_tasks():
    active_tasks = celery_app.control.inspect().active()

    if not active_tasks:
        return {"running tasks": []}

    formatted_tasks = []
    for worker, tasks in active_tasks.items():
        for task in tasks:
            formatted_tasks.append(
                {
                    "id": task["id"],
                    "name": task["name"],
                    "hostname": task["hostname"],
                    "time_started": task["time_start"],
                    "args": task["args"],
                    "kwargs": task["kwargs"],
                    "worker": worker,
                }
            )

    return {"running tasks": formatted_tasks}
