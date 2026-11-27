"""
Copyright (c) 2025 neopen
Licensed under the Apache License, Version 2.0.
See LICENSE File For Details.

@FileName: server.py
@Description: UI 服务器管理
@Author: neopen
@GitHub: https://github.com/neopen/neotask
@Time: 2026/4/1 18:19
"""

import json
import threading
import webbrowser
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from neotask.common.logger import info

_DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Task Scheduler</title>
    <style>
        body { margin: 0; padding: 2rem; background: #0f172a; color: #e2e8f0;
               font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
        h1 { font-size: 1.25rem; font-weight: 600; margin: 0 0 1.5rem; }
        .cards { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr));
                 gap: 1rem; max-width: 720px; }
        .card { background: #1e293b; border-radius: 0.75rem; padding: 1rem 1.25rem; }
        .label { color: #94a3b8; font-size: 0.8rem; }
        .value { font-size: 1.75rem; font-weight: 600; margin-top: 0.25rem; }
        .waiting .value { color: #fbbf24; }
        section { margin-top: 2rem; max-width: 960px; }
        table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
        th, td { text-align: left; padding: 0.6rem 0.75rem;
                 border-bottom: 1px solid #1e293b; }
        th { color: #94a3b8; font-weight: 500; }
        .badge { display: inline-block; padding: 0.15rem 0.6rem; border-radius: 1rem;
                 background: #f59e0b20; color: #fbbf24; font-size: 0.75rem; }
        .empty { color: #64748b; padding: 0.75rem 0; }
    </style>
</head>
<body>
    <h1>Task Scheduler Dashboard</h1>
    <div class="cards">
        <div class="card"><div class="label">队列长度</div>
            <div class="value" id="queueSize">--</div></div>
        <div class="card"><div class="label">活跃 Worker</div>
            <div class="value" id="activeWorkers">--</div></div>
        <div class="card waiting"><div class="label">等待信号</div>
            <div class="value" id="waiting">--</div></div>
    </div>
    <section>
        <h2 style="font-size:1rem;font-weight:600;">等待中的任务</h2>
        <table>
            <thead>
                <tr><th>任务 ID</th><th>等待键</th><th>超时时间</th><th>创建时间</th></tr>
            </thead>
            <tbody id="waitingTasks">
                <tr><td colspan="4" class="empty">加载中...</td></tr>
            </tbody>
        </table>
    </section>
    <script>
        function cell(text) {
            const td = document.createElement('td');
            td.textContent = text ?? '--';
            return td;
        }

        function renderWaiting(tasks) {
            const tbody = document.getElementById('waitingTasks');
            tbody.replaceChildren();
            if (!tasks || tasks.length === 0) {
                const row = document.createElement('tr');
                const td = cell('暂无等待任务');
                td.className = 'empty';
                td.colSpan = 4;
                row.appendChild(td);
                tbody.appendChild(row);
                return;
            }
            for (const task of tasks) {
                const signal = (task.data && task.data.__signal__) || {};
                const row = document.createElement('tr');
                row.appendChild(cell(task.task_id));
                const signalCell = document.createElement('td');
                const badge = document.createElement('span');
                badge.className = 'badge';
                badge.textContent = signal.signal || '--';
                signalCell.appendChild(badge);
                row.appendChild(signalCell);
                row.appendChild(cell(signal.timeout_at
                    ? new Date(signal.timeout_at * 1000).toLocaleString() : '不超时'));
                row.appendChild(cell(task.created_at
                    ? new Date(task.created_at).toLocaleString() : '--'));
                tbody.appendChild(row);
            }
        }

        async function refresh() {
            try {
                const stats = await (await fetch('/api/stats')).json();
                document.getElementById('queueSize').textContent = stats.queue_size ?? '--';
                document.getElementById('activeWorkers').textContent =
                    stats.active_workers ?? '--';
                document.getElementById('waiting').textContent = stats.waiting ?? '--';
                const data = await (await fetch('/api/tasks?status=waiting')).json();
                renderWaiting(data.tasks);
            } catch (err) {
                console.error('刷新仪表盘失败:', err);
            }
        }

        refresh();
        setInterval(refresh, 2000);
    </script>
</body>
</html>
"""


class WebUIServer:
    """Embedded web UI server running in background thread."""

    def __init__(self, scheduler: Any, host: str = "127.0.0.1",
                 port: int = 8080, auto_open: bool = False):
        self.scheduler = scheduler
        self.host = host
        self.port = port
        self.auto_open = auto_open
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[uvicorn.Server] = None
        self._running = False

    def start(self) -> None:
        """Start web UI server."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        info(f"Web UI started at http://{self.host}:{self.port}")

        if self.auto_open:
            webbrowser.open(f"http://{self.host}:{self.port}")

    def _run(self) -> None:
        """Run uvicorn server."""
        app = self._create_app()

        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False
        )
        self._server = uvicorn.Server(config)
        self._server.run()

    def _create_app(self) -> FastAPI:
        """Create FastAPI application."""


        app = FastAPI(title="Task Scheduler Dashboard")

        @app.get("/")
        async def root():
            return HTMLResponse(_DASHBOARD_HTML)

        @app.get("/api/stats")
        async def get_stats():
            return self.scheduler.get_stats()

        @app.get("/api/tasks")
        async def list_tasks(status: Optional[str] = None, limit: int = 50):
            try:
                tasks = await self.scheduler.list_tasks_async(status, limit=limit)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            for task in tasks:
                # Task.to_dict() 将 data 序列化为 JSON 字符串，面板按对象读取
                if isinstance(task.get("data"), str):
                    try:
                        task["data"] = json.loads(task["data"])
                    except ValueError:
                        task["data"] = {}
            return {"tasks": tasks}

        return app

    def stop(self) -> None:
        """Stop web UI server."""
        self._running = False
        if self._server:
            self._server.should_exit = True
