"""
@FileName: server.py
@Description: UI 服务器管理
@Author: HiPeng
@Time: 2026/4/1 18:19
"""

import threading
import webbrowser
from typing import Optional, Any
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse


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

        print(f"✓ Web UI started at http://{self.host}:{self.port}")

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
            return HTMLResponse("""
            <html>
                <head><title>Task Scheduler</title></head>
                <body>
                    <h1>Task Scheduler Dashboard</h1>
                    <p>Web UI is running. API available at <code>/api/stats</code></p>
                </body>
            </html>
            """)

        @app.get("/api/stats")
        async def get_stats():
            return self.scheduler.get_stats()

        return app

    def stop(self) -> None:
        """Stop web UI server."""
        self._running = False
        if self._server:
            self._server.should_exit = True