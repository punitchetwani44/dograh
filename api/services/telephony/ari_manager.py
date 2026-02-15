"""ARI WebSocket Event Listener Manager.

Standalone process that:
1. Queries the database for all organizations with ARI telephony configuration
2. Creates WebSocket connections to each ARI instance
3. Handles reconnection logic with exponential backoff
4. Processes StasisStart/StasisEnd events
5. Periodically refreshes configuration to detect new/removed organizations
"""

from api.logging_config import setup_logging

setup_logging()

import asyncio
import json
import signal
from typing import Any, Dict, Optional, Set
from urllib.parse import urlparse

import websockets
from loguru import logger

from api.db import db_client
from api.enums import OrganizationConfigurationKey


class ARIConnection:
    """Manages a single ARI WebSocket connection for an organization."""

    def __init__(
        self,
        organization_id: int,
        ari_endpoint: str,
        app_name: str,
        app_password: str,
    ):
        self.organization_id = organization_id
        self.ari_endpoint = ari_endpoint.rstrip("/")
        self.app_name = app_name
        self.app_password = app_password

        self._ws: Optional[websockets.ClientConnection] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_delay = 1  # Start with 1 second
        self._max_reconnect_delay = 300  # Max 300 seconds
        self._ping_interval = 30  # Send ping every 30 seconds

    @property
    def ws_url(self) -> str:
        """Build the ARI WebSocket URL."""
        parsed = urlparse(self.ari_endpoint)
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        return (
            f"{ws_scheme}://{parsed.netloc}/ari/events"
            f"?api_key={self.app_name}:{self.app_password}"
            f"&app={self.app_name}"
            f"&subscribeAll=true"
        )

    @property
    def connection_key(self) -> str:
        """Unique key for this connection based on config."""
        return f"{self.organization_id}:{self.ari_endpoint}:{self.app_name}"

    async def start(self):
        """Start the WebSocket connection in a background task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._connection_loop())
        logger.info(
            f"[ARI org={self.organization_id}] Started connection to {self.ari_endpoint}"
        )

    async def stop(self):
        """Stop the WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(
            f"[ARI org={self.organization_id}] Stopped connection to {self.ari_endpoint}"
        )

    async def _connection_loop(self):
        """Main connection loop with reconnection logic."""
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                logger.warning(
                    f"[ARI org={self.organization_id}] Connection error: {e}. "
                    f"Reconnecting in {self._reconnect_delay}s..."
                )
                await asyncio.sleep(self._reconnect_delay)
                # Exponential backoff
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

    async def _connect_and_listen(self):
        """Establish WebSocket connection and listen for events."""
        ws_url = self.ws_url
        logger.info(
            f"[ARI org={self.organization_id}] Connecting to {self.ari_endpoint}..."
        )

        async for ws in websockets.connect(
            ws_url,
            ping_interval=self._ping_interval,
            ping_timeout=10,
            close_timeout=5,
        ):
            try:
                self._ws = ws

                # Reset reconnect delay on successful connection
                self._reconnect_delay = 1

                logger.info(
                    f"[ARI org={self.organization_id}] WebSocket connected to {self.ari_endpoint}"
                )

                async for message in ws:
                    if not self._running:
                        return

                    if isinstance(message, str):
                        await self._handle_event(message)
                    else:
                        logger.debug(
                            f"[ARI org={self.organization_id}] Received binary message, ignoring"
                        )

            except websockets.ConnectionClosed as e:
                if not self._running:
                    return
                logger.warning(
                    f"[ARI org={self.organization_id}] WebSocket closed: "
                    f"code={e.code}, reason={e.reason}. Reconnecting..."
                )
                continue
            finally:
                self._ws = None

    async def _handle_event(self, raw_data: str):
        """Handle an ARI WebSocket event."""
        try:
            event = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.warning(
                f"[ARI org={self.organization_id}] Invalid JSON: {raw_data[:200]}"
            )
            return

        event_type = event.get("type", "unknown")
        channel = event.get("channel", {})
        channel_id = channel.get("id", "unknown")
        channel_state = channel.get("state", "unknown")

        if event_type == "StasisStart":
            app_args = event.get("args", [])
            caller = channel.get("caller", {})
            logger.info(
                f"[ARI org={self.organization_id}] StasisStart: "
                f"channel={channel_id}, state={channel_state}, "
                f"caller={caller.get('number', 'unknown')}, "
                f"args={app_args}"
            )
            # TODO: This is where we'll integrate with the pipeline
            # For now, just log the event

        elif event_type == "StasisEnd":
            logger.info(
                f"[ARI org={self.organization_id}] StasisEnd: "
                f"channel={channel_id}"
            )

        elif event_type == "ChannelStateChange":
            logger.debug(
                f"[ARI org={self.organization_id}] ChannelStateChange: "
                f"channel={channel_id}, state={channel_state}"
            )

        elif event_type == "ChannelDestroyed":
            cause = channel.get("cause", 0)
            cause_txt = channel.get("cause_txt", "unknown")
            logger.info(
                f"[ARI org={self.organization_id}] ChannelDestroyed: "
                f"channel={channel_id}, cause={cause} ({cause_txt})"
            )

        elif event_type == "ChannelDtmfReceived":
            digit = event.get("digit", "")
            logger.debug(
                f"[ARI org={self.organization_id}] DTMF: "
                f"channel={channel_id}, digit={digit}"
            )

        else:
            logger.debug(
                f"[ARI org={self.organization_id}] Event: {event_type} "
                f"channel={channel_id}"
            )


class ARIManager:
    """Manages ARI WebSocket connections for all organizations."""

    def __init__(self):
        self._connections: Dict[str, ARIConnection] = {}  # key -> connection
        self._running = False
        self._config_refresh_interval = 60  # Check for config changes every 60 seconds

    async def start(self):
        """Start the ARI manager."""
        self._running = True
        logger.info("ARI Manager starting...")

        # Initial load of configurations
        await self._refresh_connections()

        # Start periodic config refresh
        while self._running:
            await asyncio.sleep(self._config_refresh_interval)
            if self._running:
                await self._refresh_connections()

    async def stop(self):
        """Stop all connections and clean up."""
        self._running = False
        logger.info("ARI Manager stopping...")

        # Stop all connections
        for conn in self._connections.values():
            await conn.stop()
        self._connections.clear()
        logger.info("ARI Manager stopped")

    async def _refresh_connections(self):
        """
        Refresh connections based on current database configurations.

        - Starts new connections for new ARI configurations
        - Stops connections for removed configurations
        - Restarts connections if configuration changed
        """
        try:
            active_configs = await self._load_ari_configs()
        except Exception as e:
            logger.error(f"Failed to load ARI configurations: {e}")
            return

        active_keys: Set[str] = set()

        for config in active_configs:
            org_id = config["organization_id"]
            ari_endpoint = config["ari_endpoint"]
            app_name = config["app_name"]
            app_password = config["app_password"]

            conn = ARIConnection(org_id, ari_endpoint, app_name, app_password)
            key = conn.connection_key

            active_keys.add(key)

            if key not in self._connections:
                # New configuration - start connection
                logger.info(
                    f"[ARI Manager] New ARI config for org {org_id}: {ari_endpoint}"
                )
                self._connections[key] = conn
                await conn.start()
            else:
                # Existing configuration - check if password changed
                existing = self._connections[key]
                if existing.app_password != app_password:
                    logger.info(
                        f"[ARI Manager] Config changed for org {org_id}, reconnecting..."
                    )
                    await existing.stop()
                    self._connections[key] = conn
                    await conn.start()

        # Stop connections for removed configurations
        removed_keys = set(self._connections.keys()) - active_keys
        for key in removed_keys:
            conn = self._connections.pop(key)
            logger.info(
                f"[ARI Manager] Removing connection for org {conn.organization_id}"
            )
            await conn.stop()

        if active_configs:
            logger.info(
                f"[ARI Manager] Active connections: {len(self._connections)} "
                f"(orgs: {[c['organization_id'] for c in active_configs]})"
            )
        else:
            logger.debug("[ARI Manager] No ARI configurations found")

    async def _load_ari_configs(self) -> list:
        """Load all ARI telephony configurations from the database."""
        rows = await db_client.get_configurations_by_provider(
            OrganizationConfigurationKey.TELEPHONY_CONFIGURATION.value, "ari"
        )

        configs = []
        for row in rows:
            org_id = row["organization_id"]
            value = row["value"]

            ari_endpoint = value.get("ari_endpoint")
            app_name = value.get("app_name")
            app_password = value.get("app_password")

            if not all([ari_endpoint, app_name, app_password]):
                logger.warning(
                    f"[ARI Manager] Incomplete ARI config for org {org_id}, skipping"
                )
                continue

            configs.append(
                {
                    "organization_id": org_id,
                    "ari_endpoint": ari_endpoint,
                    "app_name": app_name,
                    "app_password": app_password,
                }
            )

        return configs


async def main():
    """Entry point for the ARI manager process."""
    manager = ARIManager()

    # Handle graceful shutdown
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Start manager in background
    manager_task = asyncio.create_task(manager.start())

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Clean up
    await manager.stop()
    manager_task.cancel()
    try:
        await manager_task
    except asyncio.CancelledError:
        pass

    logger.info("ARI Manager exited cleanly")


if __name__ == "__main__":
    asyncio.run(main())
