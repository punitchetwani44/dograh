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
from typing import Dict, Optional, Set
from urllib.parse import urlparse

import aiohttp
import redis.asyncio as aioredis
import websockets
from loguru import logger

from api.constants import REDIS_URL
from api.db import db_client
from api.enums import OrganizationConfigurationKey

# Redis key pattern and TTL for channel-to-run mapping
_CHANNEL_KEY_PREFIX = "ari:channel:"
_CHANNEL_KEY_TTL = 3600  # 1 hour safety expiry


class ARIConnection:
    """Manages a single ARI WebSocket connection for an organization."""

    def __init__(
        self,
        organization_id: int,
        ari_endpoint: str,
        app_name: str,
        app_password: str,
        ws_client_name: str = "",
    ):
        self.organization_id = organization_id
        self.ari_endpoint = ari_endpoint.rstrip("/")
        self.app_name = app_name
        self.app_password = app_password
        self.ws_client_name = ws_client_name

        self._ws: Optional[websockets.ClientConnection] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._reconnect_delay = 1  # Start with 1 second
        self._max_reconnect_delay = 300  # Max 300 seconds
        self._ping_interval = 30  # Send ping every 30 seconds

        # Redis client for channel-to-run reverse mapping (lazy init)
        self._redis_client: Optional[aioredis.Redis] = None

    async def _get_redis(self) -> aioredis.Redis:
        """Get Redis client instance (lazy init)."""
        if not self._redis_client:
            self._redis_client = await aioredis.from_url(
                REDIS_URL, decode_responses=True
            )
        return self._redis_client

    async def _set_channel_run(self, channel_id: str, workflow_run_id: str):
        """Store channel_id -> workflow_run_id mapping in Redis."""
        r = await self._get_redis()
        await r.set(
            f"{_CHANNEL_KEY_PREFIX}{channel_id}",
            workflow_run_id,
            ex=_CHANNEL_KEY_TTL,
        )

    async def _get_channel_run(self, channel_id: str) -> Optional[str]:
        """Look up workflow_run_id for a channel_id from Redis."""
        r = await self._get_redis()
        return await r.get(f"{_CHANNEL_KEY_PREFIX}{channel_id}")

    async def _delete_channel_run(self, *channel_ids: str):
        """Delete channel-to-run mapping(s) from Redis."""
        if not channel_ids:
            return
        r = await self._get_redis()
        keys = [f"{_CHANNEL_KEY_PREFIX}{cid}" for cid in channel_ids]
        await r.delete(*keys)

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

            # Parse args to extract workflow context
            args_dict = {}
            for arg in app_args:
                for pair in arg.split(","):
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        args_dict[key.strip()] = value.strip()

            workflow_run_id = args_dict.get("workflow_run_id")
            workflow_id = args_dict.get("workflow_id")
            user_id = args_dict.get("user_id")

            if not workflow_run_id or not workflow_id or not user_id:
                logger.warning(
                    f"[ARI org={self.organization_id}] StasisStart missing required args: "
                    f"workflow_run_id={workflow_run_id}, workflow_id={workflow_id}, user_id={user_id}"
                )
                return

            # Start pipeline connection in background task
            asyncio.create_task(
                self._handle_stasis_start(
                    channel_id, channel_state, workflow_run_id, workflow_id, user_id
                )
            )

        elif event_type == "StasisEnd":
            logger.info(
                f"[ARI org={self.organization_id}] StasisEnd: channel={channel_id}"
            )
            workflow_run_id = await self._get_channel_run(channel_id)
            if workflow_run_id:
                asyncio.create_task(
                    self._handle_stasis_end(channel_id, workflow_run_id)
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

    async def _ari_request(self, method: str, path: str, **kwargs) -> dict:
        """Make an ARI REST API request."""

        url = f"{self.ari_endpoint}/ari{path}"
        auth = aiohttp.BasicAuth(self.app_name, self.app_password)

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, auth=auth, **kwargs) as response:
                response_text = await response.text()
                if response.status not in (200, 201, 204):
                    logger.error(
                        f"[ARI org={self.organization_id}] REST API error: "
                        f"{method} {path} -> {response.status}: {response_text}"
                    )
                    return {}
                if response_text:
                    return json.loads(response_text)
                return {}

    async def _answer_channel(self, channel_id: str) -> bool:
        """Answer an ARI channel."""
        await self._ari_request("POST", f"/channels/{channel_id}/answer")
        # answer returns 204 No Content on success, so empty dict is OK
        logger.info(f"[ARI org={self.organization_id}] Answered channel {channel_id}")
        return True

    async def _create_external_media(
        self,
        workflow_id: str,
        user_id: str,
        workflow_run_id: str,
    ) -> str:
        """Create an external media channel via chan_websocket.

        Uses ARI externalMedia with transport=websocket so Asterisk connects
        to our backend over WebSocket (via websocket_client.conf).
        Dynamic routing params are passed as URI query params via v() in transport_data.
        """
        # v() appends URI query params to the websocket_client.conf URL
        # e.g. wss://api.dograh.com/ws/ari?workflow_id=1&user_id=2&workflow_run_id=3
        transport_data = (
            f"v(workflow_id={workflow_id},"
            f"user_id={user_id},"
            f"workflow_run_id={workflow_run_id})"
        )

        result = await self._ari_request(
            "POST",
            "/channels/externalMedia",
            params={
                "app": self.app_name,
                "external_host": self.ws_client_name,
                "format": "ulaw",
                "transport": "websocket",
                "encapsulation": "none",
                "connection_type": "client",
                "direction": "both",
                "transport_data": transport_data,
            },
        )
        ext_channel_id = result.get("id", "")
        if ext_channel_id:
            logger.info(
                f"[ARI org={self.organization_id}] Created external media channel: {ext_channel_id}"
            )
        return ext_channel_id

    async def _create_bridge_and_add_channels(self, channel_ids: list) -> str:
        """Create a bridge and add channels to it."""
        # Create bridge
        bridge_result = await self._ari_request(
            "POST",
            "/bridges",
            params={"type": "mixing", "name": f"bridge-{channel_ids[0]}"},
        )
        bridge_id = bridge_result.get("id", "")
        if not bridge_id:
            logger.error(f"[ARI org={self.organization_id}] Failed to create bridge")
            return ""

        # Add channels to bridge
        await self._ari_request(
            "POST",
            f"/bridges/{bridge_id}/addChannel",
            params={"channel": ",".join(channel_ids)},
        )
        logger.info(
            f"[ARI org={self.organization_id}] Bridge {bridge_id} created with channels: {channel_ids}"
        )
        return bridge_id

    async def _handle_stasis_start(
        self,
        channel_id: str,
        channel_state: str,
        workflow_run_id: str,
        workflow_id: str,
        user_id: str,
    ):
        """Handle StasisStart by answering (if needed), creating external media, and bridging."""
        try:
            # 1. Only answer the channel if it's not already up
            # For outbound calls, the channel enters Stasis in "Up" state
            # after the remote party answers — no need to answer again.
            # For inbound calls, the channel may be in "Ring" state.
            if channel_state != "Up":
                await self._answer_channel(channel_id)

            logger.info(
                f"[ARI org={self.organization_id}] Setting up external media for "
                f"channel {channel_id} via ws_client={self.ws_client_name}"
            )

            # 2. Track channel for StasisEnd cleanup (Redis)
            await self._set_channel_run(channel_id, workflow_run_id)

            # 3. Create external media channel via chan_websocket
            # Asterisk connects to our backend using websocket_client.conf config,
            # with routing params appended as URI query params via v()
            ext_channel_id = await self._create_external_media(
                workflow_id, user_id, workflow_run_id
            )
            if not ext_channel_id:
                logger.error(
                    f"[ARI org={self.organization_id}] Failed to create external media for {channel_id}"
                )
                return

            # 4. Track ext channel for StasisEnd cleanup (Redis)
            await self._set_channel_run(ext_channel_id, workflow_run_id)

            # 5. Bridge the call channel with the external media channel
            bridge_id = await self._create_bridge_and_add_channels(
                [channel_id, ext_channel_id]
            )
            if not bridge_id:
                logger.error(
                    f"[ARI org={self.organization_id}] Failed to bridge channels"
                )
                return

            # 6. Store ARI resource IDs in gathered_context for cleanup/debugging
            await db_client.update_workflow_run(
                run_id=int(workflow_run_id),
                gathered_context={
                    "ext_channel_id": ext_channel_id,
                    "bridge_id": bridge_id,
                },
            )
        except Exception as e:
            logger.error(
                f"[ARI org={self.organization_id}] Error handling StasisStart "
                f"for channel {channel_id}: {e}"
            )

    async def _handle_stasis_end(self, channel_id: str, workflow_run_id: str):
        """Full teardown of all ARI resources on any channel's StasisEnd.

        When either channel (call or ext) fires StasisEnd, we tear down
        the bridge and both channels — like endConferenceOnExit.
        """
        try:
            workflow_run = await db_client.get_workflow_run_by_id(int(workflow_run_id))
            if not workflow_run or not workflow_run.gathered_context:
                logger.warning(
                    f"[ARI org={self.organization_id}] StasisEnd: no gathered_context "
                    f"for workflow_run {workflow_run_id}"
                )
                # Still clean up the Redis key for the channel that ended
                await self._delete_channel_run(channel_id)
                return

            ctx = workflow_run.gathered_context
            call_id = ctx.get("call_id")
            ext_channel_id = ctx.get("ext_channel_id")
            bridge_id = ctx.get("bridge_id")

            # Delete the bridge first (removes channels from it)
            if bridge_id:
                await self._delete_bridge(bridge_id)

            # Destroy both channels, skipping the one that already ended
            for cid in (call_id, ext_channel_id):
                if cid and cid != channel_id:
                    await self._delete_channel(cid)

            # Clean up all Redis reverse-mapping keys
            keys_to_delete = [
                cid for cid in (call_id, ext_channel_id, channel_id) if cid
            ]
            if keys_to_delete:
                await self._delete_channel_run(*keys_to_delete)

            logger.info(
                f"[ARI org={self.organization_id}] StasisEnd full teardown for "
                f"channel={channel_id}, call={call_id}, ext={ext_channel_id}, bridge={bridge_id}"
            )
        except Exception as e:
            logger.error(
                f"[ARI org={self.organization_id}] Error cleaning up StasisEnd "
                f"for channel {channel_id}: {e}"
            )

    async def _delete_bridge(self, bridge_id: str):
        """Delete an ARI bridge. Ignores 404 (already gone)."""

        url = f"{self.ari_endpoint}/ari/bridges/{bridge_id}"
        auth = aiohttp.BasicAuth(self.app_name, self.app_password)

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, auth=auth) as response:
                if response.status in (200, 204):
                    logger.info(
                        f"[ARI org={self.organization_id}] Deleted bridge {bridge_id}"
                    )
                elif response.status == 404:
                    logger.debug(
                        f"[ARI org={self.organization_id}] Bridge {bridge_id} already gone"
                    )
                else:
                    text = await response.text()
                    logger.error(
                        f"[ARI org={self.organization_id}] Failed to delete bridge {bridge_id}: "
                        f"{response.status} {text}"
                    )

    async def _delete_channel(self, channel_id: str):
        """Delete (hang up) an ARI channel. Ignores 404 (already gone)."""

        url = f"{self.ari_endpoint}/ari/channels/{channel_id}"
        auth = aiohttp.BasicAuth(self.app_name, self.app_password)

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, auth=auth) as response:
                if response.status in (200, 204):
                    logger.info(
                        f"[ARI org={self.organization_id}] Deleted channel {channel_id}"
                    )
                elif response.status == 404:
                    logger.debug(
                        f"[ARI org={self.organization_id}] Channel {channel_id} already gone"
                    )
                else:
                    text = await response.text()
                    logger.error(
                        f"[ARI org={self.organization_id}] Failed to delete channel {channel_id}: "
                        f"{response.status} {text}"
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
            ws_client_name = config["ws_client_name"]

            conn = ARIConnection(
                org_id, ari_endpoint, app_name, app_password, ws_client_name
            )
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
            ws_client_name = value.get("ws_client_name", "")

            if not all([ari_endpoint, app_name, app_password]):
                logger.warning(
                    f"[ARI Manager] Incomplete ARI config for org {org_id}, skipping"
                )
                continue

            if not ws_client_name:
                logger.warning(
                    f"[ARI Manager] Missing ws_client_name for org {org_id}, "
                    f"externalMedia WebSocket won't work"
                )

            configs.append(
                {
                    "organization_id": org_id,
                    "ari_endpoint": ari_endpoint,
                    "app_name": app_name,
                    "app_password": app_password,
                    "ws_client_name": ws_client_name,
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
