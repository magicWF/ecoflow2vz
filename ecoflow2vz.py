#!/usr/bin/env python3
"""
ecoFlow2vz — EcoFlow Stream MQTT → Volkszähler Bridge

Receives live data from the EcoFlow Developer API (MQTT) and forwards it to
an instance of Volkszähler.

Supported devices: EcoFlow Stream AC / Pro / Ultra / Ultra X
"""

import hashlib
import hmac
import json
import logging
import random
import signal
import ssl
import string
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
import yaml

# =============================================================================
# Constants
# =============================================================================

VERSION = "1.0.0"
CONFIG_FILE = Path(__file__).parent / "config.yaml"

# EcoFlow MQTT key → internal name
FIELD_MAPPING = {
    "powGetPv":               "pv1_power",
    "powGetPv2":              "pv2_power",
    "powGetPv3":              "pv3_power",
    "powGetPv4":              "pv4_power",
    "powGetPvSum":            "pv_total",
    "powGetBpCms":            "battery_power",
    "sysGridConnectionPower": "grid_power",
    "cmsBattSoc":             "battery_soc",
    "bmsBattSoc":             "_bmsBattSoc",       # Fallback for SoC
    "cmsBattFullEnergy":      "_battFullEnergy",    # For stored energy calculation
}

# Channels to push directly to Volkszähler (without a leading underscore)
PUSH_CHANNELS = [
    "pv1_power",
    "pv2_power",
    "pv3_power",
    "pv4_power",
    "pv_total",
    "battery_power",
    "grid_power",
    "battery_soc",
    "battery_energy",
]

logger = logging.getLogger("ecoflow2vz")


# =============================================================================
# Load configuration
# =============================================================================

def load_config(path: Path = CONFIG_FILE) -> dict:
    """Load the YAML configuration."""
    if not path.exists():
        logger.error("Configuration file not found: %s", path)
        logger.error("Copy config.example.yaml to config.yaml and adjust the values.")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Validation
    required_keys = [
        ("ecoflow", "access_key"),
        ("ecoflow", "secret_key"),
        ("ecoflow", "device_sn"),
        ("volkszaehler", "base_url"),
    ]
    for section, key in required_keys:
        value = config.get(section, {}).get(key, "")
        if not value or value.startswith("YOUR_"):
            logger.error("Configuration incomplete: %s.%s must be set.", section, key)
            sys.exit(1)

    return config


# =============================================================================
# EcoFlow API Authentication
# =============================================================================

class EcoFlowAuth:
    """Fetch temporary MQTT credentials from the EcoFlow Developer API."""

    def __init__(self, access_key: str, secret_key: str, api_host: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.api_host = api_host

    def _generate_nonce(self, length: int = 16) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    def _generate_signature(self, nonce: str, timestamp: str) -> str:
        """HMAC-SHA256 signature according to the EcoFlow Developer API specification."""
        message = f"accessKey={self.access_key}&nonce={nonce}&timestamp={timestamp}"
        sign = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return sign

    def get_mqtt_credentials(self) -> dict:
        """
        Retrieve temporary MQTT credentials.

        Returns:
            dict with keys: url, port, certificateAccount, certificatePassword
        """
        nonce = self._generate_nonce()
        timestamp = str(int(time.time() * 1000))
        sign = self._generate_signature(nonce, timestamp)

        url = f"https://{self.api_host}/iot-open/sign/certification"
        headers = {
            "accessKey": self.access_key,
            "nonce": nonce,
            "timestamp": timestamp,
            "sign": sign,
        }

        logger.debug("Fetch MQTT credentials from %s", url)

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Error retrieving MQTT credentials: %s", e)
            raise

        data = resp.json()

        if data.get("code") != "0" and data.get("code") != 0:
            error_msg = data.get("message", "Unknown error")
            logger.error("EcoFlow API error: %s (code: %s)", error_msg, data.get("code"))
            raise RuntimeError(f"EcoFlow API error: {error_msg}")

        creds = data["data"]
        logger.info(
            "MQTT credentials received — broker: %s:%s",
            creds.get("url"),
            creds.get("port"),
        )
        return creds


# =============================================================================
# Volkszähler Client
# =============================================================================

class VolkszaehlerClient:
    """Push measurement values to the Volkszähler middleware REST API."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def push_value(self, uuid: str, value: float, timestamp_ms: int | None = None) -> bool:
        """
        Send a measurement to a Volkszähler channel.

        Args:
            uuid: Channel UUID
            value: Measurement value
            timestamp_ms: Timestamp in milliseconds (None = server time)

        Returns:
            True on success, False on error
        """
        if not uuid:
            return False

        url = f"{self.base_url}/data/{uuid}.json"
        params = {"operation": "add", "value": value}
        if timestamp_ms is not None:
            params["ts"] = timestamp_ms

        try:
            resp = self.session.post(url, params=params, timeout=10)
            resp.raise_for_status()
            logger.debug("VZ push OK: %s = %s", uuid, value)
            return True
        except requests.RequestException as e:
            logger.warning("VZ push failed for %s: %s", uuid, e)
            return False

    def close(self):
        self.session.close()


# =============================================================================
# EcoFlow → Volkszähler Bridge
# =============================================================================

class EcoFlow2VZ:
    """Main class: connects EcoFlow MQTT to Volkszähler."""

    def __init__(self, config: dict):
        self.config = config
        self.running = False

        # EcoFlow config
        eco_cfg = config["ecoflow"]
        self.device_sn = eco_cfg["device_sn"]
        self.auth = EcoFlowAuth(
            access_key=eco_cfg["access_key"],
            secret_key=eco_cfg["secret_key"],
            api_host=eco_cfg.get("api_host", "api-e.ecoflow.com"),
        )

        # Volkszähler client
        vz_cfg = config["volkszaehler"]
        self.vz = VolkszaehlerClient(base_url=vz_cfg["base_url"])

        # Channel UUIDs (non-empty only)
        self.channel_uuids: dict[str, str] = {}
        for name, uuid in config.get("channels", {}).items():
            if uuid and isinstance(uuid, str) and uuid.strip():
                self.channel_uuids[name] = uuid.strip()

        if not self.channel_uuids:
            logger.warning("No Volkszähler channel UUIDs configured!")

        # Optional settings
        self.push_interval = config.get("push_interval_seconds", 0)
        self.battery_capacity_wh = config.get("battery_capacity_wh", 3840)
        self.last_push_time: float = 0.0

        # Internal state — stores the most recently received values
        self.state: dict[str, float] = {}

        # MQTT client
        self.mqtt_client: mqtt.Client | None = None
        self.mqtt_username: str = ""

        # Reconnect
        self._reconnect_delay = 5
        self._max_reconnect_delay = 300

    def _get_client_id(self) -> str:
        """Fixed client ID (max. 10 unique IDs per day at EcoFlow)."""
        return f"ecoflow2vz-{self.device_sn}"

    def _get_topic(self) -> str:
        """MQTT topic for live data (public API)."""
        return f"/open/{self.mqtt_username}/{self.device_sn}/quota"

    # -------------------------------------------------------------------------
    # MQTT Callbacks
    # -------------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            topic = self._get_topic()
            logger.info("MQTT connected — subscribing to: %s", topic)
            client.subscribe(topic)
            self._reconnect_delay = 5  # Reset
        else:
            logger.error("MQTT connection failed: reason_code=%s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        if self.running:
            logger.warning("MQTT connection lost (reason_code=%s). Reconnecting...", reason_code)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("Invalid MQTT message: %s", e)
            return

        logger.debug("MQTT message received: %d fields", len(payload))
        self._process_message(payload)

    # -------------------------------------------------------------------------
    # Data processing
    # -------------------------------------------------------------------------

    def _process_message(self, payload: dict):
        """Process an MQTT message and update the state."""
        updated_channels: set[str] = set()
        now_ms = int(time.time() * 1000)

        # Extract relevant fields and write them to the state
        for mqtt_key, internal_name in FIELD_MAPPING.items():
            if mqtt_key in payload:
                value = payload[mqtt_key]
                if isinstance(value, (int, float)):
                    self.state[internal_name] = float(value)
                    if not internal_name.startswith("_"):
                        updated_channels.add(internal_name)

        # SoC fallback: use bmsBattSoc if cmsBattSoc is not present
        if "battery_soc" not in self.state and "_bmsBattSoc" in self.state:
            self.state["battery_soc"] = self.state["_bmsBattSoc"]
            updated_channels.add("battery_soc")

        # Calculate stored energy
        soc = self.state.get("battery_soc")
        if soc is not None:
            full_energy = self.state.get("_battFullEnergy", self.battery_capacity_wh)
            stored_energy = full_energy * soc / 100.0
            self.state["battery_energy"] = stored_energy
            updated_channels.add("battery_energy")

        # Check push throttle
        if self.push_interval > 0:
            now = time.time()
            if now - self.last_push_time < self.push_interval:
                logger.debug("Push skipped (throttle: %.1fs remaining)",
                             self.push_interval - (now - self.last_push_time))
                return
            self.last_push_time = now

        # Push to Volkszähler
        self._push_to_vz(updated_channels, now_ms)

    def _push_to_vz(self, updated_channels: set[str], timestamp_ms: int):
        """Push updated values to Volkszähler."""
        push_count = 0
        error_count = 0

        for channel_name in PUSH_CHANNELS:
            # Only push if a UUID is configured and a value is present
            uuid = self.channel_uuids.get(channel_name)
            if not uuid:
                continue

            value = self.state.get(channel_name)
            if value is None:
                continue

            # Only push changed channels when push_interval == 0
            if self.push_interval == 0 and channel_name not in updated_channels:
                continue

            if self.vz.push_value(uuid, round(value, 2), timestamp_ms):
                push_count += 1
            else:
                error_count += 1

        if push_count > 0 or error_count > 0:
            logger.info(
                "VZ push: %d OK, %d failed | PV: %.0fW, Battery: %.0fW (%.0f%%), Grid: %.0fW",
                push_count,
                error_count,
                self.state.get("pv_total", 0),
                self.state.get("battery_power", 0),
                self.state.get("battery_soc", 0),
                self.state.get("grid_power", 0),
            )

    # -------------------------------------------------------------------------
    # MQTT Connection
    # -------------------------------------------------------------------------

    def _connect_mqtt(self) -> bool:
        """Authenticate with EcoFlow and connect via MQTT."""
        try:
            creds = self.auth.get_mqtt_credentials()
        except Exception as e:
            logger.error("Authentication failed: %s", e)
            return False

        broker_url = creds.get("url", "mqtt.ecoflow.com")
        broker_port = int(creds.get("port", 8883))
        username = creds["certificateAccount"]
        password = creds["certificatePassword"]
        self.mqtt_username = username

        # Create MQTT client (paho-mqtt v2 API)
        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._get_client_id(),
            protocol=mqtt.MQTTv311,
        )

        # TLS
        self.mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)

        # Credentials
        self.mqtt_client.username_pw_set(username, password)

        # Callbacks
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

        # Connect
        logger.info("Connecting to MQTT broker: %s:%d", broker_url, broker_port)
        try:
            self.mqtt_client.connect(broker_url, broker_port, keepalive=60)
            return True
        except Exception as e:
            logger.error("MQTT connection failed: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------------

    def run(self):
        """Start the bridge service."""
        self.running = True

        logger.info("=" * 60)
        logger.info("ecoFlow2vz v%s started", VERSION)
        logger.info("Device: %s", self.device_sn)
        logger.info("VZ URL: %s", self.config["volkszaehler"]["base_url"])
        logger.info(
            "Configured channels: %s",
            ", ".join(self.channel_uuids.keys()) if self.channel_uuids else "NONE",
        )
        logger.info(
            "Push interval: %s",
            f"{self.push_interval}s" if self.push_interval > 0 else "immediately (every message)",
        )
        logger.info("=" * 60)

        while self.running:
            if not self._connect_mqtt():
                logger.warning(
                    "Retrying connection in %ds...", self._reconnect_delay
                )
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )
                continue

            # MQTT loop (blocks until disconnected or stop())
            try:
                self.mqtt_client.loop_forever()
            except Exception as e:
                logger.error("MQTT loop error: %s", e)

            # After disconnect: reconnect with delay
            if self.running:
                logger.info("Reconnect in %ds...", self._reconnect_delay)
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

        logger.info("ecoFlow2vz stopped.")

    def stop(self):
        """Stop the bridge service."""
        logger.info("Stopping ecoFlow2vz...")
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        self.vz.close()


# =============================================================================
# Main
# =============================================================================

def main():
    # Load configuration
    config = load_config()

    # Set up logging
    log_level = config.get("log_level", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    logger.info("Configuration loaded from %s", CONFIG_FILE)

    # Start bridge
    bridge = EcoFlow2VZ(config)

    # Signal handler for graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received signal %s", signal.Signals(signum).name)
        bridge.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    bridge.run()


if __name__ == "__main__":
    main()
