#!/usr/bin/env python3
"""
ecoFlow2vz — EcoFlow Stream MQTT → Volkszähler Bridge

Empfängt Live-Daten von der EcoFlow Developer API (MQTT) und leitet
sie an eine Volkszähler-Instanz weiter.

Unterstützte Geräte: EcoFlow Stream AC / Pro / Ultra / Ultra X
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
# Konstanten
# =============================================================================

VERSION = "1.0.0"
CONFIG_FILE = Path(__file__).parent / "config.yaml"

# EcoFlow MQTT Key → interner Name
FIELD_MAPPING = {
    "powGetPv":               "pv1_power",
    "powGetPv2":              "pv2_power",
    "powGetPv3":              "pv3_power",
    "powGetPv4":              "pv4_power",
    "powGetPvSum":            "pv_total",
    "powGetBpCms":            "battery_power",
    "sysGridConnectionPower": "grid_power",
    "cmsBattSoc":             "battery_soc",
    "bmsBattSoc":             "_bmsBattSoc",       # Fallback für SoC
    "cmsBattFullEnergy":      "_battFullEnergy",    # Für Berechnung stored energy
}

# Kanäle, die direkt an VZ gepusht werden (ohne führenden Unterstrich)
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
# Konfiguration laden
# =============================================================================

def load_config(path: Path = CONFIG_FILE) -> dict:
    """Lädt die YAML-Konfiguration."""
    if not path.exists():
        logger.error("Konfigurationsdatei nicht gefunden: %s", path)
        logger.error("Kopiere config.example.yaml nach config.yaml und passe die Werte an.")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Validierung
    required_keys = [
        ("ecoflow", "access_key"),
        ("ecoflow", "secret_key"),
        ("ecoflow", "device_sn"),
        ("volkszaehler", "base_url"),
    ]
    for section, key in required_keys:
        value = config.get(section, {}).get(key, "")
        if not value or value.startswith("YOUR_"):
            logger.error("Konfiguration unvollständig: %s.%s muss gesetzt werden.", section, key)
            sys.exit(1)

    return config


# =============================================================================
# EcoFlow API Authentifizierung
# =============================================================================

class EcoFlowAuth:
    """Holt temporäre MQTT-Credentials über die EcoFlow Developer API."""

    def __init__(self, access_key: str, secret_key: str, api_host: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.api_host = api_host

    def _generate_nonce(self, length: int = 16) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    def _generate_signature(self, nonce: str, timestamp: str) -> str:
        """HMAC-SHA256 Signatur nach EcoFlow Developer API Spezifikation."""
        message = f"accessKey={self.access_key}&nonce={nonce}&timestamp={timestamp}"
        sign = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return sign

    def get_mqtt_credentials(self) -> dict:
        """
        Ruft temporäre MQTT-Credentials ab.

        Returns:
            dict mit keys: url, port, certificateAccount, certificatePassword
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

        logger.debug("Hole MQTT-Credentials von %s", url)

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Fehler beim Abrufen der MQTT-Credentials: %s", e)
            raise

        data = resp.json()

        if data.get("code") != "0" and data.get("code") != 0:
            error_msg = data.get("message", "Unbekannter Fehler")
            logger.error("EcoFlow API Fehler: %s (code: %s)", error_msg, data.get("code"))
            raise RuntimeError(f"EcoFlow API Fehler: {error_msg}")

        creds = data["data"]
        logger.info(
            "MQTT-Credentials erhalten — Broker: %s:%s",
            creds.get("url"),
            creds.get("port"),
        )
        return creds


# =============================================================================
# Volkszähler Client
# =============================================================================

class VolkszaehlerClient:
    """Pusht Messwerte an die Volkszähler Middleware REST API."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def push_value(self, uuid: str, value: float, timestamp_ms: int | None = None) -> bool:
        """
        Sendet einen Messwert an einen Volkszähler-Kanal.

        Args:
            uuid: Kanal-UUID
            value: Messwert
            timestamp_ms: Zeitstempel in Millisekunden (None = Server-Zeit)

        Returns:
            True bei Erfolg, False bei Fehler
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
            logger.debug("VZ Push OK: %s = %s", uuid, value)
            return True
        except requests.RequestException as e:
            logger.warning("VZ Push fehlgeschlagen für %s: %s", uuid, e)
            return False

    def close(self):
        self.session.close()


# =============================================================================
# EcoFlow → Volkszähler Bridge
# =============================================================================

class EcoFlow2VZ:
    """Hauptklasse: Verbindet EcoFlow MQTT mit Volkszähler."""

    def __init__(self, config: dict):
        self.config = config
        self.running = False

        # EcoFlow Config
        eco_cfg = config["ecoflow"]
        self.device_sn = eco_cfg["device_sn"]
        self.auth = EcoFlowAuth(
            access_key=eco_cfg["access_key"],
            secret_key=eco_cfg["secret_key"],
            api_host=eco_cfg.get("api_host", "api-e.ecoflow.com"),
        )

        # Volkszähler Client
        vz_cfg = config["volkszaehler"]
        self.vz = VolkszaehlerClient(base_url=vz_cfg["base_url"])

        # Kanal-UUIDs (nur nicht-leere)
        self.channel_uuids: dict[str, str] = {}
        for name, uuid in config.get("channels", {}).items():
            if uuid and isinstance(uuid, str) and uuid.strip():
                self.channel_uuids[name] = uuid.strip()

        if not self.channel_uuids:
            logger.warning("Keine Volkszähler-Kanal-UUIDs konfiguriert!")

        # Optionale Einstellungen
        self.push_interval = config.get("push_interval_seconds", 0)
        self.battery_capacity_wh = config.get("battery_capacity_wh", 3840)
        self.last_push_time: float = 0.0

        # Interner State — hält die zuletzt empfangenen Werte
        self.state: dict[str, float] = {}

        # MQTT Client
        self.mqtt_client: mqtt.Client | None = None
        self.mqtt_username: str = ""

        # Reconnect
        self._reconnect_delay = 5
        self._max_reconnect_delay = 300

    def _get_client_id(self) -> str:
        """Feste Client-ID (max. 10 unique IDs/Tag bei EcoFlow)."""
        return f"ecoflow2vz-{self.device_sn}"

    def _get_topic(self) -> str:
        """MQTT-Topic für Live-Daten (Public API)."""
        return f"/open/{self.mqtt_username}/{self.device_sn}/quota"

    # -------------------------------------------------------------------------
    # MQTT Callbacks
    # -------------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            topic = self._get_topic()
            logger.info("MQTT verbunden — Subscribe auf: %s", topic)
            client.subscribe(topic)
            self._reconnect_delay = 5  # Reset
        else:
            logger.error("MQTT Verbindung fehlgeschlagen: reason_code=%s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        if self.running:
            logger.warning("MQTT Verbindung getrennt (reason_code=%s). Reconnect...", reason_code)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("Ungültige MQTT-Nachricht: %s", e)
            return

        logger.debug("MQTT Nachricht empfangen: %d Felder", len(payload))
        self._process_message(payload)

    # -------------------------------------------------------------------------
    # Datenverarbeitung
    # -------------------------------------------------------------------------

    def _process_message(self, payload: dict):
        """Verarbeitet eine MQTT-Nachricht und aktualisiert den State."""
        updated_channels: set[str] = set()
        now_ms = int(time.time() * 1000)

        # Relevante Felder extrahieren und in State schreiben
        for mqtt_key, internal_name in FIELD_MAPPING.items():
            if mqtt_key in payload:
                value = payload[mqtt_key]
                if isinstance(value, (int, float)):
                    self.state[internal_name] = float(value)
                    if not internal_name.startswith("_"):
                        updated_channels.add(internal_name)

        # SoC Fallback: bmsBattSoc verwenden wenn cmsBattSoc nicht vorhanden
        if "battery_soc" not in self.state and "_bmsBattSoc" in self.state:
            self.state["battery_soc"] = self.state["_bmsBattSoc"]
            updated_channels.add("battery_soc")

        # Gespeicherte Energie berechnen
        soc = self.state.get("battery_soc")
        if soc is not None:
            full_energy = self.state.get("_battFullEnergy", self.battery_capacity_wh)
            stored_energy = full_energy * soc / 100.0
            self.state["battery_energy"] = stored_energy
            updated_channels.add("battery_energy")

        # Push-Throttle prüfen
        if self.push_interval > 0:
            now = time.time()
            if now - self.last_push_time < self.push_interval:
                logger.debug("Push übersprungen (Throttle: %.1fs verbleibend)",
                             self.push_interval - (now - self.last_push_time))
                return
            self.last_push_time = now

        # An Volkszähler pushen
        self._push_to_vz(updated_channels, now_ms)

    def _push_to_vz(self, updated_channels: set[str], timestamp_ms: int):
        """Pusht aktualisierte Werte an Volkszähler."""
        push_count = 0
        error_count = 0

        for channel_name in PUSH_CHANNELS:
            # Nur pushen, wenn UUID konfiguriert und Wert vorhanden
            uuid = self.channel_uuids.get(channel_name)
            if not uuid:
                continue

            value = self.state.get(channel_name)
            if value is None:
                continue

            # Nur geänderte Kanäle pushen (wenn push_interval == 0)
            if self.push_interval == 0 and channel_name not in updated_channels:
                continue

            if self.vz.push_value(uuid, round(value, 2), timestamp_ms):
                push_count += 1
            else:
                error_count += 1

        if push_count > 0 or error_count > 0:
            logger.info(
                "VZ Push: %d OK, %d Fehler | PV: %.0fW, Bat: %.0fW (%.0f%%), Grid: %.0fW",
                push_count,
                error_count,
                self.state.get("pv_total", 0),
                self.state.get("battery_power", 0),
                self.state.get("battery_soc", 0),
                self.state.get("grid_power", 0),
            )

    # -------------------------------------------------------------------------
    # MQTT Verbindung
    # -------------------------------------------------------------------------

    def _connect_mqtt(self) -> bool:
        """Authentifiziert sich bei EcoFlow und verbindet per MQTT."""
        try:
            creds = self.auth.get_mqtt_credentials()
        except Exception as e:
            logger.error("Authentifizierung fehlgeschlagen: %s", e)
            return False

        broker_url = creds.get("url", "mqtt.ecoflow.com")
        broker_port = int(creds.get("port", 8883))
        username = creds["certificateAccount"]
        password = creds["certificatePassword"]
        self.mqtt_username = username

        # MQTT Client erstellen (paho-mqtt v2 API)
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

        # Verbinden
        logger.info("Verbinde mit MQTT Broker: %s:%d", broker_url, broker_port)
        try:
            self.mqtt_client.connect(broker_url, broker_port, keepalive=60)
            return True
        except Exception as e:
            logger.error("MQTT Verbindung fehlgeschlagen: %s", e)
            return False

    # -------------------------------------------------------------------------
    # Hauptschleife
    # -------------------------------------------------------------------------

    def run(self):
        """Startet den Bridge-Service."""
        self.running = True

        logger.info("=" * 60)
        logger.info("ecoFlow2vz v%s gestartet", VERSION)
        logger.info("Gerät: %s", self.device_sn)
        logger.info("VZ-URL: %s", self.config["volkszaehler"]["base_url"])
        logger.info(
            "Konfigurierte Kanäle: %s",
            ", ".join(self.channel_uuids.keys()) if self.channel_uuids else "KEINE",
        )
        logger.info(
            "Push-Intervall: %s",
            f"{self.push_interval}s" if self.push_interval > 0 else "sofort (jede Nachricht)",
        )
        logger.info("=" * 60)

        while self.running:
            if not self._connect_mqtt():
                logger.warning(
                    "Erneuter Verbindungsversuch in %ds...", self._reconnect_delay
                )
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )
                continue

            # MQTT Loop (blockiert bis Verbindung verloren oder stop())
            try:
                self.mqtt_client.loop_forever()
            except Exception as e:
                logger.error("MQTT Loop Fehler: %s", e)

            # Nach Disconnect: Reconnect mit Delay
            if self.running:
                logger.info("Reconnect in %ds...", self._reconnect_delay)
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

        logger.info("ecoFlow2vz beendet.")

    def stop(self):
        """Stoppt den Bridge-Service."""
        logger.info("Stoppe ecoFlow2vz...")
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        self.vz.close()


# =============================================================================
# Main
# =============================================================================

def main():
    # Konfiguration laden
    config = load_config()

    # Logging einrichten
    log_level = config.get("log_level", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    logger.info("Konfiguration geladen von %s", CONFIG_FILE)

    # Bridge starten
    bridge = EcoFlow2VZ(config)

    # Signal-Handler für sauberen Shutdown
    def signal_handler(signum, frame):
        logger.info("Signal %s empfangen", signal.Signals(signum).name)
        bridge.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    bridge.run()


if __name__ == "__main__":
    main()
