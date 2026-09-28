# ecoFlow2vz

EcoFlow Stream MQTT → Volkszähler Bridge

Receives live data from the EcoFlow Developer API (MQTT) and forwards it to a [Volkszähler](https://volkszaehler.org/) instance.

## Supported Devices

- EcoFlow Stream 600 AC / 800 AC
- EcoFlow Stream Pro
- EcoFlow Stream Ultra / Ultra X

## Transmitted Values

| Channel | EcoFlow MQTT Field | Unit |
|---------|--------------------|------|
| PV1 Power | `powGetPv` | W |
| PV2 Power | `powGetPv2` | W |
| PV3 Power | `powGetPv3` | W |
| PV4 Power | `powGetPv4` | W |
| Total PV Power | `powGetPvSum` | W |
| Battery Power | `powGetBpCms` | W |
| Grid Power | `sysGridConnectionPower` | W |
| Battery SoC | `cmsBattSoc` | % |
| Stored Energy | calculated from `cmsBattFullEnergy × cmsBattSoc / 100` | Wh |

> **Battery Power:** Positive = Charging, Negative = Discharging

## Prerequisites

- Python 3.10+
- EcoFlow Developer Account with `accessKey` and `secretKey`
  → Register at: [developer.ecoflow.com](https://developer.ecoflow.com)
- Volkszähler instance with Middleware API

## Installation

### 1. Clone the Repository

```bash
git clone <repo-url> ecoFlow2vz
cd ecoFlow2vz
```

### 2. Install Python Dependencies

```bash
pip3 install -r requirements.txt
```

### 3. Create Configuration

```bash
cp config.example.yaml config.yaml
nano config.yaml
```

Fill in:
- EcoFlow `access_key`, `secret_key` and `device_sn`
- Volkszähler `base_url`
- Channel UUIDs (see next section)

### 4. Create Volkszähler Channels

Create a channel in Volkszähler for each measurement value. You can do this via the Volkszähler web interface or using `curl`:

```bash
VZ_URL="https://volkszahler.org/middleware"

# Example: Create a channel for PV1 power
curl -s -X POST -d "type=powersensor&title=EcoFlow+PV1&resolution=1" \
  "$VZ_URL/channel.json" | python3 -m json.tool
```

Copy the UUID from the response into `config.yaml` under `pv1_power`. Repeat for all channels:

| Config Key | Suggested Title | Type |
|------------|-----------------|------|
| `pv1_power` | EcoFlow PV1 | `powersensor` |
| `pv2_power` | EcoFlow PV2 | `powersensor` |
| `pv3_power` | EcoFlow PV3 | `powersensor` |
| `pv4_power` | EcoFlow PV4 | `powersensor` |
| `pv_total` | EcoFlow PV Total | `powersensor` |
| `battery_power` | EcoFlow Battery | `powersensor` |
| `grid_power` | EcoFlow Grid | `powersensor` |
| `battery_soc` | EcoFlow Battery SoC | `powersensor` |
| `battery_energy` | EcoFlow Battery Energy | `powersensor` |

### 5. Test

```bash
python3 ecoflow2vz.py
```

Expected output:
```
2026-09-28 12:00:00 [INFO   ] ecoflow2vz: ecoFlow2vz v1.0.0 started
2026-09-28 12:00:00 [INFO   ] ecoflow2vz: Device: DXXXXXXXXX
2026-09-28 12:00:01 [INFO   ] ecoflow2vz: MQTT credentials received — broker: mqtt.ecoflow.com:8883
2026-09-28 12:00:02 [INFO   ] ecoflow2vz: MQTT connected — subscribed to: /open/...
2026-09-28 12:00:05 [INFO   ] ecoflow2vz: VZ push: 9 OK, 0 failed | PV: 350W, Battery: 79W (82%), Grid: 274W
```

Stop with `Ctrl+C`.

### 6. Install Systemd Service

```bash
# Review and adjust paths in the service file
nano ecoflow2vz.service

# Install and start
sudo cp ecoflow2vz.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ecoflow2vz
sudo systemctl start ecoflow2vz

# Check status
sudo systemctl status ecoflow2vz

# Follow logs
journalctl -u ecoflow2vz -f
```

## Configuration

All settings in `config.yaml`:

```yaml
ecoflow:
  access_key: "..."              # EcoFlow Developer API Access Key
  secret_key: "..."              # EcoFlow Developer API Secret Key
  device_sn: "..."               # Serial number of your Stream device
  api_host: "api-e.ecoflow.com"  # EU: api-e.ecoflow.com | Global: api.ecoflow.com

volkszaehler:
  base_url: "https://volkszaehler.org/middleware"

channels:
  pv1_power:      "uuid-1"      # Empty UUIDs are skipped
  pv2_power:      "uuid-2"
  ...

push_interval_seconds: 0         # 0 = push immediately on every message
battery_capacity_wh: 3840        # Fallback if cmsBattFullEnergy is not received
log_level: "INFO"                # DEBUG | INFO | WARNING | ERROR
```

### Push Interval

- `0` — Every MQTT message is forwarded to Volkszähler immediately (~every 2-5 seconds)
- `10` — Push at most every 10 seconds (reduces load on VZ)

### Channel UUIDs

Channels without a UUID (empty or not set) are skipped. This allows you to selectively transmit only specific values.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `EcoFlow API error` | Check Access Key / Secret Key. EU accounts require `api-e.ecoflow.com` |
| `MQTT connection failed` | Firewall: outbound port 8883 (TLS) must be open |
| `VZ push failed` | Check Volkszähler URL and UUID. Test: `curl http://vz-host/middleware/data/UUID.json` |
| No MQTT messages | Check Device SN. Device must be online (visible in EcoFlow App) |
| `Configuration incomplete` | Check `config.yaml` — replace all `YOUR_*` placeholders |

## License

MIT
