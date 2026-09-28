# ecoFlow2vz

EcoFlow Stream MQTT → Volkszähler Bridge

Empfängt Live-Daten von der EcoFlow Developer API (MQTT) und leitet sie an eine [Volkszähler](https://volkszaehler.org/)-Instanz weiter.

## Unterstützte Geräte

- EcoFlow Stream 600 AC / 800 AC
- EcoFlow Stream Pro
- EcoFlow Stream Ultra / Ultra X

## Übertragene Werte

| Kanal | EcoFlow MQTT-Feld | Einheit |
|-------|-------------------|---------|
| Leistung PV1 | `powGetPv` | W |
| Leistung PV2 | `powGetPv2` | W |
| Leistung PV3 | `powGetPv3` | W |
| Leistung PV4 | `powGetPv4` | W |
| Summe Leistung PV | `powGetPvSum` | W |
| Leistung Batterie | `powGetBpCms` | W |
| Leistung Grid | `sysGridConnectionPower` | W |
| SoC Batterie | `cmsBattSoc` | % |
| Gespeicherte Energie | berechnet aus `cmsBattFullEnergy × cmsBattSoc / 100` | Wh |

> **Batterie-Leistung:** Positiv = Laden, Negativ = Entladen

## Voraussetzungen

- Python 3.10+
- EcoFlow Developer Account mit `accessKey` und `secretKey`
  → Registrierung: [developer.ecoflow.com](https://developer.ecoflow.com)
- Volkszähler-Instanz mit Middleware API

## Installation

### 1. Repository klonen

```bash
cd /home/maik/software
git clone <repo-url> ecoFlow2vz
cd ecoFlow2vz
```

### 2. Python-Abhängigkeiten installieren

```bash
pip3 install -r requirements.txt
```

### 3. Konfiguration anlegen

```bash
cp config.example.yaml config.yaml
nano config.yaml
```

Trage ein:
- EcoFlow `access_key`, `secret_key` und `device_sn`
- Volkszähler `base_url`
- Kanal-UUIDs (siehe nächster Abschnitt)

### 4. Volkszähler-Kanäle anlegen

Lege in deinem Volkszähler für jeden Messwert einen Kanal an. Du kannst das über die Volkszähler-Weboberfläche oder per `curl` machen:

```bash
VZ_URL="http://vz-g14.home:81/middleware"

# Beispiel: Kanal für PV1-Leistung anlegen
curl -s -X POST -d "type=powersensor&title=EcoFlow+PV1&resolution=1" \
  "$VZ_URL/channel.json" | python3 -m json.tool
```

Die UUID aus der Antwort in `config.yaml` bei `pv1_power` eintragen. Wiederhole für alle Kanäle:

| Config-Key | Empfohlener Titel | Typ |
|------------|-------------------|-----|
| `pv1_power` | EcoFlow PV1 | `powersensor` |
| `pv2_power` | EcoFlow PV2 | `powersensor` |
| `pv3_power` | EcoFlow PV3 | `powersensor` |
| `pv4_power` | EcoFlow PV4 | `powersensor` |
| `pv_total` | EcoFlow PV Gesamt | `powersensor` |
| `battery_power` | EcoFlow Batterie | `powersensor` |
| `grid_power` | EcoFlow Grid | `powersensor` |
| `battery_soc` | EcoFlow Batterie SoC | `powersensor` |
| `battery_energy` | EcoFlow Batterie Energie | `powersensor` |

### 5. Test

```bash
python3 ecoflow2vz.py
```

Erwartete Ausgabe:
```
2026-09-28 12:00:00 [INFO   ] ecoflow2vz: ecoFlow2vz v1.0.0 gestartet
2026-09-28 12:00:00 [INFO   ] ecoflow2vz: Gerät: DXXXXXXXXX
2026-09-28 12:00:01 [INFO   ] ecoflow2vz: MQTT-Credentials erhalten — Broker: mqtt.ecoflow.com:8883
2026-09-28 12:00:02 [INFO   ] ecoflow2vz: MQTT verbunden — Subscribe auf: /open/...
2026-09-28 12:00:05 [INFO   ] ecoflow2vz: VZ Push: 9 OK, 0 Fehler | PV: 350W, Bat: 79W (82%), Grid: 274W
```

Mit `Ctrl+C` beenden.

### 6. Systemd-Service installieren

```bash
# Service-Datei anpassen (Pfade prüfen!)
nano ecoflow2vz.service

# Installieren und starten
sudo cp ecoflow2vz.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable ecoflow2vz
sudo systemctl start ecoflow2vz

# Status prüfen
sudo systemctl status ecoflow2vz

# Logs verfolgen
journalctl -u ecoflow2vz -f
```

## Konfiguration

Alle Einstellungen in `config.yaml`:

```yaml
ecoflow:
  access_key: "..."              # EcoFlow Developer API Access Key
  secret_key: "..."              # EcoFlow Developer API Secret Key
  device_sn: "..."               # Seriennummer des Stream-Geräts
  api_host: "api-e.ecoflow.com"  # EU: api-e.ecoflow.com | Global: api.ecoflow.com

volkszaehler:
  base_url: "http://vz-g14.home:81/middleware"

channels:
  pv1_power:      "uuid-1"      # Leere UUIDs werden übersprungen
  pv2_power:      "uuid-2"
  ...

push_interval_seconds: 0         # 0 = sofort bei jeder Nachricht
battery_capacity_wh: 3840        # Fallback falls cmsBattFullEnergy fehlt
log_level: "INFO"                # DEBUG | INFO | WARNING | ERROR
```

### Push-Intervall

- `0` — Jede MQTT-Nachricht wird sofort an Volkszähler weitergeleitet (~alle 2-5 Sekunden)
- `10` — Maximal alle 10 Sekunden pushen (reduziert Last auf VZ)

### Kanal-UUIDs

Kanäle, die keine UUID haben (leer oder nicht gesetzt), werden übersprungen. So kannst du auch nur einzelne Werte übertragen.

## Fehlerbehebung

| Problem | Lösung |
|---------|--------|
| `EcoFlow API Fehler` | Access Key / Secret Key prüfen. EU-Accounts brauchen `api-e.ecoflow.com` |
| `MQTT Verbindung fehlgeschlagen` | Firewall: Port 8883 (TLS) muss raus erreichbar sein |
| `VZ Push fehlgeschlagen` | Volkszähler-URL prüfen, UUID korrekt? Test: `curl http://vz-host/middleware/data/UUID.json` |
| Keine MQTT-Nachrichten | Device SN prüfen. Gerät muss online sein (in EcoFlow App sichtbar) |
| `Konfiguration unvollständig` | `config.yaml` prüfen — alle `YOUR_*` Platzhalter ersetzen |

## Lizenz

MIT
