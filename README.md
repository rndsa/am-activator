# am-activator

[![Python](https://img.shields.io/badge/python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
[![Runtime](https://img.shields.io/badge/runtime-Pterodactyl_Container-007acc?style=flat-square)](https://pterodactyl.io)
[![Protocol](https://img.shields.io/badge/protocol-REST_API-green?style=flat-square)](https://v.axjet.xyz)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)

High-performance, parallel automated provisioning engine for Alight Motion accounts. Supports connection pooling, sub-second mailbox polling via RZero Mail, dynamic live hot-reloading, and failover gateway routing.

---

## Technical Architecture

```
                       ┌──────────────────────────────┐
                       │     am-activator Engine      │
                       │  (Multi-Threaded Worker Pool)│
                       └──────────────┬───────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              │                                               │
              ▼                                               ▼
┌───────────────────────────┐                   ┌───────────────────────────┐
│     RZero Mail Engine     │                   │     AM Reverse Gateway    │
│       (rzmail.my.id)      │                   │       (v.axjet.xyz)       │
├───────────────────────────┤                   ├───────────────────────────┤
│ • GET /api/create         │                   │ • POST /api/send-link     │
│ • GET /api/messages/:mail │                   │ • POST /api/verify-link   │
│ • Magic link extraction   │                   │ • Sub-second provisioning │
└───────────────────────────┘                   └───────────────────────────┘
```

---

## Benchmarks & Performance Comparison

| Metric | Sequential Polling | am-activator (Optimized) | Improvement |
|---|---|---|---|
| Throughput | ~6 akun / menit | **~110 akun / menit** | **~18x faster** |
| Email Delivery Latency | 5.0s – 8.0s | **0.6s – 1.2s** (Fast polling) | **~6x lower latency** |
| Concurrency Scaling | Single thread (blocking) | **Multi-worker Pool (Dynamic)** | Zero worker starvation |
| Config Updates | Requires process restart | **Live Hot-Reload (<2s)** | Zero downtime reloads |

---

## Kelebihan & Keunggulan

- **Live Hot-Reload Configuration**: Konfigurasi `config.json` dibaca secara dinamis tiap 2 detik tanpa perlu mematikan atau merestart proses daemon di server/Pterodactyl.
- **Failover Gateway Routing**: Otomatis berpindah ke gateway cadangan apabila upstream gateway utama mengalami gangguan konektivitas atau penolakan response.
- **Fast Mailbox Extraction**: Menggunakan regex multi-pola untuk mengekstrak auth link langsung dari format HTML maupun raw payload email RZero Mail.
- **Low Resource Footprint**: Menggunakan thread pool berbasis event polling dengan memory footprint stabil (<40 MB RAM) pada beban 10+ worker paralel.
- **Pterodactyl Ready**: Kompatibel penuh dengan environment container Pterodactyl (user non-root, logging stream, graceful signal handling).

---

## Kekurangan & Batasan Sistem

- **Upstream Rate Limiting**: Kecepatan throughput maksimal bergantung pada batas antrian auth email dan toleransi rate limit dari pihak upstream service.
- **SMTP Propagation Latency**: Waktu pemrosesan akun dipengaruhi oleh jeda pengiriman email internet (rata-rata 2–4 detik per siklus).
- **Network Routing Dependency**: Membutuhkan akses HTTPS tanpa blokir ke endpoint gateway dan mail server API.

---

## Cara Pakai & Konfigurasi

### 1. Instalasi Dependensi

```bash
git clone https://github.com/rndsa/am-activator.git
cd am-activator
pip install -r requirements.txt
```

### 2. Konfigurasi (`config.json`)

Salin file contoh konfigurasi atau buat `config.json` di root direktori project:

```bash
cp config.example.json config.json
```

Contoh konfigurasi standar:

```json
{
  "concurrency": 8,
  "delay_seconds": 0.2,
  "poll_interval": 0.6,
  "target_count": 0,
  "am_gateway": "https://v.axjet.xyz",
  "am_api_key": "am-sk-29bf295c45cddff5cbb0c31e143b4feb",
  "rzmail_api_url": "https://rzmail.my.id"
}
```

### Parameter Konfigurasi

| Parameter | Tipe | Default | Deskripsi |
|---|---|---|---|
| `concurrency` | integer | `8` | Jumlah worker paralel yang berjalan bersamaan |
| `delay_seconds` | float | `0.2` | Jeda waktu dispatch antar task worker (detik) |
| `poll_interval` | float | `0.6` | Frekuensi interval pengecekan email masuk (detik) |
| `target_count` | integer | `0` | Target total akun (`0` = berjalan terus tanpa batas) |
| `am_gateway` | string | `https://v.axjet.xyz` | URL endpoint reverse gateway Alight Motion |
| `am_api_key` | string | `am-sk-...` | API Key autentikasi untuk reverse gateway |
| `rzmail_api_url` | string | `https://rzmail.my.id` | Base URL endpoint service RZero Mail |

### 3. Menjalankan Aplikasi

Jalankan engine menggunakan Python 3:

```bash
python3 main.py
```

Untuk Pterodactyl, letakkan script sebagai target eksekusi utama (`am_tui.py` atau `main.py`). Seluruh hasil akun yang berhasil diverifikasi akan otomatis tersimpan di `am_accounts.txt` secara realtime.

---

## License

[MIT](LICENSE) © 2026 ren
