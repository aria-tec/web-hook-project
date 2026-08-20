# Panduan Kontribusi & Pemeliharaan Mini-Svix

## 🏛️ The SQLite Standard: Stability & Longevity Charter
Mini-Svix berstatus **Feature-Complete, Architecturally Frozen, dan Rigorously Hardened**. Kami menjamin **Zero-API-Breakage**.

---

## 🐛 Cara Menangani Bug / Laporan Masalah
Jika Anda menemukan *edge case* atau bug:

1. **Wajibkan Skrip Reproduksi Deterministik:**  
   Buat satu fungsi unit test atau skrip yang mereproduksi kegagalan tersebut secara deterministik (ikuti format [`.github/ISSUE_TEMPLATE/bug_report.yml`](.github/ISSUE_TEMPLATE/bug_report.yml)).
2. **Perbaiki Kodenya:**  
   Tambal logika pada layer internal terkait (`internal/dispatcher`, `internal/worker`, atau `internal/queue`).
3. **Kunci Selamanya (*Zero Regression Guarantee*):**  
   Test reproduksi tersebut jangan dihapus, melainkan disimpan permanen di dalam `tests/`. Dengan demikian, bug yang sama tidak akan pernah bisa muncul kembali di masa depan.

---

## 🧪 Menjalankan Pengujian Lokal Sebelum Submit PR
```bash
# 1. Jalankan unit test dengan race detector
go test -v -race ./...

# 2. Jalankan active fuzzing
go test -v -fuzz=FuzzVerifySignature -fuzztime=30s ./tests/
go test -v -fuzz=FuzzIsRestrictedIP -fuzztime=30s ./tests/

# 3. Jalankan leak assertions
go test -v -race ./tests/ -run "TestGoroutineLeak|TestHeapMemory"

# 4. Pindai celah keamanan
govulncheck ./...
```
