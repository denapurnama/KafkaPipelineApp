# Panduan Testing End-to-End (Producer & Consumer)

Panduan ini berisi langkah-langkah lengkap untuk menguji alur pengiriman pesan (*event-driven messaging*) dari awal, mulai dari menjalankan infrastruktur Docker container, menjalankan aplikasi Consumer dan Producer, hingga verifikasi pengiriman pesan via Topic.

---

## 📋 Prasyarat (Prerequisites)
Sebelum memulai testing, pastikan tools berikut sudah terinstall di komputer Anda:
1. **Docker & Docker Desktop / Docker Compose** (aktif dan berjalan)
2. **.NET SDK** (sesuai versi proyek, e.g., .NET 8 / 9)
3. Terminal / Command Prompt / PowerShell (minimal 2-3 window terminal terpisah)

---

## 🛠️ Langkah 1: Menjalankan Infrastructure Container

Jalankan pesan broker (Kafka / RabbitMQ / Redis) yang terdefinisi di `docker-compose.yml`.

1. Buka terminal di direktori utama proyek (`root`).
2. Jalankan perintah berikut untuk mengunduh dan menyalakan container di background:
   ```bash
   docker compose up -d
   ```
3. Verifikasi bahwa semua container sudah berjalan dengan status **Running / Healthy**:
   ```bash
   docker compose ps
   ```
4. *(Opsional)* Jika ingin memantau log dari container broker:
   ```bash
   docker compose logs -f
   ```

---

## 📥 Langkah 2: Menjalankan Aplikasi Consumer

Consumer harus dijalankan terlebih dahulu agar siap mendengarkan (*subscribe*) pesan yang dikirimkan ke Topic.

1. Buka **Terminal Window #1**.
2. Masuk ke folder proyek Consumer atau arahkan langsung ke file `.csproj` Consumer:
   ```bash
   cd src/ConsumerApp   # sesuaikan dengan struktur folder Anda
   ```
3. Jalankan aplikasi Consumer:
   ```bash
   dotnet run
   ```
   *Atau jika dari root folder:*
   ```bash
   dotnet run --project src/ConsumerApp/ConsumerApp.csproj
   ```
4. Pastikan di terminal muncul log inisialisasi yang menandakan Consumer telah berhasil terhubung ke Message Broker dan mendengarkan Topic target:
   ```text
   [INFO] Connected to Message Broker at localhost:9092
   [INFO] Subscribed to topic: 'order-created-events'
   [INFO] Waiting for messages...
   ```

---

## 📤 Langkah 3: Menjalankan Aplikasi Producer (Kirim Pesan)

Setelah Consumer dalam kondisi *standby*, jalankan Producer untuk mempublikasikan pesan ke Topic.

### Opsi A: Jika Producer berupa Console App / CLI
1. Buka **Terminal Window #2**.
2. Masuk ke folder proyek Producer dan jalankan:
   ```bash
   dotnet run --project src/ProducerApp/ProducerApp.csproj
   ```
3. Masukkan data pesan jika diminta oleh prompt CLI, atau amati log hingga pengiriman selesai.

### Opsi B: Jika Producer berupa Web API (REST API / Swagger)
1. Jalankan Web API Producer:
   ```bash
   dotnet run --project src/ProducerApp/ProducerApp.csproj
   ```
2. Buka browser dan akses **Swagger UI** (misal: `https://localhost:5001/swagger`) atau gunakan **cURL / Postman**.
3. Kirim permintaan HTTP POST untuk menembak endpoint pengiriman pesan:
   ```bash
   curl -X POST "https://localhost:5001/api/events/publish" \
        -H "Content-Type: application/json" \
        -d '{
              "orderId": "ORD-12345",
              "amount": 250000,
              "customerName": "John Doe",
              "timestamp": "2026-08-28T18:30:00Z"
            }'
   ```
4. Pastikan Producer menampilkan respon sukses (misal: `200 OK` atau log `[INFO] Message published to topic 'order-created-events'`).

---

## ✅ Langkah 4: Verifikasi & End-to-End Testing

1. Periksa **Terminal Window #1 (Consumer)**.
2. Pastikan pesan yang dikirimkan oleh Producer muncul di log Consumer secara real-time:
   ```text
   [INFO] Message received from topic 'order-created-events':
   [DATA] OrderID: ORD-12345 | Amount: 250000 | Customer: John Doe
   [INFO] Message processed successfully!
   ```
3. *(Opsional)* Pengecekan via Web UI Broker:
   - **RabbitMQ:** Akses `http://localhost:15672` (default user/pass: `guest`/`guest`) -> Cek menu **Exchanges / Queues** untuk memastikan pesan terdistribusi.
   - **Kafka (Kafdrop / AKHQ):** Akses `http://localhost:9000` -> Cek nama Topic dan isi message payload.

---

## 🔍 Troubleshooting FAQ

| Permasalahan | Kemungkinan Penyebab | Solusi |
| :--- | :--- | :--- |
| **Connection Refused** | Container broker belum sepenuhnya siap (*healthy*) | Tunggu beberapa detik lalu jalankan ulang `docker compose ps` atau restart container dengan `docker compose restart`. |
| **Consumer tidak menerima pesan** | Nama Topic atau Routing Key pada Consumer dan Producer tidak cocok | Samakan nama Topic di file konfigurasi (`appsettings.json` / code) Consumer & Producer. |
| **Pesan masuk ke Dead Letter Queue (DLQ)** | Gagal deserialization JSON atau ada exception di handler Consumer | Cek format payload DTO/Model di Consumer agar cocok dengan JSON yang dikirim Producer. |

---