# 🚀 Distributed Event-Driven Pipeline (.NET & Apache Kafka)

Projek ini mengimplementasikan arsitektur *Distributed Event-Driven* menggunakan **ASP.NET Core Minimal API** dan **Apache Kafka**. 

Sistem ini memisahkan penerimaan request HTTP (Producer) dengan eksekusi tugas berat di latar belakang (Consumer `BackgroundService`), serta dilengkapi penanganan kegagalan (*resilience*) ketika broker Kafka tidak tersedia.

---

## 🏗️ Architecture & Flow

```text
[ Client / Postman ]
         │
         ▼ (HTTP POST /api/jobs)
[ Minimal API Producer ] ────── (Kafka Unavailable?) ──► Returns 503 Service Unavailable
         │
         ▼ (Produce Event)
[ Apache Kafka Broker ] ── (Topic: processing-jobs-topic)
         │
         ▼ (Poll & Consume)
[ KafkaConsumerWorker ] ──► (Process Task Asynchronously)