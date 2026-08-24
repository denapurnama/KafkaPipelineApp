using Confluent.Kafka;
using KafkaPipelineApp.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace KafkaPipelineApp.Services;

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _topic = config["Kafka:Topic"] ?? "processing-jobs-topic";

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            Acks = Acks.All, // Memastikan data terikat penuh di Kafka Leader & Replicas
            EnableDeliveryReports = true
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task ProduceJobAsync(ProcessingJob job, CancellationToken ct = default)
    {
        var key = job.JobId.ToString();
        var val = JsonSerializer.Serialize(job);

        var message = new Message<string, string>
        {
            Key = key,
            Value = val
        };

        var result = await _producer.ProduceAsync(_topic, message, ct);
        _logger.LogInformation("📤 Produced to Topic '{Topic}' | Partition: {Partition} | Offset: {Offset}",
            result.Topic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}