using Confluent.Kafka;
using KafkaPipelineApp.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace KafkaPipelineApp.Workers;

public class KafkaConsumerWorker : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topic;
    private readonly ILogger<KafkaConsumerWorker> _logger;

    public KafkaConsumerWorker(IConfiguration config, ILogger<KafkaConsumerWorker> logger)
    {
        _logger = logger;
        _topic = config["Kafka:Topic"] ?? "processing-jobs-topic";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            GroupId = config["Kafka:GroupId"],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true // Auto-commit offset setelah dibaca
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation("🚀 Kafka Consumer listening on Topic '{Topic}'...", _topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);

                    if (consumeResult?.Message == null) continue;

                    var job = JsonSerializer.Deserialize<ProcessingJob>(consumeResult.Message.Value);

                    _logger.LogInformation("⚙️ [Partition {Partition} @ Offset {Offset}] Processing Job {JobId} | Payload: {Payload}",
                        consumeResult.Partition.Value, consumeResult.Offset.Value, job?.JobId, job?.Payload);

                    // Simulasi pengerjaan I/O bound
                    Thread.Sleep(200);

                    _logger.LogInformation("✅ Job {JobId} Completed.", job?.JobId);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "❌ Kafka Consume Error: {Reason}", ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("🛑 Stopping Kafka Consumer...");
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
        }
    }
}