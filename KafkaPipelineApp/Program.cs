using Confluent.Kafka;
using KafkaPipelineApp.Models;
using KafkaPipelineApp.Services;
using KafkaPipelineApp.Workers;

var builder = WebApplication.CreateBuilder(args);

// Register Singleton Producer & Background Consumer
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerWorker>();

var app = builder.Build();

// Endpoint Producer dengan Exception Handling
app.MapPost("/api/jobs", async (string payload, IKafkaProducerService producer, ILogger<Program> logger) =>
{
    var job = new ProcessingJob(Guid.NewGuid(), payload, DateTime.UtcNow);

    try
    {
        await producer.ProduceJobAsync(job);
        return Results.Accepted($"/api/jobs/{job.JobId}", new { job.JobId, Status = "Published" });
    }
    catch (ProduceException<string, string> ex)
    {
        logger.LogError(ex, "❌ Kafka Broker is down! Unable to publish Job {JobId}", job.JobId);

        return Results.Problem(
            detail: "Service message broker sedang tidak dapat dijangkau. Silakan coba beberapa saat lagi.",
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Broker Unavailable"
        );
    }
});

app.Run();