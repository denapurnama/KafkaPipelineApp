using KafkaPipelineApp.Models;
using KafkaPipelineApp.Services;
using KafkaPipelineApp.Workers;

var builder = WebApplication.CreateBuilder(args);

// Register Singleton Producer & Background Consumer
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddHostedService<KafkaConsumerWorker>();

var app = builder.Build();

// Endpoint Producer
app.MapPost("/api/jobs", async (string payload, IKafkaProducerService producer) =>
{
    var job = new ProcessingJob(Guid.NewGuid(), payload, DateTime.UtcNow);

    await producer.ProduceJobAsync(job);

    return Results.Accepted($"/api/jobs/{job.JobId}", new { job.JobId, Status = "Published to Kafka" });
});

app.Run();