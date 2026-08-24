using KafkaPipelineApp.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaPipelineApp.Services;

public interface IKafkaProducerService
{
    Task ProduceJobAsync(ProcessingJob job, CancellationToken ct = default);
}