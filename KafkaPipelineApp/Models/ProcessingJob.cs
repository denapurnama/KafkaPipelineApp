using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaPipelineApp.Models;

public record ProcessingJob(
    Guid JobId,
    string Payload,
    DateTime CreatedAt
);