```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.26100.4946)
Unknown processor
.NET SDK 9.0.301
  [Host]     : .NET 8.0.17 (8.0.1725.26602), Arm64 RyuJIT AdvSIMD
  Job-CGHXRO : .NET 8.0.17 (8.0.1725.26602), Arm64 RyuJIT AdvSIMD

Server=True  InvocationCount=1  IterationCount=3  
UnrollFactor=1  WarmupCount=1  Error=22.306 s  
StdDev=1.223 s  

```
| Method            | ReadDelayMs | Mean    | Min     | Max     | Median  | Peak Memory (MB) | Total Rows | Total Batches | GC Time % | Gen0       | Gen1       | Gen2       | Allocated |
|------------------ |------------ |--------:|--------:|--------:|--------:|-----------------:|-----------:|--------------:|----------:|-----------:|-----------:|-----------:|----------:|
| ExecuteLargeQuery | 5           | 9.536 s | 8.679 s | 10.94 s | 8.994 s |           389.64 |  1,439,935 |           745 |      0.54 | 21000.0000 | 21000.0000 | 21000.0000 |   1.56 GB |
