```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.26100.4946)
Apple Silicon, 8 CPU, 8 logical and 8 physical cores
  [Host]     : .NET Framework 4.8.1 (4.8.9310.0), X64 RyuJIT VectorSize=128
  Job-OGLPCL : .NET Framework 4.8.1 (4.8.9310.0), X64 RyuJIT VectorSize=128

Server=True  InvocationCount=1  IterationCount=3  
UnrollFactor=1  WarmupCount=1  Error=3.344 s  
StdDev=0.1833 s  

```
| Method            | ReadDelayMs | Mean    | Min     | Max     | Median  | Peak Memory (MB) | Total Rows | Total Batches | Gen0        | Gen1       | Gen2       | Allocated |
|------------------ |------------ |--------:|--------:|--------:|--------:|-----------------:|-----------:|--------------:|------------:|-----------:|-----------:|----------:|
| ExecuteLargeQuery | 5           | 8.988 s | 8.791 s | 9.153 s | 9.021 s |           341.15 |  1,439,935 |           745 | 301000.0000 | 38000.0000 | 23000.0000 |   2.71 GB |
