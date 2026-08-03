/*
* Temporary diagnostic (NOT for merge): prove whether PartialRead's ~40MB "growth" is
* RecyclableMemoryStreamManager pool retention (LZ4) vs a real leak. Dumps GC memory AND
* the four pool sizes at each snapshot, and runs the loop with LZ4 on vs off.
*/

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using AdbcDrivers.HiveServer2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    public class PoolDiagProbe : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public PoolDiagProbe(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        private void DumpPool(string tag, DatabricksDatabase db)
        {
            var m = db.RecyclableMemoryStreamManager;
            long gc = GC.GetTotalMemory(true);
            OutputHelper?.WriteLine(
                $"[pool] {tag} GC={gc/1048576.0:F1}MB " +
                $"largeFree={m.LargePoolFreeSize/1048576.0:F1} largeInUse={m.LargePoolInUseSize/1048576.0:F1} " +
                $"smallFree={m.SmallPoolFreeSize/1048576.0:F1} smallInUse={m.SmallPoolInUseSize/1048576.0:F1}");
        }

        [SkippableTheory]
        [InlineData(true)]   // LZ4 on (default) — expect growth == pool free-size delta
        [InlineData(false)]  // LZ4 off — expect ~no growth (theory: pool is the cause)
        public async Task ProbePartialReadPool(bool lz4)
        {
            var opts = new Dictionary<string, string>
            {
                ["adbc.databricks.cloudfetch.lz4.enabled"] = lz4 ? "true" : "false",
            };
            var driver = NewDriver;
            var database = (DatabricksDatabase)driver.Open(GetDriverParameters(TestConfiguration));
            using var connection = database.Connect(opts);

            // Warm-up (single iteration, like the real test)
            using (var warm = connection.CreateStatement())
            {
                warm.SqlQuery = "SELECT * FROM RANGE(1000000)";
                using var wr = warm.ExecuteQuery().Stream;
                await wr.ReadNextRecordBatchAsync();
            }
            GC.Collect(2, GCCollectionMode.Forced, true);
            long before = GC.GetTotalMemory(true);
            DumpPool($"lz4={lz4} BEFORE", database);

            for (int i = 0; i < 5; i++)
            {
                using var st = connection.CreateStatement();
                st.SqlQuery = "SELECT * FROM RANGE(1000000)";
                using var reader = st.ExecuteQuery().Stream;
                await reader.ReadNextRecordBatchAsync();  // partial read
            }

            await Task.Delay(1000);
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true);
            long after = GC.GetTotalMemory(true);
            DumpPool($"lz4={lz4} AFTER", database);
            OutputHelper?.WriteLine($"[pool] lz4={lz4} GROWTH={(after-before)/1048576.0:F2}MB");
        }
    }
}
