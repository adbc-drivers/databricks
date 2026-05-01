/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Threading.Tasks;
using AdbcDrivers.Tests.HiveServer2.Common;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Databricks.Tests
{
    public class NumericValueTests : NumericValueTests<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public NumericValueTests(ITestOutputHelper output)
            : base(output, new DatabricksTestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [InlineData(0)]
        [InlineData(0.2)]
        [InlineData(15e-03)]
        [InlineData(1.234E+2)]
        [InlineData(double.NegativeInfinity)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NaN)]
        [InlineData(double.MinValue)]
        [InlineData(double.MaxValue)]
        public override async Task TestDoubleValuesInsertSelectDelete(double value)
        {
            await base.TestDoubleValuesInsertSelectDelete(value);
        }

        [SkippableTheory]
        [InlineData(0)]
        [InlineData(25)]
        [InlineData(float.NegativeInfinity)]
        [InlineData(float.PositiveInfinity)]
        [InlineData(float.NaN)]
        // TODO: Solve server issue when non-integer float value is used in where clause.
        //[InlineData(25.1)]
        //[InlineData(0.2)]
        //[InlineData(15e-03)]
        //[InlineData(1.234E+2)]
        //[InlineData(float.MinValue)]
        //[InlineData(float.MaxValue)]
        public override async Task TestFloatValuesInsertSelectDelete(float value)
        {
            // TODO: PECO-3005 - CommonTestEnvironment.GetValueForProtocolVersion hard-casts to HiveServer2Connection
            Skip.If(TestConfiguration.Protocol == "rest", "SEA: GetValueForProtocolVersion hard-casts to HiveServer2Connection");
            await base.TestFloatValuesInsertSelectDelete(value);
        }

        [SkippableTheory]
        [InlineData("-1")]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("99")]
        [InlineData("-99")]
        public override async Task TestSmallNumberRange(string value)
        {
            // TODO: PECO-3005 - CommonTestEnvironment.GetValueForProtocolVersion hard-casts to HiveServer2Connection
            Skip.If(TestConfiguration.Protocol == "rest", "SEA: GetValueForProtocolVersion hard-casts to HiveServer2Connection");
            await base.TestSmallNumberRange(value);
        }

        [SkippableTheory]
        [InlineData("0.00")]
        [InlineData("4.85")]
        [InlineData("-999999999999999999999999999999999999.99")]
        [InlineData("999999999999999999999999999999999999.99")]
        public override async Task TestSmallScaleNumberRange(string value)
        {
            // TODO: PECO-3005 - CommonTestEnvironment.GetValueForProtocolVersion hard-casts to HiveServer2Connection
            Skip.If(TestConfiguration.Protocol == "rest", "SEA: GetValueForProtocolVersion hard-casts to HiveServer2Connection");
            await base.TestSmallScaleNumberRange(value);
        }

        [SkippableTheory]
        [InlineData("0E-37")]
        [InlineData("-2.0030000000000000000000000000000000000")]
        [InlineData("4.8500000000000000000000000000000000000")]
        [InlineData("1E-37")]
        [InlineData("9.5545204502636499875576383003668916798")]
        public override async Task TestLargeScaleNumberRange(string value)
        {
            // TODO: PECO-3005 - CommonTestEnvironment.GetValueForProtocolVersion hard-casts to HiveServer2Connection
            Skip.If(TestConfiguration.Protocol == "rest", "SEA: GetValueForProtocolVersion hard-casts to HiveServer2Connection");
            await base.TestLargeScaleNumberRange(value);
        }

        [SkippableTheory]
        [InlineData(2.467, 2.47)]
        [InlineData(-672.613, -672.61)]
        public override async Task TestRoundingNumbers(decimal input, decimal output)
        {
            // TODO: PECO-3005 - CommonTestEnvironment.GetValueForProtocolVersion hard-casts to HiveServer2Connection
            Skip.If(TestConfiguration.Protocol == "rest", "SEA: GetValueForProtocolVersion hard-casts to HiveServer2Connection");
            await base.TestRoundingNumbers(input, output);
        }
    }
}
