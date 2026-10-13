/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocol;
using Thrift.Protocol.Entities;

namespace AdbcDrivers.Databricks
{
    // Complete in-memory serialization so cancellable FlushAsync always resets the HTTP request buffer.
    internal sealed class UninterruptibleWriteProtocol : TProtocolDecorator
    {
        public UninterruptibleWriteProtocol(TProtocol protocol) : base(protocol)
        {
        }

        public override Task WriteMessageBeginAsync(TMessage message, CancellationToken cancellationToken)
            => base.WriteMessageBeginAsync(message, CancellationToken.None);
        public override Task WriteMessageEndAsync(CancellationToken cancellationToken)
            => base.WriteMessageEndAsync(CancellationToken.None);
        public override Task WriteStructBeginAsync(TStruct @struct, CancellationToken cancellationToken)
            => base.WriteStructBeginAsync(@struct, CancellationToken.None);
        public override Task WriteStructEndAsync(CancellationToken cancellationToken)
            => base.WriteStructEndAsync(CancellationToken.None);
        public override Task WriteFieldBeginAsync(TField field, CancellationToken cancellationToken)
            => base.WriteFieldBeginAsync(field, CancellationToken.None);
        public override Task WriteFieldEndAsync(CancellationToken cancellationToken)
            => base.WriteFieldEndAsync(CancellationToken.None);
        public override Task WriteFieldStopAsync(CancellationToken cancellationToken)
            => base.WriteFieldStopAsync(CancellationToken.None);
        public override Task WriteMapBeginAsync(TMap map, CancellationToken cancellationToken)
            => base.WriteMapBeginAsync(map, CancellationToken.None);
        public override Task WriteMapEndAsync(CancellationToken cancellationToken)
            => base.WriteMapEndAsync(CancellationToken.None);
        public override Task WriteListBeginAsync(TList list, CancellationToken cancellationToken)
            => base.WriteListBeginAsync(list, CancellationToken.None);
        public override Task WriteListEndAsync(CancellationToken cancellationToken)
            => base.WriteListEndAsync(CancellationToken.None);
        public override Task WriteSetBeginAsync(TSet set, CancellationToken cancellationToken)
            => base.WriteSetBeginAsync(set, CancellationToken.None);
        public override Task WriteSetEndAsync(CancellationToken cancellationToken)
            => base.WriteSetEndAsync(CancellationToken.None);
        public override Task WriteBoolAsync(bool value, CancellationToken cancellationToken)
            => base.WriteBoolAsync(value, CancellationToken.None);
        public override Task WriteByteAsync(sbyte value, CancellationToken cancellationToken)
            => base.WriteByteAsync(value, CancellationToken.None);
        public override Task WriteI16Async(short value, CancellationToken cancellationToken)
            => base.WriteI16Async(value, CancellationToken.None);
        public override Task WriteI32Async(int value, CancellationToken cancellationToken)
            => base.WriteI32Async(value, CancellationToken.None);
        public override Task WriteI64Async(long value, CancellationToken cancellationToken)
            => base.WriteI64Async(value, CancellationToken.None);
        public override Task WriteDoubleAsync(double value, CancellationToken cancellationToken)
            => base.WriteDoubleAsync(value, CancellationToken.None);
        public override Task WriteStringAsync(string value, CancellationToken cancellationToken)
            => base.WriteStringAsync(value, CancellationToken.None);
        public override Task WriteBinaryAsync(byte[] value, CancellationToken cancellationToken)
            => base.WriteBinaryAsync(value, CancellationToken.None);
        public override Task WriteUuidAsync(Guid value, CancellationToken cancellationToken)
            => base.WriteUuidAsync(value, CancellationToken.None);
    }
}
