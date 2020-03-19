﻿using Atomex.Blockchain.Ethereum;
using LiteDB;

namespace Atomex.Common.Bson
{
    public class EthereumTransactionToBsonSerializer : BsonSerializer<EthereumTransaction>
    {
        public override void Register(BsonMapper bsonMapper)
        {
            bsonMapper.Entity<EthereumTransaction>()
                .Id(tx => tx.UniqueId)
                .Field(tx => tx.Id, "TxId");
        }
    }
}