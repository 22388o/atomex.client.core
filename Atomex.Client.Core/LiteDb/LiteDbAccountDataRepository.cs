﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Threading.Tasks;
using Atomex.Abstract;
using Atomex.Blockchain.Abstract;
using Atomex.Common;
using Atomex.Common.Bson;
using Atomex.Core;
using Atomex.Core.Entities;
using Atomex.Wallet.Abstract;
using LiteDB;
using Serilog;

namespace Atomex.LiteDb
{
    public class LiteDbAccountDataRepository : IAccountDataRepository
    {
        private const string OrdersCollectionName = "Orders";
        private const string SwapsCollectionName = "Swaps";
        private const string TransactionCollectionName = "Transactions";
        private const string OutputsCollectionName = "Outputs";
        private const string AddressesCollectionName = "Addresses";

        private const string IdKey = "_id";
        private const string CurrencyKey = nameof(WalletAddress.Currency);
        private const string AddressKey = nameof(WalletAddress.Address);
        private const string BalanceKey = nameof(WalletAddress.Balance);
        private const string UnconfirmedIncomeKey = nameof(WalletAddress.UnconfirmedIncome);
        private const string UnconfirmedOutcomeKey = nameof(WalletAddress.UnconfirmedOutcome);
        private const string ChainKey = "KeyIndex.Chain";
        private const string IndexKey = "KeyIndex.Index";
        private const string HasActivityKey = nameof(WalletAddress.HasActivity);

        private readonly string _pathToDb;
        private readonly string _sessionPassword;
        private readonly BsonMapper _bsonMapper;

        private readonly ConcurrentDictionary<long, ClientSwap> _swapById =
            new ConcurrentDictionary<long, ClientSwap>();

        private bool _swapsLoaded;
        private readonly object _syncRoot = new object();

        private string ConnectionString => $"FileName={_pathToDb};Password={_sessionPassword}";

        public LiteDbAccountDataRepository(
            string pathToDb,
            SecureString password,
            ICurrencies currencies,
            ISymbols symbols,
            Network network)
        {
            _pathToDb = pathToDb ??
                throw new ArgumentNullException(nameof(pathToDb));

            if (password == null)
                throw new ArgumentNullException(nameof(password));

            if (currencies == null)
                throw new ArgumentNullException(nameof(currencies));

            if (symbols == null)
                throw new ArgumentNullException(nameof(symbols));

            _sessionPassword = SessionPasswordHelper.GetSessionPassword(password);
            _bsonMapper = CreateBsonMapper(currencies, symbols);

            LiteDbMigrationManager.Migrate(
                pathToDb: _pathToDb,
                sessionPassword: _sessionPassword,
                network: network);
        }

        private BsonMapper CreateBsonMapper(
            ICurrencies currencies,
            ISymbols symbols)
        {
            return new BsonMapper()
                //.UseSerializer(new DateTimeToBsonSerializer())
                .UseSerializer(new BigIntegerToBsonSerializer())
                .UseSerializer(new JObjectToBsonSerializer())
                .UseSerializer(new CurrencyToBsonSerializer(currencies))
                .UseSerializer(new SymbolToBsonSerializer(symbols))
                .UseSerializer(new WalletAddressToBsonSerializer())
                .UseSerializer(new OrderToBsonSerializer())
                .UseSerializer(new BitcoinBasedTransactionToBsonSerializer(currencies))
                .UseSerializer(new BitcoinBasedTxOutputToBsonSerializer())
                //.UseSerializer(new EthereumTransactionToBsonSerializer())
                .UseSerializer(new TezosTransactionToBsonSerializer())
                .UseSerializer(new SwapToBsonSerializer(symbols));
        }

        #region Addresses

        public Task<bool> UpsertAddressAsync(WalletAddress walletAddress)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var document = _bsonMapper.ToDocument(walletAddress);

                        var addresses = db.GetCollection(AddressesCollectionName);
                        addresses.EnsureIndex(IndexKey);
                        addresses.EnsureIndex(CurrencyKey);
                        addresses.EnsureIndex(AddressKey);
                        var result = addresses.Upsert(document);

                        return Task.FromResult(result);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error updating address");
            }

            return Task.FromResult(false);
        }

        public Task<int> UpsertAddressesAsync(IEnumerable<WalletAddress> walletAddresses)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var documents = walletAddresses
                            .Select(_bsonMapper.ToDocument);

                        var addresses = db.GetCollection(AddressesCollectionName);
                        addresses.EnsureIndex(IndexKey);
                        addresses.EnsureIndex(CurrencyKey);
                        addresses.EnsureIndex(AddressKey);
                        var result = addresses.Upsert(documents);

                        return Task.FromResult(result);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error updating address");
            }

            return Task.FromResult(0);
        }

        public Task<bool> TryInsertAddressAsync(WalletAddress walletAddress)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var addresses = db.GetCollection(AddressesCollectionName);
                        addresses.EnsureIndex(IndexKey);
                        addresses.EnsureIndex(CurrencyKey);
                        addresses.EnsureIndex(AddressKey);

                        if (!addresses.Exists(Query.EQ(IdKey, walletAddress.Address)))
                        {
                            var document = _bsonMapper.ToDocument(walletAddress);

                            var id = addresses.Insert(document);

                            return Task.FromResult(id != null);
                        }

                        return Task.FromResult(false);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error updating address");
            }

            return Task.FromResult(false);
        }

        public Task<WalletAddress> GetWalletAddressAsync(Currency currency, string address)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var addresses = db.GetCollection(AddressesCollectionName);

                        var document = addresses.FindById(address);

                        var walletAddress = document != null
                            ? _bsonMapper.ToObject<WalletAddress>(document)
                            : null;

                        return Task.FromResult(walletAddress);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting wallet address");
            }

            return Task.FromResult<WalletAddress>(null);
        }

        public Task<WalletAddress> GetLastActiveWalletAddressAsync(Currency currency, int chain)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var addresses = db.GetCollection(AddressesCollectionName);

                        var document = addresses.FindOne(
                            Query.And(
                                Query.All(IndexKey, Query.Descending),
                                Query.EQ(CurrencyKey, currency.Name),
                                Query.EQ(ChainKey, chain),
                                Query.EQ(HasActivityKey, true)));

                        var walletAddress = document != null
                            ? _bsonMapper.ToObject<WalletAddress>(document)
                            : null;

                        return Task.FromResult(walletAddress);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting last active wallet address");
            }

            return Task.FromResult<WalletAddress>(null);
        }

        public Task<IEnumerable<WalletAddress>> GetUnspentAddressesAsync(
            Currency currency,
            bool includeUnconfirmed = true)
        {
            var query = includeUnconfirmed
                ? Query.And(
                    Query.EQ(CurrencyKey, currency.Name),
                    Query.Or(
                        Query.Not(BalanceKey, 0m),
                        Query.Not(UnconfirmedIncomeKey, 0m),
                        Query.Not(UnconfirmedOutcomeKey, 0m))
                    )
                : Query.And(
                    Query.EQ(CurrencyKey, currency.Name),
                    Query.GT(BalanceKey, 0m));

            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var addresses = db.GetCollection(AddressesCollectionName);

                        var unspentAddresses = addresses
                            .Find(query)
                            .Select(d => _bsonMapper.ToObject<WalletAddress>(d))
                            .ToList();

                        return Task.FromResult<IEnumerable<WalletAddress>>(unspentAddresses);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting unspent wallet addresses");
            }

            return Task.FromResult(Enumerable.Empty<WalletAddress>());
        }

        #endregion Addresses

        #region Transactions

        public Task<bool> UpsertTransactionAsync(IBlockchainTransaction tx)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var transactions = db.GetCollection(TransactionCollectionName);
                        transactions.EnsureIndex(CurrencyKey);
                        transactions.Upsert(_bsonMapper.ToDocument(tx));
                    }

                    return Task.FromResult(true);
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error adding transaction");
            }

            return Task.FromResult(false);
        }

        public Task<IBlockchainTransaction> GetTransactionByIdAsync(
            Currency currency,
            string txId)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var document = db.GetCollection(TransactionCollectionName)
                            .FindById(txId);

                        if (document != null)
                        {
                            var tx = (IBlockchainTransaction)_bsonMapper.ToObject(
                                type: currency.TransactionType,
                                doc: document);

                            return Task.FromResult(tx);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting transaction by id");
            }

            return Task.FromResult<IBlockchainTransaction>(null);
        }

        public Task<IEnumerable<IBlockchainTransaction>> GetTransactionsAsync(Currency currency)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var transactions = db.GetCollection(TransactionCollectionName)
                            .Find(Query.EQ(CurrencyKey, currency.Name))
                            .Select(d => (IBlockchainTransaction)_bsonMapper.ToObject(
                                type: currency.TransactionType,
                                doc: d))
                            .ToList();

                        return Task.FromResult<IEnumerable<IBlockchainTransaction>>(transactions);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting transactions");
            }

            return Task.FromResult(Enumerable.Empty<IBlockchainTransaction>());
        }

        public async Task<IEnumerable<IBlockchainTransaction>> GetUnconfirmedTransactionsAsync(
            Currency currency)
        {
            var transactions = await GetTransactionsAsync(currency)
                .ConfigureAwait(false);

            return transactions.Where(t => !t.IsConfirmed);
        }

        public Task<bool> RemoveTransactionByIdAsync(string id)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var transactions = db.GetCollection(TransactionCollectionName);
                        return Task.FromResult(transactions.Delete(id));
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error removing transaction");
            }

            return Task.FromResult(false);
        }

        #endregion Transactions

        #region Outputs

        public Task<bool> UpsertOutputsAsync(
            IEnumerable<ITxOutput> outputs,
            Currency currency,
            string address)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var documents = outputs
                            .Select(o =>
                            {
                                var document = _bsonMapper.ToDocument(o);
                                document[CurrencyKey] = currency.Name;
                                document[AddressKey] = address;
                                return document;
                            });

                        var outputsCollection = db.GetCollection(OutputsCollectionName);
                        outputsCollection.EnsureIndex(CurrencyKey);
                        outputsCollection.Upsert(documents);
                    }

                    return Task.FromResult(true);
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error adding transaction");
            }

            return Task.FromResult(false);
        }

        public async Task<IEnumerable<ITxOutput>> GetAvailableOutputsAsync(Currency currency)
        {
            var outputs = (await GetOutputsAsync(currency)
                .ConfigureAwait(false))
                .Where(o => !o.IsSpent)
                .ToList();

            return await GetOnlyConfirmedOutputsAsync(currency, outputs)
                .ConfigureAwait(false);
        }

        public async Task<IEnumerable<ITxOutput>> GetAvailableOutputsAsync(
            Currency currency,
            string address)
        {
            var outputs = (await GetOutputsAsync(currency, address)
                .ConfigureAwait(false))
                .Where(o => !o.IsSpent)
                .ToList();

            return await GetOnlyConfirmedOutputsAsync(currency, outputs)
                .ConfigureAwait(false);
        }

        private async Task<IEnumerable<ITxOutput>> GetOnlyConfirmedOutputsAsync(
            Currency currency,
            IEnumerable<ITxOutput> outputs)
        {
            var confirmedOutputs = new List<ITxOutput>();

            foreach (var o in outputs)
            {
                var tx = await GetTransactionByIdAsync(currency, o.TxId)
                    .ConfigureAwait(false);

                if (tx?.IsConfirmed ?? false)
                    confirmedOutputs.Add(o);
            }

            return confirmedOutputs;
        }

        public Task<IEnumerable<ITxOutput>> GetOutputsAsync(Currency currency)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var outputs = db.GetCollection(OutputsCollectionName)
                            .Find(Query.EQ(CurrencyKey, currency.Name))
                            .Select(d => (ITxOutput)_bsonMapper.ToObject(
                                type: currency.OutputType(),
                                doc: d))
                            .ToList();

                        return Task.FromResult<IEnumerable<ITxOutput>>(outputs);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting outputs");
            }

            return Task.FromResult(Enumerable.Empty<ITxOutput>());
        }

        public Task<IEnumerable<ITxOutput>> GetOutputsAsync(Currency currency, string address)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var outputs = db
                            .GetCollection(OutputsCollectionName)
                            .Find(Query.And(
                                left: Query.EQ(CurrencyKey, currency.Name),
                                right: Query.EQ(AddressKey, address)))
                            .Select(d => (ITxOutput)_bsonMapper.ToObject(
                                type: currency.OutputType(),
                                doc: d))
                            .ToList();

                        return Task.FromResult<IEnumerable<ITxOutput>>(outputs);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting outputs");
            }

            return Task.FromResult(Enumerable.Empty<ITxOutput>());
        }

        public Task<ITxOutput> GetOutputAsync(Currency currency, string txId, uint index)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var id = $"{txId}:{index}";

                        var document = db.GetCollection(OutputsCollectionName)
                            .FindById(id);

                        var output = document != null
                            ? (ITxOutput)_bsonMapper.ToObject(currency.OutputType(), document)
                            : null;

                        return Task.FromResult(output);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting output");
            }

            return Task.FromResult<ITxOutput>(null);
        }

        #endregion Outputs

        #region Orders

        public Task<bool> UpsertOrderAsync(Order order)
        {
            try
            {
                lock (_syncRoot)
                {
                    if (!CheckOrder(order))
                        return Task.FromResult(false);

                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var document = _bsonMapper.ToDocument(order);

                        var orders = db.GetCollection(OrdersCollectionName);

                        orders.Upsert(document);
                    }

                    return Task.FromResult(true);
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error adding order");
            }

            return Task.FromResult(false);
        }

        private Order GetPendingOrder(string clientOrderId)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var orders = db.GetCollection(OrdersCollectionName);

                        var document = orders.FindOne(
                            Query.And(
                                Query.EQ("_id", clientOrderId),
                                Query.EQ("OrderId", 0)));

                        return document != null
                            ? _bsonMapper.ToObject<Order>(document)
                            : null;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting pending orders");

                return null;
            }
        }

        private bool CheckOrder(Order order)
        {
            if (order.Status == OrderStatus.Pending)
            {
                var pendingOrder = GetPendingOrder(order.ClientOrderId);

                if (pendingOrder != null)
                {
                    Log.Error("Order already pending");

                    return false;
                }
            }
            else if (order.Status == OrderStatus.Placed || order.Status == OrderStatus.Rejected)
            {
                var pendingOrder = GetPendingOrder(order.ClientOrderId);

                if (pendingOrder == null)
                {
                    order.IsApproved = false;

                    // probably a different device order
                    Log.Information("Probably order from another device: {@order}",
                        order.ToString());
                }
                else
                {
                    if (pendingOrder.Status == OrderStatus.Rejected)
                    {
                        Log.Error("Order already rejected");

                        return false;
                    }

                    if (!order.IsContinuationOf(pendingOrder))
                    {
                        Log.Error("Order is not continuation of saved pending order! Order: {@order}, pending order: {@pendingOrder}",
                            order.ToString(),
                            pendingOrder.ToString());

                        return false;
                    }
                }
            }
            else
            {
                var actualOrder = GetOrderById(order.ClientOrderId);

                if (actualOrder == null)
                {
                    Log.Error(
                        messageTemplate: "Order is not continuation of saved order! Order: {@order}",
                        propertyValue: order.ToString());

                    return false;
                }

                if (!order.IsContinuationOf(actualOrder))
                {
                    Log.Error(
                        messageTemplate: "Order is not continuation of saved order! Order: {@order}, saved order: {@actualOrder}",
                        propertyValue0: order.ToString(),
                        propertyValue1: actualOrder.ToString());

                    return false;
                }
            }

            return true;
        }

        public Order GetOrderById(string clientOrderId)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var orders = db.GetCollection(OrdersCollectionName);

                        var document = orders.FindById(clientOrderId);

                        return document != null
                            ? _bsonMapper.ToObject<Order>(document)
                            : null;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting order");

                return null;
            }
        }

        public Order GetOrderById(long id)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var orders = db.GetCollection(OrdersCollectionName);

                        var documents = orders.Find(Query.EQ("OrderId", id));

                        return documents != null && documents.Any()
                            ? _bsonMapper.ToObject<Order>(documents.First())
                            : null;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting order");

                return null;
            }
        }

        #endregion Orders

        #region Swaps

        public Task<bool> AddSwapAsync(ClientSwap swap)
        {
            if (!_swapById.TryAdd(swap.Id, swap))
                return Task.FromResult(false);

            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        db.GetCollection<ClientSwap>(SwapsCollectionName)
                            .Insert(swap);
                    }

                    return Task.FromResult(true);
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Swap add error");
            }

            return Task.FromResult(false);
        }

        public Task<bool> UpdateSwapAsync(ClientSwap swap)
        {
            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var result = db.GetCollection<ClientSwap>(SwapsCollectionName)
                            .Update(swap);

                        return Task.FromResult(result);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Swap update error");
            }

            return Task.FromResult(false);
        }

        public Task<ClientSwap> GetSwapByIdAsync(long id)
        {
            if (_swapById.TryGetValue(id, out var swap))
                return Task.FromResult(swap);

            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        swap = db.GetCollection<ClientSwap>(SwapsCollectionName)
                            .FindById(id);

                        if (swap != null)
                        {
                            _swapById.TryAdd(swap.Id, swap);
                            return Task.FromResult(swap);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Error getting swap by id");
            }

            return Task.FromResult<ClientSwap>(null);
        }

        public Task<IEnumerable<ClientSwap>> GetSwapsAsync()
        {
            if (_swapsLoaded)
                return Task.FromResult<IEnumerable<ClientSwap>>(_swapById.Values);

            try
            {
                lock (_syncRoot)
                {
                    using (var db = new LiteDatabase(ConnectionString, _bsonMapper))
                    {
                        var swaps = db.GetCollection<ClientSwap>(SwapsCollectionName)
                            .Find(Query.All())
                            .ToList();

                        foreach (var swap in swaps)
                            if (!_swapById.ContainsKey(swap.Id))
                                _swapById.TryAdd(swap.Id, swap);

                        _swapsLoaded = true;

                        return Task.FromResult<IEnumerable<ClientSwap>>(_swapById.Values);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Swaps getting error");
            }

            return Task.FromResult(Enumerable.Empty<ClientSwap>());
        }

        #endregion Swaps
    }
}