﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Atomex.Blockchain.Abstract;
using Atomex.Common;
using Atomex.Core;
using Atomex.Wallet.Abstract;
using Atomex.Abstract;
using Atomex.Common.Bson;
using Serilog;
using LiteDB;
using Newtonsoft.Json;

namespace Atomex.Wallet
{
  public class AccountDataRepository : IAccountDataRepository
  {
    private readonly Dictionary<string, WalletAddress> _addresses;
    private readonly Dictionary<string, string> _addressesBson;

    private readonly Dictionary<string, IBlockchainTransaction> _transactions;
    private readonly Dictionary<string, string> _transactionsBson;

    private readonly Dictionary<string, OutputEntity> _outputs;
    private readonly Dictionary<string, string> _outputsBson;

    private readonly Dictionary<long, Swap> _swaps;
    private readonly Dictionary<long, string> _swapsBson;

    private readonly Dictionary<string, Order> _orders;
    private readonly Dictionary<string, string> _ordersBson;

    private ICurrencies _currencies;

    private readonly object _sync;

    private BsonMapper _bsonMapper;

    public Action<AvailableDataType, string, string> SaveDataCallback;

    public enum AvailableDataType
    {
      WalletAddress,
      Transaction,
      RemoveTransaction,
      Output,
      Swap,
      Order
    }

    public class OutputEntity
    {
      public ITxOutput Output { get; set; }
      public string Currency { get; set; }
      public string Address { get; set; }
    }

    public AccountDataRepository(ICurrencies currencies, string initialData = null)
    {
      _addresses = new Dictionary<string, WalletAddress>();
      _addressesBson = new Dictionary<string, string>();

      _transactions = new Dictionary<string, IBlockchainTransaction>();
      _transactionsBson = new Dictionary<string, string>();

      _outputs = new Dictionary<string, OutputEntity>();
      _outputsBson = new Dictionary<string, string>();

      _swaps = new Dictionary<long, Swap>();
      _swapsBson = new Dictionary<long, string>();

      _orders = new Dictionary<string, Order>();
      _ordersBson = new Dictionary<string, string>();

      _sync = new object();

      _bsonMapper = new BsonMapper()
                .UseSerializer(new CurrencyToBsonSerializer(currencies))
                .UseSerializer(new BigIntegerToBsonSerializer())
                .UseSerializer(new JObjectToBsonSerializer())
                .UseSerializer(new WalletAddressToBsonSerializer())
                .UseSerializer(new OrderToBsonSerializer())
                .UseSerializer(new BitcoinBasedTransactionToBsonSerializer(currencies))
                .UseSerializer(new BitcoinBasedTxOutputToBsonSerializer())
                .UseSerializer(new EthereumTransactionToBsonSerializer())
                .UseSerializer(new TezosTransactionToBsonSerializer())
                .UseSerializer(new SwapToBsonSerializer(currencies));
      _currencies = currencies;

      if (initialData != null)
      {
        List<BrowserDBData> dbData = JsonConvert.DeserializeObject<List<BrowserDBData>>(initialData);
        foreach (var dbObj in dbData)
        {
          if (dbObj.type == AvailableDataType.WalletAddress.ToString())
          {
            _addresses[dbObj.id] = _bsonMapper.ToObject<WalletAddress>(BsonSerializer.Deserialize(Convert.FromBase64String(dbObj.data)));
          }
          else

          if (dbObj.type == AvailableDataType.Transaction.ToString())
          {
            string[] parsedId = dbObj.id.Split(Convert.ToChar("/"));
            string id = parsedId[0];
            string currency = parsedId[1];

            BsonDocument bd = BsonSerializer.Deserialize(Convert.FromBase64String(dbObj.data));
            _transactions[$"{id}:{currency}"] = (IBlockchainTransaction)_bsonMapper.ToObject(doc: bd, type: _currencies.GetByName(currency).TransactionType);
          }
          else

          if (dbObj.type == AvailableDataType.Swap.ToString())
          {
            _swaps[long.Parse(dbObj.id)] = _bsonMapper.ToObject<Swap>(BsonSerializer.Deserialize(Convert.FromBase64String(dbObj.data)));
          }
          else

          if (dbObj.type == AvailableDataType.Output.ToString())
          {
            string[] parsedId = dbObj.id.Split(Convert.ToChar("/"));
            string id = parsedId[0];
            string currency = parsedId[1];
            string address = parsedId[2];

            BsonDocument bd = BsonSerializer.Deserialize(Convert.FromBase64String(dbObj.data));
            BitcoinBasedCurrency BtcBasedCurrency = _currencies.Get<BitcoinBasedCurrency>(currency);
            ITxOutput output = (ITxOutput)_bsonMapper.ToObject(doc: bd, type: BtcBasedCurrency.OutputType());

            _outputs[id] = new OutputEntity { Output = output, Currency = currency, Address = address };
          }
          else

          if (dbObj.type == AvailableDataType.Order.ToString())
          {
            _orders[dbObj.id] = _bsonMapper.ToObject<Order>(BsonSerializer.Deserialize(Convert.FromBase64String(dbObj.data)));
          }
        }
      }

    }

    #region Addresses

    public virtual Task<bool> UpsertAddressAsync(WalletAddress walletAddress)
    {
      lock (_sync)
      {
        var walletId = $"{walletAddress.Currency}:{walletAddress.Address}";

        _addresses[walletId] = walletAddress; // todo: copy?

        var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument(walletAddress)));
        _addressesBson[walletId] = data;
        SaveDataCallback?.Invoke(AvailableDataType.WalletAddress, walletId, data);

        return Task.FromResult(true);
      }
    }

    public virtual Task<int> UpsertAddressesAsync(IEnumerable<WalletAddress> walletAddresses)
    {
      lock (_sync)
      {
        foreach (var walletAddress in walletAddresses)
        {
          var walletId = $"{walletAddress.Currency}:{walletAddress.Address}";

          _addresses[walletId] = walletAddress; // todo: copy?

          var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument(walletAddress)));
          _addressesBson[walletId] = data;
          SaveDataCallback?.Invoke(AvailableDataType.WalletAddress, walletId, data);
        }

        return Task.FromResult(walletAddresses.Count());
      }
    }

    public virtual Task<bool> TryInsertAddressAsync(WalletAddress walletAddress)
    {
      lock (_sync)
      {
        var walletId = $"{walletAddress.Currency}:{walletAddress.Address}";

        if (_addresses.ContainsKey(walletId))
          return Task.FromResult(false);

        _addresses[walletId] = walletAddress; // todo: copy?

        var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument(walletAddress)));
        _addressesBson[walletId] = data;
        SaveDataCallback?.Invoke(AvailableDataType.WalletAddress, walletId, data);

        return Task.FromResult(true);
      }
    }

    public virtual Task<WalletAddress> GetWalletAddressAsync(string currency, string address)
    {
      lock (_sync)
      {
        var walletId = $"{currency}:{address}";

        if (_addresses.TryGetValue(walletId, out var walletAddress))
          return Task.FromResult(walletAddress.Copy());

        return Task.FromResult<WalletAddress>(null);
      }
    }

    public virtual Task<WalletAddress> GetLastActiveWalletAddressAsync(string currency, int chain)
    {
      lock (_sync)
      {
        var address = _addresses.Values
            .Where(w => w.Currency == currency && w.KeyIndex.Chain == chain && w.HasActivity)
            .OrderByDescending(w => w.KeyIndex.Index)
            .FirstOrDefault();

        return address != null
            ? Task.FromResult(address.Copy())
            : Task.FromResult<WalletAddress>(null);
      }
    }

    public virtual Task<IEnumerable<WalletAddress>> GetUnspentAddressesAsync(
        string currency,
        bool includeUnconfirmed = true)
    {
      lock (_sync)
      {
        var addresses = includeUnconfirmed
            ? _addresses.Values
                .Where(w => w.Currency == currency && (w.Balance != 0 || w.UnconfirmedIncome != 0 || w.UnconfirmedOutcome != 0))
            : _addresses.Values
                .Where(w => w.Currency == currency && w.Balance != 0);
        return Task.FromResult(addresses.Select(a => a.Copy()));
      }
    }
    
    public virtual Task<IEnumerable<WalletAddress>> GetAddressesAsync(
        string currency)
    {
        lock (_sync)
        {
            var addresses = _addresses.Values
                .Where(w => w.Currency == currency);

            return Task.FromResult(addresses);
        }
    }

    #endregion Addresses

    #region Transactions

    public virtual Task<bool> UpsertTransactionAsync(IBlockchainTransaction tx)
    {
      lock (_sync)
      {
        var txAge = DateTime.Now - tx.CreationTime;
        if (_transactions.ContainsKey($"{tx.Id}:{tx.Currency.Name}") && tx.State == BlockchainTransactionState.Confirmed && txAge.Value.TotalDays >= 1)
        {
          // todo: remove this;
          return Task.FromResult(true);
        }
        _transactions[$"{tx.Id}:{tx.Currency.Name}"] = tx; // todo: copy?

        var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument<IBlockchainTransaction>(tx)));
        _transactionsBson[$"{tx.Id}/{tx.Currency.Name}"] = data;

        SaveDataCallback?.Invoke(AvailableDataType.Transaction, $"{tx.Id}/{tx.Currency.Name}", data);

        return Task.FromResult(true);
      }
    }

    public virtual Task<IBlockchainTransaction> GetTransactionByIdAsync(
        string currency,
        string txId,
        Type transactionType)
    {
      lock (_sync)
      {
        if (_transactions.TryGetValue($"{txId}:{currency}", out var tx))
        {
          return Task.FromResult(tx);
        }

        return Task.FromResult<IBlockchainTransaction>(null);
      }
    }

    public virtual Task<IEnumerable<IBlockchainTransaction>> GetTransactionsAsync(
        string currency,
        Type transactionType)
    {
      lock (_sync)
      {
        var txs = _transactions.Values
            .Where(t => t.Currency.Name == currency);

        return Task.FromResult(txs);
      }
    }

    public virtual Task<IEnumerable<IBlockchainTransaction>> GetUnconfirmedTransactionsAsync(
        string currency,
        Type transactionType)
    {
      lock (_sync)
      {
        var txs = _transactions.Values
            .Where(t => t.Currency.Name == currency && !t.IsConfirmed);

        return Task.FromResult(txs);
      }
    }

    public virtual Task<bool> RemoveTransactionByIdAsync(string id)
    {
      lock (_sync)
      {
        string _txBsonKey = _transactionsBson.Keys.Where(key => key.Contains(id)).First();
        _transactionsBson.Remove(_txBsonKey);
        SaveDataCallback?.Invoke(AvailableDataType.RemoveTransaction, _txBsonKey, null);

        return Task.FromResult(_transactions.Remove(id));
      }
    }

    #endregion Transactions

    #region Outputs

    public virtual Task<bool> UpsertOutputsAsync(
        IEnumerable<ITxOutput> outputs,
        string currency,
        string address)
    {
      lock (_sync)
      {
        foreach (var output in outputs)
        {
          var id = $"{output.TxId}:{output.Index}";
          var entity = new OutputEntity
          {
            Output = output, // todo: copy?
            Currency = currency,
            Address = address
          };

          _outputs[id] = entity;

          var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument(output)));
          _outputsBson[$"{id}/{currency}/{address}"] = data;
          SaveDataCallback?.Invoke(AvailableDataType.Output, $"{id}/{currency}/{address}", data);
        }

        return Task.FromResult(true);
      }
    }

    public virtual async Task<IEnumerable<ITxOutput>> GetAvailableOutputsAsync(
        string currency,
        Type outputType,
        Type transactionType)
    {
      var outputs = (await GetOutputsAsync(currency, outputType)
          .ConfigureAwait(false))
          .Where(o => !o.IsSpent)
          .ToList();

      return await GetOnlyConfirmedOutputsAsync(currency, outputs, transactionType)
          .ConfigureAwait(false);
    }

    public virtual async Task<IEnumerable<ITxOutput>> GetAvailableOutputsAsync(
        string currency,
        string address,
        Type outputType,
        Type transactionType)
    {
      var outputs = (await GetOutputsAsync(currency, address, outputType)
          .ConfigureAwait(false))
          .Where(o => !o.IsSpent)
          .ToList();

      return await GetOnlyConfirmedOutputsAsync(currency, outputs, transactionType)
          .ConfigureAwait(false);
    }

    private async Task<IEnumerable<ITxOutput>> GetOnlyConfirmedOutputsAsync(
        string currency,
        IEnumerable<ITxOutput> outputs,
        Type transactionType)
    {
      var confirmedOutputs = new List<ITxOutput>();

      foreach (var o in outputs)
      {
        var tx = await GetTransactionByIdAsync(currency, o.TxId, transactionType)
            .ConfigureAwait(false);

        if (tx?.IsConfirmed ?? false)
          confirmedOutputs.Add(o);
      }

      return confirmedOutputs;
    }

    public virtual Task<IEnumerable<ITxOutput>> GetOutputsAsync(
        string currency,
        Type outputType)
    {
      lock (_sync)
      {
        IEnumerable<ITxOutput> outputs = _outputs.Values
            .Where(o => o.Currency == currency)
            .Select(o => o.Output)
            .ToList();

        return Task.FromResult(outputs);
      }
    }

    public virtual Task<IEnumerable<ITxOutput>> GetOutputsAsync(
        string currency,
        string address,
        Type outputType)
    {
      lock (_sync)
      {
        IEnumerable<ITxOutput> outputs = _outputs.Values
            .Where(o => o.Currency == currency && o.Address == address)
            .Select(o => o.Output)
            .ToList();

        return Task.FromResult(outputs);
      }
    }

    public virtual Task<ITxOutput> GetOutputAsync(
        string currency,
        string txId,
        uint index,
        Type outputType)
    {
      lock (_sync)
      {
        var id = $"{txId}:{index}";

        if (_outputs.TryGetValue(id, out var output))
          return Task.FromResult(output.Output);

        return Task.FromResult<ITxOutput>(null);
      }
    }

    #endregion Outputs

    #region Orders

    public virtual Task<bool> UpsertOrderAsync(Order order)
    {
      lock (_sync)
      {
        if (!VerifyOrder(order))
          return Task.FromResult(false);

        _orders[order.ClientOrderId] = order; // todo: copy?

        var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument(order)));
        _ordersBson[order.ClientOrderId] = data;
        SaveDataCallback?.Invoke(AvailableDataType.Order, order.ClientOrderId, data);

        return Task.FromResult(true);
      }
    }

    public virtual Order GetOrderById(string clientOrderId)
    {
      lock (_sync)
      {
        if (_orders.TryGetValue(clientOrderId, out var order))
          return order;

        return null;
      }
    }

    public virtual Order GetOrderById(long id)
    {
      lock (_sync)
      {
        return _orders.Values.SingleOrDefault(o => o.Id == id);
      }
    }

    private Order GetPendingOrder(string clientOrderId)
    {
      lock (_sync)
      {
        if (_orders.TryGetValue(clientOrderId, out var order))
          return order.Id == 0 ? order : null;

        return null;
      }
    }

    private bool VerifyOrder(Order order)
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
          Log.Error("Order is not continuation of saved order! Order: {@order}",
              order.ToString());

          return false;
        }

        if (!order.IsContinuationOf(actualOrder))
        {
          Log.Error("Order is not continuation of saved order! Order: {@order}, saved order: {@actualOrder}",
              order.ToString(),
              actualOrder.ToString());

          return false;
        }

        order.IsApproved = actualOrder.IsApproved; // save approve
      }

      return true;
    }

    #endregion Orders

    #region Swaps

    public virtual Task<bool> AddSwapAsync(Swap swap)
    {
      lock (_sync)
      {
        _swaps[swap.Id] = swap; // todo: copy?

        var data = Convert.ToBase64String(BsonSerializer.Serialize(_bsonMapper.ToDocument(swap)));
        _swapsBson[swap.Id] = data;
        SaveDataCallback?.Invoke(AvailableDataType.Swap, swap.Id.ToString(), data);

        return Task.FromResult(true);
      }
    }

    public virtual Task<bool> UpdateSwapAsync(Swap swap)
    {
      return AddSwapAsync(swap);
    }

    public virtual Task<Swap> GetSwapByIdAsync(long id)
    {
      lock (_sync)
      {
        if (_swaps.TryGetValue(id, out var swap))
          return Task.FromResult(swap);

        return Task.FromResult<Swap>(null);
      }
    }

    public virtual Task<IEnumerable<Swap>> GetSwapsAsync()
    {
      lock (_sync)
      {
        return Task.FromResult<IEnumerable<Swap>>(_swaps.Values.ToList()); // todo: copy!
      }
    }

    #endregion Swaps
  }

  public class BrowserDBData
  {
    public string type;
    public string id;
    public string data;
  }
}