using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Serilog;
using Microsoft.Extensions.Configuration;

using Atomex.Abstract;
using Atomex.Blockchain;
using Atomex.Blockchain.Abstract;
using Atomex.Blockchain.Helpers;
using Atomex.Core;
using Atomex.Common;
using Atomex.MarketData;
using Atomex.MarketData.Abstract;
using Atomex.Subsystems.Abstract;
using Atomex.Swaps;
using Atomex.Swaps.Abstract;
using Atomex.Wallet.Abstract;

namespace Atomex.Subsystems
{
    public class AtomexClient : IAtomexClient, ISwapClient
    {
        protected static TimeSpan DefaultMaxTransactionTimeout = TimeSpan.FromMinutes(48 * 60);

        public event EventHandler<TerminalServiceEventArgs> ServiceConnected;
        public event EventHandler<TerminalServiceEventArgs> ServiceDisconnected;
        public event EventHandler<TerminalServiceEventArgs> ServiceAuthenticated;
        public event EventHandler<TerminalErrorEventArgs> Error;
        public event EventHandler<OrderEventArgs> OrderReceived;
        public event EventHandler<MarketDataEventArgs> QuotesUpdated;
        public event EventHandler<SwapEventArgs> SwapUpdated;

        public IAccount Account { get; set; }
        private ISymbolsProvider SymbolsProvider { get; set; }
        private ICurrencyQuotesProvider QuotesProvider { get; set; }
        private IConfiguration Configuration { get; }
        private IMarketDataRepository MarketDataRepository { get; set; }
        private ISwapManager SwapManager { get; set; }

        private readonly CancellationTokenSource _cts;

        private readonly HttpClient _httpClient;
        private string _token;
        private bool _isConnected;
        private readonly AsyncQueue<(long, DateTimeOffset)> _waitingQueue;
        private readonly AsyncQueue<long> _swapsQueue;

        private TimeSpan TransactionConfirmationCheckInterval(string currency) =>
            currency == "BTC"
                ? TimeSpan.FromSeconds(120)
                : TimeSpan.FromSeconds(45);

        public AtomexClient(
            IConfiguration configuration,
            IAccount account,
            ISymbolsProvider symbolsProvider,
            ICurrencyQuotesProvider quotesProvider = null)
        {
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            Account = account ?? throw new ArgumentNullException(nameof(account));
            Account.UnconfirmedTransactionAdded += OnUnconfirmedTransactionAddedEventHandler;

            SymbolsProvider = symbolsProvider ?? throw new ArgumentNullException(nameof(symbolsProvider));
            QuotesProvider = quotesProvider;

            _cts = new CancellationTokenSource();

            _httpClient = new HttpClient { BaseAddress = new Uri(Configuration[$"Services:{Account.Network}:Exchange:Url"]) };

            _waitingQueue = new AsyncQueue<(long, DateTimeOffset)>();
            _swapsQueue = new AsyncQueue<long>();
        }

        public async Task StartAsync()
        {
            Log.Information("Start terminal services");

            // init market data repository
            MarketDataRepository = new MarketDataRepository(
                symbols: SymbolsProvider.GetSymbols(Account.Network));

            // start async unconfirmed transactions tracking
            _ = TrackUnconfirmedTransactionsAsync(_cts.Token);

            // init swap manager
            SwapManager = new SwapManager(
                account: Account,
                swapClient: this,
                quotesProvider: QuotesProvider,
                marketDataRepository: MarketDataRepository);

            SwapManager.SwapUpdated += (sender, args) => SwapUpdated?.Invoke(sender, args);

            // start async swaps restore
            _ = SwapManager.RestoreSwapsAsync(_cts.Token);

            // get auth token
            _token = await AuthAsync()
                .ConfigureAwait(false);

            if (_token == null)
                throw new Exception("Auth failed");

            _isConnected = true;

            // todo: run orders, swaps & marketdata loops
            ServiceConnected?.Invoke(this, new TerminalServiceEventArgs(TerminalService.Exchange));
            ServiceConnected?.Invoke(this, new TerminalServiceEventArgs(TerminalService.MarketData));

            _ = CancelAllOrdersAsync(_cts.Token);
            _ = SwapsTrackerAsync(_cts.Token);
            _ = SwapsWaitingAsync(_cts.Token);
            _ = SwapsUpdaterAsync(_cts.Token);
        }

        public Task StopAsync()
        {
            Log.Information("Stop terminal services");

            // cancel all terminal background tasks and loops
            _cts.Cancel();

            SwapManager.SwapUpdated -= (sender, args) => SwapUpdated?.Invoke(sender, args);
            SwapManager.Clear();

            _isConnected = false;

            ServiceDisconnected?.Invoke(this, new TerminalServiceEventArgs(TerminalService.Exchange));
            ServiceDisconnected?.Invoke(this, new TerminalServiceEventArgs(TerminalService.MarketData));

            return Task.CompletedTask;
        }

        private async Task<string> AuthAsync()
        {
            using var securePublicKey = Account.Wallet.GetServicePublicKey(index: 0);
            using var publicKey = securePublicKey.ToUnsecuredBytes();

            var timeStamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var message = "Hello, Atomex!";

            var signature = await Account.Wallet
                .SignByServiceKeyAsync(Encoding.UTF8.GetBytes($"{message}{timeStamp}"), keyIndex:0)
                .ConfigureAwait(false);

            var body = new
            {
                timeStamp = timeStamp,
                message   = message,
                publicKey = Hex.ToHexString(publicKey),
                signature = Hex.ToHexString(signature),
                algorithm = "Sha256WithEcdsa:BtcMsg"
            };

            var response = await _httpClient
                .PostAsync("token", new StringContent(
                    content: JsonConvert.SerializeObject(body),
                    encoding: Encoding.UTF8,
                    mediaType: "application/json"))
                .ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                Log.Error("Auth failed");
                return null;
            }

            var responseContent = await response.Content
                .ReadAsStringAsync()
                .ConfigureAwait(false);

            var token = JsonConvert
                .DeserializeObject<JObject>(responseContent)
                ?["token"]
                ?.Value<string>();

            if (token != null)
            {
                if (_httpClient.DefaultRequestHeaders.Contains("Authorization"))
                    _httpClient.DefaultRequestHeaders.Remove("Authorization");

                _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $"Bearer {token}");
            }

            return token;
        }

        public MarketDataOrderBook GetOrderBook(string symbol) =>
            MarketDataRepository?.OrderBookBySymbol(symbol);

        public MarketDataOrderBook GetOrderBook(Symbol symbol) =>
            MarketDataRepository?.OrderBookBySymbol(symbol.Name);

        public Quote GetQuote(Symbol symbol) =>
            MarketDataRepository?.QuoteBySymbol(symbol.Name);

        public bool IsServiceConnected(TerminalService service) => _isConnected;

        public async void OrderCancelAsync(Order order)
        {
            try
            {
                var response = await _httpClient
                    .DeleteAsync($"orders/{order.Id}?symbol={order.Symbol}&side={order.Side}")
                    .ConfigureAwait(false);

                var responseContent = await response.Content
                    .ReadAsStringAsync()
                    .ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    Log.Error($"Order cancel failed: {responseContent}");
                    return;
                }

                var result = JsonConvert
                    .DeserializeObject<JObject>(responseContent)
                    ?["result"]
                    ?.Value<bool>() ?? false;

                if (result)
                {
                    var dbOrder = Account
                        .GetOrderById(order.Id);

                    if (dbOrder != null)
                    {
                        dbOrder.Status = OrderStatus.Canceled;

                        await Account
                            .UpsertOrderAsync(dbOrder)
                            .ConfigureAwait(false);

                        OrderReceived(this, new OrderEventArgs(dbOrder));
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Order cancel error");
            }
        }

        public async void OrderSendAsync(Order order)
        {
            order.ClientOrderId = Guid.NewGuid().ToByteArray().ToHexString(0, 16);

            try
            {
                await Account
                    .UpsertOrderAsync(order)
                    .ConfigureAwait(false);

                var body = new
                {
                    clientOrderId = order.ClientOrderId,
                    symbol        = order.Symbol,
                    price         = order.Price,
                    qty           = order.Qty,
                    side          = order.Side.ToString(),
                    type          = order.Type.ToString(),
                    
                    // todo: proof of funds

                    requisites = new
                    {
                        baseCurrencyContract  = GetSwapContract(order.Symbol.BaseCurrency()),
                        quoteCurrencyContract = GetSwapContract(order.Symbol.QuoteCurrency())
                    }
                };

                var response = await _httpClient
                    .PostAsync("orders", new StringContent(
                        content: JsonConvert.SerializeObject(body),
                        encoding: Encoding.UTF8,
                        mediaType: "application/json"))
                    .ConfigureAwait(false);

                var responseContent = await response.Content
                    .ReadAsStringAsync()
                    .ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    Log.Error($"Order send failed: {responseContent}");
                    return;
                }

                order.Id = JsonConvert.DeserializeObject<JObject>(responseContent)?["orderId"]?.Value<long>() ?? 0;

                if (order.Id != 0)
                {
                    order.Status = OrderStatus.Placed;

                    await Account
                        .UpsertOrderAsync(order)
                        .ConfigureAwait(false);

                    OrderReceived(this, new OrderEventArgs(order));
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Order send error");
            }
        }

        private string GetSwapContract(string currency) =>
            currency switch
            {
                "ETH" => Account.Currencies.Get<Ethereum>("ETH").SwapContractAddress,
                "XTZ" => Account.Currencies.Get<Tezos>("XTZ").SwapContractAddress,
                _ => null
            };

        public void SubscribeToMarketData(SubscriptionType type)
        {
            // nothing to do...
        }

        private async Task CancelAllOrdersAsync(
            CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await _httpClient
                    .GetAsync($"orders?limit=1000&active=true")
                    .ConfigureAwait(false);

                var responseContent = await response.Content
                    .ReadAsStringAsync()
                    .ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    Log.Error($"Orders getting failed: {responseContent}");
                    return;
                }

                var receivedOrders = JsonConvert.DeserializeObject<JArray>(responseContent);

                foreach (var order in receivedOrders)
                {
                    OrderCancelAsync(new Order
                    {
                        Id     = order["id"].Value<long>(),
                        Symbol = order["symbol"].Value<string>(),
                        Side   = (Side)Enum.Parse(typeof(Side), order["side"].Value<string>())
                    });
                }
            }
            catch (OperationCanceledException)
            {
                Log.Debug("CancelAllOrdersAsync canceled.");
            }
            catch (Exception e)
            {
                Log.Error(e, "CancelAllOrdersAsync error");
            }
        }

        public enum PartyStatus
        {
            Created,
            Involved,
            PartiallyInitiated,
            Initiated,
            Redeemed,
            Refunded,
            Lost,
            Jackpot
        }

        private async Task SwapsTrackerAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var swapId = await _swapsQueue
                        .TakeAsync(cancellationToken)
                        .ConfigureAwait(false);

                    var response = await _httpClient
                        .GetAsync($"swaps/{swapId}")
                        .ConfigureAwait(false);

                    var responseContent = await response.Content
                        .ReadAsStringAsync()
                        .ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        Log.Error($"Swap status getting failed: {responseContent}");
                        return;
                    }

                    var receivedSwap = JsonConvert.DeserializeObject<JObject>(responseContent);

                    var swap = ParseSwap(receivedSwap);

                    await SwapManager
                        .HandleSwapAsync(swap, cancellationToken)
                        .ConfigureAwait(false);

                    await Task
                        .Delay(1000, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                Log.Debug("SwapsTrackerAsync canceled.");
            }
            catch (Exception e)
            {
                Log.Error(e, "Swaps tracking error");
            }
        }

        private Swap ParseSwap(JToken receivedSwap)
        {
            var status = SwapStatus.Empty;

            var userStatus  = (PartyStatus)Enum.Parse(typeof(PartyStatus), receivedSwap["user"]["status"].Value<string>());
            var partyStatus = (PartyStatus)Enum.Parse(typeof(PartyStatus), receivedSwap["user"]["status"].Value<string>());

            if (userStatus > PartyStatus.Created)
                status |= SwapStatus.Initiated;

            if (partyStatus > PartyStatus.Created)
                status |= SwapStatus.Accepted;

            var id = receivedSwap["id"].Value<long>();

            if (userStatus == PartyStatus.Created || partyStatus == PartyStatus.Created)
            {
                // needs to wait
                _waitingQueue.Add((id, DateTimeOffset.UtcNow));
            }

            return new Swap
            {
                Id         = receivedSwap["id"].Value<long>(),
                SecretHash = Hex.FromString(receivedSwap["secretHash"].Value<string>()),
                Status     = status,

                TimeStamp    = receivedSwap["timeStamp"].Value<DateTime>(),
                Symbol       = receivedSwap["symbol"].Value<string>(),
                Side         = (Side)Enum.Parse(typeof(Side), receivedSwap["side"].Value<string>()),
                Price        = receivedSwap["price"].Value<decimal>(),
                Qty          = receivedSwap["qty"].Value<decimal>(),
                IsInitiative = receivedSwap["isInitiator"].Value<bool>(),

                ToAddress       = receivedSwap["user"]?["requisites"]?["receivingAddress"]?.Value<string>(),
                RewardForRedeem = receivedSwap["user"]?["requisites"]?["rewardForRedeem"]?.Value<decimal>() ?? 0,

                PartyAddress         = receivedSwap["counterParty"]?["requisites"]?["receivingAddress"]?.Value<string>(),
                PartyRewardForRedeem = receivedSwap["counterParty"]?["requisites"]?["rewardForRedeem"]?.Value<decimal>() ?? 0,
            };
        }

        private async Task SwapsWaitingAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var (swapId, timeStamp) = await _waitingQueue
                        .TakeAsync(cancellationToken)
                        .ConfigureAwait(false);

                    if (DateTimeOffset.UtcNow - timeStamp < TimeSpan.FromSeconds(3))
                    {
                        await Task
                            .Delay(DateTimeOffset.UtcNow - timeStamp, cancellationToken)
                            .ConfigureAwait(false);
                    }

                    _swapsQueue.Add(swapId);
                }
            }
            catch (OperationCanceledException)
            {
                Log.Debug("SwapsWaitingAsync canceled.");
            }
            catch (Exception e)
            {
                Log.Error(e, "Swaps waiting error");
            }
        }

        private async Task SwapsUpdaterAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                var swaps = await Account
                    .GetSwapsAsync()
                    .ConfigureAwait(false);

                var lastSwapId = swaps.Any()
                    ? swaps.MaxBy(s => s.Id).Id
                    : 0;

                while (!cancellationToken.IsCancellationRequested)
                {
                    var response = await _httpClient
                        .GetAsync($"swaps?afterId={lastSwapId}")
                        .ConfigureAwait(false);

                    var responseContent = await response.Content
                        .ReadAsStringAsync()
                        .ConfigureAwait(false);

                    if (!response.IsSuccessStatusCode)
                    {
                        Log.Error($"Swaps getting failed: {responseContent}");
                        return;
                    }

                    var receivedSwaps = JsonConvert.DeserializeObject<JArray>(responseContent);

                    foreach ( var receivedSwap in receivedSwaps)
                    {
                        var swap = ParseSwap(receivedSwap);

                        Log.Debug($"Swap with id {swap.Id} found.");

                        if (lastSwapId < swap.Id)
                            lastSwapId = swap.Id;
                    }

                    await Task
                        .Delay(TimeSpan.FromSeconds(5))
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                Log.Debug("SwapsUpdaterAsync canceled.");
            }
            catch (Exception e)
            {
                Log.Error(e, "Swaps updater error");
            }
        }

        #region ISwapInterface

        public async void SwapInitiateAsync(Swap swap)
        {
            var body = new
            {
                secretHash       = swap.SecretHash.ToHexString(),
                receivingAddress = swap.ToAddress,
                refundAddress    = swap.RefundAddress,
                rewardForRedeem  = swap.RewardForRedeem,
                lockTime         = CurrencySwap.DefaultInitiatorLockTimeInSeconds
            };

            var response = await _httpClient
                .PostAsync($"swaps/{swap.Id}/requisites", new StringContent(
                    content: JsonConvert.SerializeObject(body),
                    encoding: Encoding.UTF8,
                    mediaType: "application/json"))
                .ConfigureAwait(false);

            var responseContent = await response.Content
                .ReadAsStringAsync()
                .ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                Log.Error($"Swap initiated send failed: {responseContent}");
                return;
            }

            // todo: check boolean response
        }

        public void SwapAcceptAsync(Swap swap)
        {
            // nothing to do by swap initiator
        }

        public void SwapStatusAsync(Request<Swap> swap)
        {
            _swapsQueue.Add(swap.Data.Id);
        }

        #endregion ISwapInterface

        #region TransactionTracking

        private void OnUnconfirmedTransactionAddedEventHandler(object sender, TransactionEventArgs e)
        {
            if (!e.Transaction.IsConfirmed && e.Transaction.State != BlockchainTransactionState.Failed)
                TrackTransactionAsync(e.Transaction, _cts.Token);
        }

        private async Task TrackUnconfirmedTransactionsAsync(
            CancellationToken cancellationToken)
        {
            try
            {
                var txs = await Account
                    .GetTransactionsAsync()
                    .ConfigureAwait(false);

                foreach (var tx in txs)
                    if (!tx.IsConfirmed && tx.State != BlockchainTransactionState.Failed)
                        _ = TrackTransactionAsync(tx, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Log.Debug("TrackUnconfirmedTransactionsAsync canceled.");
            }
            catch (Exception e)
            {
                Log.Error(e, "Unconfirmed transactions track error.");
            }
        }

        private Task TrackTransactionAsync(
            IBlockchainTransaction transaction,
            CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await transaction
                        .IsTransactionConfirmed(
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    if (result.HasError)
                        break;

                    if (result.Value.IsConfirmed || (result.Value.Transaction != null && result.Value.Transaction.State == BlockchainTransactionState.Failed))
                    {
                        TransactionProcessedHandler(result.Value.Transaction, cancellationToken);
                        break;
                    }

                    // mark old unconfirmed txs as failed
                    if (transaction.CreationTime != null &&
                        DateTime.UtcNow > transaction.CreationTime.Value.ToUniversalTime() + DefaultMaxTransactionTimeout &&
                        !Currencies.IsBitcoinBased(transaction.Currency.Name))
                    {
                        transaction.State = BlockchainTransactionState.Failed;

                        TransactionProcessedHandler(transaction, cancellationToken);
                        break;
                    }

                    await Task.Delay(TransactionConfirmationCheckInterval(transaction?.Currency.Name), cancellationToken)
                        .ConfigureAwait(false);
                }
            }, _cts.Token);
        }

        private async void TransactionProcessedHandler(
            IBlockchainTransaction tx,
            CancellationToken cancellationToken)
        {
            try
            {
                await Account
                    .UpsertTransactionAsync(tx, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                await Account
                    .UpdateBalanceAsync(tx.Currency.Name, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                Log.Debug("Transaction processed handler task canceled.");
            }
            catch (Exception e)
            {
                Log.Error(e, "Error in transaction processed handler.");
            }
        }

        #endregion TransactionTracking
    }
}