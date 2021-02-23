using System;
using System.Threading;
using System.Threading.Tasks;
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
        }

        public async Task StartAsync()
        {
            Log.Information("Start terminal services");

            // init market data repository
            MarketDataRepository = new MarketDataRepository(
                symbols: SymbolsProvider.GetSymbols(Account.Network));

            // start async unconfirmed transactions tracking
            TrackUnconfirmedTransactionsAsync(_cts.Token).FireAndForget();

            // init swap manager
            SwapManager = new SwapManager(
                account: Account,
                swapClient: this,
                quotesProvider: QuotesProvider,
                marketDataRepository: MarketDataRepository);

            SwapManager.SwapUpdated += (sender, args) => SwapUpdated?.Invoke(sender, args);

            // start async swaps restore
            SwapManager.RestoreSwapsAsync(_cts.Token).FireAndForget();

            // get auth token
            _token = await AuthAsync()
                .ConfigureAwait(false);

            if (_token == null)
                throw new Exception("Auth failed");

            _isConnected = true;

            // todo: run orders, swaps & marketdata loops
            ServiceConnected?.Invoke(this, new TerminalServiceEventArgs(TerminalService.Exchange));
            ServiceConnected?.Invoke(this, new TerminalServiceEventArgs(TerminalService.MarketData));
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
                .PostAsync("/token", new StringContent(
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
                    .DeleteAsync($"/orders/{order.Id}?symbol={order.Symbol}&side={order.Side}")
                    .ConfigureAwait(false);

                var responseContent = await response.Content
                    .ReadAsStringAsync()
                    .ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    Log.Error($"Order cancel failed: {responseContent}");
                    return;
                }

                // todo: if true raise OrderUpdated -> Canceled
            }
            catch (Exception e)
            {
                Log.Error(e, "Order cancel error");
            }
        }

        public async void OrderSendAsync(Order order)
        {
            order.ClientOrderId = Guid.NewGuid().ToString();

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
                        baseCurrencyContract  = "",
                        quoteCurrencyContract = ""
                    }
                };

                var response = await _httpClient
                    .PostAsync("/orders", new StringContent(
                        content: JsonConvert.SerializeObject(body),
                        encoding: Encoding.UTF8,
                        mediaType: "application/json"))
                    .ConfigureAwait(false);

                var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    Log.Error($"Order send failed: {responseContent}");
                    return;
                }

                // todo: get order id and save to db??
                // todo: if order id not null taise OrderUpdated -> Placed
            }
            catch (Exception e)
            {
                Log.Error(e, "Order send error");
            }
        }

        public void SubscribeToMarketData(SubscriptionType type)
        {
            // nothing to do...
        }

        #region ISwapInterface

        public void SwapInitiateAsync(Swap swap)
        {
            // todo: call add swap requisites
            throw new NotImplementedException();
        }

        public void SwapAcceptAsync(Swap swap)
        {
            // nothing to do by swap initiator
        }

        public void SwapStatusAsync(Request<Swap> swap)
        {
            // todo: call get swap and raise SwapManager.UpdateSwap
            throw new NotImplementedException();
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
                        TrackTransactionAsync(tx, cancellationToken).FireAndForget();
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