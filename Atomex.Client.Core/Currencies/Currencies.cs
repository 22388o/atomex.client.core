﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Configuration;
using Serilog;

using Atomex.Abstract;
using Atomex.Core;
using Atomex.EthereumTokens;
using Atomex.TezosTokens;

namespace Atomex
{
    public class Currencies : ICurrencies
    {
        private readonly string[] _currenciesOrder = new[]
        {
            "BTC",
            "ETH",
            "LTC",
            "XTZ",
            "USDT",
            "TZBTC",
            "KUSD",
            "WBTC",
            "TBTC"
        };

        private readonly object _sync = new();
        private IDictionary<string, CurrencyConfig> _currencies;

        public Currencies(IConfiguration configuration)
        {
            Update(configuration);
        }

        public void Update(IConfiguration configuration)
        {
            lock (_sync)
            {
                var currencies = new List<CurrencyConfig>();

                foreach (var section in configuration.GetChildren())
                {
                    try
                    {
                        var currencyConfig = GetFromSection(section);

                        if (currencyConfig != null)
                            currencies.Add(currencyConfig);
                    }
                    catch (Exception e)
                    {
                        Log.Warning(e, "Currency configuration update error.");
                    }
                }

                if (_currencies != null)
                {
                    var difference = _currencies.Keys
                        .Except(currencies.Select(c => c.Name))
                        .Select(c => _currencies[c]);

                    if (difference.Any())
                        currencies.AddRange(difference);
                }

                _currencies = currencies
                    .ToDictionary(c => c.Name, c => c);
            }
        }

        public CurrencyConfig GetByName(string name)
        {
            lock (_sync)
            {
                return _currencies.TryGetValue(name, out var currency)
                    ? currency
                    : null;
            }
        }

        public T Get<T>(string name) where T : CurrencyConfig =>
            GetByName(name) as T;

        private CurrencyConfig GetFromSection(IConfigurationSection configurationSection)
        {
            return configurationSection.Key switch
            {
                "BTC"   => (CurrencyConfig) new BitcoinConfig(configurationSection),
                "LTC"   => new LitecoinConfig(configurationSection),
                "ETH"   => new EthereumConfig(configurationSection),
                "XTZ"   => new TezosConfig(configurationSection),
                "USDT"  => new Erc20Config(configurationSection),
                "TBTC"  => new Erc20Config(configurationSection),
                "WBTC"  => new Erc20Config(configurationSection),
                "TZBTC" => new Fa12Config(configurationSection),
                "KUSD"  => new Fa12Config(configurationSection),

                "FA12"  => new Fa12Config(configurationSection),
                "FA2"   => new Fa2Config(configurationSection),
                _       => null
            };
        }

        public IEnumerator<CurrencyConfig> GetEnumerator()
        {
            lock (_sync)
            {
                var result = new List<CurrencyConfig>(_currencies.Values.Count);

                foreach (var currencyByOrder in _currenciesOrder)
                    if (_currencies.TryGetValue(currencyByOrder, out var currency))
                        result.Add(currency);

                return result.GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public static bool IsBitcoinBased(string name) =>
            name == "BTC" ||
            name == "LTC";

        public static bool IsTezosBased(string name) =>
            name == "XTZ" || IsTezosToken(name);

        public static bool IsTezosToken(string name) =>
            name == "TZBTC" ||
            name == "KUSD" ||
            name == "FA2" ||
            name == "FA12";

        public static bool HasTokens(string name) =>
            name == "ETH" ||
            name == "XTZ";

        public static bool IsEthereumToken(string name) =>
            name == "USDT" ||
            name == "WBTC" ||
            name == "TBTC";
    }
}