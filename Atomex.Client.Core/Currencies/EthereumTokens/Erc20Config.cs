﻿using System.Globalization;
using System.Numerics;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Configuration;

using Atomex.Blockchain.Ethereum;
using Atomex.Common;
using Atomex.Wallet.Bip;
using System;

namespace Atomex.EthereumTokens
{
    public class Erc20Config : EthereumConfig
    {
        public decimal TransferGasLimit { get; private set; }
        public decimal ApproveGasLimit { get; private set; }

        public decimal ApproveFeeAmount(decimal gasPrice) =>
            ApproveGasLimit * gasPrice / GweiInEth;

        public string ERC20ContractAddress { get; private set; }
        public ulong ERC20ContractBlockNumber { get; private set; }

        public Erc20Config()
        {
        }

        public Erc20Config(IConfiguration configuration)
        {
            Update(configuration);
        }

        public override void Update(IConfiguration configuration)
        {
            Name                       = configuration["Name"];
            Description                = configuration["Description"];
            DigitsMultiplier           = decimal.Parse(configuration["DigitsMultiplier"]);
            DustDigitsMultiplier       = long.Parse(configuration["DustDigitsMultiplier"]);
            Digits                     = (int)Math.Round(BigInteger.Log10(new BigInteger(DigitsMultiplier)));
            Format                     = $"F{(Digits < 9 ? Digits : 9)}";
            IsToken                    = bool.Parse(configuration["IsToken"]);

            FeeDigits                  = Digits;
            FeeCode                    = "ETH";
            FeeFormat                  = $"F{(FeeDigits < 9 ? FeeDigits : 9)}";
            FeeCurrencyName            = "ETH";

            HasFeePrice                = true;
            FeePriceCode               = DefaultGasPriceCode;
            FeePriceFormat             = DefaultGasPriceFormat;

            MaxRewardPercent           = configuration[nameof(MaxRewardPercent)] != null
                ? decimal.Parse(configuration[nameof(MaxRewardPercent)], CultureInfo.InvariantCulture)
                : 0m;
            MaxRewardPercentInBase     = configuration[nameof(MaxRewardPercentInBase)] != null
                ? decimal.Parse(configuration[nameof(MaxRewardPercentInBase)], CultureInfo.InvariantCulture)
                : 0m;
            FeeCurrencyToBaseSymbol    = configuration[nameof(FeeCurrencyToBaseSymbol)];
            FeeCurrencySymbol          = configuration[nameof(FeeCurrencySymbol)];

            TransferGasLimit           = decimal.Parse(configuration["TransferGasLimit"], CultureInfo.InvariantCulture);
            ApproveGasLimit            = decimal.Parse(configuration["ApproveGasLimit"], CultureInfo.InvariantCulture);
            InitiateGasLimit           = decimal.Parse(configuration["InitiateGasLimit"], CultureInfo.InvariantCulture);
            InitiateWithRewardGasLimit = decimal.Parse(configuration["InitiateWithRewardGasLimit"], CultureInfo.InvariantCulture);
            AddGasLimit                = decimal.Parse(configuration["AddGasLimit"], CultureInfo.InvariantCulture);
            RefundGasLimit             = decimal.Parse(configuration["RefundGasLimit"], CultureInfo.InvariantCulture);
            RedeemGasLimit             = decimal.Parse(configuration["RedeemGasLimit"], CultureInfo.InvariantCulture);
            EstimatedRedeemGasLimit    = decimal.Parse(configuration["EstimatedRedeemGasLimit"], CultureInfo.InvariantCulture);
            EstimatedRedeemWithRewardGasLimit = decimal.Parse(configuration["EstimatedRedeemWithRewardGasLimit"], CultureInfo.InvariantCulture);
            GasPriceInGwei             = decimal.Parse(configuration["GasPriceInGwei"], CultureInfo.InvariantCulture);

            MaxGasPriceInGwei = configuration[nameof(MaxGasPriceInGwei)] != null
                ? decimal.Parse(configuration[nameof(MaxGasPriceInGwei)], CultureInfo.InvariantCulture)
                : 650m;

            Chain                      = ResolveChain(configuration);

            ERC20ContractAddress       = configuration["ERC20Contract"];
            ERC20ContractBlockNumber   = ulong.Parse(configuration["ERC20ContractBlockNumber"], CultureInfo.InvariantCulture);

            SwapContractAddress        = configuration["SwapContract"];
            SwapContractBlockNumber    = ulong.Parse(configuration["SwapContractBlockNumber"], CultureInfo.InvariantCulture);

            BlockchainApiBaseUri       = configuration["BlockchainApiBaseUri"];
            BlockchainApi              = ResolveBlockchainApi(
                configuration: configuration,
                currency: this);

            TxExplorerUri              = configuration["TxExplorerUri"];
            AddressExplorerUri         = configuration["AddressExplorerUri"];
            TransactionType            = typeof(EthereumTransaction);

            IsSwapAvailable            = true;
            Bip44Code                  = Bip44.Ethereum;  //TODO ?
        }

        public BigInteger TokensToTokenDigits(decimal tokens) =>
            new BigInteger(tokens * DigitsMultiplier);

        public decimal TokenDigitsToTokens(BigInteger tokenDigits) =>
            (decimal)tokenDigits / DigitsMultiplier;

        public override async Task<decimal> GetRewardForRedeemAsync(
            decimal maxRewardPercent,
            decimal maxRewardPercentInBase,
            string feeCurrencyToBaseSymbol,
            decimal feeCurrencyToBasePrice,
            string feeCurrencySymbol = null,
            decimal feeCurrencyPrice = 0,
            CancellationToken cancellationToken = default)
        {
            var rewardForRedeemInEth = await base.GetRewardForRedeemAsync(
                maxRewardPercent: maxRewardPercent,
                maxRewardPercentInBase: maxRewardPercentInBase,
                feeCurrencyToBaseSymbol: feeCurrencyToBaseSymbol,
                feeCurrencyToBasePrice: feeCurrencyToBasePrice,
                feeCurrencySymbol: feeCurrencySymbol,
                feeCurrencyPrice: feeCurrencyPrice,
                cancellationToken: cancellationToken);

            if (feeCurrencySymbol == null || feeCurrencyPrice == 0)
                return 0m;

            return AmountHelper.RoundDown(feeCurrencySymbol.IsBaseCurrency(Name)
                ? rewardForRedeemInEth / feeCurrencyPrice
                : rewardForRedeemInEth * feeCurrencyPrice, DigitsMultiplier);
        }

        public override decimal GetDefaultFee() =>
            TransferGasLimit;
    }
}