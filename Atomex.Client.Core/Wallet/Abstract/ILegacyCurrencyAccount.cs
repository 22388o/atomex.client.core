﻿using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Atomex.Blockchain.Abstract;
using Atomex.Core;

namespace Atomex.Wallet.Abstract
{
    public interface ILegacyCurrencyAccount : ICurrencyAccount
    {
        #region Common

        Task<Error> SendAsync(
            string from,
            string to,
            decimal amount,
            decimal fee,
            decimal feePrice,
            bool useDefaultFee = false,
            CancellationToken cancellationToken = default);

        Task<decimal?> EstimateFeeAsync(
            string from,
            string to,
            decimal amount,
            BlockchainTransactionType type,
            CancellationToken cancellationToken = default);

        Task<(decimal, decimal, decimal)> EstimateMaxAmountToSendAsync(
            string from,
            string to,
            BlockchainTransactionType type,
            decimal fee = 0,
            decimal feePrice = 0,
            bool reserve = false,
            CancellationToken cancellationToken = default);

        #endregion Common

        #region Addresses

        Task<WalletAddress> GetRedeemAddressAsync(
            CancellationToken cancellationToken = default);

        Task<IEnumerable<WalletAddress>> GetUnspentTokenAddressesAsync(
            CancellationToken cancellationToken = default);

        #endregion Addresses
    }
}