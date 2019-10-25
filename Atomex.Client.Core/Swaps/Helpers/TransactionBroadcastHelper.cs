﻿using Serilog;
using System;
using System.Threading;
using System.Threading.Tasks;
using Atomex.Blockchain.Abstract;
using Atomex.Core.Entities;

namespace Atomex.Swaps.Helpers
{
    public static class TransactionBroadcastHelper
    {
        public static Task<string> ForceBroadcast(
            this IBlockchainTransaction tx,
            ClientSwap swap,
            TimeSpan interval,
            Action<ClientSwap, string, CancellationToken> completionHandler = null,
            CancellationToken cancellationToken = default)
        {
            return Task.Run(async () =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var broadcastResult = await tx.Currency.BlockchainApi
                            .BroadcastAsync(tx)
                            .ConfigureAwait(false);

                        if (!broadcastResult.HasError)
                        {
                            if (broadcastResult.Value != null)
                            {
                                completionHandler?.Invoke(swap, broadcastResult.Value, cancellationToken);
                                return broadcastResult.Value;
                            }
                        }
                        else
                        {
                            Log.Error("Error while broadcast {@currency} tx with. Code: {@code}. Description: {@desc}",
                                tx.Currency.Name,
                                broadcastResult.Error.Code,
                                broadcastResult.Error.Description);
                        }

                        await Task.Delay(interval, cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, "Error while broadcast {@currency} tx.", tx.Currency.Name);
                }

                return null;

            }, cancellationToken);
        }
    }
}