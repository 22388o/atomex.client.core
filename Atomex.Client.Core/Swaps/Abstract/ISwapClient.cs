﻿using Atomex.Core;

namespace Atomex.Swaps.Abstract
{
    public interface ISwapClient
    {
        void SwapInitiateAsync(Swap swap);
        void SwapAcceptAsync(Swap swap);
        void SwapPaymentAsync(Swap swap);
    }
}