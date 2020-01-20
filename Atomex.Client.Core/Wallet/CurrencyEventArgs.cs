﻿using System;
using Atomex.Core;

namespace Atomex.Wallet
{
    public class CurrencyEventArgs : EventArgs
    {
        public Currency Currency { get; set; }

        public CurrencyEventArgs(Currency currency)
        {
            Currency = currency;
        }
    }
}