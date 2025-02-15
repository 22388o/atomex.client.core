﻿using Atomex.Common;
using Atomex.Core;

namespace Atomex.Wallet.Abstract
{
    public interface IKeyStorage
    {
        SecureBytes GetPrivateKey(
            CurrencyConfig currency,
            KeyIndex keyIndex,
            int keyType);

        SecureBytes GetPublicKey(
            CurrencyConfig currency,
            KeyIndex keyIndex,
            int keyType);
    }
}