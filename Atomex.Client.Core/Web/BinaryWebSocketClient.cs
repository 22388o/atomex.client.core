﻿using System;
using System.IO;
using Serilog;

using Atomex.Api;
using Atomex.Api.Proto;

namespace Atomex.Web
{
    public class BinaryWebSocketClient : WebSocketClient
    {
        private const int MaxHandlersCount = 32;
        private Action<MemoryStream>[] Handlers { get; } = new Action<MemoryStream>[MaxHandlersCount];
        protected ProtoSchemes Schemes { get; }

        public virtual string Name { get; }
        public AuthNonce Nonce { get; private set; }
        public event EventHandler AuthOk;
        public event EventHandler AuthNonce;
        public event EventHandler<Core.ErrorEventArgs> Error;

        public void SendHeartBeatAsync() =>
            SendAsync(Schemes.HeartBeat.SerializeWithMessageId("ping"));

        protected void AddHandler(byte messageId, Action<MemoryStream> handler)
        {
            Handlers[messageId] = handler;
        }

        protected BinaryWebSocketClient(string url, ProtoSchemes schemes)
            : base(url)
        {
            Schemes = schemes;

            AddHandler(Schemes.AuthNonce.MessageId, AuthNonceHandler);
            AddHandler(Schemes.AuthOk.MessageId, AuthOkHandler);
            AddHandler(Schemes.Error.MessageId, ErrorHandler);
            AddHandler(Schemes.HeartBeat.MessageId, HeartBeatHandler);
        }

        protected override void OnBinaryMessage(byte[] data)
        {
            using var stream = new MemoryStream(data);

            while (stream.Position < stream.Length)
            {
                var messageId = (byte)stream.ReadByte();

                if (messageId < Handlers.Length && Handlers[messageId] != null)
                    Handlers[messageId]?.Invoke(stream);
            }
        }

        private void AuthNonceHandler(MemoryStream stream)
        {
            Nonce = Schemes.AuthNonce.DeserializeWithLengthPrefix(stream);

            AuthNonce?.Invoke(this, EventArgs.Empty);
        }

        private void AuthOkHandler(MemoryStream stream)
        {
            var authOk = Schemes.AuthOk.DeserializeWithLengthPrefix(stream);

            AuthOk?.Invoke(this, EventArgs.Empty);
        }

        private void ErrorHandler(MemoryStream stream)
        {
            var error = Schemes.Error.DeserializeWithLengthPrefix(stream);

            Error?.Invoke(this, new Core.ErrorEventArgs(error));
        }

        private void HeartBeatHandler(MemoryStream stream)
        {
            var pong = Schemes.HeartBeat.DeserializeWithLengthPrefix(stream);

            if (pong.ToLowerInvariant() == "pong")
                Log.Debug($"Pong received from {Name}");
            else
                Log.Error("Invalid heart beat response");    
        }
    }
}