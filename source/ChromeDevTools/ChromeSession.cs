#if !NETSTANDARD1_5
using MasterDevs.ChromeDevTools.Serialization;
using Newtonsoft.Json;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MasterDevs.ChromeDevTools
{
    public class ChromeSession : IChromeSession
    {
        private readonly string _endpoint;
        private readonly ConcurrentDictionary<string, ConcurrentBag<Action<object>>> _handlers = new ConcurrentDictionary<string, ConcurrentBag<Action<object>>>();
        private ICommandFactory _commandFactory;
        private IEventFactory _eventFactory;
        private ManualResetEvent _publishEvent = new ManualResetEvent(false);
        private ConcurrentDictionary<long, ManualResetEventSlim> _requestWaitHandles = new ConcurrentDictionary<long, ManualResetEventSlim>();
        private ICommandResponseFactory _responseFactory;
        private ConcurrentDictionary<long, ICommandResponse> _responses = new ConcurrentDictionary<long, ICommandResponse>();
        private ClientWebSocket _webSocket;
        private static object _Lock = new object();

        public ChromeSession(string endpoint, ICommandFactory commandFactory, ICommandResponseFactory responseFactory, IEventFactory eventFactory)
        {
            _endpoint = endpoint;
            _commandFactory = commandFactory;
            _responseFactory = responseFactory;
            _eventFactory = eventFactory;
        }

        public void Dispose()
        {
            if (null == _webSocket) return;
            if (_webSocket.State == WebSocketState.Open)
            {
                _webSocket.CloseAsync(WebSocketCloseStatus.Empty, "Program Dispose", CancellationToken.None).Wait();
            }
            _webSocket.Dispose();
        }

        private void EnsureInit()
        {
            if (null == _webSocket)
            {
                lock (_Lock)
                {
                    if (null == _webSocket)
                    {
                        Init().Wait();
                    }
                }
            }
        }

        private async Task Init()
        {

            _webSocket = new ClientWebSocket();
            await _webSocket.ConnectAsync(new Uri(_endpoint), CancellationToken.None);

            //_webSocket.EnableAutoSendPing = false;
            //_webSocket.MessageReceived += WebSocket_MessageReceived;
            //_webSocket.Error += WebSocket_Error;
            //_webSocket.Closed += WebSocket_Closed;
            //_webSocket.DataReceived += WebSocket_DataReceived;

            //_webSocket.Open();

        }

        public Task<ICommandResponse> SendAsync<T>(CancellationToken cancellationToken)
        {
            var command = _commandFactory.Create<T>();
            return SendCommand(command, cancellationToken);
        }

        public Task<CommandResponse<T>> SendAsync<T>(ICommand<T> parameter, CancellationToken cancellationToken)
        {
            var command = _commandFactory.Create(parameter);
            var task = SendCommand(command, cancellationToken);
            return CastTaskResult<ICommandResponse, CommandResponse<T>>(task);
        }

        private Task<TDerived> CastTaskResult<TBase, TDerived>(Task<TBase> task) where TDerived : TBase
        {
            var tcs = new TaskCompletionSource<TDerived>();
            task.ContinueWith(t => tcs.SetResult((TDerived)t.Result),
                TaskContinuationOptions.OnlyOnRanToCompletion);
            task.ContinueWith(t => tcs.SetException(t.Exception.InnerExceptions),
                TaskContinuationOptions.OnlyOnFaulted);
            task.ContinueWith(t => tcs.SetCanceled(),
                TaskContinuationOptions.OnlyOnCanceled);
            return tcs.Task;
        }

        public void Subscribe<T>(Action<T> handler) where T : class
        {
            var handlerType = typeof(T);
            var handlerForBag = new Action<object>(obj => handler((T)obj));
            _handlers.AddOrUpdate(handlerType.FullName,
                (m) => new ConcurrentBag<Action<object>>(new[] { handlerForBag }),
                (m, currentBag) =>
                {
                    currentBag.Add(handlerForBag);
                    return currentBag;
                });
        }

        private void HandleEvent(IEvent evnt)
        {
            if (null == evnt
                || null == evnt)
            {
                return;
            }
            var type = evnt.GetType().GetGenericArguments().FirstOrDefault();
            if (null == type)
            {
                return;
            }
            var handlerKey = type.FullName;
            ConcurrentBag<Action<object>> handlers = null;
            if (_handlers.TryGetValue(handlerKey, out handlers))
            {
                var localHandlers = handlers.ToArray();
                foreach (var handler in localHandlers)
                {
                    ExecuteHandler(handler, evnt);
                }
            }
        }

        private void ExecuteHandler(Action<object> handler, dynamic evnt)
        {
            if (evnt.GetType().GetGenericTypeDefinition() == typeof(Event<>))
            {
                handler(evnt.Params);
            }
            else
            {
                handler(evnt);
            }
        }

        private void HandleResponse(ICommandResponse response)
        {
            if (null == response) return;
            ManualResetEventSlim requestMre;
            if (_requestWaitHandles.TryGetValue(response.Id, out requestMre))
            {
                _responses.AddOrUpdate(response.Id, id => response, (key, value) => response);
                requestMre.Set();
            }
            else
            {
                // in the case of an error, we don't always get the request Id back :(
                // if there is only one pending requests, we know what to do ... otherwise
                if (1 == _requestWaitHandles.Count)
                {
                    var requestId = _requestWaitHandles.Keys.First();
                    _requestWaitHandles.TryGetValue(requestId, out requestMre);
                    _responses.AddOrUpdate(requestId, id => response, (key, value) => response);
                    requestMre.Set();
                }
            }
        }

        private async Task<ICommandResponse> SendCommand(Command command, CancellationToken cancellationToken)
        {

            var settings = new JsonSerializerSettings
            {
                ContractResolver = new MessageContractResolver(),
                NullValueHandling = NullValueHandling.Ignore,
            };
            var requestString = JsonConvert.SerializeObject(command, settings);
            Console.WriteLine(requestString);

            var reqContent = Encoding.UTF8.GetBytes(requestString);
            EnsureInit();
            await _webSocket.SendAsync(new ArraySegment<byte>(reqContent), WebSocketMessageType.Text, true, cancellationToken);
            var data = new List<byte>();
            bool end = false;
            using var buffer2 = MemoryPool<byte>.Shared.Rent(1024);
            while (!end)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(1024);
                var a = await _webSocket.ReceiveAsync(buffer2.Memory, cancellationToken);
                for (int i = 0; i < a.Count; i++)
                {
                    data.Add(buffer2.Memory.Span[i]);
                }
                end = a.EndOfMessage;
            }

            var response = _responseFactory.Create(Encoding.UTF8.GetString(data.ToArray()));
            Console.WriteLine(response.ToString());
            //_responses.TryRemove(command.Id, out response);
            //_requestWaitHandles.TryRemove(command.Id, out _);
            return response;

        }

        private bool TryGetCommandResponse(byte[] data, out ICommandResponse response)
        {
            response = _responseFactory.Create(data);
            return null != response;
        }

        private bool TryGetCommandResponse(string message, out ICommandResponse response)
        {
            response = _responseFactory.Create(message);
            return null != response;
        }

        private bool TryGetEvent(byte[] data, out IEvent evnt)
        {
            evnt = _eventFactory.Create(data);
            return null != evnt;
        }

        private bool TryGetEvent(string message, out IEvent evnt)
        {
            evnt = _eventFactory.Create(message);
            return null != evnt;
        }

    }
}
#endif
