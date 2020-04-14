using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using dotnet_etcd.helper;
using Etcdserverpb;

using Google.Protobuf;

using Grpc.Core;
using Mvccpb;
using static Mvccpb.Event.Types;
using Metadata = Grpc.Core.Metadata;

namespace dotnet_etcd
{
    /// <summary>
    /// WatchEvent class is used for retrieval of minimal
    /// data from watch events on etcd.
    /// </summary>
    public class WatchEvent
    {
        /// <summary>
        /// etcd Key
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// etcd value
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// etcd watch event type (PUT,DELETE etc.)
        /// </summary>
        public EventType Type { get; set; }

    }

    // TODO: Add range methods for methods having  key as input instead of a whole watch request
    // TODO: Update documentation on how to call range requests for watch using watch request as input param
    public partial class EtcdClient : IDisposable
    {
        #region Watch Key

        /// <summary>
        /// Watches a key according to the specified watch request and
        /// passes the watch response to the method provided.
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest request, Action<WatchResponse> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update);
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key according to the specified watch request and
        /// passes the watch response to the methods provided. 
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest request, Action<WatchResponse>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchResponse> method in methods)
                            {
                                method(update);
                            }
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key according to the specified watch request and
        /// passes the minimal watch event data to the method provided. 
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest request, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update.Events.Select(i =>
                                {
                                    return new WatchEvent
                                    {
                                        Key = i.Kv.Key.ToStringUtf8(),
                                        Value = i.Kv.Value.ToStringUtf8(),
                                        Type = i.Type
                                    };
                                }).ToArray()
                            );
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key according to the specified watch request and
        /// passes the minimal watch event data to the methods provided. 
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest request, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchEvent[]> method in methods)
                            {
                                method(update.Events.Select(i =>
                                    {
                                        return new WatchEvent
                                        {
                                            Key = i.Kv.Key.ToStringUtf8(),
                                            Value = i.Kv.Value.ToStringUtf8(),
                                            Type = i.Type
                                        };
                                    }).ToArray()
                                );
                            }
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches the keys according to the specified watch requests and
        /// passes the watch response to the method provided.
        /// </summary>
        /// <param name="requests">Watch Requests containing keys to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest[] requests, Action<WatchResponse> method,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update);
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key according to the specified watch requests and
        /// passes the watch response to the methods provided. 
        /// </summary>
        /// <param name="requests">Watch Requests containing keys to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest[] requests, Action<WatchResponse>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchResponse> method in methods)
                            {
                                method(update);
                            }
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key according to the specified watch request and
        /// passes the minimal watch event data to the method provided. 
        /// </summary>
        /// <param name="requests">Watch Requests containing keys to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest[] requests, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update.Events.Select(i =>
                                {
                                    return new WatchEvent
                                    {
                                        Key = i.Kv.Key.ToStringUtf8(),
                                        Value = i.Kv.Value.ToStringUtf8(),
                                        Type = i.Type
                                    };
                                }).ToArray()
                            );
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key according to the specified watch requests and
        /// passes the minimal watch event data to the methods provided. 
        /// </summary>
        /// <param name="requests">Watch Request containing keys to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task Watch(WatchRequest[] requests, Action<WatchEvent[]>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchEvent[]> method in methods)
                            {
                                method(update.Events.Select(i =>
                                    {
                                        return new WatchEvent
                                        {
                                            Key = i.Kv.Key.ToStringUtf8(),
                                            Value = i.Kv.Value.ToStringUtf8(),
                                            Type = i.Type
                                        };
                                    }).ToArray()
                                );
                            }
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches the specified key and passes the watch response to the method provided.
        /// </summary>
        /// <param name="key">Key to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string key, Action<WatchResponse> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(key)
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key and passes the watch response to the methods provided.
        /// </summary>
        /// <param name="key">Key to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string key, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(key)
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, methods, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key and passes the minimal watch events data to the method provided.
        /// </summary>
        /// <param name="key">Key to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string key, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(key)
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key and passes the minimal watch events data to the methods provided.
        /// </summary>
        /// <param name="key">Key to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string key, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(key)
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, methods, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified keys and passes the watch response to the method provided.
        /// </summary>
        /// <param name="keys">Keys to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string[] keys, Action<WatchResponse> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string key in keys)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(key)
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified keys and passes the watch response to the method provided.
        /// </summary>
        /// <param name="keys">Keys to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string[] keys, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string key in keys)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(key)
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), methods, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified keys and passes the minimal watch events data to the method provided.
        /// </summary>
        /// <param name="keys">Keys to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string[] keys, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string key in keys)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(key)
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified keys and passes the minimal watch events data to the method provided.
        /// </summary>
        /// <param name="keys">Keys to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void Watch(string[] keys, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string key in keys)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(key)
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), methods, headers, deadline, cancellationToken));
        }

        #endregion

        #region Watch Range of keys

        /// <summary>
        /// Watches a key range according to the specified watch request and
        /// passes the watch response to the method provided.
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest request, Action<WatchResponse> method,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update);
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key range according to the specified watch request and
        /// passes the watch response to the methods provided. 
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest request, Action<WatchResponse>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchResponse> method in methods)
                            {
                                method(update);
                            }
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key range according to the specified watch request and
        /// passes the minimal watch event data to the method provided. 
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest request, Action<WatchEvent[]> method,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update.Events.Select(i =>
                                {
                                    return new WatchEvent
                                    {
                                        Key = i.Kv.Key.ToStringUtf8(),
                                        Value = i.Kv.Value.ToStringUtf8(),
                                        Type = i.Type
                                    };
                                }).ToArray()
                            );
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key range according to the specified watch request and
        /// passes the minimal watch event data to the methods provided. 
        /// </summary>
        /// <param name="request">Watch Request containing key to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest request, Action<WatchEvent[]>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchEvent[]> method in methods)
                            {
                                method(update.Events.Select(i =>
                                    {
                                        return new WatchEvent
                                        {
                                            Key = i.Kv.Key.ToStringUtf8(),
                                            Value = i.Kv.Value.ToStringUtf8(),
                                            Type = i.Type
                                        };
                                    }).ToArray()
                                );
                            }
                        }
                    }, cancellationToken);

                    await watcher.RequestStream.WriteAsync(request);
                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches the key range according to the specified watch requests and
        /// passes the watch response to the method provided.
        /// </summary>
        /// <param name="requests">Watch Requests containing keys to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest[] requests, Action<WatchResponse> method,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update);
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key range according to the specified watch requests and
        /// passes the watch response to the methods provided. 
        /// </summary>
        /// <param name="requests">Watch Requests containing keys to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest[] requests, Action<WatchResponse>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchResponse> method in methods)
                            {
                                method(update);
                            }
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key range according to the specified watch request and
        /// passes the minimal watch event data to the method provided. 
        /// </summary>
        /// <param name="requests">Watch Requests containing keys to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest[] requests, Action<WatchEvent[]> method,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            method(update.Events.Select(i =>
                                {
                                    return new WatchEvent
                                    {
                                        Key = i.Kv.Key.ToStringUtf8(),
                                        Value = i.Kv.Value.ToStringUtf8(),
                                        Type = i.Type
                                    };
                                }).ToArray()
                            );
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches a key range according to the specified watch requests and
        /// passes the minimal watch event data to the methods provided. 
        /// </summary>
        /// <param name="requests">Watch Request containing keys to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public async Task WatchRange(WatchRequest[] requests, Action<WatchEvent[]>[] methods,
            Grpc.Core.Metadata headers = null, DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            await CallEtcdAsync(async (connection) =>
            {
                using (AsyncDuplexStreamingCall<WatchRequest, WatchResponse> watcher =
                    connection.watchClient.Watch(headers, deadline, cancellationToken))
                {
                    Task watcherTask = Task.Run(async () =>
                    {
                        while (await watcher.ResponseStream.MoveNext(cancellationToken))
                        {
                            WatchResponse update = watcher.ResponseStream.Current;
                            foreach (Action<WatchEvent[]> method in methods)
                            {
                                method(update.Events.Select(i =>
                                    {
                                        return new WatchEvent
                                        {
                                            Key = i.Kv.Key.ToStringUtf8(),
                                            Value = i.Kv.Value.ToStringUtf8(),
                                            Type = i.Type
                                        };
                                    }).ToArray()
                                );
                            }
                        }
                    }, cancellationToken);

                    foreach (WatchRequest request in requests)
                    {
                        await watcher.RequestStream.WriteAsync(request);
                    }

                    await watcher.RequestStream.CompleteAsync();
                    await watcherTask;
                }
            });
        }

        /// <summary>
        /// Watches the specified key range and passes the watch response to the method provided.
        /// </summary>
        /// <param name="path">Path to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string path, Action<WatchResponse> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(path),
                    RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the watch response to the methods provided.
        /// </summary>
        /// <param name="path">Path to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string path, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(path),
                    RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, methods, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the minimal watch events data to the method provided.
        /// </summary>
        /// <param name="path">Path to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string path, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(path),
                    RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the minimal watch events data to the methods provided.
        /// </summary>
        /// <param name="path">Path to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string path, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            WatchRequest request = new WatchRequest()
            {
                CreateRequest = new WatchCreateRequest()
                {
                    Key = ByteString.CopyFromUtf8(path),
                    RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                }
            };

            AsyncHelper.RunSync(async () => await Watch(request, methods, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the watch response to the method provided.
        /// </summary>
        /// <param name="paths">Paths to be watched</param>
        /// <param name="method">Method to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string[] paths, Action<WatchResponse> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string path in paths)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(path),
                        RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the watch response to the method provided.
        /// </summary>
        /// <param name="paths">Paths to be watched</param>
        /// <param name="methods">Methods to which watch response should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string[] paths, Action<WatchResponse>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string path in paths)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(path),
                        RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), methods, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the minimal watch events data to the method provided.
        /// </summary>
        /// <param name="paths">Paths to be watched</param>
        /// <param name="method">Method to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string[] paths, Action<WatchEvent[]> method, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string path in paths)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(path),
                        RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), method, headers, deadline, cancellationToken));
        }

        /// <summary>
        /// Watches the specified key range and passes the minimal watch events data to the method provided.
        /// </summary>
        /// <param name="paths">Paths to be watched</param>
        /// <param name="methods">Methods to which minimal watch events data should be passed on</param>
        /// <param name="headers">The initial metadata to send with the call. This parameter is optional.</param>
        /// <param name="deadline">An optional deadline for the call. The call will be cancelled if deadline is hit.</param>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        public void WatchRange(string[] paths, Action<WatchEvent[]>[] methods, Grpc.Core.Metadata headers = null,
            DateTime? deadline = null,
            CancellationToken cancellationToken = default)
        {
            List<WatchRequest> requests = new List<WatchRequest>();

            foreach (string path in paths)
            {
                WatchRequest request = new WatchRequest()
                {
                    CreateRequest = new WatchCreateRequest()
                    {
                        Key = ByteString.CopyFromUtf8(path),
                        RangeEnd = ByteString.CopyFromUtf8(GetRangeEnd(path))
                    }
                };
                requests.Add(request);
            }

            AsyncHelper.RunSync(async () =>
                await Watch(requests.ToArray(), methods, headers, deadline, cancellationToken));
        }

        #endregion

        #region Watch Manager

        private readonly List<WatchManager> watchManagers = new List<WatchManager>();

        /// <summary>
        /// Create a new <seealso cref="WatchManager"/> that uses this <seealso cref="EtcdClient"/> to manage multiple <seealso cref="WatchManager.Subscription"/>
        /// </summary>
        public WatchManager CreateWatchManager()
        {
            var wm = new WatchManager(this);
            watchManagers.Add(wm);
            return wm;
        }

        private void RemoveWatchManager(WatchManager wm)
        {
            if (!wm.Disposing)
            {
                wm.Dispose();
            }

            watchManagers.Remove(wm);
        }

        /// <summary>
        /// The watch manager manages multiple watches through a <seealso cref="Subscription"/> object
        /// </summary>
        public class WatchManager : IDisposable
        {
            // TODO: check behaviour if etcd does a compact and keys that should be sent are impacted, channel is clusted with compaction error: do we create a continuation modus for the manager: 1. clear manager on exception 2. resub and skip the missing events

            internal WatchManager(EtcdClient etcdClient)
            {
                etcdClientInstance = etcdClient;
                channelRecreationTask = Task.Run(ChannelRecreation, cancellationTokenSource.Token);
            }

            /// <summary>
            /// This event will be raised if an watch response is received by the <seealso cref="WatchManager"/> but there is an error processing the response
            /// </summary>
            public event EventHandler<WatchResponseProcessingException> WatchResponseProcessingFailed;

            public Metadata ChannelMetadata { get; private set; }

            private readonly Dictionary<long, Subscription> activeSubscriptions = new Dictionary<long, Subscription>();
            private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            private readonly ManualResetEvent newSubscriptionsAccepted = new ManualResetEvent(false);
            private readonly ManualResetEvent reinitializeChannel = new ManualResetEvent(true);
            private readonly AutoResetEvent synchronousWriteToRequestStream = new AutoResetEvent(true);
            private readonly EtcdClient etcdClientInstance;
            private readonly Task channelRecreationTask;
            
            private long watchIdCounter = 1;
            private AsyncDuplexStreamingCall<WatchRequest, WatchResponse> activeChannel;
            private Task responseReaderTask;
            private CancellationTokenSource responseReaderTaskCancellationTokenSource;

            /// <summary>
            /// Create a new watch subscription
            /// </summary>
            /// <param name="watchCreationRequest">The watch creation request to send to the server</param>
            /// <param name="watchResponseFunc">The function that is called when a update is received from the etcd server</param>
            /// <param name="watchResponseFuncExceptionHandlerFunc">Optional exception handler that is called when an error is encountered in the execution of the <paramref name="watchResponseFunc"/>. This this action throws an error it's silently discarded.</param>
            /// <returns>A <seealso cref="Subscription"/> that manages the created watch</returns>
            public Subscription CreateSubscription(WatchCreateRequest watchCreationRequest, Action<WatchManager.SubscriptionUpdate> watchResponseFunc, Action<Exception> watchResponseFuncExceptionHandlerFunc = null)
            {
                return AsyncHelper.RunSync(async () => await CreateSubscriptionAsync(watchCreationRequest, watchResponseFunc, watchResponseFuncExceptionHandlerFunc));
            }

            /// <summary>
            /// Create a new watch subscription
            /// </summary>
            /// <param name="watchCreationRequest">The watch creation request to send to the server</param>
            /// <param name="watchResponseFunc">The function that is called when a update is received from the etcd server</param>
            /// <param name="watchResponseFuncExceptionHandlerFunc">Optional exception handler that is called when an error is encountered in the execution of the <paramref name="watchResponseFunc"/>. This this action throws an error it's silently discarded.</param>
            /// <returns>A <seealso cref="Subscription"/> that manages the created watch</returns>
            public async Task<Subscription> CreateSubscriptionAsync(WatchCreateRequest watchCreationRequest, Action<WatchManager.SubscriptionUpdate> watchResponseFunc, Action<Exception> watchResponseFuncExceptionHandlerFunc = null)
            {
                // Wait for the new subscriptions block to be removed
                newSubscriptionsAccepted.WaitOne();

                var watchId = GetWatchId();
                var subscription = new Subscription(this, watchId, watchCreationRequest, watchResponseFunc, watchResponseFuncExceptionHandlerFunc, cancellationTokenSource.Token);
                activeSubscriptions.Add(watchId, subscription);

                // Only await the send of the request, the creation confirmation is received async
                await SendSubscriptionWatchRequest(subscription);

                return subscription;
            }

            internal bool Disposing { get; private set; }

            public void Dispose()
            {
                if (!Disposing)
                {
                    Disposing = true;
                    cancellationTokenSource.Cancel();
                    etcdClientInstance.RemoveWatchManager(this);

                    // Stop all active subscriptions for this manager
                    foreach (var activeSubscription in activeSubscriptions)
                    {
                        activeSubscription.Value.Dispose();
                    }

                    activeChannel?.Dispose();
                    cancellationTokenSource.Dispose();
                }
            }

            private async Task SendSubscriptionWatchRequest(Subscription subscription)
            {
                var watchRequest = subscription.CreateWatchCreateRequest();
                synchronousWriteToRequestStream.WaitOne();
                try
                {
                    await activeChannel.RequestStream.WriteAsync(watchRequest);
                }
                finally
                {
                    synchronousWriteToRequestStream.Set(); 
                }
            }

            private async void RemoveSubscription(Subscription subscription)
            {
                var watchRequest = subscription.CreateWatchCancelRequest();
                synchronousWriteToRequestStream.WaitOne();
                try
                {
                    await activeChannel.RequestStream.WriteAsync(watchRequest);
                }
                finally
                {
                    synchronousWriteToRequestStream.Set();
                }
            }

            private void HandleUpdateFromServer(WatchResponse response)
            {
                try
                {
                    if (activeSubscriptions.TryGetValue(response.WatchId, out var subscription))
                    {
                        subscription.HandleUpdate(response);
                    }
                    else
                    {
                        throw new WatchResponseProcessingException(response);
                    }
                }
                catch (Exception ex)
                {
                    if (!(ex is WatchResponseProcessingException wex))
                    {
                        wex = new WatchResponseProcessingException(response, $"An error occurred while processing {nameof(WatchResponse)}.", ex);
                    }

                    WatchResponseProcessingFailed?.Invoke(this, wex);
                }
            }

            private async void ChannelRecreation()
            {
                while (!cancellationTokenSource.Token.IsCancellationRequested)
                {
                    reinitializeChannel.WaitOne();
                    try
                    {
                        newSubscriptionsAccepted.Reset();

                        // Close and cleanup old channel
                        ChannelMetadata = null;

                        if (activeChannel != null)
                        {
                            activeChannel.Dispose();
                            activeChannel = null;
                        }

                        if (responseReaderTaskCancellationTokenSource != null)
                        {
                            responseReaderTaskCancellationTokenSource.Cancel();
                            responseReaderTaskCancellationTokenSource.Dispose();
                            responseReaderTaskCancellationTokenSource = null;
                        }

                        responseReaderTask?.Dispose();

                        // Create and initialize a new channel
                        var connection = etcdClientInstance._balancer.GetConnection();
                        activeChannel = connection.watchClient.Watch(cancellationToken: cancellationTokenSource.Token);
                        ChannelMetadata = await activeChannel.ResponseHeadersAsync;
                        responseReaderTaskCancellationTokenSource = new CancellationTokenSource();
                        responseReaderTask = Task.Run(async () =>
                        {
                            try
                            {
                                while (await activeChannel.ResponseStream.MoveNext(responseReaderTaskCancellationTokenSource.Token))
                                {
                                    var update = activeChannel.ResponseStream.Current;
                                    HandleUpdateFromServer(update);
                                }
                            }
                            catch (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable)
                            {
                                // trigger a channel recreation
                                reinitializeChannel.Set();
                            }
                        }, responseReaderTaskCancellationTokenSource.Token);

                        newSubscriptionsAccepted.Set();

                        foreach (var activeSubscription in activeSubscriptions.Values)
                        {
                            await SendSubscriptionWatchRequest(activeSubscription);
                        }
                    }
                    catch
                    {
                        // reinitialization of channel failed, will try again
                        continue;
                    }

                    reinitializeChannel.Reset();
                }
            }

            private long GetWatchId()
            {
                return Interlocked.Increment(ref watchIdCounter);
            }

            /// <summary>
            /// Represents a single watch subscription and is managed by <seealso cref="WatchManager"/>
            /// </summary>
            public class Subscription : IDisposable
            {
                internal Subscription(WatchManager watchManager, long watchId, WatchCreateRequest watchCreationRequest, Action<SubscriptionUpdate> watchResponseFunc,
                    Action<Exception> watchResponseFuncExceptionHandlerFunc, CancellationToken cancellationToken)
                {
                    this.watchManager = watchManager;
                    this.watchId = watchId;
                    this.initialWatchCreationRequest = watchCreationRequest;
                    this.watchResponseFunc = watchResponseFunc;
                    this.watchResponseFuncExceptionHandlerFunc = watchResponseFuncExceptionHandlerFunc;
                    this.cancellationToken = cancellationToken;

                    State = SubscriptionState.Creating;
                    watchResponseQueue = new BlockingCollection<SubscriptionUpdate>(new ConcurrentQueue<SubscriptionUpdate>());
                    watchResponseQueueHandleTask = Task.Run(HandleResponseQueue, cancellationToken);
                }

                private readonly WatchManager watchManager;
                private readonly long watchId;
                private readonly WatchCreateRequest initialWatchCreationRequest;
                private readonly Action<SubscriptionUpdate> watchResponseFunc;
                private readonly Action<Exception> watchResponseFuncExceptionHandlerFunc;
                private readonly CancellationToken cancellationToken;
                private readonly BlockingCollection<SubscriptionUpdate> watchResponseQueue;
                private readonly Task watchResponseQueueHandleTask;


                /// <summary>
                /// nextRev is the minimum expected next revision
                /// </summary>
                private long? nextRevision = null;

                private bool disposing = false;

                /// <summary>
                /// The current state of the subscription
                /// </summary>
                public SubscriptionState State { get; private set; }

                /// <summary>
                /// Handles updates from the etcd server for this watch subscription
                /// </summary>
                /// <param name="response">The <seealso cref="WatchResponse"/> from the etcd server</param>
                internal void HandleUpdate(WatchResponse response)
                {
                    if (response.Created)
                    {
                        State = SubscriptionState.Created;
                        if (nextRevision == 0)
                        {
                            nextRevision = response.Header.Revision;
                        }
                    }
                    else
                    {
                        nextRevision = response.Header.Revision;
                    }

                    if (response.Canceled)
                    {
                        State = SubscriptionState.Canceled;
                    }

                    var newEvents = response.Events
                        .Where(ev => !nextRevision.HasValue || ev.Kv.ModRevision > nextRevision) // filter events that are duplicates based on expected next revision
                        .ToList();

                    if (newEvents.Count > 0)
                    {
                        watchResponseQueue.Add(new SubscriptionUpdate(response.Header, response.CompactRevision, newEvents), cancellationToken);
                        nextRevision = response.Events.Last().Kv.ModRevision + 1;
                    }
                }

                internal WatchRequest CreateWatchCreateRequest()
                {
                    var watchCreationRequest = new WatchCreateRequest(initialWatchCreationRequest);

                    if (nextRevision.HasValue)
                    {
                        watchCreationRequest.StartRevision = nextRevision.Value;
                    }

                    return new WatchRequest
                    {
                        CreateRequest = watchCreationRequest
                    };
                }

                internal WatchRequest CreateWatchCancelRequest()
                {
                    return new WatchRequest
                    {
                        CancelRequest = new WatchCancelRequest
                        {
                            WatchId = watchId
                        }
                    };
                }

                /// <inheritdoc />
                public void Dispose()
                {
                    if (!disposing)
                    {
                        watchManager.RemoveSubscription(this);
                    }
                }

                private void HandleResponseQueue()
                {
                    foreach (var watchResponse in watchResponseQueue.GetConsumingEnumerable(cancellationToken))
                    {
                        try
                        {
                            watchResponseFunc?.Invoke(watchResponse);
                        }
                        catch (Exception ex)
                        {
                            try
                            {
                                watchResponseFuncExceptionHandlerFunc?.Invoke(ex);
                            }
                            catch
                            {
                                // exception handlers should not throw exceptions
                            }
                        }
                    }
                }
            }

            public enum SubscriptionState
            {
                Creating = 1,
                Created = 2,
                Canceled = 3
            }

            [Serializable]
            public class WatchResponseProcessingException : Exception
            {
                public WatchResponseProcessingException(WatchResponse response)
                {
                    WatchResponse = response;
                }

                public WatchResponseProcessingException(WatchResponse response, string message)
                    : base(message)
                {
                    WatchResponse = response;
                }

                public WatchResponseProcessingException(WatchResponse response, string message,
                    Exception innerException)
                    : base(message, innerException)
                {
                    WatchResponse = response;
                }

                protected WatchResponseProcessingException(SerializationInfo info, StreamingContext context)
                    : base(info, context) { }

                public WatchResponse WatchResponse { get; private set; }
            }

            /// <summary>
            /// Contains update from an subscription update received from the etcd server
            /// </summary>
            public class SubscriptionUpdate
            {
                internal SubscriptionUpdate(ResponseHeader responseHeader, long compactRevision, List<Event> events)
                {
                    ResponseHeader = responseHeader;
                    CompactRevision = compactRevision;
                    Events = events;
                }

                /// <summary>
                /// Cluster metadata
                /// </summary>
                public ResponseHeader ResponseHeader { get; }

                /// <summary>
                /// CompactRevision is the minimum revision the watcher may receive.
                /// </summary>
                public long CompactRevision { get; }

                /// <summary>
                /// Events received from the etcd server
                /// </summary>
                public List<Event> Events { get; }
            }
        }

        #endregion
    }
}