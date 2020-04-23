using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
                WatchManagerInstanceName = $"wm-{Guid.NewGuid():n}";
                _etcdClientInstance = etcdClient;

                _requestSenderThread = new Thread(async () => await SenderThreadWork())
                {
                    Name = $"{WatchManagerInstanceName}-Sender"
                };
                _requestSenderThread.Start();

                _responseReceiverThread = new Thread(async () => await ReceiverThreadWork())
                {
                    Name = $"{WatchManagerInstanceName}-Receiver"
                };
                _responseReceiverThread.Start();

                _channelRecreationTask = Task.Run(ChannelRecreation, _watchManagerCancellationTokenSource.Token);
            }

            /// <summary>
            /// This event will be raised if an watch response is received by the <seealso cref="WatchManager"/> but there is an error processing the response
            /// </summary>
            public event EventHandler<WatchResponseProcessingException> WatchResponseProcessingFailed;

            /// <summary>
            /// The name of the current <seealso cref="WatchManager"/>
            /// </summary>
            public string WatchManagerInstanceName { get; }

            private readonly EtcdClient _etcdClientInstance;

            private readonly CancellationTokenSource _watchManagerCancellationTokenSource = new CancellationTokenSource();
            private readonly AutoResetEvent _reinitializeChannel = new AutoResetEvent(true);
            private readonly ManualResetEvent _channelReady = new ManualResetEvent(false);
            private readonly ConcurrentDictionary<long, Subscription> activeSubscriptions = new ConcurrentDictionary<long, Subscription>();
            private readonly BlockingCollection<WatchRequest> _outgoingMessagesQueue = new BlockingCollection<WatchRequest>(new ConcurrentQueue<WatchRequest>());

            private readonly Task _channelRecreationTask; // Keep a reference to the Task
            private readonly Thread _responseReceiverThread;
            private readonly Thread _requestSenderThread;

            private long _watchIdCounter;
            private AsyncDuplexStreamingCall<WatchRequest, WatchResponse> _activeChannel;

            /// <summary>
            /// Create a new watch subscription
            /// </summary>
            /// <param name="watchCreationRequest">The watch creation request to send to the server</param>
            /// <param name="watchResponseFunc">The function that is called when a update is received from the etcd server. The object that is provided is a state object and can be initially set through the <paramref name="initialWatchResponseFuncStateObject"/></param>
            /// <param name="initialWatchResponseFuncStateObject">Optional initial state for the state object that is passed between all calls to <paramref name="watchResponseFunc"/></param>
            /// <param name="watchResponseFuncExceptionHandlerFunc">Optional exception handler that is called when an error is encountered in the execution of the <paramref name="watchResponseFunc"/>. This this action throws an error it's silently discarded.</param>
            /// <returns>A <seealso cref="Subscription"/> that manages the created watch</returns>
            public Subscription CreateSubscription(WatchCreateRequest watchCreationRequest, Action<WatchManager.SubscriptionUpdate, object> watchResponseFunc, object initialWatchResponseFuncStateObject = null, Action<Exception> watchResponseFuncExceptionHandlerFunc = null)
            {
                if (watchCreationRequest.WatchId != default)
                {
                    throw new ArgumentException($"WatchId cannot be set because it is managed by {nameof(WatchManager)}", nameof(watchCreationRequest));
                }

                var watchId = GetNextWatchId();
                var subscription = new Subscription(this, watchId, watchCreationRequest, watchResponseFunc, initialWatchResponseFuncStateObject, watchResponseFuncExceptionHandlerFunc, _watchManagerCancellationTokenSource.Token);
                activeSubscriptions.TryAdd(watchId, subscription);
                SendSubscriptionWatchRequest(subscription);
                return subscription;
            }

            internal bool Disposing { get; private set; }

            public void Dispose()
            {
                if (!Disposing)
                {
                    Disposing = true;
                    _watchManagerCancellationTokenSource.Cancel();
                    _etcdClientInstance.RemoveWatchManager(this);

                    // Stop all active subscriptions for this manager
                    foreach (var activeSubscription in activeSubscriptions)
                    {
                        activeSubscription.Value.Dispose();
                    }

                    _activeChannel?.Dispose();
                    _watchManagerCancellationTokenSource.Dispose();
                }
            }

            private void SendSubscriptionWatchRequest(Subscription subscription)
            {
                var watchRequest = subscription.CreateWatchCreateRequest();
                _outgoingMessagesQueue.Add(watchRequest);
            }

            private void RemoveSubscription(Subscription subscription)
            {
                var watchRequest = subscription.CreateWatchCancelRequest();
                _outgoingMessagesQueue.Add(watchRequest);
            }

            private void HandleUpdateFromServer(WatchResponse response)
            {
                try
                {
                    if (activeSubscriptions.TryGetValue(response.WatchId, out var subscription))
                    {
                        subscription.HandleUpdate(response);
                    }
                    else if (response.WatchId == -1)
                    {
                        // TODO generic update
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

            private void ChannelRecreation()
            {
                while (!_watchManagerCancellationTokenSource.Token.IsCancellationRequested)
                {
                    _reinitializeChannel.WaitOne();
                    _channelReady.Reset();

                    if (_watchManagerCancellationTokenSource.Token.IsCancellationRequested)
                    {
                        return;
                    }

                    try
                    {
                        // Close and cleanup old channel
                        if (_activeChannel != null)
                        {
                            _activeChannel.Dispose();
                            _activeChannel = null;
                        }

                        // Create and initialize a new channel
                        var connection = _etcdClientInstance._balancer.GetConnection();
                        _activeChannel = connection.watchClient.Watch(cancellationToken: _watchManagerCancellationTokenSource.Token);

                        foreach (var activeSubscription in activeSubscriptions.Values)
                        {
                            SendSubscriptionWatchRequest(activeSubscription);
                        }
                    }
                    catch
                    {
                        // reinitialization of channel failed, will try again
                        continue;
                    }
                    _channelReady.Set();
                }
            }

            private async Task SenderThreadWork()
            {
                while (!_watchManagerCancellationTokenSource.Token.IsCancellationRequested)
                {
                    await SendMessagesFromOutgoingQueue();
                }

                async Task SendMessagesFromOutgoingQueue()
                {
                    WatchRequest watchRequest = null;
                    try
                    {
                        _channelReady.WaitOne();
                        while (!_watchManagerCancellationTokenSource.Token.IsCancellationRequested)
                        {
                            if (_outgoingMessagesQueue.TryTake(out var wr))
                            {
                                await _activeChannel.RequestStream.WriteAsync(wr);
                            }
                        }
                    }
                    catch // (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable)
                    {
                        if (watchRequest != null)
                        {
                            _outgoingMessagesQueue.Add(watchRequest);
                        }

                        // trigger a channel recreation
                        _channelReady.Reset();
                        _reinitializeChannel.Set();
                    }
                }
            }            
            
            private async Task ReceiverThreadWork()
            {
                while (!_watchManagerCancellationTokenSource.Token.IsCancellationRequested)
                {
                    await ReceiveMessages();
                }

                async Task ReceiveMessages()
                {
                    try
                    {
                        _channelReady.WaitOne();
                        while (await _activeChannel.ResponseStream.MoveNext(_watchManagerCancellationTokenSource.Token))
                        {
                            Console.WriteLine("[" + DateTimeOffset.Now.ToString("HH:mm:ss.fffffff") + "] Next item read from response stream");

                            var update = _activeChannel.ResponseStream.Current;
                            HandleUpdateFromServer(update);

                            Console.WriteLine("[" + DateTimeOffset.Now.ToString("HH:mm:ss.fffffff") + "] ready to read next item from response stream");
                        }
                    }
                    catch // (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable)
                    {
                        // trigger a channel recreation
                        _channelReady.Reset();
                        _reinitializeChannel.Set();
                    }
                }
            }

            private long GetNextWatchId()
            {
                return Interlocked.Increment(ref _watchIdCounter);
            }

            /// <summary>
            /// Represents a single watch subscription and is managed by <seealso cref="WatchManager"/>
            /// </summary>
            public class Subscription : IDisposable
            {
                internal Subscription(WatchManager watchManager, long watchId, WatchCreateRequest watchCreationRequest, Action<WatchManager.SubscriptionUpdate, object> watchResponseFunc, object initialStateObj,
                    Action<Exception> watchResponseFuncExceptionHandlerFunc, CancellationToken watchManagerCancellationToken)
                {
                    this._watchManager = watchManager;
                    this._watchId = watchId;
                    this._initialWatchCreationRequest = watchCreationRequest;
                    this._watchResponseFunc = watchResponseFunc;
                    this._stateObj = initialStateObj;
                    this._watchResponseFuncExceptionHandlerFunc = watchResponseFuncExceptionHandlerFunc;

                    State = SubscriptionState.Creating;
                    _watchResponseQueue = new BlockingCollection<SubscriptionUpdate>(new ConcurrentQueue<SubscriptionUpdate>());
                    this._subscriptionCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(watchManagerCancellationToken);
                    _watchResponseQueueHandleTask = Task.Run(HandleResponseQueue, _subscriptionCancellationTokenSource.Token);
                }


                /// <summary>
                /// The current state of the subscription
                /// </summary>
                public SubscriptionState State { get; private set; }

                private readonly WatchManager _watchManager;
                private readonly long _watchId;
                private readonly WatchCreateRequest _initialWatchCreationRequest;
                private readonly Action<SubscriptionUpdate, object> _watchResponseFunc;
                private readonly Action<Exception> _watchResponseFuncExceptionHandlerFunc;
                private readonly CancellationTokenSource _subscriptionCancellationTokenSource;
                private readonly BlockingCollection<SubscriptionUpdate> _watchResponseQueue;
                private readonly Task _watchResponseQueueHandleTask;  // Keep a reference to the Task
                private readonly object _stateObj;

                /// <summary>
                /// nextRev is the minimum expected next revision
                /// </summary>
                private long? _nextRevision;

                private bool disposing = false;

                /// <summary>
                /// Handles updates from the etcd server for this watch subscription
                /// </summary>
                /// <param name="response">The <seealso cref="WatchResponse"/> from the etcd server</param>
                internal void HandleUpdate(WatchResponse response)
                {
                    var newEvents = response.Events
                                            // filter events that are duplicates based on expected next revision
                                            .Where(ev => !_nextRevision.HasValue || ev.Kv.ModRevision >= _nextRevision) 
                                            .ToList();

                    if (response.Created)
                    {
                        State = SubscriptionState.Created;
                        if (_nextRevision == 0)
                        {
                            _nextRevision = response.Header.Revision + 1;
                        }
                    }
                    else
                    {
                        _nextRevision = response.Header.Revision + 1;
                    }

                    if (response.Canceled)
                    {
                        State = SubscriptionState.Canceled;
                    }

                    if (newEvents.Count > 0)
                    {
                        _watchResponseQueue.Add(new SubscriptionUpdate(response.Header, response.CompactRevision, newEvents), _subscriptionCancellationTokenSource.Token);
                        _nextRevision = response.Events.Last().Kv.ModRevision + 1;
                    }
                }

                internal WatchRequest CreateWatchCreateRequest()
                {
                    var watchCreationRequest = new WatchCreateRequest(_initialWatchCreationRequest)
                    {
                        WatchId = _watchId
                    };

                    if (_nextRevision.HasValue)
                    {
                        watchCreationRequest.StartRevision = _nextRevision.Value;
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
                            WatchId = _watchId
                        }
                    };
                }

                /// <inheritdoc />
                public void Dispose()
                {
                    if (!disposing)
                    {
                        _watchManager.RemoveSubscription(this);
                    }
                    _subscriptionCancellationTokenSource.Cancel();
                    _subscriptionCancellationTokenSource.Dispose();
                    _watchResponseQueue?.Dispose();

                    // _watchResponseQueueHandleTask needs to be in completed form before it can be disposed
                    // the cancellation token that is added to the task on creation is cancelled so this should not take long
                    //var sw = new Stopwatch();
                    //sw.Start();
                    //var w = _watchResponseQueueHandleTask?.Wait(TimeSpan.FromSeconds(1));
                    //if (w == true)
                    //{
                    //    _watchResponseQueueHandleTask?.Dispose();
                    //}
                    //sw.Stop();
                    //Console.WriteLine("[" + DateTimeOffset.Now.ToString("HH:mm:ss.fffffff") + $"] Waited for dispose of watch response queue task: {sw.Elapsed}");
                }


                private void HandleResponseQueue()
                {
                    if (!_subscriptionCancellationTokenSource.IsCancellationRequested)
                    {
                        foreach (var watchResponse in _watchResponseQueue.GetConsumingEnumerable(_subscriptionCancellationTokenSource.Token))
                        {
                            try
                            {
                                //Console.WriteLine("[" + DateTimeOffset.Now.ToString("HH:mm:ss.fffffff") + "] Handle response queue item");
                                _watchResponseFunc?.Invoke(watchResponse, _stateObj);
                            }
                            catch (Exception ex)
                            {
                                try
                                {
                                    _watchResponseFuncExceptionHandlerFunc?.Invoke(ex);
                                }
                                catch
                                {
                                    // exception handlers should not throw exceptions
                                }
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