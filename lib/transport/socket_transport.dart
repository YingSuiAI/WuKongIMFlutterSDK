import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:web_socket_channel/io.dart';

/// Internal byte transport. Readiness only means transport establishment;
/// the connection manager must still authenticate with protocol CONNACK.
class SocketTransport {
  SocketTransport.connect(
    String address, {
    Duration timeout = const Duration(seconds: 5),
  }) {
    ready = _connect(address).timeout(
      timeout,
      onTimeout: () {
        unawaited(close());
        throw TimeoutException('Transport handshake timed out');
      },
    );
  }

  late final Future<void> ready;
  Socket? _tcp;
  ConnectionTask<Socket>? _tcpTask;
  WebSocket? _webSocket;
  IOWebSocketChannel? _channel;
  HttpClient? _client;
  StreamSubscription<dynamic>? _subscription;
  bool _closed = false;
  bool _closeNotified = false;
  Future<void> _writeTail = Future<void>.value();
  Future<void>? _closeFuture;

  Future<void> _connect(String address) async {
    final uri = Uri.parse(address.contains('://') ? address : 'tcp://$address');
    if (uri.host.isEmpty || uri.userInfo.isNotEmpty || uri.hasFragment) {
      throw const FormatException('Invalid transport endpoint');
    }
    if (uri.scheme == 'tcp') {
      if (!uri.hasPort || uri.path.isNotEmpty || uri.hasQuery) {
        throw const FormatException('Invalid TCP endpoint');
      }
      final task = await Socket.startConnect(uri.host, uri.port);
      _tcpTask = task;
      if (_closed) task.cancel();
      final socket = await task.socket;
      _tcpTask = null;
      if (_closed) {
        socket.destroy();
        throw StateError('Transport closed during handshake');
      }
      _tcp = socket;
    } else if (uri.scheme == 'ws' || uri.scheme == 'wss') {
      // Own the HTTP handshake so timeout/disconnect releases pending requests.
      // Default platform trust and hostname validation are never overridden.
      final client = _HandshakeClient(HttpClient());
      _client = client;
      try {
        final socket = await WebSocket.connect(
          uri.toString(),
          customClient: client,
        );
        if (_closed) {
          unawaited(socket.close());
          throw StateError('Transport closed during handshake');
        }
        _webSocket = socket;
        final channel = IOWebSocketChannel(socket);
        _channel = channel;
        await channel.ready;
      } finally {
        client.close(force: true);
        if (identical(_client, client)) _client = null;
      }
    } else {
      throw const FormatException('Unsupported transport scheme');
    }
  }

  Future<void> send(Uint8List data) {
    final operation = _writeTail.then((_) async {
      if (_closed) throw StateError('Transport closed before write');
      final tcp = _tcp;
      if (tcp != null) {
        tcp.add(data);
        await tcp.flush();
      } else if (_webSocket?.readyState == WebSocket.open) {
        _channel!.sink.add(data);
      } else {
        throw StateError('Transport is not writable');
      }
    });
    _writeTail = operation.catchError((_) {});
    return operation;
  }

  void listen(void Function(Uint8List) onData, void Function() onClosed) {
    if (_closed || _subscription != null) return;
    final stream = _tcp ?? _channel!.stream;
    _subscription = stream.listen(
      (dynamic data) {
        if (_closed) return;
        if (data is! List<int>) {
          // Native protocol is binary; a text frame is not a protocol packet.
          _notifyClosed(onClosed);
          unawaited(close());
          return;
        }
        onData(data is Uint8List ? data : Uint8List.fromList(data));
      },
      onError: (Object _) => _notifyClosed(onClosed),
      onDone: () => _notifyClosed(onClosed),
    );
  }

  void _notifyClosed(void Function() callback) {
    if (_closed || _closeNotified) return;
    _closeNotified = true;
    callback();
  }

  Future<void> close() {
    if (_closeFuture != null) return _closeFuture!;
    _closed = true;
    _client?.close(force: true);
    _client = null;
    _tcpTask?.cancel();
    _tcpTask = null;
    _tcp?.destroy();
    _tcp = null;
    final subscription = _subscription;
    _subscription = null;
    if (subscription != null) unawaited(subscription.cancel());
    final channel = _channel;
    _channel = null;
    if (channel != null) unawaited(channel.sink.close());
    _closeFuture = _writeTail;
    return _closeFuture!;
  }
}

// WebSocket.connect otherwise follows GET redirects, including HTTPS -> HTTP.
// Only the two HttpClient operations used by that SDK handshake are delegated.
class _HandshakeClient implements HttpClient {
  _HandshakeClient(this._client);
  final HttpClient _client;

  @override
  Future<HttpClientRequest> openUrl(String method, Uri url) async {
    final request = await _client.openUrl(method, url);
    request.followRedirects = false;
    return request;
  }

  @override
  void close({bool force = false}) => _client.close(force: force);

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
