import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/proto/write_read.dart';
import 'package:wukongimfluttersdk/transport/socket_transport.dart';
import 'package:wukongimfluttersdk/type/const.dart';
import 'package:wukongimfluttersdk/wkim.dart';

class _TrustFixture extends HttpOverrides {
  _TrustFixture(this.context);
  final SecurityContext context;
  @override
  HttpClient createHttpClient(SecurityContext? _) =>
      super.createHttpClient(context);
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  late Directory directory;
  late SecurityContext serverContext;
  late SecurityContext clientContext;
  late HttpServer server;
  final peers = <WebSocket>[];
  final transports = <SocketTransport>[];
  var index = 0;

  setUpAll(() async {
    // Ephemeral local certificate/key, never a production trust override.
    directory = await Directory.systemTemp.createTemp('wk-wss-test-');
    final result = await Process.run('openssl', [
      'req',
      '-x509',
      '-newkey',
      'rsa:2048',
      '-nodes',
      '-days',
      '1',
      '-keyout',
      '${directory.path}/key.pem',
      '-out',
      '${directory.path}/cert.pem',
      '-subj',
      '/CN=localhost',
      '-addext',
      'subjectAltName=DNS:localhost,IP:127.0.0.1',
    ]);
    expect(result.exitCode, 0, reason: 'local test certificate generation');
    serverContext = SecurityContext()
      ..useCertificateChain('${directory.path}/cert.pem')
      ..usePrivateKey('${directory.path}/key.pem');
    clientContext = SecurityContext(withTrustedRoots: true)
      ..setTrustedCertificates('${directory.path}/cert.pem');
  });

  setUp(() async {
    index++;
    HttpOverrides.global = _TrustFixture(clientContext);
    server = await HttpServer.bindSecure(
      InternetAddress.loopbackIPv4,
      0,
      serverContext,
    );
    WKIM.shared.runMode = Model.web;
    WKIM.shared.options =
        Options.newDefault(
            'wss-user-$index',
            'test-token',
            addr: 'wss://127.0.0.1:${server.port}/native',
          )
          ..installationID = 'installation-$index'
          ..appInstanceID = 'app-instance-$index'
          ..installationGeneration = index
          ..sessionGeneration = index;
    WKIM.shared.connectionManager.disconnect(false);
  });

  tearDown(() async {
    WKIM.shared.connectionManager.disconnect(false);
    for (final transport in transports) {
      await transport.close();
    }
    transports.clear();
    for (final peer in peers) {
      unawaited(peer.close());
    }
    peers.clear();
    await server.close(force: true);
    HttpOverrides.global = null;
  });
  tearDownAll(() => directory.delete(recursive: true));

  test(
    'trusted WSS preserves URI and binary protocol v7; ready is not CONNACK',
    () async {
      final packet = Completer<List<int>>();
      final peerReady = Completer<WebSocket>();
      server.listen((request) async {
        expect(request.uri.path, '/native');
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peerReady.complete(peer);
        peer.listen((data) {
          expect(data, isA<List<int>>());
          if (!packet.isCompleted) packet.complete(data as List<int>);
        });
      });
      final statuses = <int>[];
      final manager = WKIM.shared.connectionManager;
      manager.addOnConnectionStatus(
        'wss',
        (status, _, _) => statuses.add(status),
      );
      addTearDown(() => manager.removeOnConnectionStatus('wss'));
      manager.connect();
      final bytes = await packet.future.timeout(const Duration(seconds: 3));
      expect(bytes.first >> 4, 1); // CONNECT
      final wire = ReadData(Uint8List.fromList(bytes));
      decodeHeader(wire);
      expect(wire.readUint8(), 7);
      wire.readUint8(); // device flag
      expect(wire.readString(), 'installation-$index');
      expect(wire.readString(), 'wss-user-$index');
      wire.readString(); // fixture token
      wire.readUint64AsInt(); // timestamp
      wire.readString(); // ephemeral public key
      expect(wire.readString(), 'app-instance-$index');
      expect(wire.readUint64AsInt(), index);
      expect(wire.readUint64AsInt(), index);
      expect(statuses, isNot(contains(WKConnectStatus.success)));
      final body = WriteData()
        ..writeUint8(7)
        ..writeUint64(BigInt.zero)
        ..writeUint8(1)
        ..writeString(base64Encode(CryptoUtils.dhPublicKey!))
        ..writeString('1234567890123456')
        ..writeUint64(BigInt.zero);
      final peer = await peerReady.future;
      peer.add(
        Uint8List.fromList([
          0x21,
          ...encodeVariableLength(body.data.length),
          ...body.data,
        ]),
      );
      await _eventually(() => statuses.contains(WKConnectStatus.success));
    },
  );

  test('WSS redirect never downgrades or contacts another endpoint', () async {
    final target = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => target.close(force: true));
    var targetRequests = 0;
    target.listen((request) async {
      targetRequests++;
      final peer = await WebSocketTransformer.upgrade(request);
      peers.add(peer);
    });
    server.listen((request) {
      request.response.statusCode = HttpStatus.found;
      request.response.headers.set(
        HttpHeaders.locationHeader,
        'http://127.0.0.1:${target.port}/downgrade',
      );
      request.response.close();
    });
    final transport = SocketTransport.connect(
      'wss://127.0.0.1:${server.port}/native',
    );
    transports.add(transport);
    await expectLater(transport.ready, throwsA(isA<WebSocketException>()));
    expect(targetRequests, 0);
  });

  test('untrusted TLS fails before upgrade without TCP fallback', () async {
    HttpOverrides.global = null;
    var upgrades = 0;
    server.listen((request) {
      upgrades++;
      request.response.close();
    });
    final transport = SocketTransport.connect(
      'wss://127.0.0.1:${server.port}/native',
    );
    transports.add(transport);
    await expectLater(transport.ready, throwsA(isA<HandshakeException>()));
    expect(upgrades, 0);
  });

  test('binary frames pass unchanged; text closes native transport', () async {
    final peerReady = Completer<WebSocket>();
    server.listen((request) async {
      final peer = await WebSocketTransformer.upgrade(request);
      peers.add(peer);
      peerReady.complete(peer);
    });
    final transport = SocketTransport.connect(
      'wss://127.0.0.1:${server.port}/native',
    );
    transports.add(transport);
    await transport.ready;
    final data = <List<int>>[];
    final closed = Completer<void>();
    transport.listen(data.add, () {
      if (!closed.isCompleted) closed.complete();
    });
    final peer = await peerReady.future;
    peer.add(Uint8List.fromList([0xd0]));
    await _eventually(() => data.isNotEmpty);
    expect(data.single, [0xd0]);
    peer.add('not a native packet');
    await closed.future.timeout(const Duration(seconds: 2));
    expect(data.length, 1);
    await expectLater(
      transport.send(Uint8List.fromList([0xc0])),
      throwsStateError,
    );
  });

  test('handshake timeout closes pending HTTP connection', () async {
    final requested = Completer<HttpRequest>();
    server.listen(requested.complete);
    final transport = SocketTransport.connect(
      'wss://127.0.0.1:${server.port}/native',
      timeout: const Duration(milliseconds: 150),
    );
    transports.add(transport);
    final failure = expectLater(
      transport.ready,
      throwsA(isA<TimeoutException>()),
    );
    final request = await requested.future;
    await failure;
    // A late upgrade cannot become a live transport after the timeout.
    try {
      final peer = await WebSocketTransformer.upgrade(request);
      peers.add(peer);
    } catch (_) {
      /* Aborted handshake may already have closed the response. */
    }
    await expectLater(
      transport.send(Uint8List.fromList([0xc0])),
      throwsStateError,
    );
  });

  test('identity rotation during TLS upgrade cannot authenticate', () async {
    final requested = Completer<HttpRequest>();
    server.listen(requested.complete);
    final manager = WKIM.shared.connectionManager;
    manager.connect();
    final request = await requested.future.timeout(const Duration(seconds: 3));
    WKIM.shared.options.sessionGeneration++;
    final peer = await WebSocketTransformer.upgrade(request);
    peers.add(peer);
    var packets = 0;
    final closed = Completer<void>();
    peer.listen((_) => packets++, onDone: closed.complete);
    await closed.future.timeout(const Duration(seconds: 3));
    expect(packets, 0);
  });

  test(
    'disconnect during upgrade never sends CONNECT for a stale session',
    () async {
      final requested = Completer<HttpRequest>();
      server.listen(requested.complete);
      final manager = WKIM.shared.connectionManager;
      manager.connect();
      final request = await requested.future.timeout(
        const Duration(seconds: 3),
      );
      manager.disconnect(false);
      WKIM.shared.options.sessionGeneration++;
      var packets = 0;
      try {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((_) => packets++);
      } catch (_) {
        /* Expected if client cancellation won the race. */
      }
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(packets, 0);
    },
  );
}

Future<void> _eventually(bool Function() predicate) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) fail('condition timed out');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
