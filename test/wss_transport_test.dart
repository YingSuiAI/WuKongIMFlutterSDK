import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:flutter/services.dart';
import 'package:fake_async/fake_async.dart';
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

  test('silent WSS CONNACK has a bounded authentication deadline', () async {
    final manager = WKIM.shared.connectionManager;
    var connects = 0;
    server.listen((request) async {
      final peer = await WebSocketTransformer.upgrade(request);
      peers.add(peer);
      peer.listen((data) {
        if ((data as List<int>).first >> 4 == 1) connects++;
      });
    });
    late FakeAsync clock;
    fakeAsync((value) {
      clock = value;
      manager.connect();
    });
    await _eventuallyWithClock(() => connects == 1, clock);
    expect(manager.isReadyForSending, isFalse);
    clock.elapse(const Duration(seconds: 13));
    await _eventuallyWithClock(() => connects >= 2, clock);
    expect(manager.isReadyForSending, isFalse);
  });

  test(
    'network restoration survives retirement of the offline WSS socket',
    () async {
      const channel = MethodChannel('dev.fluttercommunity.plus/connectivity');
      var connectivity = 'wifi';
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, (_) async => [connectivity]);
      addTearDown(
        () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
            .setMockMethodCallHandler(channel, null),
      );
      final manager = WKIM.shared.connectionManager;
      var connects = 0;
      var closedFailure = false;
      manager.addOnConnectionStatus('network-retirement', (status, _, info) {
        if (status == WKConnectStatus.fail &&
            info?.failureStage == WKConnectionFailureStage.connectionClosed) {
          closedFailure = true;
          expect(manager.isReadyForSending, isFalse);
        }
      });
      addTearDown(() => manager.removeOnConnectionStatus('network-retirement'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) {
            connects++;
            peer.add(_successfulConnack());
          }
        });
      });
      late FakeAsync clock;
      fakeAsync((value) {
        clock = value;
        manager.connect();
      });
      await _eventuallyWithClock(() => manager.isReadyForSending, clock);
      connectivity = 'none';
      clock.elapse(const Duration(seconds: 1));
      clock.flushMicrotasks();
      expect(manager.isNetworkUnavailable, isTrue);
      await peers.single.close();
      await _eventuallyWithClock(() => closedFailure, clock);
      clock.elapse(const Duration(seconds: 2));
      expect(connects, 1);
      connectivity = 'wifi';
      clock.elapse(const Duration(seconds: 3));
      await _eventuallyWithClock(
        () => connects >= 2 && manager.isReadyForSending,
        clock,
      );
      expect(manager.isNetworkUnavailable, isFalse);
    },
  );

  test(
    'real WSS heartbeat reconnects after missing PONG and retires its timer',
    () async {
      const channel = MethodChannel('dev.fluttercommunity.plus/connectivity');
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, (_) async => ['wifi']);
      addTearDown(
        () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
            .setMockMethodCallHandler(channel, null),
      );
      final manager = WKIM.shared.connectionManager;
      var connects = 0;
      var pings = 0;
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          final type = (data as List<int>).first >> 4;
          if (type == 1) {
            connects++;
            peer.add(_successfulConnack());
          } else if (type == 7) {
            pings++;
          }
        });
      });
      late FakeAsync clock;
      fakeAsync((value) {
        clock = value;
        manager.connect();
      });
      await _eventuallyWithClock(() => manager.isReadyForSending, clock);
      clock.elapse(const Duration(seconds: 30));
      await _eventuallyWithClock(() => pings == 1, clock);
      clock.elapse(const Duration(seconds: 30));
      clock.elapse(const Duration(seconds: 2));
      await _eventuallyWithClock(
        () => connects == 2 && manager.isReadyForSending,
        clock,
      );
      manager.disconnect(false);
      clock.elapse(const Duration(minutes: 2));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(connects, 2);
      expect(pings, 1);
      expect(manager.heartTimer, isNull);
    },
  );

  test(
    'caller owns WSS reconnect after typed authentication failure',
    () async {
      const channel = MethodChannel('dev.fluttercommunity.plus/connectivity');
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, (_) async => ['wifi']);
      addTearDown(
        () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
            .setMockMethodCallHandler(channel, null),
      );
      final manager = WKIM.shared.connectionManager;
      WKIM.shared.options.connectionManagedByCaller = true;
      var connects = 0;
      final failures = <WKConnectionFailureStage?>[];
      manager.addOnConnectionStatus('managed-auth', (status, _, info) {
        if (status == WKConnectStatus.fail) {
          expect(manager.isReadyForSending, isFalse);
          failures.add(info?.failureStage);
        }
      });
      addTearDown(() => manager.removeOnConnectionStatus('managed-auth'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) {
            connects++;
            if (connects > 1) peer.add(_successfulConnack());
          }
        });
      });
      final clock = FakeAsync();
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(() => connects == 1, clock);
      clock.elapse(const Duration(seconds: 11));
      expect(failures, [WKConnectionFailureStage.protocolHandshake]);
      clock.elapse(const Duration(seconds: 20));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(connects, 1);
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(
        () => connects == 2 && manager.isReadyForSending,
        clock,
      );
      clock.elapse(const Duration(seconds: 6));
      expect(manager.isReadyForSending, isTrue);
      expect(failures, [WKConnectionFailureStage.protocolHandshake]);
    },
  );

  test(
    'late network result cannot offline a replacement WSS generation',
    () async {
      const channel = MethodChannel('dev.fluttercommunity.plus/connectivity');
      final delayed = Completer<List<String>>();
      var checks = 0;
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, (_) {
            checks++;
            return checks == 1 ? delayed.future : Future.value(['wifi']);
          });
      addTearDown(
        () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
            .setMockMethodCallHandler(channel, null),
      );
      final manager = WKIM.shared.connectionManager;
      var connects = 0;
      var offlineEvents = 0;
      manager.addOnConnectionStatus('network-generation', (status, _, _) {
        if (status == WKConnectStatus.noNetwork) offlineEvents++;
      });
      addTearDown(() => manager.removeOnConnectionStatus('network-generation'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) {
            connects++;
            peer.add(_successfulConnack());
          }
        });
      });
      final clock = FakeAsync();
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(() => manager.isReadyForSending, clock);
      clock.elapse(const Duration(seconds: 1));
      expect(checks, 1);
      clock.run((_) {
        manager.prepareForSessionSetup();
        WKIM.shared.options.sessionGeneration++;
        manager.connect();
      });
      await _eventuallyWithClock(
        () => connects == 2 && manager.isReadyForSending,
        clock,
      );
      delayed.complete(['none']);
      clock.flushMicrotasks();
      clock.elapse(const Duration(seconds: 6));
      expect(manager.isReadyForSending, isTrue);
      expect(manager.isNetworkUnavailable, isFalse);
      expect(offlineEvents, 0);
      expect(connects, 2);
    },
  );

  test(
    'retired WSS authentication timer cannot close a replacement socket',
    () async {
      final manager = WKIM.shared.connectionManager;
      WKIM.shared.options.connectionManagedByCaller = true;
      var connects = 0;
      var failed = 0;
      manager.addOnConnectionStatus('auth-generation', (status, _, _) {
        if (status == WKConnectStatus.fail) failed++;
      });
      addTearDown(() => manager.removeOnConnectionStatus('auth-generation'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) {
            connects++;
            if (connects == 2) peer.add(_successfulConnack());
          }
        });
      });
      final clock = FakeAsync();
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(() => connects == 1, clock);
      clock.elapse(const Duration(seconds: 4));
      clock.run((_) {
        manager.prepareForSessionSetup();
        WKIM.shared.options.sessionGeneration++;
        manager.connect();
      });
      await _eventuallyWithClock(
        () => connects == 2 && manager.isReadyForSending,
        clock,
      );
      clock.elapse(const Duration(seconds: 11));
      expect(manager.isReadyForSending, isTrue);
      expect(failed, 0);
      expect(connects, 2);
    },
  );
  test(
    'WSS binary fragments merge and malformed frames report after retirement',
    () async {
      final manager = WKIM.shared.connectionManager;
      WKIM.shared.options.connectionManagedByCaller = true;
      final ready = Completer<WebSocket>();
      var authenticated = 0;
      final failures = <WKConnectionFailureStage?>[];
      manager.addOnConnectionStatus('frame-boundaries', (status, _, info) {
        if (status == WKConnectStatus.success) authenticated++;
        if (status == WKConnectStatus.fail) {
          expect(manager.isReadyForSending, isFalse);
          failures.add(info?.failureStage);
        }
      });
      addTearDown(() => manager.removeOnConnectionStatus('frame-boundaries'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) ready.complete(peer);
        });
      });
      manager.connect();
      final peer = await ready.future.timeout(const Duration(seconds: 3));
      final frame = _successfulConnack();
      peer.add(frame.sublist(0, 1));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(manager.isReadyForSending, isFalse);
      peer.add(frame.sublist(1, 4));
      peer.add(Uint8List.fromList([...frame.sublist(4), 0xf0, 0, 0x80]));
      await _eventually(() => manager.isReadyForSending);
      expect(authenticated, 1);
      expect(failures, isEmpty);
      peer.add(Uint8List.fromList([0xc0, 0xff, 0xff, 0xff, 0x7f]));
      await _eventually(() => failures.isNotEmpty);
      expect(failures, [WKConnectionFailureStage.protocolFrame]);
    },
  );

  test(
    'SDK-owned reconnect uses capped exponential jitter and can cancel backoff',
    () async {
      const channel = MethodChannel('dev.fluttercommunity.plus/connectivity');
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, (_) async => ['wifi']);
      addTearDown(
        () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
            .setMockMethodCallHandler(channel, null),
      );
      final manager = WKIM.shared.connectionManager;
      var connects = 0;
      var failures = 0;
      manager.addOnConnectionStatus('backoff', (status, _, info) {
        if (status == WKConnectStatus.fail && info?.failureStage != null) {
          expect(
            info?.failureStage,
            WKConnectionFailureStage.protocolHandshake,
          );
          failures++;
        }
      });
      addTearDown(() => manager.removeOnConnectionStatus('backoff'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) connects++;
        });
      });
      final clock = FakeAsync();
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(() => connects == 1, clock);
      for (var attempt = 0; attempt < 7; attempt++) {
        clock.elapse(const Duration(seconds: 10));
        expect(failures, attempt + 1);
        final retry = clock.pendingTimers.singleWhere(
          (timer) => timer.creationStackTrace.toString().contains(
            'WKConnectionManager._scheduleReconnect (',
          ),
        );
        final ceiling = (1500 * (1 << attempt)).clamp(0, 30000);
        expect(
          retry.duration.inMilliseconds,
          inInclusiveRange(ceiling ~/ 2, ceiling),
        );
        clock.elapse(retry.duration);
        await _eventuallyWithClock(() => connects == attempt + 2, clock);
      }
      clock.elapse(const Duration(seconds: 10));
      manager.disconnect(false);
      clock.elapse(const Duration(minutes: 2));
      expect(connects, 8);
    },
  );
  test(
    'caller-owned recovery retires WSS on wifi to mobile without autonomous dial',
    () async {
      const channel = MethodChannel('dev.fluttercommunity.plus/connectivity');
      var connectivity = 'wifi';
      TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
          .setMockMethodCallHandler(channel, (_) async => [connectivity]);
      addTearDown(
        () => TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
            .setMockMethodCallHandler(channel, null),
      );
      final manager = WKIM.shared.connectionManager;
      WKIM.shared.options.connectionManagedByCaller = true;
      var connects = 0;
      final failures = <WKConnectionFailureStage?>[];
      manager.addOnConnectionStatus('mobile-switch', (status, _, info) {
        if (status == WKConnectStatus.fail) {
          expect(manager.isReadyForSending, isFalse);
          failures.add(info?.failureStage);
        }
      });
      addTearDown(() => manager.removeOnConnectionStatus('mobile-switch'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) {
            connects++;
            peer.add(_successfulConnack());
          }
        });
      });
      final clock = FakeAsync();
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(() => manager.isReadyForSending, clock);
      clock.elapse(const Duration(seconds: 1));
      await _eventuallyWithClock(
        () => manager.lastConnectivityResult?.name == 'wifi',
        clock,
      );
      connectivity = 'mobile';
      clock.elapse(const Duration(seconds: 1));
      await _eventuallyWithClock(() => failures.isNotEmpty, clock);
      expect(failures, [WKConnectionFailureStage.connectionClosed]);
      clock.elapse(const Duration(seconds: 20));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(connects, 1);
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(
        () => connects == 2 && manager.isReadyForSending,
        clock,
      );
    },
  );
  test(
    'caller-managed rejected CONNACK ends automatic recovery for that attempt',
    () async {
      final manager = WKIM.shared.connectionManager;
      WKIM.shared.options.connectionManagedByCaller = true;
      var connects = 0;
      final reasons = <int?>[];
      manager.addOnConnectionStatus('connack-reject', (status, reason, _) {
        if (status == WKConnectStatus.fail) reasons.add(reason);
      });
      addTearDown(() => manager.removeOnConnectionStatus('connack-reject'));
      server.listen((request) async {
        final peer = await WebSocketTransformer.upgrade(request);
        peers.add(peer);
        peer.listen((data) {
          if ((data as List<int>).first >> 4 == 1) {
            connects++;
            peer.add(_successfulConnack(reasonCode: 2));
          }
        });
      });
      final clock = FakeAsync();
      clock.run((_) => manager.connect());
      await _eventuallyWithClock(() => reasons.isNotEmpty, clock);
      expect(reasons, [2]);
      clock.elapse(const Duration(minutes: 2));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(connects, 1);
      expect(manager.isReadyForSending, isFalse);
    },
  );
  test('healthy six-second CONNACK fits the authentication budget', () async {
    final manager = WKIM.shared.connectionManager;
    WKIM.shared.options.connectionManagedByCaller = true;
    final requestReady = Completer<WebSocket>();
    var failures = 0;
    manager.addOnConnectionStatus('slow-auth', (status, _, info) {
      if (status == WKConnectStatus.fail && info?.failureStage != null) {
        failures++;
      }
    });
    addTearDown(() => manager.removeOnConnectionStatus('slow-auth'));
    server.listen((request) async {
      final peer = await WebSocketTransformer.upgrade(request);
      peers.add(peer);
      peer.listen((data) {
        if ((data as List<int>).first >> 4 == 1) requestReady.complete(peer);
      });
    });
    final clock = FakeAsync();
    clock.run((_) => manager.connect());
    await _eventuallyWithClock(() => requestReady.isCompleted, clock);
    clock.elapse(const Duration(seconds: 6));
    expect(manager.isReadyForSending, isFalse);
    expect(failures, 0);
    final peer = await requestReady.future;
    peer.add(_successfulConnack());
    await _eventuallyWithClock(() => manager.isReadyForSending, clock);
    clock.elapse(const Duration(seconds: 5));
    expect(manager.isReadyForSending, isTrue);
    expect(failures, 0);
  });
}

Uint8List _successfulConnack({int reasonCode = 1}) {
  final body = WriteData()
    ..writeUint8(7)
    ..writeUint64(BigInt.zero)
    ..writeUint8(reasonCode)
    ..writeString(base64Encode(CryptoUtils.dhPublicKey!))
    ..writeString('1234567890123456')
    ..writeUint64(BigInt.zero);
  return Uint8List.fromList([
    0x21,
    ...encodeVariableLength(body.data.length),
    ...body.data,
  ]);
}

Future<void> _eventuallyWithClock(
  bool Function() predicate,
  FakeAsync clock,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!predicate()) {
    clock.elapse(Duration.zero);
    clock.flushMicrotasks();
    if (DateTime.now().isAfter(deadline)) fail('condition timed out');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

Future<void> _eventually(bool Function() predicate) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) fail('condition timed out');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
