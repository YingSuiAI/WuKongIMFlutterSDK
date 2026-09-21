import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/proto/write_read.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/type/const.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  late ServerSocket server;
  final clients = <Socket>[];
  final receivedBytes = <Socket, int>{};
  final readSubscriptions = <Socket, StreamSubscription<Uint8List>>{};
  var testIndex = 0;

  setUp(() async {
    testIndex++;
    server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((socket) {
      clients.add(socket);
      receivedBytes[socket] = 0;
      readSubscriptions[socket] = socket.listen((data) {
        receivedBytes[socket] = receivedBytes[socket]! + data.length;
      }, onDone: () {});
    });
    WKIM.shared.runMode = Model.web;
    WKIM.shared.options =
        Options.newDefault(
            'lifecycle-test-user-$testIndex',
            'lifecycle-test-token',
            addr: '127.0.0.1:${server.port}',
          )
          ..installationID = 'lifecycle-installation-$testIndex'
          ..appInstanceID = 'lifecycle-app-instance-$testIndex'
          ..installationGeneration = testIndex
          ..sessionGeneration = testIndex;
    WKIM.shared.connectionManager.disconnect(false);
  });

  tearDown(() async {
    WKIM.shared.connectionManager.disconnect(false);
    for (final client in clients) {
      await readSubscriptions[client]?.cancel();
      client.destroy();
    }
    await server.close();
    clients.clear();
    receivedBytes.clear();
    readSubscriptions.clear();
  });

  test(
    'serializes concurrent sends and tolerates close during flush',
    () async {
      WKIM.shared.connectionManager.connect();
      await _eventually(() => clients.isNotEmpty);
      await _eventually(() => receivedBytes[clients.single]! > 0);
      final authenticated = Completer<void>();
      WKIM.shared.connectionManager.addOnConnectionStatus('flush-test', (
        status,
        _,
        _,
      ) {
        if (status == WKConnectStatus.success && !authenticated.isCompleted) {
          authenticated.complete();
        }
      });
      final connack = WriteData()
        ..writeUint8(7)
        ..writeUint64(BigInt.zero)
        ..writeUint8(1)
        ..writeString(base64Encode(CryptoUtils.dhPublicKey!))
        ..writeString('1234567890123456')
        ..writeUint64(BigInt.zero);
      clients.single.add([
        0x21,
        ...encodeVariableLength(connack.data.length),
        ...connack.data,
      ]);
      await authenticated.future;
      WKIM.shared.connectionManager.removeOnConnectionStatus('flush-test');
      readSubscriptions[clients.single]!.pause();
      CryptoUtils.aesKey = '0123456789abcdef';
      CryptoUtils.salt = '1234567890123456';
      final payload = 'x' * 32768;
      final sends = <Future<void>>[];
      final failures = <Object>[];

      for (var i = 0; i < 64; i++) {
        final msg = WKMsg()
          ..fromUID = WKIM.shared.options.uid!
          ..clientSeq = i + 1
          ..channelID = 'peer'
          ..channelType = 1
          ..content = '{"content":"$payload"}'
          ..header.noPersist = true;
        sends.add(
          WKIM.shared.connectionManager
              .sendMessage(msg)
              .then<void>(
                (_) {},
                onError: (Object error, StackTrace stack) {
                  failures.add(error);
                },
              ),
        );
      }
      WKIM.shared.connectionManager.disconnect(false);
      WKIM.shared.connectionManager.disconnect(false);
      await Future.wait(sends);
      expect(failures, isNotEmpty);
    },
  );

  test(
    'remote close schedules one reconnect, intentional disconnect cancels it',
    () async {
      WKIM.shared.connectionManager.connect();
      await _eventually(() => clients.length == 1);
      clients.single.destroy();
      await _eventually(
        () => clients.length == 2,
        timeout: const Duration(seconds: 3),
      );

      WKIM.shared.connectionManager.disconnect(false);
      clients.last.destroy();
      await Future<void>.delayed(const Duration(milliseconds: 1700));
      expect(clients.length, 2);
    },
  );

  test('disconnect resets network gate for a fresh connection', () async {
    final manager = WKIM.shared.connectionManager;
    manager.isNetworkUnavailable = true;
    manager.isReconnection = true;
    manager.disconnect(false);
    manager.connect();
    await _eventually(() => clients.length == 1);
  });

  test('protocol v7 fails closed without exact session identity', () async {
    WKIM.shared.options.installationID = null;

    WKIM.shared.connectionManager.connect();
    await Future<void>.delayed(const Duration(milliseconds: 100));

    expect(clients, isEmpty);
  });

  test('protocol v7 setup rejects a missing installation identity', () async {
    final previous = WKIM.shared.options;
    final accepted = await WKIM.shared.setup(
      Options.newDefault('setup-user', 'setup-token', addr: previous.addr)
        ..appInstanceID = 'setup-app-instance'
        ..installationGeneration = 1
        ..sessionGeneration = 1,
    );

    expect(accepted, isFalse);
    expect(identical(WKIM.shared.options, previous), isTrue);
  });

  test('delayed address lookup cannot connect a replacement session', () async {
    void Function(String)? completeAddress;
    WKIM.shared.options.getAddr = (complete) => completeAddress = complete;
    WKIM.shared.connectionManager.connect();
    WKIM.shared.options.sessionGeneration++;
    completeAddress!('127.0.0.1:${server.port}');
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(clients, isEmpty);
  });

  test('late address callback is ignored after disconnect', () async {
    Completer<void> callbackStarted = Completer<void>();
    void Function(String)? completeAddress;
    WKIM.shared.options.getAddr = (complete) {
      completeAddress = complete;
      callbackStarted.complete();
    };

    WKIM.shared.connectionManager.connect();
    await callbackStarted.future;
    WKIM.shared.connectionManager.disconnect(false);
    completeAddress!('127.0.0.1:${server.port}');
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(clients, isEmpty);
  });
}

Future<void> _eventually(
  bool Function() condition, {
  Duration timeout = const Duration(seconds: 1),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('condition was not met before timeout');
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
}
