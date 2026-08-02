import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:flutter/services.dart';
import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  Completer<void>? holdPreferences;
  Completer<void>? preferencesRequested;
  const preferencesChannel =
      MethodChannel('plugins.flutter.io/shared_preferences');
  TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
      .setMockMethodCallHandler(preferencesChannel, (call) async {
    if (call.method == 'getAll' || call.method == 'setString') {
      if (preferencesRequested != null && !preferencesRequested!.isCompleted) {
        preferencesRequested!.complete();
      }
      final hold = holdPreferences;
      if (hold != null) {
        await hold.future;
      }
      if (call.method == 'getAll') {
        return <String, Object>{};
      }
      return true;
    }
    if (call.method == 'setBool' ||
        call.method == 'setInt' ||
        call.method == 'setDouble' ||
        call.method == 'setStringList' ||
        call.method == 'remove') {
      return true;
    }
    return null;
  });
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
    holdPreferences = null;
    preferencesRequested = null;
    WKIM.shared.runMode = Model.web;
    WKIM.shared.options = Options.newDefault(
      'lifecycle-test-user-$testIndex',
      'lifecycle-test-token',
      addr: '127.0.0.1:${server.port}',
    );
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

  test('serializes concurrent sends and tolerates close during flush',
      () async {
    WKIM.shared.connectionManager.connect();
    await _eventually(() => clients.isNotEmpty);
    readSubscriptions[clients.single]!.pause();
    CryptoUtils.aesKey = '0123456789abcdef';
    CryptoUtils.salt = '1234567890123456';
    final payload = 'x' * 32768;

    for (var i = 0; i < 64; i++) {
      final msg = WKMsg()
        ..clientSeq = i + 1
        ..channelID = 'peer'
        ..channelType = 1
        ..content = '{"content":"$payload"}'
        ..header.noPersist = true;
      WKIM.shared.connectionManager.sendMessage(msg);
    }
    WKIM.shared.connectionManager.disconnect(false);
    WKIM.shared.connectionManager.disconnect(false);
    await Future<void>.delayed(const Duration(milliseconds: 100));
  });

  test(
      'remote close schedules one reconnect, intentional disconnect cancels it',
      () async {
    WKIM.shared.connectionManager.connect();
    await _eventually(() => clients.length == 1);
    clients.single.destroy();
    await _eventually(() => clients.length == 2,
        timeout: const Duration(seconds: 3));

    WKIM.shared.connectionManager.disconnect(false);
    clients.last.destroy();
    await Future<void>.delayed(const Duration(milliseconds: 1700));
    expect(clients.length, 2);
  });

  test('disconnect resets network gate for a fresh connection', () async {
    final manager = WKIM.shared.connectionManager;
    manager.isNetworkUnavailable = true;
    manager.isReconnection = true;
    manager.disconnect(false);
    manager.connect();
    await _eventually(() => clients.length == 1);
  });

  test('old delayed handshake does not write after disconnect', () async {
    holdPreferences = Completer<void>();
    preferencesRequested = Completer<void>();
    WKIM.shared.connectionManager.connect();
    await _eventually(() => clients.length == 1);
    await preferencesRequested!.future;
    final bytesBeforeDisconnect = receivedBytes[clients.single]!;
    WKIM.shared.connectionManager.disconnect(false);
    holdPreferences!.complete();
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(receivedBytes[clients.single], bytesBeforeDisconnect);
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

Future<void> _eventually(bool Function() condition,
    {Duration timeout = const Duration(seconds: 1)}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('condition was not met before timeout');
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
}
