import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/manager/conversation_manager.dart';
import 'package:wukongimfluttersdk/manager/message_manager.dart';
import 'package:wukongimfluttersdk/proto/packet.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/type/const.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();
  late ServerSocket server;
  late StreamSubscription<Socket> serverSubscription;
  final sockets = <Socket>[];
  final readers = <StreamSubscription<Uint8List>>[];
  late _RecordingProto proto;
  late _Messages messages;
  final originalMessages = WKIM.shared.messageManager;
  final originalConversations = WKIM.shared.conversationManager;
  final originalMode = WKIM.shared.runMode;
  final originalOptions = WKIM.shared.options;
  final originalDatabaseFactory = databaseFactoryOrNull;
  Directory? databaseDirectory;

  Future<void> connect() async {
    final expected = sockets.length + 1;
    WKIM.shared.connectionManager.connect();
    await _eventually(() => sockets.length == expected);
    await _eventually(() => proto.connects == expected);
    proto.nextPacket = ConnackPacket(
      reasonCode: 1,
      serverKey: base64Encode(CryptoUtils.dhPublicKey!),
      salt: '0123456789abcdef',
    )..header.packetType = PacketType.connack;
    sockets.last.add([PacketType.connack.index << 4, 0]);
    await _eventually(() => proto.decoded == expected);
    await Future<void>.delayed(Duration.zero);
  }

  WKMsg message(int seq, String no) => WKMsg()
    ..fromUID = WKIM.shared.options.uid!
    ..clientSeq = seq
    ..clientMsgNO = no
    ..channelID = 'recipient'
    ..channelType = 1
    ..content = '{"type":1,"content":"hello"}';

  setUp(() async {
    proto = _RecordingProto();
    messages = _Messages();
    WKIM.shared.messageManager = messages;
    WKIM.shared.conversationManager = _Conversations();
    WKIM.shared.runMode = Model.web;
    WKIM.shared.connectionManager.disconnect(true);
    server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    serverSubscription = server.listen((socket) {
      sockets.add(socket);
      readers.add(socket.listen((_) {}));
    });
    WKIM.shared.options =
        Options.newDefault('alice', 'token', addr: '127.0.0.1:${server.port}')
          ..installationID = 'installation'
          ..appInstanceID = 'instance'
          ..installationGeneration = 1
          ..sessionGeneration = 1
          ..proto = proto;
  });

  tearDown(() async {
    WKIM.shared.connectionManager.disconnect(true);
    for (final reader in readers) {
      await reader.cancel();
    }
    for (final socket in sockets) {
      socket.destroy();
    }
    await serverSubscription.cancel();
    await server.close();
    readers.clear();
    sockets.clear();
    WKIM.shared.messageManager = originalMessages;
    WKIM.shared.conversationManager = originalConversations;
    WKIM.shared.runMode = originalMode;
    await WKDBHelper.shared.close();
    databaseFactoryOrNull = originalDatabaseFactory;
    WKIM.shared.options = originalOptions;
    await databaseDirectory?.delete(recursive: true);
    databaseDirectory = null;
  });

  test('reconnect retains only the same session pending sends', () async {
    await connect();
    await WKIM.shared.connectionManager.sendMessage(message(1, 'alice-1'));
    expect(proto.sends, ['alice-1']);

    WKIM.shared.connectionManager.disconnect(false);
    await connect();
    expect(proto.sends, ['alice-1', 'alice-1']);

    WKIM.shared.options.sessionGeneration++;
    await connect();
    expect(proto.sends, ['alice-1', 'alice-1']);
  });

  test(
    'SEND waits for authenticated CONNACK without requiring conversation sync',
    () async {
      WKIM.shared.conversationManager = originalConversations;
      WKIM.shared.connectionManager.connect();
      await _eventually(() => sockets.length == 1 && proto.connects == 1);
      await WKIM.shared.connectionManager.sendMessage(message(1, 'pre-auth'));
      expect(proto.sends, isEmpty);
      proto.nextPacket = ConnackPacket(
        reasonCode: 1,
        serverKey: base64Encode(CryptoUtils.dhPublicKey!),
        salt: '0123456789abcdef',
      )..header.packetType = PacketType.connack;
      sockets.single.add([PacketType.connack.index << 4, 0]);
      await _eventually(() => proto.sends.isNotEmpty);
      expect(proto.sends, ['pre-auth']);
    },
  );

  test('logout cannot replay another account pending messages', () async {
    await connect();
    await WKIM.shared.connectionManager.sendMessage(message(1, 'alice-1'));
    WKIM.shared.connectionManager.disconnect(true);
    WKIM.shared.options.uid = 'bob';
    WKIM.shared.options.token = 'bob-token';
    await connect();
    expect(proto.sends, ['alice-1']);
  });

  test(
    'changing identity prevents writes on the old authenticated socket',
    () async {
      await connect();
      WKIM.shared.options.uid = 'bob';
      await WKIM.shared.connectionManager.sendMessage(message(2, 'bob-2'));
      expect(proto.sends, isEmpty);
      await connect();
      expect(proto.sends, ['bob-2']);
    },
  );

  test(
    'product principal sender need not equal the opaque transport UID',
    () async {
      await connect();
      await WKIM.shared.connectionManager.sendMessage(
        message(1, 'product-sender')..fromUID = 'product-principal',
      );
      expect(proto.sends, ['product-sender']);
    },
  );

  test('ACK requires both pending sequence and message identity', () async {
    await connect();
    await WKIM.shared.connectionManager.sendMessage(message(3, 'current'));
    final wireSeq = proto.sentPackets.single.clientSeq;
    for (final (seq, no) in [
      (9999, 'current'),
      (wireSeq, 'stale'),
      (wireSeq, 'current'),
    ]) {
      proto.nextPacket = SendAckPacket()
        ..clientSeq = seq
        ..clientMsgNO = no
        ..messageID = '42'
        ..messageSeq = 10
        ..reasonCode = 1;
      final before = proto.decoded;
      sockets.last.add([PacketType.sendack.index << 4, 0]);
      await _eventually(() => proto.decoded > before);
    }
    expect(messages.acks, [3]);
  });

  test('socket encoding failures reach the send caller', () async {
    await connect();
    proto.failSend = true;
    await expectLater(
      WKIM.shared.connectionManager.sendMessage(message(1, 'failed')),
      throwsStateError,
    );
  });

  test(
    'explicit retry gives a new wire identity and ignores the old ACK',
    () async {
      await connect();
      final msg = message(7, 'retry');
      await WKIM.shared.connectionManager.sendMessage(msg);
      await WKIM.shared.connectionManager.sendMessage(msg);
      final first = proto.sentPackets[0].clientSeq;
      final second = proto.sentPackets[1].clientSeq;
      expect(second, isNot(first));
      for (final seq in [first, second]) {
        proto.nextPacket = SendAckPacket()
          ..clientSeq = seq
          ..clientMsgNO = 'retry'
          ..messageID = '42'
          ..messageSeq = 10
          ..reasonCode = 1;
        final before = proto.decoded;
        sockets.last.add([PacketType.sendack.index << 4, 0]);
        await _eventually(() => proto.decoded > before);
        expect(messages.acks, seq == first ? isEmpty : [7]);
      }
    },
  );

  test(
    'invalid receive integrity never acknowledges a dropped message',
    () async {
      await connect();
      proto.nextPacket = RecvPacket()
        ..header.packetType = PacketType.recv
        ..messageID = BigInt.from(42)
        ..messageSeq = 10
        ..msgKey = 'invalid'
        ..payload = 'corrupt';
      sockets.last.add([PacketType.recv.index << 4, 0]);
      await _eventually(() => proto.decoded == 2);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(proto.receiveAcks, 0);
    },
  );
  test(
    'server disconnect publishes kicked after retiring the session',
    () async {
      await connect();
      final statuses = <int>[];
      WKIM.shared.connectionManager.addOnConnectionStatus(
        'kicked-test',
        (status, _, _) => statuses.add(status),
      );
      try {
        proto.nextPacket = Packet()..header.packetType = PacketType.disconnect;
        sockets.last.add([PacketType.disconnect.index << 4, 0]);
        await _eventually(() => statuses.isNotEmpty);
        expect(statuses, [WKConnectStatus.kicked]);
        expect(WKIM.shared.options.uid, isEmpty);
      } finally {
        WKIM.shared.connectionManager.removeOnConnectionStatus('kicked-test');
      }
    },
  );

  test(
    'RECV ACK follows atomic storage and repeated delivery is idempotent',
    () async {
      databaseDirectory = await Directory.systemTemp.createTemp('wk_receive_');
      databaseFactory = databaseFactoryFfi;
      await databaseFactory.setDatabasesPath(databaseDirectory!.path);
      WKIM.shared.messageManager = originalMessages;
      WKIM.shared.conversationManager = originalConversations;
      WKIM.shared.runMode = Model.app;
      expect(await WKIM.shared.setup(WKIM.shared.options), isTrue);
      await connect();
      final db = WKDBHelper.shared.getDB()!;
      await db.execute('''
CREATE TRIGGER fail_receive BEFORE INSERT ON conversation
BEGIN SELECT RAISE(FAIL, 'test projection failure'); END
''');
      RecvPacket receive() => RecvPacket()
        ..header.packetType = PacketType.recv
        ..header.showUnread = true
        ..setting.noEncrypt = 1
        ..messageID = BigInt.from(42)
        ..messageSeq = 10
        ..clientMsgNO = 'received-42'
        ..fromUID = 'sender'
        ..channelID = 'peer'
        ..channelType = 1
        ..payload = '{"type":1,"content":"received"}';
      proto.nextPacket = receive();
      sockets.last.add([PacketType.recv.index << 4, 0]);
      await _eventually(() => proto.decoded == 2);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(proto.receiveAcks, 0);
      expect(await db.query('message'), isEmpty);
      expect(await db.query('conversation'), isEmpty);

      await db.execute('DROP TRIGGER fail_receive');
      for (var delivery = 1; delivery <= 2; delivery++) {
        proto.nextPacket = receive();
        sockets.last.add([PacketType.recv.index << 4, 0]);
        await _eventually(() => proto.receiveAcks == delivery);
        final rows = await db.query('message');
        expect(rows, hasLength(1));
        expect(rows.single['client_msg_no'], 'received-42');
        expect(rows.single['is_deleted'], 0);
        final conversations = await db.query('conversation');
        expect(conversations, hasLength(1));
        expect(conversations.single['unread_count'], 1);
      }
      final outgoing = message(0, 'own-echo')
        ..fromUID = 'product-principal'
        ..channelID = 'peer';
      final localSeq = await originalMessages.saveMsg(outgoing);
      await originalMessages.updateSendResult('43', localSeq, 11, 1);
      proto.nextPacket = receive()
        ..messageID = BigInt.from(43)
        ..messageSeq = 11
        ..clientMsgNO = 'own-echo'
        ..fromUID = WKIM.shared.options.uid!;
      sockets.last.add([PacketType.recv.index << 4, 0]);
      await _eventually(() => proto.receiveAcks == 3);
      final echo = await db.query(
        'message',
        where: 'client_seq = ?',
        whereArgs: [localSeq],
      );
      expect(echo.single['from_uid'], 'product-principal');
      expect(echo.single['client_msg_no'], 'own-echo');
      expect(echo.single['message_id'], '43');
      expect(echo.single['content'], '{"type":1,"content":"received"}');
      expect(await db.query('message'), hasLength(2));
      expect((await db.query('conversation')).single['unread_count'], 1);
    },
  );

  test(
    'failed ACK persistence keeps the pending attempt recoverable',
    () async {
      await connect();
      await WKIM.shared.connectionManager.sendMessage(
        message(3, 'durable-ack'),
      );
      proto.nextPacket = SendAckPacket()
        ..clientSeq = proto.sentPackets.single.clientSeq
        ..clientMsgNO = 'durable-ack'
        ..messageID = '42'
        ..messageSeq = 10
        ..reasonCode = 1;
      messages.failAck = true;
      sockets.last.add([PacketType.sendack.index << 4, 0]);
      await _eventually(() => messages.ackAttempts == 1);
      expect(messages.acks, isEmpty);
      messages.failAck = false;
      sockets.last.add([PacketType.sendack.index << 4, 0]);
      await _eventually(() => messages.acks.isNotEmpty);
      expect(messages.acks, [3]);
    },
  );
}

class _RecordingProto extends Proto {
  final sends = <String>[];
  final sentPackets = <SendPacket>[];
  int connects = 0;
  int decoded = 0;
  int receiveAcks = 0;
  bool failSend = false;
  Packet? nextPacket;

  @override
  Uint8List encode(Packet packet) {
    if (packet is ConnectPacket) connects++;
    if (packet is RecvAckPacket) receiveAcks++;
    if (packet is SendPacket) {
      if (failSend) throw StateError('test encoding failed');
      sends.add(packet.clientMsgNO);
      sentPackets.add(packet);
    }
    return Uint8List.fromList([PacketType.ping.index << 4]);
  }

  @override
  Packet decode(Uint8List data) {
    decoded++;
    return nextPacket!;
  }
}

class _Conversations implements WKConversationManager {
  @override
  Future<void> setSyncConversation(Function() callback) async => callback();
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _Messages implements WKMessageManager {
  final acks = <int>[];
  bool failAck = false;
  int ackAttempts = 0;

  @override
  String generateClientMsgNo() => 'test-message';

  @override
  Future<void> updateSendResult(
    String messageID,
    int clientSeq,
    int messageSeq,
    int reasonCode, {
    String applicationMessageID = '',
    bool Function()? isCurrent,
  }) async {
    ackAttempts++;
    if (failAck) throw StateError('test ACK persistence failure');
    if (isCurrent?.call() ?? true) acks.add(clientSeq);
  }

  @override
  Future<void> updateSendingMsgFail() async {}

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

Future<void> _eventually(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 2));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition did not become true');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
