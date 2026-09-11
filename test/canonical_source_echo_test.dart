import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/db/message.dart';
import 'package:wukongimfluttersdk/db/wk_database_migrator.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/entity/cmd.dart';
import 'package:wukongimfluttersdk/proto/packet.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/proto/write_read.dart';
import 'package:wukongimfluttersdk/wkim.dart';

const _canonical =
    '{"type":"message.committed","version":1,"id":"019f0000-0000-7000-8000-000000000042",'
    '"created_at":"2026-09-11T08:00:00Z","payload":{"content":"canonical text"}}';
const _applicationID = '019f0000-0000-7000-8000-000000000042';
// Independent `sha256sum` oracle for the exact UTF-8 request in send().
const _requestSHA256 = 'ffac8c766efe34509a2bebb9f7f8fd6ad2f13c956a8edcd8aaecbd71eb52458a';

// Uses the real socket parser, AES integrity/decryption, v7 codec, SQLite
// transaction and public SDK callbacks. No fake decoder/storage consumer.
void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();
  late Directory directory;
  late ServerSocket server;
  late Socket peer;
  late StreamSubscription<Socket> accept;
  final sockets = <Socket>[];
  final drains = <StreamSubscription<Uint8List>>[];
  late _ObservedProto proto;
  final sdk = WKIM.shared;
  final previousOptions = WKIM.shared.options;
  final previousMode = WKIM.shared.runMode;
  final previousFactory = databaseFactoryOrNull;
  final delivered = <WKMsg>[];
  final refreshed = <WKMsg>[];

  void authenticate() {
    final connack = WriteData()
      ..writeUint8(7)
      ..writeUint64(BigInt.zero)
      ..writeUint8(1)
      ..writeString(base64Encode(CryptoUtils.dhPublicKey!))
      ..writeString('0123456789abcdef')
      ..writeUint64(BigInt.one);
    peer.add(_frame(0x21, connack));
  }

  setUp(() async {
    sdk.connectionManager.disconnect(true);
    await WKDBHelper.shared.close();
    directory = await Directory.systemTemp.createTemp('wk_canonical_');
    databaseFactory = databaseFactoryFfi;
    await databaseFactory.setDatabasesPath(directory.path);
    server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final accepted = Completer<void>();
    accept = server.listen((socket) {
      peer = socket;
      sockets.add(socket);
      drains.add(socket.listen((_) {}));
      if (!accepted.isCompleted) accepted.complete();
    });
    proto = _ObservedProto();
    sdk.runMode = Model.app;
    final options =
        Options.newDefault(
            'transport-owner',
            'token',
            addr: '127.0.0.1:${server.port}',
          )
          ..installationID = 'installation'
          ..appInstanceID = 'instance'
          ..installationGeneration = 1
          ..sessionGeneration = 1
          ..debug = false
          ..proto = proto;
    expect(await sdk.setup(options), isTrue);
    sdk.messageManager.addOnNewMsgListener('canonical', delivered.addAll);
    sdk.messageManager.addOnRefreshMsgListener('canonical', refreshed.add);
    sdk.connectionManager.connect();
    await accepted.future;
    await _until(() => proto.connects == 1);
    authenticate();
    await _until(() => proto.connacks == 1);
  });

  tearDown(() async {
    sdk.connectionManager.disconnect(true);
    sdk.messageManager.removeNewMsgListener('canonical');
    sdk.messageManager.removeOnRefreshMsgListener('canonical');
    for (final drain in drains) {
      await drain.cancel();
    }
    for (final socket in sockets) {
      socket.destroy();
    }
    drains.clear();
    sockets.clear();
    await accept.cancel();
    await server.close();
    await WKDBHelper.shared.close();
    sdk.options = previousOptions;
    sdk.runMode = previousMode;
    databaseFactoryOrNull = previousFactory;
    delivered.clear();
    refreshed.clear();
    await directory.delete(recursive: true);
  });

  Future<WKMsg> send() async {
    final msg = WKMsg()
      ..fromUID = 'product-principal'
      ..channelID = 'peer'
      ..clientMsgNO = 'request-42'
      ..content = '{"type":"message.send","payload":{"content":"request"}}';
    await sdk.messageManager.saveOutgoingMessage(msg);
    await sdk.connectionManager.sendMessage(msg);
    await _until(() => proto.sends.isNotEmpty);
    return msg;
  }

  void ack({int reasonCode = 1}) {
    final body = WriteData()
      ..writeUint64(BigInt.from(reasonCode == 1 ? 42 : 0))
      ..writeUint32(proto.sends.single.clientSeq)
      ..writeUint64(BigInt.from(reasonCode == 1 ? 10 : 0))
      ..writeUint8(reasonCode)
      ..writeString('request-42')
      ..writeString(reasonCode == 1 ? _applicationID : '');
    peer.add(_frame(0x40, body));
  }

  Uint8List echo({
    String content = _canonical,
    String fromUID = 'transport-owner',
    String channelID = 'peer',
    bool noPersist = false,
    bool sendFrame = true,
  }) {
    final encrypted = CryptoUtils.aesEncrypt(content);
    final key = CryptoUtils.generateMD5(
      CryptoUtils.aesEncrypt(
        '4210request-421788899200$fromUID${channelID}1$encrypted',
      ),
    );
    final body = WriteData()
      ..writeUint8(0)
      ..writeString(key)
      ..writeString(fromUID)
      ..writeString(channelID)
      ..writeUint8(1)
      ..writeUint32(0)
      ..writeString('request-42')
      ..writeUint64(BigInt.from(42))
      ..writeUint64(BigInt.from(10))
      ..writeUint32(1788899200)
      ..writeBytes(utf8.encode(encrypted));
    final frame = _frame(noPersist ? 0x51 : 0x52, body);
    if (sendFrame) peer.add(frame);
    return frame;
  }

  Uint8List eventFrame(
    String type,
    int sequence, {
    String channelID = 'peer',
    int messageID = 42,
  }) {
    final body = WriteData()
      ..writeString('event-$channelID-$sequence')
      ..writeString(type)
      ..writeUint64(BigInt.from(1788899200000))
      ..writeBytes(
        utf8.encode(
          jsonEncode({
            'message_id': messageID,
            'client_msg_no': 'request-$messageID',
            'channel_id': channelID,
            'channel_type': 1,
            'run_id': 'run-$messageID',
            'event_type': type,
            'event_key': 'event-$sequence',
            'msg_event_seq': sequence,
            'payload': {'authority_sequence': sequence},
          }),
        ),
      );
    return _frame(0xc0, body);
  }

  Future<void> migrateOldRow(Map<String, Object?> row) async {
    await WKDBHelper.shared.close();
    final path = '${directory.path}/wk_transport-owner.db';
    // This test-owned database is recreated at the actual preceding schema;
    // exercise the production initializer to apply the new receipt migration.
    await databaseFactory.deleteDatabase(path);
    final old = await databaseFactory.openDatabase(path);
    final names = (await File('assets/sql.txt').readAsString())
        .split(';')
        .map((name) => name.trim())
        .where((name) => name.isNotEmpty && int.parse(name) < 202609111800);
    await WKDatabaseMigrator().migrate(old, {
      for (final name in names)
        int.parse(name): await File('assets/$name.sql').readAsString(),
    });
    await old.insert('message', row);
    await old.close();
    expect(await WKDBHelper.shared.init(), isTrue);
  }

  for (final ackFirst in [true, false]) {
    test(
      'source echo merges one row with ACK ${ackFirst ? 'before' : 'after'} RECV',
      () async {
        final msg = await send();
        if (ackFirst) {
          ack();
          await _until(() => refreshed.isNotEmpty);
          expect(refreshed.single.applicationMessageID, _applicationID);
          expect(refreshed.single.messageID, '42');
        }
        echo();
        await _until(() => proto.receiveAcks == 1);
        if (!ackFirst) {
          ack();
          await _until(() => proto.sendacks == 1);
          expect(refreshed, isEmpty);
        }
        final stored = await MessageDB.shared.queryWithClientSeq(msg.clientSeq);
        expect(stored!.content, _canonical);
        expect(stored.messageID, '42');
        expect(stored.payloadCommitted, isTrue);
        expect(stored.originalPayloadSHA256, _requestSHA256);
        expect(stored.fromUID, 'product-principal');
        expect(delivered, hasLength(1));
        expect(delivered.single.clientSeq, msg.clientSeq);
        expect(
          (await WKDBHelper.shared.getDB()!.query(
            'conversation',
          )).single['unread_count'],
          0,
        );

        // Receipt state must survive restart, not just live in the pending map.
        await WKDBHelper.shared.close();
        await WKDBHelper.shared.init();
        final reopened = await MessageDB.shared.queryWithClientSeq(msg.clientSeq);
        expect(reopened!.originalPayloadSHA256, _requestSHA256);
        expect(reopened.originalPayloadSHA256,
          isNot('0756a9fd61f997017adb78c9fc4fa52d0b71e0317c251d22039d34010eec1c39'),
          reason: 'same client number does not prove a different original body');
        echo();
        await _until(() => proto.receiveAcks == 2);
        expect(delivered, hasLength(1));
        expect(await WKDBHelper.shared.getDB()!.query('message'), hasLength(1));
        echo(
          content: _canonical.replaceFirst(
            'canonical text',
            'conflicting text',
          ),
        );
        await _until(() => proto.recvs == 3);
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(proto.receiveAcks, 2);
        expect(delivered, hasLength(1));
        expect(
          (await MessageDB.shared.queryWithClientSeq(msg.clientSeq))!.content,
          _canonical,
        );
      },
    );
  }

  test('source echo retains local tombstone and media/read state', () async {
    final msg = await send();
    await MessageDB.shared.updateMsgWithField({
      'is_deleted': 1,
      'voice_status': 1,
      'viewed': 1,
      'viewed_at': 123,
      'extra': '{"local":"keep"}',
    }, msg.clientSeq);
    echo();
    await _until(() => proto.receiveAcks == 1);
    final stored = await MessageDB.shared.queryWithClientSeq(msg.clientSeq);
    expect(stored!.isDeleted, 1);
    expect(stored.voiceStatus, 1);
    expect(stored.viewed, 1);
    expect(stored.viewedAt, 123);
    expect(stored.localExtraMap, {'local': 'keep'});
    expect(stored.content, _canonical);
    expect(stored.originalPayloadSHA256, _requestSHA256);
    expect(delivered, isEmpty);
  });

  test('peer RECV cannot manufacture a sender original-payload proof', () async {
    final payload = jsonDecode(_canonical) as Map<String, dynamic>;
    payload['original_payload_sha256'] = _requestSHA256;
    echo(fromUID: 'remote-peer', content: jsonEncode(payload));
    await _until(() => proto.receiveAcks == 1);
    final stored = await MessageDB.shared.queryWithClientMsgNo('request-42');
    expect(stored!.payloadCommitted, isTrue);
    expect(stored.originalPayloadSHA256, isEmpty);
    expect(delivered.single.originalPayloadSHA256, isEmpty);
  });

  test(
    'a late failed SENDACK cannot uncommit an admitted source echo',
    () async {
      final msg = await send();
      echo();
      await _until(() => proto.receiveAcks == 1);
      ack(reasonCode: 2);
      await _until(() => proto.sendacks == 1);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      final stored = await MessageDB.shared.queryWithClientSeq(msg.clientSeq);
      expect(stored!.messageID, '42');
      expect(stored.messageSeq, 10);
      expect(stored.status, 1);
      expect(stored.content, _canonical);
    },
  );

  test(
    'coalesced ACK and duplicate source packets publish canonical once',
    () async {
      final events = <String>[];
      sdk.messageManager.addOnNewMsgListener('canonical-order', (messages) {
        events.addAll(messages.map((msg) => 'recv:${msg.content}'));
      });
      sdk.messageManager.addOnRefreshMsgListener('canonical-order', (msg) {
        events.add('ack:${msg.content}');
      });
      addTearDown(() {
        sdk.messageManager.removeNewMsgListener('canonical-order');
        sdk.messageManager.removeOnRefreshMsgListener('canonical-order');
      });
      final msg = await send();
      echo();
      ack();
      echo();
      await _until(() => proto.receiveAcks == 2 && proto.sendacks == 1);
      expect(delivered, hasLength(1));
      final deliveredAt = events.indexOf('recv:$_canonical');
      expect(
        events.skip(deliveredAt).every((event) => event.endsWith(_canonical)),
        isTrue,
      );
      expect(
        (await MessageDB.shared.queryWithClientSeq(msg.clientSeq))!.content,
        _canonical,
      );
      await sdk.messageManager.updateMsgStatusFail(msg.clientSeq);
      expect(
        (await MessageDB.shared.queryWithClientSeq(msg.clientSeq))!.status,
        1,
      );
    },
  );

  test(
    'source-first with lost SENDACK never resends the completed request',
    () async {
      final msg = await send();
      echo();
      await _until(() => proto.receiveAcks == 1);
      sdk.connectionManager.disconnect(false);
      sdk.connectionManager.connect();
      await _until(() => proto.connects == 2 && sockets.length == 2);
      authenticate();
      await _until(() => proto.connacks == 2);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(proto.sends, hasLength(1));
      expect(delivered, hasLength(1));
      expect(
        (await MessageDB.shared.queryWithClientSeq(msg.clientSeq))!.status,
        1,
      );
    },
  );

  test(
    'migration accepts unchanged old peer receipt without rewriting it',
    () async {
      await migrateOldRow({
        'client_seq': 42,
        'client_msg_no': 'request-42',
        'message_id': '42',
        'message_seq': 10,
        'from_uid': 'remote-peer',
        'channel_id': 'peer',
        'channel_type': 1,
        'content': _canonical,
        'status': 1,
        'is_deleted': 1,
      });
      final before = (await WKDBHelper.shared.getDB()!.query('message')).single;
      echo(fromUID: 'remote-peer');
      await _until(() => proto.receiveAcks == 1);
      expect(
        (await WKDBHelper.shared.getDB()!.query('message')).single,
        before,
      );
      expect(delivered, isEmpty);
      expect(refreshed, isEmpty);
    },
  );

  for (final ackFirst in [false, true]) {
    test(
      'migration retains empty-ID pending with ACK ${ackFirst ? 'first' : 'lost'}',
      () async {
        await migrateOldRow({
          'client_seq': 42,
          'client_msg_no': 'request-42',
          'message_id': '',
          'message_seq': 0,
          'from_uid': 'product-principal',
          'channel_id': 'peer',
          'channel_type': 1,
          'content': '{"type":"message.send","payload":{"content":"request"}}',
          'status': 0,
        });
        if (ackFirst) {
          final pending = await MessageDB.shared.queryWithClientSeq(42);
          await sdk.connectionManager.sendMessage(pending!);
          await _until(() => proto.sends.isNotEmpty);
          ack();
          await _until(() => refreshed.isNotEmpty);
        }
        echo();
        await _until(() => proto.receiveAcks == 1);
        final stored = await MessageDB.shared.queryWithClientSeq(42);
        expect(stored!.messageID, '42');
        expect(stored.content, _canonical);
        expect(stored.payloadCommitted, isTrue);
        expect(stored.fromUID, 'product-principal');
        expect(stored.status, 1);
        expect(delivered, hasLength(1));
        expect(await WKDBHelper.shared.getDB()!.query('message'), hasLength(1));
      },
    );
  }

  test(
    'command origin comes from verified RECV, not claimed JSON identity',
    () async {
      final commands = <WKCMD>[];
      sdk.cmdManager.addOnCmdListener('canonical-control', commands.add);
      addTearDown(() => sdk.cmdManager.removeCmdListener('canonical-control'));
      const payload =
          '{"type":99,"cmd":"im.account.control.changed",'
          '"from_uid":"claimed-sender","channel_id":"claimed-channel",'
          '"param":{"from_uid":"claimed-sender","channel_id":"claimed-channel"}}';
      echo(
        content: payload,
        fromUID: 'system',
        channelID: 'transport-owner',
        noPersist: true,
      );
      await _until(() => commands.length == 1);
      expect(commands.single.fromUID, 'system');
      // The personal-channel presentation rewrite must not change wire origin.
      expect(commands.single.channelID, 'transport-owner');
      expect(commands.single.channelType, 1);
      expect(commands.single.param['from_uid'], 'claimed-sender');
      expect(delivered, isEmpty);
      expect(await WKDBHelper.shared.getDB()!.query('message'), isEmpty);

      sdk.cmdManager.handleCMD(jsonDecode(payload));
      expect(commands.last.fromUID, isEmpty);
      expect(commands.last.channelID, isEmpty);
      expect(commands.last.channelType, 0);
    },
  );

  test(
    'command metadata never mutates its strict application payload',
    () async {
      final commands = <WKCMD>[];
      sdk.cmdManager.addOnCmdListener('strict-control', commands.add);
      addTearDown(() => sdk.cmdManager.removeCmdListener('strict-control'));
      echo(
        content: '{"type":99,"cmd":"control.changed","param":{"cursor":7}}',
        fromUID: 'system',
        channelID: 'transport-owner',
        noPersist: true,
      );
      await _until(() => commands.length == 1);
      expect(commands.single.param, {'cursor': 7});
      expect(commands.single.channelID, 'transport-owner');
    },
  );
  test(
    'coalesced RECV anchor must publish before its following EVENTs',
    () async {
      final order = <String>[];
      final observedAnchors = <Future<WKMsg?>>[];
      sdk.messageManager.addOnNewMsgListener('wire-order-review', (_) {
        order.add('message');
      });
      sdk.connectionManager.addOnEventListener('wire-order-review', (event) {
        order.add('event:${event.eventType}');
        observedAnchors.add(
          MessageDB.shared.queryWithClientMsgNo('request-42'),
        );
      });
      addTearDown(() {
        sdk.messageManager.removeNewMsgListener('wire-order-review');
        sdk.connectionManager.removeOnEventListener('wire-order-review');
      });
      // One TCP write containing a verified encrypted anchor followed by the
      // corresponding open/delta packets, exactly in server wire order.
      peer.add([
        ...echo(sendFrame: false),
        ...eventFrame('open', 1),
        ...eventFrame('delta', 2),
      ]);
      await _until(() => proto.receiveAcks == 1 && order.length == 3);
      final anchors = await Future.wait(observedAnchors);
      expect(
        order,
        ['message', 'event:open', 'event:delta'],
        reason:
            'anchor rows visible at event callbacks: ${anchors.map((row) => row?.messageID).toList()}',
      );
    },
  );

  test(
    'slow RECV storage holds only its own provider binding EVENTs',
    () async {
      final events = <String>[];
      sdk.connectionManager.addOnEventListener('binding-isolation', (event) {
        events.add(event.decodeJsonData()!['channel_id'] as String);
      });
      addTearDown(
        () => sdk.connectionManager.removeOnEventListener('binding-isolation'),
      );
      final entered = Completer<void>();
      final release = Completer<void>();
      final held = WKDBHelper.shared.getDB()!.transaction((_) async {
        entered.complete();
        await release.future;
      });
      addTearDown(() async {
        if (!release.isCompleted) release.complete();
        await held;
      });
      await entered.future;
      final malformed = WriteData()
        ..writeString('bad-event')
        ..writeString('delta')
        ..writeUint64(BigInt.one)
        ..writeBytes(utf8.encode('{"channel_id":12,"channel_type":"bad"}'));
      peer.add([
        ...echo(sendFrame: false),
        ...eventFrame('open', 1),
        ..._frame(0xc0, malformed),
        0xf0,
        0x01,
        0x2a, // Unknown frame must not become a shared queue/barrier.
        ...eventFrame('open', 1, channelID: 'other-binding', messageID: 43),
      ]);
      await _until(() => events.contains('other-binding'));
      expect(events, ['other-binding']);
      expect(delivered, isEmpty);
      release.complete();
      await held;
      await _until(() => proto.receiveAcks == 1 && events.length == 2);
      expect(events, ['other-binding', 'peer']);
    },
  );

  for (final replaceSession in [false, true]) {
    test(
      'queued EVENT cancels after ${replaceSession ? 'identity replacement' : 'disconnect'}',
      () async {
        final events = <EventPacket>[];
        sdk.connectionManager.addOnEventListener('queue-owner', events.add);
        addTearDown(
          () => sdk.connectionManager.removeOnEventListener('queue-owner'),
        );
        final entered = Completer<void>();
        final release = Completer<void>();
        final held = WKDBHelper.shared.getDB()!.transaction((_) async {
          entered.complete();
          await release.future;
        });
        addTearDown(() async {
          if (!release.isCompleted) release.complete();
          await held;
        });
        await entered.future;
        peer.add([...echo(sendFrame: false), ...eventFrame('open', 1)]);
        await _until(() => proto.recvs == 1 && proto.events == 1);
        if (replaceSession) {
          sdk.options.sessionGeneration++;
        } else {
          sdk.connectionManager.disconnect(false);
        }
        release.complete();
        await held;
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(events, isEmpty);
        expect(delivered, isEmpty);
        expect(proto.receiveAcks, 0);
        expect(await WKDBHelper.shared.getDB()!.query('message'), isEmpty);

        sdk.connectionManager.connect();
        await _until(() => proto.connects == 2 && sockets.length == 2);
        authenticate();
        await _until(() => proto.connacks == 2);
        peer.add(eventFrame('open', 1));
        await _until(() => events.length == 1);
        expect(events.single.eventID, 'event-peer-1');
      },
    );
  }
}

class _ObservedProto extends Proto {
  int connects = 0;
  int connacks = 0;
  int recvs = 0;
  int sendacks = 0;
  int events = 0;
  int receiveAcks = 0;
  final sends = <SendPacket>[];

  @override
  Uint8List encode(Packet packet) {
    final bytes = super.encode(packet);
    if (packet is ConnectPacket) connects++;
    if (packet is SendPacket) sends.add(packet);
    if (packet is RecvAckPacket) receiveAcks++;
    return bytes;
  }

  @override
  Packet decode(Uint8List bytes) {
    final packet = super.decode(bytes);
    if (packet is ConnackPacket) connacks++;
    if (packet is RecvPacket) recvs++;
    if (packet is SendAckPacket) sendacks++;
    if (packet is EventPacket) events++;
    return packet;
  }
}

Uint8List _frame(int header, WriteData body) => Uint8List.fromList([
  header,
  ...encodeVariableLength(body.toUint8List().length),
  ...body.toUint8List(),
]);

Future<void> _until(FutureOr<bool> Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition did not become true');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}
