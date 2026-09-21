import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:hex/hex.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/proto/packet.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/proto/write_read.dart';
import 'package:wukongimfluttersdk/wkim.dart';

// Independent v7 oracle: test/fixtures/generate_protocol_golden.go calls the
// server's actual codec and crypto. SEND/EVENT/uncrypted vectors retain the
// unchanged v6 layout from b52db059a49e309ba164b27496144e0557643fd0.
// No Dart encoder produced these golden bytes.
const _connack =
    '213007fffffffffffffc1801000a7365727665722d6b6579001066656463626139383736353433323130000000000000002a';
const _send =
    '3f7488010203040009636c69656e742d34320006e4bc9ae8af9d0200000e10002033366538363761623464616630343935646566313032636364323437323134660007746f7069632d31685a493275456a48466b4231397450694830616b454f54516b6f593370746f5870424c5256366d726b79493d';
const _recv =
    '5a8c018800203865623865643965643866323432343831303162346139393138633831633739000673656e6465720006e4bc9ae8af9d0200000e100009636c69656e742d3432002000000000000100000001000000036aa1f3000007746f7069632d31685a493275456a48466b4231397450694830616b454f54516b6f593370746f5870424c5256366d726b79493d';
const _sendack =
    '40460020000000000001010203040000000100000003010009636c69656e742d3432002430313963303030302d303030302d373030302d383030302d303030303030303030303031';

Uint8List _hex(String value) => Uint8List.fromList(HEX.decode(value));

void main() {
  late Options previousOptions;
  late String previousKey;
  late String previousSalt;
  setUp(() {
    previousOptions = WKIM.shared.options;
    previousKey = CryptoUtils.aesKey;
    previousSalt = CryptoUtils.salt;
    WKIM.shared.options = Options();
    CryptoUtils.aesKey = '0123456789abcdef';
    CryptoUtils.salt = 'fedcba9876543210';
  });
  tearDown(() {
    WKIM.shared.options = previousOptions;
    CryptoUtils.aesKey = previousKey;
    CryptoUtils.salt = previousSalt;
  });

  test('CONNACK preserves the server signed negative time difference', () {
    final packet = Proto().decode(_hex(_connack)) as ConnackPacket;
    expect(packet.timeDiff, -1000);
    expect(packet.reasonCode, 1);
    expect(packet.serviceProtoVersion, 7);
    expect(packet.nodeId, 42);
  });

  test('CONNECT identity fields match the deployed Go codec', () {
    final packet = ConnectPacket(
      version: 7,
      deviceFlag: 1,
      deviceID: 'install-1',
      uid: 'u1',
      token: 'token-1',
      clientTimestamp: 1786521600000,
      clientKey: 'client-key',
      appInstanceID: 'app-1',
      installationGeneration: 3,
      sessionGeneration: 7,
    );
    expect(
      HEX.encode(Proto().encode(packet)),
      '104507010009696e7374616c6c2d31000275310007746f6b656e2d310000019ff4fc4000000a636c69656e742d6b657900056170702d3100000000000000030000000000000007',
    );
  });

  test('rejects server protocol downgrade and unimplemented upgrade', () {
    for (final version in [5, 6, 8]) {
      final bytes = _hex(_connack)..[2] = version;
      expect(() => Proto().decode(bytes), throwsFormatException);
      expect(
        () => Proto().encode(ConnectPacket(version: version)),
        throwsFormatException,
      );
    }
  });

  test('successful CONNACK must explicitly declare v7; auth failure need not', () {
    final original = _hex(_connack);
    final unversioned = Uint8List.fromList([
      0x20, original[1] - 1, ...original.sublist(3),
    ]);
    expect(() => Proto().decode(unversioned), throwsFormatException);
    unversioned[10] = 2; // ReasonAuthFail after int64 TimeDiff.
    expect((Proto().decode(unversioned) as ConnackPacket).reasonCode, 2);
  });

  test('EVENT and DISCONNECT match Go and PING/PONG remain header-only', () {
    final event =
        Proto().decode(
              _hex(
                'c03200056576742d31000564656c7461000001a0889d38007b226d73675f6576656e745f736571223a343239343936373239397d',
              ),
            )
            as EventPacket;
    expect(event.eventID, 'evt-1');
    expect(event.eventType, 'delta');
    expect(event.timestamp, 1788998400000);
    expect(event.decodeJsonData(), {'msg_event_seq': 4294967299});
    final disconnect =
        Proto().decode(_hex('9006010003627965')) as DisconnectPacket;
    expect(disconnect.reasonCode, 1);
    expect(disconnect.reason, 'bye');
    expect(Proto().decode(_hex('70')), isA<PingPacket>());
    expect(Proto().decode(_hex('80')), isA<PongPacket>());
    expect(HEX.encode(Proto().encode(PingPacket())), '70');
    expect(HEX.encode(Proto().encode(PongPacket())), '80');
  });

  test('SEND bytes and encrypted verification key match Go', () {
    final packet =
        SendPacket(
            clientSeq: 0x1020304,
            clientMsgNO: 'client-42',
            channelID: '会话',
            channelType: 2,
            topic: 'topic-1',
          )
          ..expire = 3600
          ..payload = '{"type":1,"content":"你好"}';
    packet.setting
      ..receipt = 1
      ..topic = 1;
    packet.header
      ..noPersist = true
      ..showUnread = true
      ..syncOnce = true;
    packet.header.dup = true;
    expect(HEX.encode(Proto().encode(packet)), _send);
    expect(packet.encodeMsgKey(), '36e867ab4daf0495def102ccd247214f');
  });

  test('RECV preserves flags, UTF-8 names, uint64 sequence and message ID', () {
    final packet = Proto().decode(_hex(_recv)) as RecvPacket;
    expect(packet.messageID.toString(), '9007199254740993');
    expect(packet.messageSeq, 0x100000003);
    expect(packet.channelID, '会话');
    expect(packet.fromUID, 'sender');
    expect(packet.topic, 'topic-1');
    expect(packet.expire, 3600);
    expect(packet.messageTime, 1788998400);
    expect(packet.setting.encode(), 0x88);
    expect(packet.header.showUnread, isTrue);
    expect(packet.header.dup, isTrue);
    expect(packet.msgKey, '8eb8ed9ed8f24248101b4a9918c81c79');
    expect(CryptoUtils.aesDecrypt(packet.payload), '{"type":1,"content":"你好"}');
  });

  test('SENDACK and RECVACK preserve exact wide integers', () {
    final packet = Proto().decode(_hex(_sendack)) as SendAckPacket;
    expect(packet.messageID, '9007199254740993');
    expect(packet.messageSeq, 0x100000003);
    expect(packet.clientSeq, 0x1020304);
    expect(packet.clientMsgNO, 'client-42');
    expect(packet.applicationMessageID, '019c0000-0000-7000-8000-000000000001');
    final ack = RecvAckPacket(messageSeq: packet.messageSeq)
      ..messageID = BigInt.parse(packet.messageID);
    expect(
      HEX.encode(Proto().encode(ack)),
      '601000200000000000010000000100000003',
    );
  });

  test('explicit unencrypted RECV preserves UTF-8 payload from Go', () {
    final packet =
        Proto().decode(
              _hex(
                '5054100000000673656e6465720006e4bc9ae8af9d02000000000009636c69656e742d3432002000000000000100000001000000036aa1f3007b2274797065223a312c22636f6e74656e74223a22e4bda0e5a5bd227d',
              ),
            )
            as RecvPacket;
    expect(packet.setting.encode(), 0x10);
    expect(packet.setting.noEncrypt, 1);
    expect(packet.payload, '{"type":1,"content":"你好"}');
  });

  test('explicit unencrypted SEND matches Go without session AES keys', () {
    CryptoUtils.aesKey = '';
    CryptoUtils.salt = '';
    final packet = SendPacket(
      clientSeq: 0x1020304,
      clientMsgNO: 'client-42',
      channelID: '会话',
      channelType: 2,
    )..payload = '{"type":1,"content":"你好"}';
    packet.setting.noEncrypt = 1;
    expect(
      HEX.encode(Proto().encode(packet)),
      '303c10010203040009636c69656e742d34320006e4bc9ae8af9d020000000000007b2274797065223a312c22636f6e74656e74223a22e4bda0e5a5bd227d',
    );
  });

  test('setting round-trip preserves every received wire flag', () {
    for (var flags = 0; flags <= 0xff; flags++) {
      expect(Setting().decode(flags).encode(), flags);
    }
  });

  test('decoding a view uses the view offset and length', () {
    final bytes = _hex(_sendack);
    final buffer = Uint8List.fromList([0xde, 0xad, ...bytes, 0xbe, 0xef]);
    final packet =
        Proto().decode(Uint8List.sublistView(buffer, 2, buffer.length - 2))
            as SendAckPacket;
    expect(packet.clientSeq, 0x1020304);
    expect(packet.clientMsgNO, 'client-42');
  });

  test('rejects truncated and overlong declared packet bodies', () {
    for (final bytes in [
      <int>[],
      [0x40],
      [0x40, 0x80],
      [..._hex(_sendack)]..[1] += 1,
      [..._hex(_sendack)]..[1] -= 1,
      [..._hex(_sendack), 0x80],
      [0x80, 0x80],
      [0x90, 0x01, 0x01],
    ]) {
      expect(
        () => Proto().decode(Uint8List.fromList(bytes)),
        throwsFormatException,
        reason: HEX.encode(bytes),
      );
    }
  });

  test('rejects SENDACK body suffix beyond the declared string', () {
    final bytes = Uint8List.fromList([..._hex(_sendack), 0]);
    bytes[1] += 1;
    expect(() => Proto().decode(bytes), throwsFormatException);
  });

  test('remaining length supports fragmentation but rejects fifth byte', () {
    expect(ReadData(_hex('80')).readVariableLength(), -1);
    expect(ReadData(_hex('8001')).readVariableLength(), 128);
    expect(ReadData(_hex('8000')).readVariableLength(), 0);
    expect(ReadData(_hex('ffffff7f')).readVariableLength(), 0xfffffff);
    expect(
      () => ReadData(_hex('80808080')).readVariableLength(),
      throwsFormatException,
    );
  });

  test(
    'RECV formatting never exposes payload, verification key or identities',
    () {
      final packet = RecvPacket()
        ..payload = 'private-message-sentinel'
        ..msgKey = 'private-verification-key'
        ..fromUID = 'private-sender'
        ..channelID = 'private-conversation'
        ..clientMsgNO = 'private-client-number';
      expect(packet.toString(), isNot(contains('private-')));
      expect(packet.toString(), contains('messageSeq:'));
    },
  );

  test('UTF-8 string length cannot silently wrap uint16', () {
    final writer = WriteData();
    expect(() => writer.writeString('界' * 21846), throwsRangeError);
    expect(writer.toUint8List(), isEmpty);
  });

  test('integer writes reject overflow rather than changing wire identity', () {
    for (final write in [
      () => WriteData().writeUint8(256),
      () => WriteData().writeUint16(65536),
      () => WriteData().writeUint32(0x100000000),
      () => WriteData().writeUint32(-1),
      () => WriteData().writeUint64(BigInt.one << 64),
      () => WriteData().writeUint64(-BigInt.one),
    ]) {
      expect(write, throwsRangeError);
    }
    expect(
      ReadData(_hex('ffffffffffffffff')).readUint64(),
      BigInt.parse('18446744073709551615'),
    );
  });

  test(
    'uint64 sequence outside runtime int range is never silently clamped',
    () {
      final bytes = _hex(_sendack);
      bytes.fillRange(14, 22, 0xff);
      expect(() => Proto().decode(bytes), throwsFormatException);
    },
  );
}
