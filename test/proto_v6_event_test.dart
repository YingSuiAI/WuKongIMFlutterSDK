import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hex/hex.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/manager/connect_manager.dart';
import 'package:wukongimfluttersdk/manager/event_manager.dart';
import 'package:wukongimfluttersdk/proto/packet.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/wkim.dart';

const _goGoldenPath =
    '../pkg/worktrees/wukong-event-v6/pkg/protocol/codec/testdata/event_v6_reducer_golden.json';
const _goGoldenSha256 =
    'd3ea1635d7ed485be2468bc510bd3e2584a539c6d7e8930987637966c086b8b6';
const _goConnectGoldenPath =
    '../pkg/worktrees/wukong-event-v6/pkg/protocol/codec/testdata/connect_v6_golden.json';
const _goConnectGoldenSha256 =
    '7e91b9e5f4d592a280715629d8b3ef6445e3f2e11d624409b586cf278b53645d';

void main() {
  setUp(() {
    WKIM.shared.options = Options()..protoVersion = 6;
  });

  test('decodes the Go EVENT golden frame', () {
    final frame = Uint8List.fromList(
      HEX.decode('c01a000631323334353600047465737400000000499602d274657374'),
    );

    final packet = Proto().decode(frame) as EventPacket;

    expect(packet.eventID, '123456');
    expect(packet.eventType, 'test');
    expect(packet.timestamp, 1234567890);
    expect(packet.data, 'test'.codeUnits);
  });

  test('uses uint64 message sequence fields in protocol v6', () {
    const sequence = 0x100000001;
    final ack = RecvAckPacket()
      ..messageID = BigInt.one
      ..messageSeq = sequence;

    final encoded = Proto().encode(ack);

    expect(encoded.length, 18);
    expect(HEX.encode(encoded.sublist(10)), '0000000100000001');
  });

  test('encodes the canonical Go WKProto v6 CONNECT frame', () {
    final fixture = _jsonFixture('connect_v6_golden.json');
    final packet = ConnectPacket(
      version: fixture['version']! as int,
      deviceFlag: fixture['device_flag']! as int,
      deviceID: fixture['device_id']! as String,
      uid: fixture['uid']! as String,
      token: fixture['token']! as String,
      clientTimestamp: fixture['client_timestamp']! as int,
      clientKey: fixture['client_key']! as String,
      appInstanceID: fixture['app_instance_id']! as String,
      sessionGeneration: fixture['session_generation']! as int,
    );

    final encoded = Proto().encode(packet);

    expect(HEX.encode(encoded), fixture['frame_hex']);
  });

  test('pre-v6 CONNECT does not append v6 session identity fields', () {
    final packet = ConnectPacket(
      version: 5,
      deviceID: 'legacy-device',
      uid: 'legacy-user',
      token: 'legacy-token',
      clientTimestamp: 1,
      clientKey: 'legacy-key',
      appInstanceID: 'must-not-be-encoded',
      sessionGeneration: 9,
    );

    final encoded = Proto().encode(packet);
    final bodyLength = encoded.length - 2;
    const expectedBodyLength = 1 + 1 + 2 + 13 + 2 + 11 + 2 + 12 + 8 + 2 + 10;

    expect(bodyLength, expectedBodyLength);
  });

  test('decodes the optional SENDACK client message number suffix', () {
    final frame = Uint8List.fromList(
      HEX.decode(
        '40200000000000000001000000020000000100000003010009636c69656e742d3432',
      ),
    );

    final packet = Proto().decode(frame) as SendAckPacket;

    expect(packet.messageID, '1');
    expect(packet.clientSeq, 2);
    expect(packet.messageSeq, 0x100000003);
    expect(packet.reasonCode, 1);
    expect(packet.clientMsgNO, 'client-42');
  });

  test('decodes an unknown frame without throwing', () {
    final packet = Proto().decode(Uint8List.fromList([0xf0, 0x01, 0x2a]));

    expect(packet, isA<UnknownPacket>());
    expect(packet.header.packetType, PacketType.unknown);
    expect((packet as UnknownPacket).data, [0x2a]);
  });

  test('connection manager dispatches one copy of a repeated EVENT', () {
    final manager = WKConnectionManager.shared;
    WKEventManager.shared.reset();
    final received = <EventPacket>[];
    manager.addOnEventListener('proto-v6-test', received.add);
    final data = utf8.encode(jsonEncode(_goldenCase('open_main')['data']));
    final event = EventPacket()
      ..eventID = 'evt-connection'
      ..eventType = 'open'
      ..data = data;
    final frame = _encodeEventFrame(event);

    manager.testCutData(Uint8List.fromList([...frame, ...frame]));

    expect(received, hasLength(1));
    manager.removeOnEventListener('proto-v6-test');
  });

  test('socket parser skips a short unknown frame and continues', () {
    final manager = WKConnectionManager.shared;
    WKEventManager.shared.reset();
    final received = <EventPacket>[];
    manager.addOnEventListener('unknown-frame-test', received.add);
    final event = _encodeEventFrame(_eventFromCase(_goldenCase('open_main')));

    manager.testCutData(Uint8List.fromList([0xf0, 0x01, 0x2a, ...event]));

    expect(received, hasLength(1));
    manager.removeOnEventListener('unknown-frame-test');
  });

  test('socket parser waits for a split remaining-length header', () {
    final manager = WKConnectionManager.shared;
    WKEventManager.shared.reset();
    final received = <EventPacket>[];
    manager.addOnEventListener('split-header-test', received.add);
    final event = _eventFromCase(_goldenCase('open_main'));
    final body = _eventBody(event);
    final encodedLength = _encodeVariableLength(body.length, padded: true);
    final frame = Uint8List.fromList([0xc0, ...encodedLength, ...body]);

    manager.testCutData(frame.sublist(0, 2));
    expect(received, isEmpty);
    manager.testCutData(frame.sublist(2));

    expect(received, hasLength(1));
    manager.removeOnEventListener('split-header-test');
  });

  test(
    'shared reducer fixture is byte-identical to the canonical Go golden',
    () {
      final local = File(
        'test/testdata/event_v6_reducer_golden.json',
      ).readAsBytesSync();
      expect(sha256.convert(local).toString(), _goGoldenSha256);
      final canonical = File(_goGoldenPath);
      if (canonical.existsSync()) {
        expect(local, canonical.readAsBytesSync());
      }
    },
  );

  test('shared CONNECT fixture is byte-identical to the canonical Go golden',
      () {
    final local =
        File('test/testdata/connect_v6_golden.json').readAsBytesSync();
    expect(sha256.convert(local).toString(), _goConnectGoldenSha256);
    final canonical = File(_goConnectGoldenPath);
    if (canonical.existsSync()) expect(local, canonical.readAsBytesSync());
  });

  test('event manager follows every canonical reducer golden case', () {
    for (final rawCase in _goldenCases()) {
      final name = rawCase['name']! as String;
      final expected = rawCase['expect']! as Map<String, dynamic>;
      final data = rawCase['data']! as Map<String, dynamic>;
      final manager = WKEventManager.shared;
      manager.reset();
      final received = <EventPacket>[];
      final gaps = <WKEventGap>[];
      manager.addListener('golden-$name', received.add);
      manager.setGapListener(gaps.add);
      final runID = data['run_id']! as String;
      final previous = rawCase['previous_sequence'] as int? ?? 0;
      final messageID = data['message_id']! as int;
      if (previous > 0) {
        manager.recoverRun(
          messageID,
          runID,
          previous,
          terminal: expected['run_terminal'] == true &&
              expected['action'] == 'ignore_terminal',
        );
      }
      final event = _eventFromCase(rawCase);

      manager.handle(event);

      switch (expected['action']) {
        case 'apply':
        case 'apply_snapshot':
          expect(received, [event], reason: name);
          expect(gaps, isEmpty, reason: name);
          break;
        case 'snapshot_recovery':
          expect(received, isEmpty, reason: name);
          expect(gaps, hasLength(1), reason: name);
          expect(gaps.single.streamKey, expected['watermark_key'],
              reason: name);
          expect(gaps.single.expectedSequence, previous + 1, reason: name);
          expect(
            gaps.single.receivedSequence,
            expected['sequence'],
            reason: name,
          );
          break;
        case 'ignore_duplicate':
        case 'ignore_terminal':
        case 'ignore_unknown':
          expect(received, isEmpty, reason: name);
          expect(gaps, isEmpty, reason: name);
          break;
        default:
          fail('Unsupported golden action ${expected['action']}');
      }
      manager.removeListener('golden-$name');
      manager.setGapListener(null);
    }
  });

  test('message event sequence stays continuous across interleaved lanes', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    final gaps = <WKEventGap>[];
    manager.addListener('interleaved-lanes-test', received.add);
    manager.setGapListener(gaps.add);

    EventPacket event(String id, String key, int sequence) => EventPacket()
      ..eventID = id
      ..eventType = 'delta'
      ..data = utf8.encode(
        jsonEncode({
          'message_id': 9001,
          'run_id': 'run-interleaved',
          'event_type': 'delta',
          'event_key': key,
          'msg_event_seq': sequence,
        }),
      );

    manager.handle(event('evt-answer-1', 'answer', 1));
    manager.handle(event('evt-tool-2', 'tool', 2));
    manager.handle(event('evt-answer-3', 'answer', 3));

    expect(received, hasLength(3));
    expect(gaps, isEmpty);
    manager.removeListener('interleaved-lanes-test');
    manager.setGapListener(null);
  });

  test('event manager reads metadata only from the top-level envelope', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('payload-metadata-test', received.add);
    final event = EventPacket()
      ..eventID = 'evt-nested-only'
      ..eventType = 'delta'
      ..data = utf8.encode(
        jsonEncode({
          'payload': {
            'run_id': 'run-nested',
            'event_type': 'delta',
            'event_key': 'main',
            'msg_event_seq': 1,
          },
        }),
      );

    manager.handle(event);

    expect(received, isEmpty);
    manager.removeListener('payload-metadata-test');
  });

  test('event manager requires frame type to equal envelope event_type', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('type-mismatch-test', received.add);
    final rawCase = Map<String, dynamic>.from(_goldenCase('delta_main'));
    final event = _eventFromCase(rawCase)..eventType = 'snapshot';

    manager.handle(event);

    expect(received, isEmpty);
    manager.removeListener('type-mismatch-test');
  });

  test('event manager rejects event keys that repeat the run prefix', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('prefixed-key-test', received.add);
    final rawCase = Map<String, dynamic>.from(_goldenCase('delta_main'));
    final data = Map<String, dynamic>.from(rawCase['data']! as Map);
    data['event_key'] = 'run-42:main';
    rawCase['data'] = data;
    final event = _eventFromCase(rawCase);

    manager.handle(event);

    expect(received, isEmpty);
    manager.removeListener('prefixed-key-test');
  });

  test('event manager resumes after applying an authoritative snapshot', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('snapshot-test', received.add);
    final rawCase = Map<String, dynamic>.from(_goldenCase('snapshot_recovery'));
    final data = Map<String, dynamic>.from(rawCase['data']! as Map);
    data['run_id'] = 'run-snapshot-test';
    data['msg_event_seq'] = 6;
    rawCase['event_id'] = 'snapshot-followup';
    rawCase['data'] = data;
    final event = _eventFromCase(rawCase);
    manager.recoverRun(9001, 'run-snapshot-test', 3);

    manager.handle(event);

    expect(received, [event]);
    manager.removeListener('snapshot-test');
  });

  test('ordinary delta with the same gap is rejected', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    final gaps = <WKEventGap>[];
    manager.addListener('delta-gap-test', received.add);
    manager.setGapListener(gaps.add);
    final rawCase = Map<String, dynamic>.from(_goldenCase('gap'));
    final data = Map<String, dynamic>.from(rawCase['data']! as Map);
    data['run_id'] = 'run-delta-gap-test';
    rawCase['data'] = data;
    manager.recoverRun(9001, 'run-delta-gap-test', 3);

    manager.handle(_eventFromCase(rawCase));

    expect(received, isEmpty);
    expect(gaps, hasLength(1));
    expect(gaps.single.expectedSequence, 4);
    expect(gaps.single.receivedSequence, 6);
    manager.removeListener('delta-gap-test');
    manager.setGapListener(null);
  });

  test('finish terminates every lane in the run', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('run-finish-test', received.add);
    final finish = _eventFromCase(_goldenCase('finish_on_main'));
    manager.recoverRun(9001, 'run-42', 6);
    manager.handle(finish);
    final rawCase = Map<String, dynamic>.from(
      _goldenCase('delta_tool_interleaved'),
    );
    final data = Map<String, dynamic>.from(rawCase['data']! as Map);
    data['event_key'] = 'tool';
    data['msg_event_seq'] = 8;
    rawCase['event_id'] = 'evt-after-finish';
    rawCase['data'] = data;

    manager.handle(_eventFromCase(rawCase));

    expect(received, [finish]);
    manager.removeListener('run-finish-test');
  });

  test('same run ID on another message has an independent watermark', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('anchor-isolation-test', received.add);
    final first = _eventFromCase(_goldenCase('open_main'));
    final secondCase = Map<String, dynamic>.from(_goldenCase('open_main'));
    final secondData = Map<String, dynamic>.from(secondCase['data']! as Map);
    secondData['message_id'] = 9002;
    secondCase['event_id'] = 'evt-1';
    secondCase['data'] = secondData;
    final second = _eventFromCase(secondCase);

    manager.handle(first);
    manager.handle(second);

    expect(received, [first, second]);
    manager.removeListener('anchor-isolation-test');
  });

  test('recoverRun never moves a terminal or advanced watermark backwards', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('recover-monotonic-test', received.add);
    manager.recoverRun(9001, 'run-recover-monotonic', 5);
    manager.recoverRun(9001, 'run-recover-monotonic', 3);
    final deltaCase = Map<String, dynamic>.from(_goldenCase('delta_main'));
    final deltaData = Map<String, dynamic>.from(deltaCase['data']! as Map)
      ..['run_id'] = 'run-recover-monotonic'
      ..['msg_event_seq'] = 4;
    deltaCase['event_id'] = 'evt-stale-after-recovery';
    deltaCase['data'] = deltaData;
    manager.handle(_eventFromCase(deltaCase));
    manager.recoverRun(9001, 'run-recover-monotonic', 6, terminal: true);
    final lateCase = Map<String, dynamic>.from(_goldenCase('delta_main'));
    final lateData = Map<String, dynamic>.from(lateCase['data']! as Map)
      ..['run_id'] = 'run-recover-monotonic'
      ..['msg_event_seq'] = 7;
    lateCase['event_id'] = 'evt-after-terminal-recovery';
    lateCase['data'] = lateData;
    manager.handle(_eventFromCase(lateCase));

    expect(received, isEmpty);
    manager.removeListener('recover-monotonic-test');
  });
}

EventPacket _eventFromCase(Map<String, dynamic> rawCase) => EventPacket()
  ..eventID = rawCase['event_id']! as String
  ..eventType = rawCase['frame_type']! as String
  ..timestamp = rawCase['timestamp']! as int
  ..data = utf8.encode(jsonEncode(rawCase['data']));

Map<String, dynamic> _goldenCase(String name) =>
    _goldenCases().singleWhere((rawCase) => rawCase['name'] == name);

List<Map<String, dynamic>> _goldenCases() {
  final fixture = _jsonFixture('event_v6_reducer_golden.json');
  return (fixture['cases']! as List<dynamic>).cast<Map<String, dynamic>>();
}

Map<String, dynamic> _jsonFixture(String name) => jsonDecode(
      File('test/testdata/$name').readAsStringSync(),
    ) as Map<String, dynamic>;

Uint8List _encodeEventFrame(EventPacket event) {
  final body = _eventBody(event);
  return Uint8List.fromList([
    0xc0,
    ..._encodeVariableLength(body.length),
    ...body,
  ]);
}

List<int> _eventBody(EventPacket event) {
  final eventID = utf8.encode(event.eventID);
  final eventType = utf8.encode(event.eventType);
  final timestamp = BigInt.from(event.timestamp);
  final bytes = <int>[
    eventID.length >> 8,
    eventID.length & 0xff,
    ...eventID,
    eventType.length >> 8,
    eventType.length & 0xff,
    ...eventType,
  ];
  for (var shift = 56; shift >= 0; shift -= 8) {
    bytes.add(((timestamp >> shift) & BigInt.from(0xff)).toInt());
  }
  return [...bytes, ...event.data];
}

List<int> _encodeVariableLength(int value, {bool padded = false}) {
  final bytes = <int>[];
  do {
    var digit = value % 0x80;
    value ~/= 0x80;
    if (value > 0 || (padded && bytes.isEmpty)) digit |= 0x80;
    bytes.add(digit);
  } while (value > 0);
  if (padded && bytes.length == 1) bytes.add(0);
  return bytes;
}
