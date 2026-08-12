import 'dart:convert';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:hex/hex.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/manager/connect_manager.dart';
import 'package:wukongimfluttersdk/manager/event_manager.dart';
import 'package:wukongimfluttersdk/proto/packet.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/proto/write_read.dart';
import 'package:wukongimfluttersdk/wkim.dart';

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

  test('encodes both installation and session generations in v6 CONNECT', () {
    final packet = ConnectPacket(
      version: 6,
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

    final encoded = Proto().encode(packet);
    final reader = ReadData(encoded);
    expect(reader.readUint8() >> 4, PacketType.connect.index);
    expect(reader.readVariableLength(), reader.remainingLength);
    expect(reader.readUint8(), 6);
    expect(reader.readUint8(), 1);
    expect(reader.readString(), 'install-1');
    expect(reader.readString(), 'u1');
    expect(reader.readString(), 'token-1');
    expect(reader.readUint64(), BigInt.from(1786521600000));
    expect(reader.readString(), 'client-key');
    expect(reader.readString(), 'app-1');
    expect(reader.readUint64(), BigInt.from(3));
    expect(reader.readUint64(), BigInt.from(7));

    expect(reader.remainingLength, 0);
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
      installationGeneration: 8,
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
    final event = _event(
      id: 'evt-connection',
      type: 'open',
      sequence: 1,
    );
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
    final event = _encodeEventFrame(
      _event(id: 'evt-unknown-followup', type: 'open', sequence: 1),
    );

    manager.testCutData(Uint8List.fromList([0xf0, 0x01, 0x2a, ...event]));

    expect(received, hasLength(1));
    manager.removeOnEventListener('unknown-frame-test');
  });

  test('socket parser waits for a split remaining-length header', () {
    final manager = WKConnectionManager.shared;
    WKEventManager.shared.reset();
    final received = <EventPacket>[];
    manager.addOnEventListener('split-header-test', received.add);
    final event = _event(id: 'evt-split', type: 'open', sequence: 1);
    final body = _eventBody(event);
    final encodedLength = _encodeVariableLength(body.length, padded: true);
    final frame = Uint8List.fromList([0xc0, ...encodedLength, ...body]);

    manager.testCutData(frame.sublist(0, 2));
    expect(received, isEmpty);
    manager.testCutData(frame.sublist(2));

    expect(received, hasLength(1));
    manager.removeOnEventListener('split-header-test');
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
    final event = _event(id: 'evt-type-mismatch', type: 'delta', sequence: 1)
      ..eventType = 'snapshot';

    manager.handle(event);

    expect(received, isEmpty);
    manager.removeListener('type-mismatch-test');
  });

  test('event manager rejects an empty event key', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('prefixed-key-test', received.add);
    final event = _event(
      id: 'evt-empty-key',
      type: 'delta',
      sequence: 1,
      eventKey: '',
    );

    manager.handle(event);

    expect(received, isEmpty);
    manager.removeListener('prefixed-key-test');
  });

  test('event manager resumes only after Platform snapshot covers a gap', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    final gaps = <WKEventGap>[];
    manager.addListener('snapshot-test', received.add);
    manager.setGapListener(gaps.add);
    final event = _event(
      id: 'snapshot-followup',
      type: 'snapshot',
      sequence: 6,
      runID: 'run-snapshot-test',
    );
    manager.restoreRunTransportWatermark(9001, 'run-snapshot-test', 3);

    manager.handle(event);

    expect(received, isEmpty);
    expect(gaps, hasLength(1));
    expect(
      manager.completeGapRecovery(
        gaps.single,
        missingEventAuthoritySequence: 40,
        snapshotAuthoritySequence: 39,
      ),
      isFalse,
    );
    expect(
      manager.completeGapRecovery(
        gaps.single,
        missingEventAuthoritySequence: 40,
        snapshotAuthoritySequence: 40,
      ),
      isTrue,
    );
    final next = _event(
      id: 'event-after-snapshot-recovery',
      type: 'delta',
      sequence: 7,
      runID: 'run-snapshot-test',
    );

    manager.handle(next);

    expect(received, [next]);
    manager.removeListener('snapshot-test');
    manager.setGapListener(null);
  });

  test('ordinary delta with the same gap is rejected', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    final gaps = <WKEventGap>[];
    manager.addListener('delta-gap-test', received.add);
    manager.setGapListener(gaps.add);
    manager.restoreRunTransportWatermark(9001, 'run-delta-gap-test', 3);

    manager.handle(
      _event(
        id: 'evt-delta-gap',
        type: 'delta',
        sequence: 6,
        runID: 'run-delta-gap-test',
      ),
    );

    expect(received, isEmpty);
    expect(gaps, hasLength(1));
    expect(gaps.single.expectedMsgEventSequence, 4);
    expect(gaps.single.receivedMsgEventSequence, 6);
    manager.removeListener('delta-gap-test');
    manager.setGapListener(null);
  });

  test('finish terminates every lane in the run', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('run-finish-test', received.add);
    final finish = _event(
      id: 'evt-finish',
      type: 'finish',
      sequence: 7,
    );
    manager.restoreRunTransportWatermark(9001, 'run-42', 6);
    manager.handle(finish);

    manager.handle(
      _event(
        id: 'evt-after-finish',
        type: 'delta',
        sequence: 8,
        eventKey: 'tool',
      ),
    );

    expect(received, [finish]);
    manager.removeListener('run-finish-test');
  });

  test('same run ID on another message has an independent watermark', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('anchor-isolation-test', received.add);
    final first = _event(id: 'evt-1', type: 'open', sequence: 1);
    final second = _event(
      id: 'evt-1',
      type: 'open',
      sequence: 1,
      messageID: 9002,
    );

    manager.handle(first);
    manager.handle(second);

    expect(received, [first, second]);
    manager.removeListener('anchor-isolation-test');
  });

  test('transport recovery never uses or stores Platform authority sequence',
      () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('recover-monotonic-test', received.add);
    expect(
      manager.restoreRunTransportWatermark(
        9001,
        'run-recover-monotonic',
        5,
      ),
      isTrue,
    );
    expect(
      manager.restoreRunTransportWatermark(
        9001,
        'run-recover-monotonic',
        3,
      ),
      isFalse,
    );
    manager.handle(
      _event(
        id: 'evt-stale-after-recovery',
        type: 'delta',
        sequence: 4,
        runID: 'run-recover-monotonic',
      ),
    );
    expect(
      manager.restoreRunTransportWatermark(
        9001,
        'run-recover-monotonic',
        6,
        terminal: true,
      ),
      isTrue,
    );
    manager.handle(
      _event(
        id: 'evt-after-terminal-recovery',
        type: 'delta',
        sequence: 7,
        runID: 'run-recover-monotonic',
      ),
    );

    expect(received, isEmpty);
    manager.removeListener('recover-monotonic-test');
  });
}

EventPacket _event({
  required String id,
  required String type,
  required int sequence,
  int messageID = 9001,
  String runID = 'run-42',
  String eventKey = 'main',
}) =>
    EventPacket()
      ..eventID = id
      ..eventType = type
      ..timestamp = 1786521600000
      ..data = utf8.encode(
        jsonEncode({
          'message_id': messageID,
          'run_id': runID,
          'event_type': type,
          'event_key': eventKey,
          'msg_event_seq': sequence,
          'payload': type == 'delta'
              ? {'text_delta': 'hello'}
              : {
                  'snapshot': {'state': 'running', 'text': 'hello'},
                },
        }),
      );

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
