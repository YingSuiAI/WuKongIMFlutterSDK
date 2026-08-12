import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:hex/hex.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/manager/connect_manager.dart';
import 'package:wukongimfluttersdk/manager/event_manager.dart';
import 'package:wukongimfluttersdk/proto/packet.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  setUp(() {
    WKIM.shared.options = Options()..protoVersion = 6;
  });

  test('decodes the Go EVENT golden frame', () {
    final frame = Uint8List.fromList(
        HEX.decode('c01a000631323334353600047465737400000000499602d274657374'));

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

  test('decodes the optional SENDACK client message number suffix', () {
    final frame = Uint8List.fromList(HEX.decode(
        '40200000000000000001000000020000000100000003010009636c69656e742d3432'));

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
    final frame = Uint8List.fromList(
        HEX.decode('c01a000631323334353600047465737400000000499602d274657374'));

    manager.testCutData(Uint8List.fromList([...frame, ...frame]));

    expect(received, hasLength(1));
    manager.removeOnEventListener('proto-v6-test');
  });

  test('socket parser skips a short unknown frame and continues', () {
    final manager = WKConnectionManager.shared;
    WKEventManager.shared.reset();
    final received = <EventPacket>[];
    manager.addOnEventListener('unknown-frame-test', received.add);
    final event =
        HEX.decode('c01a000631323334353600047465737400000000499602d274657374');

    manager.testCutData(Uint8List.fromList([0xf0, 0x01, 0x2a, ...event]));

    expect(received, hasLength(1));
    manager.removeOnEventListener('unknown-frame-test');
  });

  test('socket parser waits for a split remaining-length header', () {
    final manager = WKConnectionManager.shared;
    WKEventManager.shared.reset();
    final received = <EventPacket>[];
    manager.addOnEventListener('split-header-test', received.add);
    final frame = Uint8List.fromList([
      0xc0,
      0x80 | 26,
      0x00,
      ...HEX.decode('000631323334353600047465737400000000499602d274657374'),
    ]);

    manager.testCutData(frame.sublist(0, 2));
    expect(received, isEmpty);
    manager.testCutData(frame.sublist(2));

    expect(received, hasLength(1));
    manager.removeOnEventListener('split-header-test');
  });

  test('event manager reports a sequence gap and ignores late terminal data',
      () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    final gaps = <WKEventGap>[];
    manager.addListener('reducer-test', received.add);
    manager.setGapListener(gaps.add);

    EventPacket event(String id, int sequence, String type) => EventPacket()
      ..eventID = id
      ..eventType = 'agent.run.event'
      ..data =
          '''{"payload":{"run_id":"run-reducer-test","event_key":"main","event_type":"$type","authority_sequence":$sequence}}'''
              .codeUnits;

    manager.handle(event('event-1', 1, 'delta'));
    manager.handle(event('event-3', 3, 'delta'));
    manager.handle(event('event-2', 2, 'finish'));
    manager.handle(event('event-4', 3, 'delta'));

    expect(received, hasLength(2));
    expect(gaps, hasLength(1));
    expect(gaps.single.expectedSequence, 2);
    expect(gaps.single.receivedSequence, 3);
    expect(gaps.single.event.eventID, 'event-3');
    manager.removeListener('reducer-test');
    manager.setGapListener(null);
  });

  test('event manager resumes after applying a compact snapshot', () {
    final manager = WKEventManager.shared;
    manager.reset();
    final received = <EventPacket>[];
    manager.addListener('snapshot-test', received.add);

    final event = EventPacket()
      ..eventID = 'snapshot-followup'
      ..eventType = 'agent.run.event'
      ..data =
          '''{"payload":{"run_id":"run-snapshot-test","event_key":"main","event_type":"delta","authority_sequence":4}}'''
              .codeUnits;
    manager.recoverStream('run-snapshot-test', 'main', 3);
    manager.handle(event);

    expect(received, [event]);
    manager.removeListener('snapshot-test');
  });
}
