import 'dart:collection';

import '../proto/packet.dart';

class WKEventGap {
  final EventPacket event;
  final String streamKey;
  final int expectedSequence;
  final int receivedSequence;

  const WKEventGap(
    this.event,
    this.streamKey,
    this.expectedSequence,
    this.receivedSequence,
  );
}

class _WKEventEnvelope {
  final int messageID;
  final String runID;
  final String eventType;
  final String eventKey;
  final int sequence;

  const _WKEventEnvelope(
    this.messageID,
    this.runID,
    this.eventType,
    this.eventKey,
    this.sequence,
  );

  String get watermarkKey => '$messageID:$runID';
}

class WKEventManager {
  WKEventManager._privateConstructor();

  static final WKEventManager shared = WKEventManager._privateConstructor();

  final HashMap<String, void Function(EventPacket)> _listeners = HashMap();
  final LinkedHashSet<String> _recentEventIDs = LinkedHashSet();
  final HashMap<String, int> _runSequences = HashMap();
  final HashSet<String> _terminalRuns = HashSet();
  void Function(WKEventGap)? _gapListener;

  void addListener(String key, void Function(EventPacket) listener) {
    _listeners[key] = listener;
  }

  void removeListener(String key) {
    _listeners.remove(key);
  }

  void setGapListener(void Function(WKEventGap)? listener) {
    _gapListener = listener;
  }

  void recoverRun(
    int messageID,
    String runID,
    int authoritySequence, {
    bool terminal = false,
  }) {
    final normalizedRunID = runID.trim();
    if (messageID <= 0 ||
        normalizedRunID.isEmpty ||
        normalizedRunID.contains(':') ||
        authoritySequence <= 0) {
      return;
    }
    final watermarkKey = '$messageID:$normalizedRunID';
    final previous = _runSequences[watermarkKey] ?? 0;
    if (authoritySequence < previous || _terminalRuns.contains(watermarkKey)) {
      return;
    }
    _runSequences[watermarkKey] = authoritySequence;
    if (terminal) _terminalRuns.add(watermarkKey);
  }

  void reset() {
    _recentEventIDs.clear();
    _runSequences.clear();
    _terminalRuns.clear();
  }

  void handle(EventPacket event) {
    final eventID = event.eventID.trim();
    final envelope = _decodeEnvelope(event);
    if (envelope == null ||
        !_supportedEventTypes.contains(envelope.eventType)) {
      return;
    }
    final watermarkKey = envelope.watermarkKey;
    final deduplicationKey = '$watermarkKey\u0000$eventID';
    if (eventID.isEmpty || _recentEventIDs.contains(deduplicationKey)) {
      return;
    }
    if (_terminalRuns.contains(watermarkKey)) return;
    final lastSequence = _runSequences[watermarkKey] ?? 0;
    if (envelope.sequence <= lastSequence) return;
    final isAuthoritativeSnapshot = envelope.eventType == 'snapshot';
    if (!isAuthoritativeSnapshot && envelope.sequence != lastSequence + 1) {
      _gapListener?.call(
        WKEventGap(event, watermarkKey, lastSequence + 1, envelope.sequence),
      );
      return;
    }
    _runSequences[watermarkKey] = envelope.sequence;
    if (envelope.eventType == 'finish') {
      _terminalRuns.add(watermarkKey);
    }

    _recentEventIDs.add(deduplicationKey);
    if (_recentEventIDs.length > 4096) {
      _recentEventIDs.remove(_recentEventIDs.first);
    }

    for (final listener in List.of(_listeners.values)) {
      listener(event);
    }
  }

  static _WKEventEnvelope? _decodeEnvelope(EventPacket event) {
    final data = event.decodeJsonData();
    if (data == null) return null;
    final messageID = data['message_id'];
    final runID = data['run_id'];
    final eventType = data['event_type'];
    final eventKey = data['event_key'];
    final sequence = data['msg_event_seq'];
    if (messageID is! int ||
        messageID <= 0 ||
        runID is! String ||
        runID.trim().isEmpty ||
        runID.contains(':') ||
        eventType is! String ||
        eventType != event.eventType ||
        eventKey is! String ||
        eventKey.trim().isEmpty ||
        eventKey.contains(':') ||
        sequence is! int ||
        sequence <= 0) {
      return null;
    }
    return _WKEventEnvelope(
      messageID,
      runID.trim(),
      eventType,
      eventKey.trim(),
      sequence,
    );
  }

  static const Set<String> _supportedEventTypes = {
    'open',
    'delta',
    'snapshot',
    'finish',
  };
}
