import 'dart:collection';

import '../proto/packet.dart';

class WKEventGap {
  final EventPacket event;
  final String streamKey;
  final int expectedSequence;
  final int receivedSequence;

  const WKEventGap(
      this.event, this.streamKey, this.expectedSequence, this.receivedSequence);
}

class WKEventManager {
  WKEventManager._privateConstructor();

  static final WKEventManager shared = WKEventManager._privateConstructor();

  final HashMap<String, void Function(EventPacket)> _listeners = HashMap();
  final LinkedHashSet<String> _recentEventIDs = LinkedHashSet();
  final HashMap<String, int> _sequences = HashMap();
  final HashSet<String> _terminalStreams = HashSet();
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

  void recoverStream(String runID, String eventKey, int authoritySequence,
      {bool terminal = false}) {
    final streamKey = '$runID:$eventKey';
    _sequences[streamKey] = authoritySequence;
    if (terminal) {
      _terminalStreams.add(streamKey);
    } else {
      _terminalStreams.remove(streamKey);
    }
  }

  void reset() {
    _recentEventIDs.clear();
    _sequences.clear();
    _terminalStreams.clear();
  }

  void handle(EventPacket event) {
    if (_recentEventIDs.contains(event.eventID)) {
      return;
    }

    final envelope = event.decodeJsonData();
    final payload = envelope?['payload'];
    if (payload is Map<String, dynamic>) {
      final runID = payload['run_id'];
      final eventKey = payload['event_key'];
      final sequence = payload['authority_sequence'];
      final eventType = payload['event_type'];
      if (runID is String &&
          eventKey is String &&
          sequence is int &&
          sequence > 0) {
        final streamKey = '$runID:$eventKey';
        final lastSequence = _sequences[streamKey] ?? 0;
        if (_terminalStreams.contains(streamKey) || sequence <= lastSequence) {
          return;
        }
        if (lastSequence > 0 && sequence != lastSequence + 1) {
          _gapListener
              ?.call(WKEventGap(event, streamKey, lastSequence + 1, sequence));
          return;
        }
        _sequences[streamKey] = sequence;
        if (eventType == 'finish') {
          _terminalStreams.add(streamKey);
        }
      }
    }

    _recentEventIDs.add(event.eventID);
    if (_recentEventIDs.length > 4096) {
      _recentEventIDs.remove(_recentEventIDs.first);
    }

    for (final listener in List.of(_listeners.values)) {
      listener(event);
    }
  }
}
