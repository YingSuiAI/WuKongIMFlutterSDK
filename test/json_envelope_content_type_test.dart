import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:wukongimfluttersdk/db/const.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/type/const.dart';

void main() {
  test('request-scoped delivery uses the WKProto temporary channel type', () {
    expect(WKChannelType.temporary, 8);
  });

  test('project JSON envelope keeps raw content and gets a dedicated type', () {
    final payload = <String, dynamic>{
      'type': 'project.event',
      'event': 'message.created',
    };

    expect(
      WKDBConst.resolvePayloadContentType(payload),
      WkMessageContentType.projectJson,
    );

    final syncMessage = WKSyncMsg()..payload = payload;
    final message = syncMessage.getWKMsg();
    expect(message.content, jsonEncode(payload));
    expect(message.contentType, WkMessageContentType.projectJson);
  });

  test('raw project JSON payload keeps exact bytes during sync mapping', () {
    const rawPayload =
        '  {\n  "event":"message.created", "type":"project.event" }\n';
    expect(
      WKDBConst.resolvePayloadContentType(rawPayload),
      WkMessageContentType.projectJson,
    );
    final syncMessage = WKSyncMsg()..payload = rawPayload;

    final message = syncMessage.getWKMsg();

    expect(message.content, rawPayload);
    expect(message.contentType, WkMessageContentType.projectJson);
  });

  test('numeric and missing payload types retain legacy semantics', () {
    expect(WKDBConst.resolvePayloadContentType({'type': 2}), 2);
    expect(WKDBConst.resolvePayloadContentType({'type': '2'}), 2);
    expect(WKDBConst.resolvePayloadContentType({'type': '2.0'}), 0);
    expect(WKDBConst.resolvePayloadContentType({}), 0);
    expect(WKDBConst.resolvePayloadContentType({'type': ''}), 0);
    expect(WKDBConst.resolvePayloadContentType('{not-json'), 0);
    expect(WKDBConst.resolvePayloadContentType(<dynamic>[1, 2, 3]), 0);
  });

  test('malformed and non-map sync payloads do not throw', () {
    expect(
      () => (WKSyncMsg()..payload = '{not-json').getWKMsg(),
      returnsNormally,
    );
    expect(
      () => (WKSyncMsg()..payload = <dynamic>[1, 2, 3]).getWKMsg(),
      returnsNormally,
    );
  });

  test(
    'persisted project JSON type remains query-visible and content is raw',
    () {
      final rawContent = jsonEncode({
        'type': 'project.event',
        'event': 'message.created',
      });
      final message = WKDBConst.serializeWKMsg({
        'message_id': 'message-1',
        'content': rawContent,
        'type': WkMessageContentType.projectJson,
      });

      expect(message.contentType, WkMessageContentType.projectJson);
      expect(message.content, rawContent);
    },
  );
}
