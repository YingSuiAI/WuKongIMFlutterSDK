import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/db/message.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();
  late Directory directory;

  setUp(() async {
    await WKDBHelper.shared.close();
    directory = await Directory.systemTemp.createTemp('wk_history_');
    databaseFactory = databaseFactoryFfi;
    await databaseFactory.setDatabasesPath(directory.path);
    WKIM.shared.options = Options()..uid = 'history-owner';
    await WKDBHelper.shared.init();
  });

  tearDown(() async {
    WKIM.shared.messageManager.addOnSyncChannelMsgListener(null);
    await WKDBHelper.shared.close();
    await directory.delete(recursive: true);
  });

  WKMsg history(String clientNo, String content) => WKMsg()
    ..clientMsgNO = clientNo
    ..channelID = 'peer'
    ..channelType = 1
    ..fromUID = 'history-owner'
    ..messageID = '9001'
    ..messageSeq = 9
    ..orderSeq = 9000
    ..payloadCommitted = true
    ..contentType = 1
    ..content = content;

  test(
    'single history insert completes durably or returns its error',
    () async {
      final first = history('single-history', '{"type":1,"content":"server"}');
      expect(await MessageDB.shared.insertMsgList([first]), isTrue);
      expect(
        (await WKDBHelper.shared.getDB()!.query('message')).single['content'],
        first.content,
      );

      await WKDBHelper.shared.close();
      await expectLater(
        MessageDB.shared.insertMsgList([history('closed', '{}')]),
        throwsA(isA<StateError>()),
      );
    },
  );

  test(
    'history merges a pending local send under its stable identity',
    () async {
      const clientNo = 'sender-request';
      final local = WKMsg()
        ..clientMsgNO = clientNo
        ..channelID = 'peer'
        ..channelType = 1
        ..fromUID = 'history-owner'
        ..messageID = '9001'
        ..contentType = 1
        ..content = '{"type":1,"content":"request"}'
        ..originalPayloadSHA256 = 'sender-intent'
        ..viewed = 1
        ..isDeleted = 1;
      final clientSeq = await MessageDB.shared.insert(local);
      final canonical = history(clientNo, '{"type":1,"content":"canonical"}');

      expect(await MessageDB.shared.insertMsgList([canonical]), isTrue);
      final rows = await WKDBHelper.shared.getDB()!.query('message');
      expect(rows, hasLength(1));
      expect(rows.single['client_seq'], clientSeq);
      expect(rows.single['client_msg_no'], clientNo);
      expect(rows.single['content'], canonical.content);
      expect(rows.single['payload_committed'], 1);
      expect(rows.single['original_payload_sha256'], 'sender-intent');
      expect(rows.single['is_deleted'], 1);
      expect(rows.single['viewed'], 1);
      expect(canonical.clientMsgNO, clientNo);
      expect(canonical.isDeleted, 0);
    },
  );

  test(
    'bulk history applies canonical body and keeps other messages',
    () async {
      final local = WKMsg()
        ..clientMsgNO = 'pending'
        ..channelID = 'peer'
        ..channelType = 1
        ..fromUID = 'history-owner'
        ..content = 'request';
      final clientSeq = await MessageDB.shared.insert(local);
      final canonical = history('pending', 'canonical');
      final next = history('next', 'later')
        ..messageID = '9002'
        ..messageSeq = 10;
      expect(await MessageDB.shared.insertMsgList([canonical, next]), isTrue);
      final rows = await WKDBHelper.shared.getDB()!.query(
        'message',
        orderBy: 'message_seq',
      );
      expect(rows, hasLength(2));
      expect(rows.first['client_seq'], clientSeq);
      expect(rows.first['content'], 'canonical');
      expect(rows.first['client_msg_no'], 'pending');
      expect(rows.last['client_msg_no'], 'next');
    },
  );

  test('conflicting history identity does not hide either message', () async {
    final local = WKMsg()
      ..clientMsgNO = 'shared-key'
      ..channelID = 'other-peer'
      ..channelType = 1
      ..fromUID = 'history-owner'
      ..content = 'original';
    await MessageDB.shared.insert(local);
    final canonical = history('shared-key', 'canonical');
    await expectLater(
      MessageDB.shared.insertMsgList([canonical]),
      throwsA(isA<StateError>()),
    );
    final rows = await WKDBHelper.shared.getDB()!.query('message');
    expect(rows, hasLength(1));
    expect(rows.single['content'], 'original');
    expect(rows.single['is_deleted'], 0);
    expect(canonical.clientMsgNO, 'shared-key');
  });

  test('a conflicted history page rolls back earlier inserts', () async {
    final local = WKMsg()
      ..clientMsgNO = 'shared-key'
      ..channelID = 'other-peer'
      ..channelType = 1
      ..fromUID = 'history-owner';
    await MessageDB.shared.insert(local);
    final first = history('new-key', 'first');
    final conflict = history('shared-key', 'conflict');
    await expectLater(
      MessageDB.shared.insertMsgList([first, conflict]),
      throwsA(isA<StateError>()),
    );
    final rows = await WKDBHelper.shared.getDB()!.query('message');
    expect(rows, hasLength(1));
    expect(rows.single['client_msg_no'], 'shared-key');
    expect(first.clientMsgNO, 'new-key');
  });

  test('replayed identical history remains one committed row', () async {
    final first = history('replayed-key', 'same');
    await MessageDB.shared.insertMsgList([first]);
    final firstRows = await WKDBHelper.shared.getDB()!.query('message');
    final replay = history('replayed-key', 'same');
    await MessageDB.shared.insertMsgList([replay]);
    final rows = await WKDBHelper.shared.getDB()!.query('message');
    expect(rows, hasLength(1));
    expect(rows.single['client_seq'], firstRows.single['client_seq']);
    expect(rows.single['is_deleted'], 0);
  });

  test('project envelope history replaces the local send body', () async {
    final local = WKMsg()
      ..clientMsgNO = 'project-key'
      ..channelID = 'peer'
      ..channelType = 1
      ..fromUID = 'history-owner'
      ..content = '{"type":"human.message","content":"request"}';
    await MessageDB.shared.insert(local);
    final nativeHistory = WKSyncMsg()
      ..clientMsgNO = 'project-key'
      ..channelID = 'peer'
      ..channelType = 1
      ..fromUID = 'history-owner'
      ..messageID = '9010'
      ..messageSeq = 10
      ..payload = {'type': 'human.message', 'content': 'canonical'};
    await MessageDB.shared.insertMsgList([nativeHistory.getWKMsg()]);
    final rows = await WKDBHelper.shared.getDB()!.query('message');
    expect(rows, hasLength(1));
    expect(rows.single['client_msg_no'], 'project-key');
    expect(
      rows.single['content'],
      '{"type":"human.message","content":"canonical"}',
    );
    expect(rows.single['payload_committed'], 1);
  });

  test('sync callback only succeeds after persistence succeeds', () async {
    await WKDBHelper.shared.close();
    WKIM.shared.messageManager.addOnSyncChannelMsgListener((
      channelID,
      channelType,
      start,
      end,
      limit,
      pullMode,
      back,
    ) {
      back(
        WKSyncChannelMsg()
          ..messages = [
            WKSyncMsg()
              ..clientMsgNO = 'cannot-persist'
              ..channelID = channelID
              ..channelType = channelType
              ..messageID = '9003'
              ..payload = '{"type":1,"content":"remote"}',
          ],
      );
    });
    final completed = Completer<WKSyncChannelMsg?>();
    WKIM.shared.messageManager.setSyncChannelMsgListener(
      'peer',
      1,
      0,
      0,
      20,
      1,
      completed.complete,
    );
    expect(await completed.future, isNull);
  });

  test(
    'public manager history API forwards a failed write without a page',
    () async {
      final listenerReady = Completer<void>();
      late void Function(WKSyncChannelMsg?) deliver;
      WKIM.shared.messageManager.addOnSyncChannelMsgListener((
        channelID,
        channelType,
        start,
        end,
        limit,
        pullMode,
        back,
      ) {
        deliver = back;
        listenerReady.complete();
      });
      final failed = Completer<Object>();
      var returnedPage = false;
      final invocation = WKIM.shared.messageManager.getOrSyncHistoryMessages(
        'peer',
        1,
        0,
        false,
        0,
        20,
        0,
        (_) => returnedPage = true,
        () {},
        onError: (error, _) => failed.complete(error),
      );
      await listenerReady.future;
      await WKDBHelper.shared.close();
      deliver(
        WKSyncChannelMsg()
          ..messages = [
            WKSyncMsg()
              ..clientMsgNO = 'write-fails'
              ..channelID = 'peer'
              ..channelType = 1
              ..messageID = '9004'
              ..payload = '{"type":1,"content":"remote"}',
          ],
      );
      expect(await failed.future, isA<StateError>());
      expect(returnedPage, isFalse);
      await invocation;
    },
  );
}
