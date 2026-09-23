import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/entity/conversation.dart';
import 'package:wukongimfluttersdk/manager/conversation_manager.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();
  late Directory directory;
  late Options previousOptions;
  final conversations = WKConversationManager.shared;

  setUp(() async {
    previousOptions = WKIM.shared.options;
    await WKDBHelper.shared.close();
    directory = await Directory.systemTemp.createTemp('wk_conversation_sync_');
    databaseFactory = databaseFactoryFfi;
    await databaseFactory.setDatabasesPath(directory.path);
    WKIM.shared.options = Options()..uid = 'sync-owner';
    await WKDBHelper.shared.init();
  });

  tearDown(() async {
    WKIM.shared.options = previousOptions;
    await WKDBHelper.shared.close();
    await directory.delete(recursive: true);
  });

  test(
    'without a sync provider, connection sync completes immediately',
    () async {
      var completed = 0;
      await conversations.setSyncConversation(() => completed++);
      expect(completed, 1);
    },
  );

  test('sync completes only after its conversation row is durable', () async {
    late Function(WKSyncConversation) deliver;
    conversations.addOnSyncConversationListener((_, __, ___, callback) {
      deliver = callback;
    });
    var completed = 0;
    await conversations.setSyncConversation(() => completed++);

    final entered = Completer<void>();
    final release = Completer<void>();
    final locked = WKDBHelper.shared.getDB()!.transaction((transaction) async {
      entered.complete();
      await release.future;
    });
    await entered.future;

    final row = WKSyncConvMsg()
      ..channelID = 'room'
      ..channelType = 2
      ..lastMsgSeq = 7
      ..version = 2;
    try {
      deliver(WKSyncConversation()..conversations = [row]);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(completed, 0);
    } finally {
      release.complete();
    }
    await locked;
    final deadline = DateTime.now().add(const Duration(seconds: 2));
    while (completed == 0 && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    expect(completed, 1);
    final rows = await WKDBHelper.shared.getDB()!.query(
      'conversation',
      where: 'channel_id = ?',
      whereArgs: ['room'],
    );
    expect(rows.single['last_msg_seq'], 7);
  });

  test('a response from a retired connection cannot publish sync', () async {
    late Function(WKSyncConversation) deliver;
    conversations.addOnSyncConversationListener((_, __, ___, callback) {
      deliver = callback;
    });
    var current = true;
    var completed = 0;
    await conversations.setSyncConversation(
      () => completed++,
      isCurrent: () => current,
    );
    current = false;
    deliver(
      WKSyncConversation()
        ..conversations = [
          WKSyncConvMsg()
            ..channelID = 'retired'
            ..channelType = 2,
        ],
    );
    await Future<void>.delayed(Duration.zero);
    expect(completed, 0);
    expect(await WKDBHelper.shared.getDB()!.query('conversation'), isEmpty);
  });
}
