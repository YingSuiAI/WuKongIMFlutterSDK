import 'dart:async';

import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite/sqflite.dart';
// ignore: depend_on_referenced_packages
import 'package:sqflite_common/src/factory.dart';
import 'package:wukongimfluttersdk/db/message.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  const preferences = MethodChannel('plugins.flutter.io/shared_preferences');
  TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
      .setMockMethodCallHandler(preferences, (call) async {
    if (call.method == 'getAll') return <String, Object>{};
    return true;
  });

  test('initial sequence one asks the sync listener for remote history',
      () async {
    final database = _HistoryDatabase();
    databaseFactory = _HistoryDatabaseFactory(database);
    WKIM.shared.options.uid = 'history-regression';
    await WKDBHelper.shared.init();

    var syncRequests = 0;
    WKIM.shared.messageManager.addOnSyncChannelMsgListener((
      channelId,
      channelType,
      start,
      end,
      limit,
      pullMode,
      complete,
    ) {
      syncRequests++;
      complete(WKSyncChannelMsg()
        ..messages = [
          WKSyncMsg()
            ..messageSeq = 2
            ..channelID = channelId
            ..channelType = channelType,
        ]);
    });

    final result = Completer<List<WKMsg>>();
    MessageDB.shared.getOrSyncHistoryMessages(
      'peer',
      1,
      0,
      false,
      0,
      20,
      result.complete,
      () {},
    );
    final messages = await result.future;

    expect(syncRequests, 1);
    expect(messages.map((message) => message.messageSeq), [2, 1]);
    WKIM.shared.messageManager.addOnSyncChannelMsgListener(null);
    WKDBHelper.shared.close();
  });

  test('without a sync listener the initial page remains local-only', () async {
    final database = _HistoryDatabase();
    databaseFactory = _HistoryDatabaseFactory(database);
    WKIM.shared.options.uid = 'history-no-loader';
    await WKDBHelper.shared.init();
    WKIM.shared.messageManager.addOnSyncChannelMsgListener(null);

    final result = Completer<List<WKMsg>>();
    MessageDB.shared.getOrSyncHistoryMessages(
      'peer',
      1,
      0,
      false,
      0,
      20,
      result.complete,
      () {},
    );

    final messages = await result.future;
    expect(messages.map((message) => message.messageSeq), [1]);
    WKDBHelper.shared.close();
  });
}

class _HistoryDatabaseFactory extends SqfliteDatabaseFactory {
  _HistoryDatabaseFactory(this.database);

  final _HistoryDatabase database;

  @override
  Future<Database> openDatabase(String path,
          {OpenDatabaseOptions? options}) async =>
      database;

  @override
  Future<String> getDatabasesPath() async => '/tmp';

  @override
  Future<void> setDatabasesPath(String path) async {}

  @override
  Future<void> deleteDatabase(String path) async {}

  @override
  Future<bool> databaseExists(String path) async => true;

  @override
  Future<void> writeDatabaseBytes(String path, Uint8List bytes) async {}

  @override
  Future<Uint8List> readDatabaseBytes(String path) async => Uint8List(0);

  @override
  Future<T> wrapDatabaseException<T>(Future<T> Function() action) => action();

  @override
  noSuchMethod(Invocation invocation) => null;
}

class _HistoryDatabase implements Database {
  var _rawQueryCount = 0;

  @override
  String get path => ':memory:';

  @override
  bool get isOpen => true;

  @override
  Future<void> close() async {}

  @override
  Future<List<Map<String, Object?>>> rawQuery(String sql,
      [List<Object?>? arguments]) async {
    if (sql.trimLeft().toUpperCase().startsWith('PRAGMA ')) {
      return <Map<String, Object?>>[];
    }
    _rawQueryCount++;
    if (_rawQueryCount == 1) return [_messageRow(1)];
    return [_messageRow(1), _messageRow(2)];
  }

  @override
  Future<List<Map<String, Object?>>> query(String table,
          {bool? distinct,
          List<String>? columns,
          String? where,
          List<Object?>? whereArgs,
          String? groupBy,
          String? having,
          String? orderBy,
          int? limit,
          int? offset}) async =>
      <Map<String, Object?>>[];

  @override
  Future<int> insert(String table, Map<String, Object?> values,
          {String? nullColumnHack,
          ConflictAlgorithm? conflictAlgorithm}) async =>
      1;

  @override
  Future<void> execute(String sql, [List<Object?>? arguments]) async {}

  @override
  Future<T> transaction<T>(
    Future<T> Function(Transaction transaction) action, {
    bool? exclusive,
  }) => action(_HistoryTransaction(this));

  @override
  noSuchMethod(Invocation invocation) => null;
}

class _HistoryTransaction implements Transaction {
  _HistoryTransaction(this.database);

  @override
  final _HistoryDatabase database;

  @override
  Future<List<Map<String, Object?>>> query(String table,
          {bool? distinct,
          List<String>? columns,
          String? where,
          List<Object?>? whereArgs,
          String? groupBy,
          String? having,
          String? orderBy,
          int? limit,
          int? offset}) =>
      database.query(
        table,
        distinct: distinct,
        columns: columns,
        where: where,
        whereArgs: whereArgs,
        groupBy: groupBy,
        having: having,
        orderBy: orderBy,
        limit: limit,
        offset: offset,
      );

  @override
  Future<int> insert(String table, Map<String, Object?> values,
          {String? nullColumnHack,
          ConflictAlgorithm? conflictAlgorithm}) =>
      database.insert(
        table,
        values,
        nullColumnHack: nullColumnHack,
        conflictAlgorithm: conflictAlgorithm,
      );

  @override
  Future<void> execute(String sql, [List<Object?>? arguments]) =>
      database.execute(sql, arguments);

  @override
  Future<List<Map<String, Object?>>> rawQuery(String sql,
          [List<Object?>? arguments]) =>
      database.rawQuery(sql, arguments);

  @override
  noSuchMethod(Invocation invocation) => null;
}

Map<String, Object?> _messageRow(int sequence) => <String, Object?>{
      'client_seq': sequence,
      'message_id': 'message-$sequence',
      'message_seq': sequence,
      'channel_id': 'peer',
      'channel_type': 1,
      'timestamp': sequence,
      'topic_id': '',
      'from_uid': '',
      'type': 1,
      'content': '{}',
      'status': 1,
      'voice_status': 0,
      'created_at': '',
      'updated_at': '',
      'searchable_word': '',
      'client_msg_no': 'client-$sequence',
      'setting': 0,
      'order_seq': sequence * 1000,
      'extra': '',
      'is_deleted': 0,
      'flame': 0,
      'flame_second': 0,
      'viewed': 0,
      'viewed_at': 0,
      'expire_time': 0,
      'expire_timestamp': 0,
      'readed': 0,
      'readed_count': 0,
      'unread_count': 0,
      'revoke': 0,
      'revoker': '',
      'extra_version': 0,
      'is_mutual_deleted': 0,
      'need_upload': 0,
      'content_edit': '',
      'edited_at': 0,
    };
