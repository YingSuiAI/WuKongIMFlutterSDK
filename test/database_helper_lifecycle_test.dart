import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
// ignore: depend_on_referenced_packages
import 'package:sqflite_common/src/factory.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/db/wk_database_migrator.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();

  late Directory directory;

  setUp(() async {
    await WKDBHelper.shared.close();
    directory = await Directory.systemTemp.createTemp('wk_helper_');
  });

  tearDown(() async {
    await WKDBHelper.shared.close();
    await directory.delete(recursive: true);
  });

  test('publishes only a successful current initialization', () async {
    final factory = _ControlledDatabaseFactory(
      databaseFactoryFfi,
      directory.path,
      delayFirstOpen: true,
    );
    databaseFactory = factory;
    WKIM.shared.options.uid = 'lifecycle-close';

    final first = WKDBHelper.shared.init();
    final concurrent = WKDBHelper.shared.init();
    expect(identical(first, concurrent), isTrue);
    await factory.firstOpenStarted.future;
    expect(WKDBHelper.shared.getDB(), isNull);

    final close = WKDBHelper.shared.close();
    factory.allowFirstOpen();

    expect(await first, isFalse);
    await close;
    expect(WKDBHelper.shared.getDB(), isNull);

    expect(await WKDBHelper.shared.init(), isTrue);
    final published = WKDBHelper.shared.getDB();
    expect(published, isNotNull);
    expect(published!.isOpen, isTrue);
    expect(await WKDBHelper.shared.init(), isTrue);
    expect(identical(WKDBHelper.shared.getDB(), published), isTrue);
    expect(factory.openCount, 2);
  });

  test('closes a failed migration connection and permits retry', () async {
    final factory = _ControlledDatabaseFactory(
      databaseFactoryFfi,
      directory.path,
      corruptFirstDatabase: true,
    );
    databaseFactory = factory;
    WKIM.shared.options.uid = 'lifecycle-retry';

    await expectLater(WKDBHelper.shared.init(), throwsA(anything));

    expect(WKDBHelper.shared.getDB(), isNull);
    expect(factory.firstDatabase, isNotNull);
    expect(factory.firstDatabase!.isOpen, isFalse);
    expect(await WKDBHelper.shared.init(), isTrue);
    expect(WKDBHelper.shared.getDB()!.isOpen, isTrue);
    expect(factory.openCount, 2);
  });
}

class _ControlledDatabaseFactory extends SqfliteDatabaseFactory {
  _ControlledDatabaseFactory(
    this.delegate,
    this.databasePath, {
    this.delayFirstOpen = false,
    this.corruptFirstDatabase = false,
  });

  final DatabaseFactory delegate;
  final String databasePath;
  final bool delayFirstOpen;
  final bool corruptFirstDatabase;
  final Completer<void> firstOpenStarted = Completer<void>();
  final Completer<void> _firstOpenGate = Completer<void>();
  int openCount = 0;
  Database? firstDatabase;

  void allowFirstOpen() {
    if (!_firstOpenGate.isCompleted) _firstOpenGate.complete();
  }

  @override
  Future<Database> openDatabase(
    String path, {
    OpenDatabaseOptions? options,
  }) async {
    openCount++;
    if (openCount == 1) {
      if (!firstOpenStarted.isCompleted) firstOpenStarted.complete();
      if (delayFirstOpen) await _firstOpenGate.future;
    }
    if (corruptFirstDatabase && openCount == 2) {
      await delegate.deleteDatabase(path);
    }
    final database = await delegate.openDatabase(path, options: options);
    if (openCount == 1) {
      firstDatabase = database;
      if (corruptFirstDatabase) {
        await database.execute('''
CREATE TABLE ${WKDatabaseMigrator.migrationTable} (invalid TEXT)
''');
      }
    }
    return database;
  }

  @override
  Future<String> getDatabasesPath() async => databasePath;

  @override
  Future<void> setDatabasesPath(String path) => delegate.setDatabasesPath(path);

  @override
  Future<void> deleteDatabase(String path) => delegate.deleteDatabase(path);

  @override
  Future<bool> databaseExists(String path) => delegate.databaseExists(path);

  @override
  Future<void> writeDatabaseBytes(String path, Uint8List bytes) =>
      delegate.writeDatabaseBytes(path, bytes);

  @override
  Future<Uint8List> readDatabaseBytes(String path) =>
      delegate.readDatabaseBytes(path);

  @override
  Future<T> wrapDatabaseException<T>(Future<T> Function() action) => action();

  @override
  noSuchMethod(Invocation invocation) => null;
}
