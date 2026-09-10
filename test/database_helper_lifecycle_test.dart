import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;
// ignore: depend_on_referenced_packages
import 'package:sqflite_common/src/factory.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:wukongimfluttersdk/db/wk_database_migrator.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();

  late Directory directory;
  DatabaseFactory? originalFactory;
  String? originalUid;

  setUp(() async {
    await WKDBHelper.shared.close();
    originalFactory = databaseFactoryOrNull;
    originalUid = WKIM.shared.options.uid;
    SharedPreferences.setMockInitialValues({});
    directory = await Directory.systemTemp.createTemp('wk_helper_');
  });

  tearDown(() async {
    await WKDBHelper.shared.close();
    databaseFactoryOrNull = originalFactory;
    WKIM.shared.options.uid = originalUid;
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
      failFirstMigration: true,
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
    expect(factory.deleteCount, 0);
  });

  test(
    'rejects an untracked schema without adopting preferences or deleting data',
    () async {
      const uid = 'legacy-watermark';
      const latestVersion = 202604271625;
      final factory = _ControlledDatabaseFactory(
        databaseFactoryFfi,
        directory.path,
      );
      databaseFactory = factory;
      final database = await databaseFactoryFfi.openDatabase(
        p.join(directory.path, 'wk_$uid.db'),
      );
      await WKDatabaseMigrator().migrate(database, await _assetMigrations());
      await database.execute('DROP TABLE ${WKDatabaseMigrator.migrationTable}');
      await database.execute('CREATE TABLE retained (value TEXT)');
      await database.insert('retained', {'value': 'keep'});
      await database.close();
      SharedPreferences.setMockInitialValues({
        'wk_max_sql_version_$uid': latestVersion,
      });
      WKIM.shared.options.uid = uid;

      await expectLater(WKDBHelper.shared.init(), throwsStateError);
      expect(WKDBHelper.shared.getDB(), isNull);
      expect(factory.firstDatabase!.isOpen, isFalse);
      expect(factory.deleteCount, 0);
      final retained = await databaseFactoryFfi.openDatabase(
        p.join(directory.path, 'wk_$uid.db'),
      );
      try {
        expect(await retained.query('retained'), [
          {'value': 'keep'},
        ]);
        expect(
          await retained.rawQuery(
            'SELECT name FROM sqlite_master WHERE name = ?',
            [WKDatabaseMigrator.migrationTable],
          ),
          isEmpty,
        );
      } finally {
        await retained.close();
      }
    },
  );

  test('ignores a preferences watermark when creating a database', () async {
    const uid = 'restored-preferences-only';
    final factory = _ControlledDatabaseFactory(
      databaseFactoryFfi,
      directory.path,
    );
    databaseFactory = factory;
    SharedPreferences.setMockInitialValues({
      'wk_max_sql_version_$uid': 202604271625,
    });
    WKIM.shared.options.uid = uid;

    expect(await WKDBHelper.shared.init(), isTrue);

    final opened = WKDBHelper.shared.getDB()!;
    final tables = await opened.query(
      'sqlite_master',
      columns: const ['name'],
      where: 'type = ? AND name = ?',
      whereArgs: const ['table', 'message'],
    );
    expect(tables, isNotEmpty);
  });

  test('reopens a current database from its ledger and retains data', () async {
    final factory = _ControlledDatabaseFactory(
      databaseFactoryFfi,
      directory.path,
    );
    databaseFactory = factory;
    WKIM.shared.options.uid = 'current-ledger';
    expect(await WKDBHelper.shared.init(), isTrue);
    final first = WKDBHelper.shared.getDB()!;
    await first.execute('CREATE TABLE retained (value TEXT)');
    await first.insert('retained', {'value': 'keep'});
    await WKDBHelper.shared.close();
    expect(first.isOpen, isFalse);
    expect(await WKDBHelper.shared.init(), isTrue);
    expect(await WKDBHelper.shared.getDB()!.query('retained'), [
      {'value': 'keep'},
    ]);
    expect(factory.deleteCount, 0);
  });

  test(
    'queued initialization does not report another account as ready',
    () async {
      final factory = _ControlledDatabaseFactory(
        databaseFactoryFfi,
        directory.path,
        delayFirstOpen: true,
      );
      databaseFactory = factory;
      WKIM.shared.options.uid = 'account-a';
      final first = WKDBHelper.shared.init();
      await factory.firstOpenStarted.future;
      WKIM.shared.options.uid = 'account-b';
      final superseded = WKDBHelper.shared.init();
      WKIM.shared.options.uid = 'account-c';
      final latest = WKDBHelper.shared.init();
      factory.allowFirstOpen();
      expect(await first, isFalse);
      expect(await superseded, isFalse);
      expect(await latest, isTrue);
      expect(WKDBHelper.shared.getDB()!.path, endsWith('wk_account-c.db'));
      expect(factory.openCount, 2);
    },
  );

  test(
    'close cancels queued initialization without reopening a database',
    () async {
      final factory = _ControlledDatabaseFactory(
        databaseFactoryFfi,
        directory.path,
        delayFirstOpen: true,
      );
      databaseFactory = factory;
      WKIM.shared.options.uid = 'account-a';
      final first = WKDBHelper.shared.init();
      await factory.firstOpenStarted.future;
      WKIM.shared.options.uid = 'account-b';
      final queued = WKDBHelper.shared.init();
      final close = WKDBHelper.shared.close();
      factory.allowFirstOpen();
      expect(await first, isFalse);
      expect(await queued, isFalse);
      await close;
      expect(WKDBHelper.shared.getDB(), isNull);
      expect(factory.openCount, 1);
    },
  );
}

Future<Map<int, String>> _assetMigrations() async {
  final versions = (await File('assets/sql.txt').readAsString())
      .split(';')
      .where((value) => value.isNotEmpty)
      .map(int.parse);
  return {
    for (final version in versions)
      version: await File('assets/$version.sql').readAsString(),
  };
}

class _ControlledDatabaseFactory extends SqfliteDatabaseFactory {
  _ControlledDatabaseFactory(
    this.delegate,
    this.databasePath, {
    this.delayFirstOpen = false,
    this.failFirstMigration = false,
  });

  final DatabaseFactory delegate;
  final String databasePath;
  final bool delayFirstOpen;
  final bool failFirstMigration;
  final Completer<void> firstOpenStarted = Completer<void>();
  final Completer<void> _firstOpenGate = Completer<void>();
  int openCount = 0;
  int deleteCount = 0;
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
    final database = await delegate.openDatabase(path, options: options);
    if (openCount == 1) {
      firstDatabase = database;
      if (failFirstMigration) return _FailedMigrationDatabase(database);
    }
    return database;
  }

  @override
  Future<String> getDatabasesPath() async => databasePath;

  @override
  Future<void> setDatabasesPath(String path) => delegate.setDatabasesPath(path);

  @override
  Future<void> deleteDatabase(String path) {
    deleteCount++;
    return delegate.deleteDatabase(path);
  }

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

class _FailedMigrationDatabase implements Database {
  _FailedMigrationDatabase(this.delegate);
  final Database delegate;

  @override
  bool get isOpen => delegate.isOpen;

  @override
  Future<void> close() => delegate.close();

  @override
  Future<T> transaction<T>(
    Future<T> Function(Transaction txn) action, {
    bool? exclusive,
  }) => Future<T>.error(StateError('Injected migration failure'));

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
