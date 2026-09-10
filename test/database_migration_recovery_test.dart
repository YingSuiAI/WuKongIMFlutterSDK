import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/db/wk_database_migrator.dart';

void main() {
  sqfliteFfiInit();

  late Database database;

  setUp(() async {
    database = await databaseFactoryFfi.openDatabase(inMemoryDatabasePath);
  });

  tearDown(() => database.close());

  test('applies and records every migration in SQLite', () async {
    final latest = await WKDatabaseMigrator().migrate(database, {
      1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
      2: 'ALTER TABLE message ADD COLUMN body TEXT;',
    });

    expect(latest, 2);
    expect(await _columns(database, 'message'), containsAll(['id', 'body']));
    expect(await _versions(database), [1, 2]);
  });

  test('configures busy timeout through the query-compatible API', () async {
    final androidCompatible = _RejectPragmaExecuteDatabase(database);

    await WKDatabaseMigrator().migrate(androidCompatible, {
      1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
    });

    expect(androidCompatible.busyTimeoutQueries, 1);
  });

  test('rolls back a failed migration and can retry it safely', () async {
    final migrator = WKDatabaseMigrator();

    await expectLater(
      migrator.migrate(database, {
        1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
        2: 'CREATE TABLE partial (id INTEGER); INVALID SQL;',
      }),
      throwsA(anything),
    );

    expect(await _tables(database), isNot(contains('partial')));
    expect(await _versions(database), [1]);

    await migrator.migrate(database, {
      1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
      2: 'CREATE TABLE partial (id INTEGER);',
    });

    expect(await _tables(database), contains('partial'));
    expect(await _versions(database), [1, 2]);
  });

  test(
    'adopts a completed legacy watermark before replaying migrations',
    () async {
      await database.execute('CREATE TABLE message (id INTEGER PRIMARY KEY)');
      await database.execute('''
CREATE TABLE ${WKDatabaseMigrator.migrationTable} (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
)
''');

      final latest = await WKDatabaseMigrator().migrate(database, {
        1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
        2: 'ALTER TABLE message ADD COLUMN body TEXT;',
      }, legacyAppliedThrough: 1);

      expect(latest, 2);
      expect(await _columns(database, 'message'), containsAll(['id', 'body']));
      expect(await _versions(database), [1, 2]);
    },
  );

  test('applies the real migration history to a fresh database', () async {
    final migrations = await _assetMigrations();

    expect(
      await WKDatabaseMigrator().migrate(database, migrations),
      202604271625,
    );

    expect(await _tables(database), contains('message_reaction'));
    expect(await _versions(database), migrations.keys.toList()..sort());
  });

  test('serializes migration replay across database connections', () async {
    final directory = await Directory.systemTemp.createTemp('wk_migration_');
    final path = '${directory.path}/shared.db';
    final options = OpenDatabaseOptions(singleInstance: false);
    final first = await databaseFactoryFfi.openDatabase(path, options: options);
    final second = await databaseFactoryFfi.openDatabase(
      path,
      options: options,
    );
    try {
      final migrations = {
        1: '''
CREATE TABLE event (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO event (id, value) VALUES (1, 'once');
''',
      };

      await Future.wait([
        WKDatabaseMigrator().migrate(first, migrations),
        WKDatabaseMigrator().migrate(second, migrations),
      ]);

      expect(await first.query('event'), [
        {'id': 1, 'value': 'once'},
      ]);
      expect(await _versions(first), [1]);
    } finally {
      await first.close();
      await second.close();
      await directory.delete(recursive: true);
    }
  });
}

Future<List<String>> _tables(Database database) async {
  final rows = await database.query(
    'sqlite_master',
    columns: ['name'],
    where: 'type = ?',
    whereArgs: ['table'],
  );
  return rows.map((row) => row['name'] as String).toList();
}

Future<List<String>> _columns(Database database, String table) async {
  final rows = await database.rawQuery('PRAGMA table_info($table)');
  return rows.map((row) => row['name'] as String).toList();
}

Future<List<int>> _versions(Database database) async {
  final rows = await database.query(
    WKDatabaseMigrator.migrationTable,
    columns: ['version'],
    orderBy: 'version',
  );
  return rows.map((row) => row['version'] as int).toList();
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

class _RejectPragmaExecuteDatabase implements Database {
  _RejectPragmaExecuteDatabase(this._delegate);

  final Database _delegate;
  var busyTimeoutQueries = 0;

  @override
  Future<void> execute(String sql, [List<Object?>? arguments]) {
    if (sql.trimLeft().toUpperCase().startsWith('PRAGMA')) {
      throw UnsupportedError('Android requires PRAGMA through rawQuery');
    }
    return _delegate.execute(sql, arguments);
  }

  @override
  Future<List<Map<String, Object?>>> rawQuery(
    String sql, [
    List<Object?>? arguments,
  ]) {
    if (sql.trimLeft().toUpperCase().startsWith('PRAGMA BUSY_TIMEOUT')) {
      busyTimeoutQueries += 1;
    }
    return _delegate.rawQuery(sql, arguments);
  }

  @override
  Future<T> transaction<T>(
    Future<T> Function(Transaction txn) action, {
    bool? exclusive,
  }) => _delegate.transaction(action, exclusive: exclusive);

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
