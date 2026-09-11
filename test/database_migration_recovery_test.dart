import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
// ignore: implementation_imports, depend_on_referenced_packages
import 'package:sqflite_common/src/exception.dart';
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

  test(
    'initializes a fresh Android database with its platform metadata',
    () async {
      await database.execute('CREATE TABLE android_metadata (locale TEXT)');
      await database.insert('android_metadata', {'locale': 'en_US'});
      await WKDatabaseMigrator().migrate(database, {
        1: 'CREATE TABLE message (body TEXT);',
      });
      expect(await _versions(database), [1]);
      expect(await database.query('android_metadata'), [
        {'locale': 'en_US'},
      ]);
    },
  );

  test('does not issue row-returning PRAGMA through Android execute', () async {
    final androidCompatible = _ObservedDatabase(database);

    await WKDatabaseMigrator().migrate(androidCompatible, {
      1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
    });

    expect(androidCompatible.pragmaQueries, 0);
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

  test('does not infer migration progress from an untracked schema', () async {
    await database.execute('CREATE TABLE message (body TEXT)');
    await database.insert('message', {'body': 'keep'});

    await expectLater(
      WKDatabaseMigrator().migrate(database, {
        1: 'CREATE TABLE IF NOT EXISTS message (body TEXT);',
        2: 'DELETE FROM message;',
      }),
      throwsStateError,
    );

    expect(await database.query('message'), [
      {'body': 'keep'},
    ]);
    expect(
      await _tables(database),
      isNot(contains(WKDatabaseMigrator.migrationTable)),
    );
  });

  test('reopens the current ledger without replaying data changes', () async {
    final migrations = {
      1: "CREATE TABLE message (body TEXT); INSERT INTO message VALUES ('once');",
      2: 'ALTER TABLE message ADD COLUMN extra TEXT;',
    };
    await WKDatabaseMigrator().migrate(database, migrations);
    await WKDatabaseMigrator().migrate(database, migrations);
    expect(await database.query('message'), [
      {'body': 'once', 'extra': null},
    ]);
    expect(await _versions(database), [1, 2]);
  });

  test('rejects empty migrations before writing a ledger', () async {
    await expectLater(
      WKDatabaseMigrator().migrate(database, {1: ' ; '}),
      throwsStateError,
    );
    expect(await _tables(database), isEmpty);
  });

  test('rejects a ledger which is not a prefix of this catalog', () async {
    await WKDatabaseMigrator().migrate(database, {
      2: 'CREATE TABLE message (body TEXT);',
    });
    await expectLater(
      WKDatabaseMigrator().migrate(database, {
        1: 'DROP TABLE message;',
        2: 'CREATE TABLE message (body TEXT);',
      }),
      throwsStateError,
    );
    expect(await _tables(database), contains('message'));
    expect(await _versions(database), [2]);
  });

  for (final code in [5, 6, 261, 262]) {
    test('retries Android lock acquisition result $code', () async {
      final observed = _ObservedDatabase(
        database,
        failures: [
          SqfliteDatabaseException(
            'database is locked (code $code SQLITE_BUSY)',
            null,
          ),
        ],
      );
      await WKDatabaseMigrator().migrate(observed, {
        1: 'CREATE TABLE message (body TEXT);',
      });
      expect(observed.attempts, 3);
      expect(await _versions(database), [1]);
    });
  }

  test('stops after bounded lock acquisition attempts', () async {
    final locked = SqfliteDatabaseException(
      'database is locked (code 5 SQLITE_BUSY)',
      null,
    );
    final observed = _ObservedDatabase(
      database,
      failures: List.filled(20, locked),
    );
    await expectLater(
      WKDatabaseMigrator().migrate(observed, {
        1: 'CREATE TABLE message (body TEXT);',
      }),
      throwsA(same(locked)),
    );
    expect(observed.attempts, 8);
    expect(await _tables(database), isEmpty);
  });

  test('does not retry non-lock database failures', () async {
    final failure = SqfliteDatabaseException(
      'syntax error (code 1 SQLITE_ERROR)',
      null,
    );
    final observed = _ObservedDatabase(database, failures: [failure]);
    await expectLater(
      WKDatabaseMigrator().migrate(observed, {
        1: 'CREATE TABLE message (body TEXT);',
      }),
      throwsA(same(failure)),
    );
    expect(observed.attempts, 1);
  });

  test('does not replay after the transaction action has started', () async {
    final failure = SqfliteDatabaseException(
      'database is locked (code 5 SQLITE_BUSY)',
      null,
    );
    final observed = _ObservedDatabase(database, afterActionFailure: failure);
    await expectLater(
      WKDatabaseMigrator().migrate(observed, {
        1: 'CREATE TABLE message (body TEXT);',
      }),
      throwsA(same(failure)),
    );
    expect(observed.attempts, 1);
    expect(await _tables(database), isEmpty);
  });

  test('applies the real migration history to a fresh database', () async {
    final migrations = await _assetMigrations();

    expect(
      await WKDatabaseMigrator().migrate(database, migrations),
      202609111800,
    );

    expect(await _tables(database), contains('message_reaction'));
    expect(await _versions(database), migrations.keys.toList()..sort());
  });

  test('payload receipt migration preserves admitted local rows', () async {
    final migrations = await _assetMigrations();
    final preceding = Map<int, String>.from(migrations)..remove(202609111800);
    await WKDatabaseMigrator().migrate(database, preceding);
    await database.insert('message', {
      'client_seq': 42,
      'client_msg_no': 'retained-request',
      'content': '{"type":"message.send"}',
      'is_deleted': 1,
    });
    await WKDatabaseMigrator().migrate(database, migrations);
    final row = (await database.query('message')).single;
    expect(row['client_seq'], 42);
    expect(row['client_msg_no'], 'retained-request');
    expect(row['content'], '{"type":"message.send"}');
    expect(row['is_deleted'], 1);
    expect(row['payload_committed'], 0);
    await database.update('message', {'payload_committed': 1});
    await WKDatabaseMigrator().migrate(database, migrations);
    expect((await database.query('message')).single['payload_committed'], 1);
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
    final releaseLock = Completer<void>();
    Future<void>? blocker;
    try {
      final migrations = {
        1: '''
CREATE TABLE event (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO event (id, value) VALUES (1, 'once');
''',
      };

      final lockHeld = Completer<void>();
      blocker = first.transaction((transaction) async {
        lockHeld.complete();
        await releaseLock.future;
      }, exclusive: true);
      await lockHeld.future;
      final observed = _ObservedDatabase(second);
      final pending = WKDatabaseMigrator().migrate(observed, migrations);
      await observed.lockFailure.future.timeout(const Duration(seconds: 5));
      releaseLock.complete();
      await blocker;
      await Future.wait([
        WKDatabaseMigrator().migrate(first, migrations),
        pending,
      ]);

      expect(await first.query('event'), [
        {'id': 1, 'value': 'once'},
      ]);
      expect(await _versions(first), [1]);
    } finally {
      if (!releaseLock.isCompleted) releaseLock.complete();
      await blocker;
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
      .map((value) => value.trim())
      .where((value) => value.isNotEmpty)
      .map(int.parse);
  return {
    for (final version in versions)
      version: await File('assets/$version.sql').readAsString(),
  };
}

class _ObservedDatabase implements Database {
  _ObservedDatabase(
    this._delegate, {
    List<DatabaseException>? failures,
    this.afterActionFailure,
  }) : failures = [...?failures];

  final Database _delegate;
  final List<DatabaseException> failures;
  final DatabaseException? afterActionFailure;
  final lockFailure = Completer<void>();
  var pragmaQueries = 0;
  var attempts = 0;

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
    if (sql.trimLeft().toUpperCase().startsWith('PRAGMA')) {
      pragmaQueries += 1;
    }
    return _delegate.rawQuery(sql, arguments);
  }

  @override
  Future<T> transaction<T>(
    Future<T> Function(Transaction txn) action, {
    bool? exclusive,
  }) async {
    expect(exclusive, isTrue);
    attempts++;
    if (failures.isNotEmpty) throw failures.removeAt(0);
    try {
      return await _delegate.transaction((transaction) async {
        final result = await action(transaction);
        if (afterActionFailure != null) throw afterActionFailure!;
        return result;
      }, exclusive: exclusive);
    } on DatabaseException catch (error) {
      final code = error.getResultCode();
      if (code != null && (code & 0xff) == 5 && !lockFailure.isCompleted) {
        lockFailure.complete();
      }
      rethrow;
    }
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
