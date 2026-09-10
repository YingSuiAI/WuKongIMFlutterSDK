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

  test(
    'repairs a partially applied legacy schema without deleting data',
    () async {
      await database.execute(
        'CREATE TABLE message (id INTEGER PRIMARY KEY, body TEXT)',
      );
      await database.insert('message', {'id': 7, 'body': 'preserved'});

      await WKDatabaseMigrator().migrate(database, {
        1: '''
CREATE TABLE message (id INTEGER PRIMARY KEY, body TEXT);
CREATE TABLE conversation (id INTEGER PRIMARY KEY);
CREATE INDEX message_body_index ON message (body);
''',
        2: 'ALTER TABLE message ADD COLUMN topic_id TEXT;',
      });

      expect(await _tables(database), contains('conversation'));
      expect(await _columns(database, 'message'), contains('topic_id'));
      expect(await database.query('message', where: 'id = ?', whereArgs: [7]), [
        containsPair('body', 'preserved'),
      ]);
      expect(await _versions(database), [1, 2]);
    },
  );

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

  test('imports a completed legacy version into SQLite metadata', () async {
    await database.execute('CREATE TABLE message (id INTEGER PRIMARY KEY)');

    await WKDatabaseMigrator().migrate(database, {
      1: 'CREATE TABLE message (id INTEGER PRIMARY KEY);',
      2: 'ALTER TABLE message ADD COLUMN body TEXT;',
    }, legacyMaxVersion: 1);

    expect(await _columns(database, 'message'), contains('body'));
    expect(await _versions(database), [1, 2]);
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
