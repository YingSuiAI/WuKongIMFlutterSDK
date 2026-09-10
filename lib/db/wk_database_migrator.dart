import 'package:sqflite/sqflite.dart';

/// Applies SDK schema migrations transactionally and records their completion
/// in the same SQLite database as the schema they modify.
class WKDatabaseMigrator {
  static const migrationTable = 'wk_schema_migrations';

  Future<int> migrate(
    Database database,
    Map<int, String> migrations, {
    int legacyMaxVersion = 0,
  }) async {
    await database.execute('''
CREATE TABLE IF NOT EXISTS $migrationTable (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
)
''');

    final versions = migrations.keys.toList()..sort();
    if (legacyMaxVersion > 0) {
      await database.transaction((transaction) async {
        for (final version in versions.where(
          (version) => version <= legacyMaxVersion,
        )) {
          await _recordApplied(transaction, version);
        }
      });
    }

    final completed = await _completedVersions(database);
    for (final version in versions) {
      if (completed.contains(version)) continue;
      await database.transaction((transaction) async {
        for (final statement in _statements(migrations[version]!)) {
          await _executeRecoverably(transaction, statement);
        }
        await _recordApplied(transaction, version);
      });
      completed.add(version);
    }
    return completed.isEmpty ? 0 : completed.reduce((a, b) => a > b ? a : b);
  }

  Future<Set<int>> _completedVersions(DatabaseExecutor database) async {
    final rows = await database.query(migrationTable, columns: ['version']);
    return rows.map((row) => row['version'] as int).toSet();
  }

  Future<void> _recordApplied(DatabaseExecutor database, int version) async {
    await database.insert(migrationTable, {
      'version': version,
      'applied_at': DateTime.now().millisecondsSinceEpoch,
    }, conflictAlgorithm: ConflictAlgorithm.ignore);
  }

  Iterable<String> _statements(String script) sync* {
    for (final statement in script.split(';')) {
      final normalized = statement.replaceAll('\n', ' ').trim();
      if (normalized.isNotEmpty) yield normalized;
    }
  }

  Future<void> _executeRecoverably(
    DatabaseExecutor database,
    String statement,
  ) async {
    final createTable = RegExp(
      r'^create\s+table\s+(?:if\s+not\s+exists\s+)?[\x60\x27\x22]?([A-Za-z_][A-Za-z0-9_]*)',
      caseSensitive: false,
    ).firstMatch(statement);
    if (createTable != null &&
        await _schemaObjectExists(database, 'table', createTable.group(1)!)) {
      return;
    }

    final createIndex = RegExp(
      r'^create\s+(?:unique\s+)?index\s+(?:if\s+not\s+exists\s+)?[\x60\x27\x22]?([A-Za-z_][A-Za-z0-9_]*)',
      caseSensitive: false,
    ).firstMatch(statement);
    if (createIndex != null &&
        await _schemaObjectExists(database, 'index', createIndex.group(1)!)) {
      return;
    }

    final addColumn = RegExp(
      r'^alter\s+table\s+[\x60\x27\x22]?([A-Za-z_][A-Za-z0-9_]*)[\x60\x27\x22]?\s+add(?:\s+column)?\s+[\x60\x27\x22]?([A-Za-z_][A-Za-z0-9_]*)',
      caseSensitive: false,
    ).firstMatch(statement);
    if (addColumn != null &&
        await _columnExists(
          database,
          addColumn.group(1)!,
          addColumn.group(2)!,
        )) {
      return;
    }

    await database.execute(statement);
  }

  Future<bool> _schemaObjectExists(
    DatabaseExecutor database,
    String type,
    String name,
  ) async {
    final rows = await database.query(
      'sqlite_master',
      columns: const ['name'],
      where: 'type = ? AND name = ?',
      whereArgs: [type, name],
      limit: 1,
    );
    return rows.isNotEmpty;
  }

  Future<bool> _columnExists(
    DatabaseExecutor database,
    String table,
    String column,
  ) async {
    final rows = await database.rawQuery('PRAGMA table_info($table)');
    return rows.any((row) => row['name'] == column);
  }
}
