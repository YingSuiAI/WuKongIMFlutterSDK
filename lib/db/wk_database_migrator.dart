import 'package:sqflite/sqflite.dart';

/// Applies SDK schema migrations transactionally and records their completion
/// in the same SQLite database as the schema they modify.
class WKDatabaseMigrator {
  static const migrationTable = 'wk_schema_migrations';

  Future<int> migrate(Database database, Map<int, String> migrations) async {
    final versions = migrations.keys.toList()..sort();

    // SQLite otherwise fails BEGIN EXCLUSIVE immediately when another SDK
    // connection is completing the same migration.
    await database.execute('PRAGMA busy_timeout = 10000');
    await _exclusiveTransaction(database, (transaction) async {
      await transaction.execute('''
CREATE TABLE IF NOT EXISTS $migrationTable (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
)
''');
    });

    for (final version in versions) {
      // Serializing before the completion check prevents separate database
      // connections from both replaying a migration containing data changes.
      await _exclusiveTransaction(database, (transaction) async {
        if (await _isApplied(transaction, version)) return;
        for (final statement in _statements(migrations[version]!)) {
          await transaction.execute(statement);
        }
        await transaction.insert(migrationTable, {
          'version': version,
          'applied_at': DateTime.now().millisecondsSinceEpoch,
        });
      });
    }

    return versions.isEmpty ? 0 : versions.last;
  }

  Future<T> _exclusiveTransaction<T>(
    Database database,
    Future<T> Function(Transaction transaction) action,
  ) async {
    final deadline = DateTime.now().add(const Duration(seconds: 10));
    var delay = const Duration(milliseconds: 20);
    while (true) {
      try {
        return await database.transaction(action, exclusive: true);
      } on DatabaseException catch (error) {
        final resultCode = error.getResultCode();
        final primaryCode = resultCode == null ? null : resultCode & 0xff;
        final locked = primaryCode == 5 || primaryCode == 6;
        if (!locked || DateTime.now().isAfter(deadline)) rethrow;
        await Future<void>.delayed(delay);
        if (delay < const Duration(milliseconds: 500)) delay *= 2;
      }
    }
  }

  Future<bool> _isApplied(DatabaseExecutor database, int version) async {
    final rows = await database.query(
      migrationTable,
      columns: const ['version'],
      where: 'version = ?',
      whereArgs: [version],
      limit: 1,
    );
    return rows.isNotEmpty;
  }

  Iterable<String> _statements(String script) sync* {
    for (final statement in script.split(';')) {
      final trimmed = statement.trim();
      if (trimmed.isNotEmpty) yield trimmed;
    }
  }
}
