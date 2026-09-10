import 'package:sqflite/sqflite.dart';

/// Applies SDK schema migrations transactionally and records their completion
/// in the same SQLite database as the schema they modify.
class WKDatabaseMigrator {
  static const migrationTable = 'wk_schema_migrations';

  Future<int> migrate(Database database, Map<int, String> migrations) async {
    final versions = migrations.keys.toList()..sort();
    for (final version in versions) {
      if (_statements(migrations[version]!).isEmpty) {
        throw StateError('Migration $version has no executable statements.');
      }
    }

    await _exclusiveTransaction(database, (transaction) async {
      await transaction.execute('''
CREATE TABLE IF NOT EXISTS $migrationTable (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
)
''');
      final recorded = await _recordedVersions(transaction);
      _requirePrefix(recorded, versions, label: 'Migration ledger');

      if (recorded.isEmpty) {
        final existingTables = await transaction.query(
          'sqlite_master',
          columns: const ['name'],
          // Android creates its locale metadata before SDK initialization.
          where:
              "type = 'table' AND name NOT GLOB 'sqlite_*' "
              "AND name != 'android_metadata' AND name != ?",
          whereArgs: const [migrationTable],
          limit: 1,
        );
        if (existingTables.isNotEmpty) {
          throw StateError(
            'An existing schema requires a SQLite migration ledger.',
          );
        }
      }
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
    var delay = const Duration(milliseconds: 20);
    for (var attempt = 1; ; attempt++) {
      var actionStarted = false;
      try {
        return await database.transaction((transaction) {
          actionStarted = true;
          return action(transaction);
        }, exclusive: true);
      } on DatabaseException catch (error) {
        final resultCode = error.getResultCode();
        final primaryCode = resultCode == null ? null : resultCode & 0xff;
        final locked = primaryCode == 5 || primaryCode == 6;
        // Only retry lock acquisition. Once the action starts, especially if
        // COMMIT fails, its connection state must not be blindly replayed.
        // Do not set PRAGMA busy_timeout: Android execute rejects row results.
        if (!locked || actionStarted || attempt >= 8) rethrow;
        await Future<void>.delayed(delay);
        final nextDelay = delay.inMilliseconds * 2;
        delay = Duration(milliseconds: nextDelay > 500 ? 500 : nextDelay);
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

  Future<List<int>> _recordedVersions(DatabaseExecutor database) async {
    final rows = await database.query(
      migrationTable,
      columns: const ['version'],
      orderBy: 'version',
    );
    return rows.map((row) {
      final version = row['version'];
      if (version is! int) {
        throw StateError('Migration ledger contains an invalid version.');
      }
      return version;
    }).toList();
  }

  void _requirePrefix(
    List<int> candidate,
    List<int> catalog, {
    required String label,
  }) {
    if (candidate.length > catalog.length) {
      throw StateError('$label contains an unknown migration version.');
    }
    for (var index = 0; index < candidate.length; index++) {
      if (candidate[index] != catalog[index]) {
        throw StateError('$label is not a contiguous migration prefix.');
      }
    }
  }

  Iterable<String> _statements(String script) sync* {
    for (final statement in script.split(';')) {
      final trimmed = statement.trim();
      if (trimmed.isNotEmpty) yield trimmed;
    }
  }
}
