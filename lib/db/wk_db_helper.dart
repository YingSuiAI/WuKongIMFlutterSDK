import 'package:shared_preferences/shared_preferences.dart';
import 'package:sqflite/sqflite.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart' as p;

import '../wkim.dart';
import 'wk_database_migrator.dart';

class WKDBHelper {
  WKDBHelper._privateConstructor();
  static final WKDBHelper _instance = WKDBHelper._privateConstructor();
  static WKDBHelper get shared => _instance;
  final dbVersion = 1;
  Database? _database;
  String? _databaseUid;
  Future<bool>? _initialization;
  String? _initializingUid;
  int? _initializationGeneration;
  int _lifecycleGeneration = 0;
  Future<void> _closing = Future<void>.value();

  Future<bool> init() {
    final uid = WKIM.shared.options.uid;
    if (uid == null || uid.isEmpty) {
      return Future<bool>.error(
        StateError('A non-empty uid is required before opening the database.'),
      );
    }

    final current = _database;
    if (current != null &&
        current.isOpen &&
        _databaseUid == uid &&
        _initialization == null) {
      return Future<bool>.value(true);
    }

    final active = _initialization;
    if (active != null) {
      if (_initializingUid == uid &&
          _initializationGeneration == _lifecycleGeneration) {
        return active;
      }
      if (_initializationGeneration == _lifecycleGeneration) {
        _lifecycleGeneration++;
      }
      return active.then((_) => init(), onError: (_, __) => init());
    }

    if (current != null) {
      _database = null;
      _databaseUid = null;
      _queueClose(current);
    }

    final generation = _lifecycleGeneration;
    late final Future<bool> attempt;
    _initializingUid = uid;
    _initializationGeneration = generation;
    attempt = _openAndMigrate(uid, generation).whenComplete(() {
      if (identical(_initialization, attempt)) {
        _initialization = null;
        _initializingUid = null;
        _initializationGeneration = null;
      }
    });
    _initialization = attempt;
    return attempt;
  }

  Future<bool> _openAndMigrate(String uid, int generation) async {
    await _closing;
    if (generation != _lifecycleGeneration || WKIM.shared.options.uid != uid) {
      return false;
    }
    var databasesPath = await getDatabasesPath();
    if (generation != _lifecycleGeneration || WKIM.shared.options.uid != uid) {
      return false;
    }
    String path = p.join(databasesPath, 'wk_$uid.db');
    Database? openedDatabase;
    try {
      openedDatabase = await openDatabase(path, version: dbVersion);
      final result = await onUpgrade(openedDatabase, databaseUid: uid);
      if (!result ||
          generation != _lifecycleGeneration ||
          WKIM.shared.options.uid != uid) {
        await openedDatabase.close();
        return false;
      }
      _database = openedDatabase;
      _databaseUid = uid;
      return true;
    } catch (error, stackTrace) {
      if (openedDatabase != null && openedDatabase.isOpen) {
        try {
          await openedDatabase.close();
        } catch (_) {}
      }
      Error.throwWithStackTrace(error, stackTrace);
    }
  }

  Future<bool> onUpgrade(Database db, {String? databaseUid}) async {
    String path = await rootBundle.loadString(
      'packages/wukongimfluttersdk/assets/sql.txt',
    );
    List<String> names = path.split(';');
    final migrations = <int, String>{};
    for (int i = 0; i < names.length; i++) {
      if (names[i] == '') {
        continue;
      }
      int version = int.parse(names[i]);
      migrations[version] = await rootBundle.loadString(
        'packages/wukongimfluttersdk/assets/$version.sql',
      );
    }
    // Releases before the SQLite migration ledger stored this watermark only
    // after every migration through it completed successfully. Adopt that
    // one-way upgrade evidence so an existing database is not replayed as a
    // fresh one. All later progress remains transactionally owned by SQLite.
    final preferences = await SharedPreferences.getInstance();
    final uid = databaseUid ?? WKIM.shared.options.uid!;
    final legacyWatermark = preferences.getInt('wk_max_sql_version_$uid') ?? 0;
    final legacyAppliedThrough =
        legacyWatermark > 0 && await _hasLegacyBaseSchema(db)
        ? legacyWatermark
        : 0;
    await WKDatabaseMigrator().migrate(
      db,
      migrations,
      legacyAppliedThrough: legacyAppliedThrough,
    );
    return true;
  }

  Future<bool> _hasLegacyBaseSchema(Database db) async {
    const baseTables = {
      'message',
      'conversation',
      'channel',
      'channel_members',
      'message_reaction',
    };
    final rows = await db.query(
      'sqlite_master',
      columns: const ['name'],
      where: "type = 'table'",
    );
    final tables = rows.map((row) => row['name']).whereType<String>().toSet();
    return tables.containsAll(baseTables);
  }

  Database? getDB() {
    return _database;
  }

  Future<void> close() async {
    _lifecycleGeneration++;
    final database = _database;
    _database = null;
    _databaseUid = null;
    if (database != null) _queueClose(database);

    final active = _initialization;
    if (active != null) {
      try {
        await active;
      } catch (_) {}
    }
    try {
      await _closing;
    } catch (_) {}
  }

  void _queueClose(Database database) {
    final previous = _closing;
    _closing = () async {
      try {
        await previous;
      } catch (_) {}
      if (database.isOpen) await database.close();
    }();
  }
}
