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
      final generation = _lifecycleGeneration;
      Future<bool> resume() {
        if (generation != _lifecycleGeneration ||
            WKIM.shared.options.uid != uid) {
          return Future<bool>.value(false);
        }
        return init();
      }

      return active.then((_) => resume(), onError: (_, __) => resume());
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
      final result = await onUpgrade(openedDatabase);
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

  Future<bool> onUpgrade(Database db) async {
    String path = await rootBundle.loadString(
      'packages/wukongimfluttersdk/assets/sql.txt',
    );
    List<String> names = path.split(';');
    final migrations = <int, String>{};
    for (int i = 0; i < names.length; i++) {
      final name = names[i].trim();
      if (name.isEmpty) {
        continue;
      }
      int version = int.parse(name);
      migrations[version] = await rootBundle.loadString(
        'packages/wukongimfluttersdk/assets/$version.sql',
      );
    }
    await WKDatabaseMigrator().migrate(db, migrations);
    return true;
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
