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
  Future<bool>? _initialization;

  Future<bool> init() {
    final active = _initialization;
    if (active != null) return active;
    late final Future<bool> attempt;
    attempt = _openAndMigrate().whenComplete(() {
      if (identical(_initialization, attempt)) _initialization = null;
    });
    _initialization = attempt;
    return attempt;
  }

  Future<bool> _openAndMigrate() async {
    var databasesPath = await getDatabasesPath();
    String path = p.join(databasesPath, 'wk_${WKIM.shared.options.uid}.db');
    try {
      _database = await openDatabase(path, version: dbVersion);
      bool result = await onUpgrade(_database!);
      return _database != null && result;
    } catch (_) {
      final failedDatabase = _database;
      _database = null;
      if (failedDatabase != null) await failedDatabase.close();
      rethrow;
    }
  }

  Future<bool> onUpgrade(Database db) async {
    String path = await rootBundle.loadString(
      'packages/wukongimfluttersdk/assets/sql.txt',
    );
    List<String> names = path.split(';');
    SharedPreferences preferences = await SharedPreferences.getInstance();
    String wkUid = WKIM.shared.options.uid!;
    int maxVersion = preferences.getInt('wk_max_sql_version_$wkUid') ?? 0;
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
    final appliedVersion = await WKDatabaseMigrator().migrate(
      db,
      migrations,
      legacyMaxVersion: maxVersion,
    );
    if (appliedVersion > maxVersion) {
      await preferences.setInt('wk_max_sql_version_$wkUid', appliedVersion);
    }
    return true;
  }

  Database? getDB() {
    return _database;
  }

  close() {
    _initialization = null;
    if (_database != null) {
      _database!.close();
      _database = null;
    }
  }
}
