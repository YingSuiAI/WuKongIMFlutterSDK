import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
// ignore: depend_on_referenced_packages
import 'package:sqflite_common/src/factory.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/manager/message_manager.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();
  late Directory directory;
  late WKMessageManager originalManager;
  late Options originalOptions;
  late Model originalMode;
  late _PendingCleanup manager;

  setUp(() async {
    await WKDBHelper.shared.close();
    directory = await Directory.systemTemp.createTemp('wk_setup_');
    databaseFactory = _SetupDatabaseFactory(directory.path);
    originalManager = WKIM.shared.messageManager;
    originalOptions = WKIM.shared.options;
    originalMode = WKIM.shared.runMode;
    manager = _PendingCleanup();
    WKIM.shared.messageManager = manager;
    WKIM.shared.runMode = Model.app;
  });

  tearDown(() async {
    if (!manager.release.isCompleted) manager.release.complete();
    await WKDBHelper.shared.close();
    WKIM.shared.messageManager = originalManager;
    WKIM.shared.options = originalOptions;
    WKIM.shared.runMode = originalMode;
    databaseFactory = databaseFactoryFfi;
    await directory.delete(recursive: true);
  });

  test(
    'setup waits for pending-message recovery before reporting ready',
    () async {
      var completed = false;
      final setup = WKIM.shared.setup(_options('owner')).then((value) {
        completed = true;
        return value;
      });
      await manager.started.future;
      await Future<void>.delayed(Duration.zero);
      expect(completed, isFalse);
      manager.release.complete();
      expect(await setup, isTrue);
    },
  );

  test('setup propagates pending-message recovery failure', () async {
    final setup = WKIM.shared.setup(_options('owner'));
    final check = expectLater(setup, throwsStateError);
    await manager.started.future;
    manager.release.completeError(StateError('recovery failed'));
    await check;
  });

  test('superseded setup cannot report another session ready', () async {
    final first = WKIM.shared.setup(_options('owner-a'));
    await manager.started.future;
    WKIM.shared.runMode = Model.web;
    expect(await WKIM.shared.setup(_options('owner-b')), isTrue);
    manager.release.complete();
    expect(await first, isFalse);
  });

  test('closing the database during recovery cannot report ready', () async {
    final setup = WKIM.shared.setup(_options('owner'));
    await manager.started.future;
    await WKDBHelper.shared.close();
    manager.release.complete();
    expect(await setup, isFalse);
  });

  test('mutating the same options object invalidates pending setup', () async {
    final options = _options('owner');
    final setup = WKIM.shared.setup(options);
    await manager.started.future;
    options.sessionGeneration++;
    manager.release.complete();
    expect(await setup, isFalse);
  });

  test(
    'setup rejects retired protocol versions before replacing identity',
    () async {
      final previous = WKIM.shared.options;
      manager.release.complete();
      expect(
        await WKIM.shared.setup(_options('owner')..protoVersion = 5),
        isFalse,
      );
      expect(identical(WKIM.shared.options, previous), isTrue);
    },
  );
}

Options _options(String uid) => Options.newDefault(uid, 'test-token')
  ..installationID = 'test-installation'
  ..appInstanceID = 'test-instance'
  ..installationGeneration = 1
  ..sessionGeneration = 1;

class _PendingCleanup implements WKMessageManager {
  final started = Completer<void>();
  final release = Completer<void>();

  @override
  Future<void> updateSendingMsgFail() async {
    if (!started.isCompleted) started.complete();
    await release.future;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => null;
}

class _SetupDatabaseFactory extends SqfliteDatabaseFactory {
  _SetupDatabaseFactory(this.path);
  final String path;

  @override
  Future<String> getDatabasesPath() async => path;

  @override
  Future<Database> openDatabase(String path, {OpenDatabaseOptions? options}) =>
      databaseFactoryFfi.openDatabase(path, options: options);

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
