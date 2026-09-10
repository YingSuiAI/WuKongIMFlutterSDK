import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:wukongimfluttersdk/common/options.dart';
import 'package:wukongimfluttersdk/db/message.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/entity/channel.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/model/wk_text_content.dart';
import 'package:wukongimfluttersdk/model/wk_image_content.dart';
import 'package:wukongimfluttersdk/manager/connect_manager.dart';
import 'package:wukongimfluttersdk/wkim.dart';

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  sqfliteFfiInit();
  late Directory directory;
  late WKConnectionManager originalConnection;
  late _RecordingConnection connection;

  setUp(() async {
    await WKDBHelper.shared.close();
    directory = await Directory.systemTemp.createTemp('wk_message_');
    databaseFactory = databaseFactoryFfi;
    await databaseFactory.setDatabasesPath(directory.path);
    WKIM.shared.options = Options()..uid = 'message-owner';
    await WKDBHelper.shared.init();
    originalConnection = WKIM.shared.connectionManager;
    connection = _RecordingConnection();
    WKIM.shared.connectionManager = connection;
  });

  tearDown(() async {
    WKIM.shared.messageManager.removeOnRefreshMsgListener('lifecycle');
    WKIM.shared.connectionManager = originalConnection;
    await WKDBHelper.shared.close();
    await directory.delete(recursive: true);
  });

  test('sendMessage returns the persistence failure to its caller', () async {
    await WKDBHelper.shared.close();
    Object? returnedError;
    final detachedErrors = <Object>[];
    await runZonedGuarded(() async {
      try {
        await WKIM.shared.messageManager.sendMessage(
          WKTextContent('not persisted'),
          WKChannel('peer', 1),
        );
      } catch (error) {
        returnedError = error;
      }
      await Future<void>.delayed(Duration.zero);
    }, (error, _) => detachedErrors.add(error));
    expect(returnedError, isNotNull);
    expect(detachedErrors, isEmpty);
  });

  test(
    'outgoing admission keeps caller envelope and serializes local order',
    () async {
      const envelope =
          '{"type":1,"app":{"sender":"principal-42"},"content":"hello"}';
      final first = WKMsg()
        ..fromUID = 'principal-42'
        ..channelID = 'peer'
        ..clientMsgNO = 'caller-first'
        ..contentType = 1
        ..content = envelope;
      final second = WKMsg()
        ..fromUID = 'principal-42'
        ..channelID = 'peer'
        ..clientMsgNO = 'caller-second'
        ..contentType = 1
        ..content = envelope;
      await Future.wait([
        WKIM.shared.messageManager.saveOutgoingMessage(first),
        WKIM.shared.messageManager.saveOutgoingMessage(second),
      ]);
      expect(first.clientSeq, greaterThan(0));
      expect(second.clientSeq, isNot(first.clientSeq));
      expect([first.orderSeq, second.orderSeq]..sort(), [1, 2]);
      final rows = await WKDBHelper.shared.getDB()!.query(
        'message',
        orderBy: 'order_seq',
      );
      expect(rows.map((row) => row['content']), [envelope, envelope]);
      expect(rows.map((row) => row['from_uid']), [
        'principal-42',
        'principal-42',
      ]);
      expect(rows.map((row) => row['client_msg_no']).toSet(), {
        'caller-first',
        'caller-second',
      });
      final conversation = await WKDBHelper.shared.getDB()!.query(
        'conversation',
      );
      expect(
        conversation.single['last_client_msg_no'],
        rows.last['client_msg_no'],
      );
      expect(connection.sent, isEmpty);
    },
  );

  test(
    'outgoing cancellation after message insert rolls back both projections',
    () async {
      final message = WKMsg()
        ..channelID = 'peer'
        ..contentType = 1;
      final identity = message.clientMsgNO;
      var checks = 0;
      final cancelled = StateError('caller session cancelled');
      await expectLater(
        WKIM.shared.messageManager.saveOutgoingMessage(
          message,
          isCurrent: () {
            // Admission, transaction entry, allocated order, completed INSERT.
            if (++checks == 4) throw cancelled;
            return true;
          },
        ),
        throwsA(same(cancelled)),
      );
      expect(await WKDBHelper.shared.getDB()!.query('message'), isEmpty);
      expect(await WKDBHelper.shared.getDB()!.query('conversation'), isEmpty);
      expect(message.clientSeq, 0);
      expect(message.orderSeq, 0);
      expect(message.clientMsgNO, identity);
    },
  );

  test(
    'duplicate outgoing key never renames or deletes the admitted message',
    () async {
      final first = WKMsg()
        ..channelID = 'peer'
        ..contentType = 1;
      await WKIM.shared.messageManager.saveOutgoingMessage(first);
      final duplicate = WKMsg()
        ..channelID = 'peer'
        ..contentType = 1
        ..clientMsgNO = first.clientMsgNO;
      await expectLater(
        WKIM.shared.messageManager.saveOutgoingMessage(duplicate),
        throwsA(isA<DatabaseException>()),
      );
      final rows = await WKDBHelper.shared.getDB()!.query('message');
      expect(rows, hasLength(1));
      expect(rows.single['client_msg_no'], first.clientMsgNO);
      expect(rows.single['is_deleted'], 0);
      expect(duplicate.clientMsgNO, first.clientMsgNO);
      expect(duplicate.clientSeq, 0);
      expect(duplicate.orderSeq, 0);
    },
  );

  test(
    'ACK persists message and conversation before publishing refresh',
    () async {
      final message = WKMsg()
        ..channelID = 'peer'
        ..fromUID = 'message-owner'
        ..contentType = 1
        ..content = '{"type":1,"content":"hello"}';
      message.clientSeq = await MessageDB.shared.insert(message);
      final observations = <Future<List<Map<String, Object?>>>>[];
      WKIM.shared.messageManager.addOnRefreshMsgListener('lifecycle', (_) {
        observations.add(WKDBHelper.shared.getDB()!.query('conversation'));
      });

      await WKIM.shared.messageManager.updateSendResult(
        '9007199254740993',
        message.clientSeq,
        12,
        1,
      );
      final rows = await WKDBHelper.shared.getDB()!.query('message');
      expect(rows.single['message_id'], '9007199254740993');
      expect(rows.single['order_seq'], 12000);
      expect(rows.single['status'], 1);
      expect(observations, hasLength(1));
      expect(await observations.single, hasLength(1));
    },
  );

  test('send persists both projections before enqueueing the packet', () async {
    await WKIM.shared.messageManager.sendMessage(
      WKTextContent('persist first'),
      WKChannel('peer', 1),
    );
    expect(connection.sent, hasLength(1));
    expect(
      connection.persisted.single.single['content'],
      '{"content":"persist first","type":1}',
    );
    expect(
      connection.conversations.single.single['last_client_msg_no'],
      connection.sent.single.clientMsgNO,
    );
  });

  test('stale ACK is ignored after its initial asynchronous read', () async {
    final message = WKMsg()
      ..channelID = 'peer'
      ..contentType = 1;
    message.clientSeq = await MessageDB.shared.insert(message);
    var current = true;
    final completion = WKIM.shared.messageManager.updateSendResult(
      '90',
      message.clientSeq,
      9,
      1,
      isCurrent: () => current,
    );
    current = false;
    await completion;
    final rows = await WKDBHelper.shared.getDB()!.query('message');
    expect(rows.single['message_id'], '');
    expect(rows.single['status'], 0);
    expect(await WKDBHelper.shared.getDB()!.query('conversation'), isEmpty);
  });

  test(
    'session replacement after ACK write rolls the whole transaction back',
    () async {
      final message = WKMsg()
        ..channelID = 'peer'
        ..contentType = 1;
      message.clientSeq = await MessageDB.shared.insert(message);
      var ownershipChecks = 0;
      final db = WKDBHelper.shared.getDB()!;
      await WKIM.shared.messageManager.updateSendResult(
        '90',
        message.clientSeq,
        9,
        1,
        isCurrent: () {
          ownershipChecks++;
          // Admission, completed read, transaction entry, then completed write.
          return ownershipChecks < 4;
        },
      );
      expect(ownershipChecks, 4);
      final rows = await db.query('message');
      expect(rows.single['message_id'], '');
      expect(rows.single['status'], 0);
      expect(await db.query('conversation'), isEmpty);
    },
  );

  test('older ACK cannot replace the newer conversation preview', () async {
    await WKIM.shared.messageManager.sendMessage(
      WKTextContent('first'),
      WKChannel('peer', 1),
    );
    await WKIM.shared.messageManager.sendMessage(
      WKTextContent('second'),
      WKChannel('peer', 1),
    );
    final first = connection.sent.first;
    final second = connection.sent.last;
    await WKIM.shared.messageManager.updateSendResult(
      '90',
      first.clientSeq,
      9,
      1,
    );
    final rows = await WKDBHelper.shared.getDB()!.query('conversation');
    expect(rows.single['last_client_msg_no'], second.clientMsgNO);
  });

  test(
    'late attachment completion never enters a replacement session',
    () async {
      final started = Completer<void>();
      late void Function(bool, WKMsg) completeUpload;
      late WKMsg uploading;
      WKIM.shared.messageManager.addOnUploadAttachmentListener((
        message,
        complete,
      ) {
        uploading = message;
        completeUpload = complete;
        started.complete();
      });
      final sending = WKIM.shared.messageManager.sendMessage(
        WKImageContent(1, 1),
        WKChannel('peer', 1),
      );
      final outcome = expectLater(sending, throwsStateError);
      await started.future;
      WKIM.shared.options = Options()..uid = 'replacement';
      completeUpload(true, uploading);
      await outcome;
      expect(connection.sent, isEmpty);
      final rows = await WKDBHelper.shared.getDB()!.query('message');
      expect(rows.single['from_uid'], 'message-owner');
      expect(rows.single['status'], 0);
    },
  );

  test(
    'attachment completion preserves metadata and sends only once',
    () async {
      final content = WKImageContent(10, 20)
        ..localPath = '/private/photo.jpg'
        ..mentionInfo = (WKMentionInfo()..uids = ['peer']);
      WKIM.shared.messageManager.addOnUploadAttachmentListener((
        message,
        complete,
      ) {
        (message.messageContent! as WKImageContent).url =
            'https://media.test/photo';
        complete(true, message);
        complete(true, message);
      });
      await WKIM.shared.messageManager.sendMessage(
        content,
        WKChannel('peer', 1),
      );
      expect(connection.sent, hasLength(1));
      final wire =
          jsonDecode(connection.sent.single.content) as Map<String, dynamic>;
      expect(wire['url'], 'https://media.test/photo');
      expect(wire['mention'], {
        'uids': ['peer'],
      });
      expect(wire.containsKey('localPath'), isFalse);
      final saved = jsonDecode(
        connection.persisted.single.single['content']! as String,
      );
      expect(saved['localPath'], '/private/photo.jpg');
      expect(saved['mention'], {
        'uids': ['peer'],
      });
    },
  );

  test(
    'attachment uploader cannot mutate the admitted channel identity',
    () async {
      WKIM.shared.messageManager.addOnUploadAttachmentListener((
        message,
        complete,
      ) {
        message.channelID = 'different-peer';
        complete(true, message);
      });
      await expectLater(
        WKIM.shared.messageManager.sendMessage(
          WKImageContent(10, 20),
          WKChannel('peer', 1),
        ),
        throwsStateError,
      );
      expect(connection.sent, isEmpty);
      final rows = await WKDBHelper.shared.getDB()!.query('message');
      expect(rows.single['channel_id'], 'peer');
    },
  );
}

class _RecordingConnection implements WKConnectionManager {
  final sent = <WKMsg>[];
  final persisted = <List<Map<String, Object?>>>[];
  final conversations = <List<Map<String, Object?>>>[];

  @override
  Future<void> sendMessage(WKMsg message) async {
    persisted.add(await WKDBHelper.shared.getDB()!.query('message'));
    conversations.add(await WKDBHelper.shared.getDB()!.query('conversation'));
    sent.add(message);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
