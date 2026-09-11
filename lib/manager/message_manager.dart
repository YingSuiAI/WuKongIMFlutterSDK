import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'package:sqflite/sqflite.dart';

import 'package:uuid/uuid.dart';
import 'package:wukongimfluttersdk/common/logs.dart';
import 'package:wukongimfluttersdk/db/const.dart';
import 'package:wukongimfluttersdk/db/conversation.dart';
import 'package:wukongimfluttersdk/db/message.dart';
import 'package:wukongimfluttersdk/db/reaction.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/model/wk_media_message_content.dart';
import 'package:wukongimfluttersdk/proto/proto.dart';
import 'package:wukongimfluttersdk/type/const.dart';

import '../entity/channel.dart';
import '../db/wk_db_helper.dart';
import '../entity/conversation.dart';
import '../model/wk_message_content.dart';
import '../model/wk_unknown_content.dart';
import '../wkim.dart';

/// Binds asynchronous message work to the session and database that admitted it.
class _MessageOwner {
  final Database? database = WKDBHelper.shared.getDB();
  final options = WKIM.shared.options;
  final String? uid = WKIM.shared.options.uid;
  final identity = WKIM.shared.options.sessionIdentity;

  bool get isCurrent =>
      identical(options, WKIM.shared.options) &&
      identity == WKIM.shared.options.sessionIdentity &&
      identical(database, WKDBHelper.shared.getDB());

  void ensureCurrent() {
    if (!isCurrent) throw StateError('The message session has been replaced.');
  }
}

class _StaleMessageOperation implements Exception {
  const _StaleMessageOperation();
}

class WKMessageManager {
  WKMessageManager._privateConstructor();
  static final WKMessageManager _instance =
      WKMessageManager._privateConstructor();
  static WKMessageManager get shared => _instance;

  final Map<int, WKMessageContent Function(dynamic data)> _msgContentList =
      HashMap<int, WKMessageContent Function(dynamic data)>();
  Function(WKMsg wkMsg, Function(bool isSuccess, WKMsg wkMsg))?
  _uploadAttachmentBack;
  Function(WKMsg msg)? _msgInsertedBack;
  Function(WKMsgExtra)? _iUploadMsgExtraListener;
  HashMap<String, Function(List<WKMsg>)>? _newMsgBack;
  HashMap<String, Function(WKMsg)>? _refreshMsgBack;
  HashMap<String, Function(String)>? _deleteMsgBack;
  HashMap<String, Function(String, int)>? _clearChannelMsgBack;
  Function(
    String channelID,
    int channelType,
    int startMessageSeq,
    int endMessageSeq,
    int limit,
    int pullMode,
    Function(WKSyncChannelMsg?) back,
  )?
  _syncChannelMsgBack;

  final int wkOrderSeqFactor = 1000;

  void registerMsgContent(
      int type, WKMessageContent Function(dynamic data) createMsgContent) {
    _msgContentList[type] = createMsgContent;
  }

  WKMessageContent? getMessageModel(int type, dynamic json) {
    WKMessageContent? content;
    if (_msgContentList.containsKey(type)) {
      var messageCreateCallback = _msgContentList[type];
      if (messageCreateCallback != null) {
        content = messageCreateCallback(json);
      }
    }
    content ??= WKUnknownContent();
    // 回复
    var replyJson = json['reply'];
    if (replyJson != null) {
      var reply = WKReply().decode(replyJson);
      content.reply = reply;
    }
    // var entities = WKDBConst.readString(json, 'entities');
    var jsonArray = json['entities'];
    if (jsonArray != null && jsonArray is List) {
      // var jsonArray = jsonDecode(entities);
      List<WKMsgEntity> list = [];
      for (var entityJson in jsonArray) {
        WKMsgEntity entity = WKMsgEntity();
        entity.type = WKDBConst.readString(entityJson, 'type');
        entity.offset = WKDBConst.readInt(entityJson, 'offset');
        entity.length = WKDBConst.readInt(entityJson, 'length');
        entity.value = WKDBConst.readString(entityJson, 'value');
        list.add(entity);
      }
      content.entities = list;
    }
    // 解析艾特
    var mentionJson = json['mention'];
    if (mentionJson != null) {
      var mentionInfo = WKMentionInfo();
      var mentionAll = WKDBConst.readInt(mentionJson, 'all');
      var uidList = mentionJson['uids'];
      if (uidList != null && uidList is List) {
        List<String> uids = [];
        for (var uid in uidList) {
          uids.add(uid);
          if (uid == WKIM.shared.options.uid) {
            mentionInfo.isMentionMe = true;
          }
        }
        mentionInfo.uids = uids;
      }
      if (mentionAll == 1) {
        mentionInfo.mentionAll = true;
        mentionInfo.isMentionMe = true;
      }
      content.mentionInfo = mentionInfo;
    }
    return content;
  }

  void parsingMsg(WKMsg wkMsg, {
    String transportFromUID = '',
    String transportChannelID = '',
    int transportChannelType = 0,
  }) {
    if (wkMsg.content == '') {
      wkMsg.contentType = WkMessageContentType.contentFormatError;
      return;
    }
    try {
      dynamic json = jsonDecode(wkMsg.content);
      if (json == null) {
        wkMsg.contentType = WkMessageContentType.contentFormatError;
        return;
      }
      if (wkMsg.fromUID == "") {
        wkMsg.fromUID = WKDBConst.readString(json, 'from_uid');
      }
      if (wkMsg.channelType == WKChannelType.personal &&
          wkMsg.channelID != '' &&
          wkMsg.fromUID != '' &&
          wkMsg.channelID == WKIM.shared.options.uid) {
        wkMsg.channelID = wkMsg.fromUID;
      }
      if (wkMsg.contentType == WkMessageContentType.insideMsg) {
        if (json != null) {
          json['channel_id'] = wkMsg.channelID;
          json['channel_type'] = wkMsg.channelType;
        }
        WKIM.shared.cmdManager.handleCMD(
          json,
          fromUID: transportFromUID,
          channelID: transportChannelID,
          channelType: transportChannelType,
        );
      }
    } catch (e) {
      wkMsg.contentType = WkMessageContentType.contentFormatError;
      Logs.error('parsingMsg error: $e');
    }
  }

  // 全局搜索
  Future<List<WKMessageSearchResult>> search(String keyword) {
    return MessageDB.shared.search(keyword);
  }

  /*
     * 搜索某个频道到消息
     *
     * @param searchKey   关键字
     * @param channelID   频道ID
     * @param channelType 频道类型
     * @return List<WKMsg>
     */
  Future<List<WKMsg>> searchWithChannel(
      String keyword, String channelID, int channelType) {
    return MessageDB.shared.searchWithChannel(keyword, channelID, channelType);
  }

  /*
     * 查询某个频道的固定类型消息
     *
     * @param channelID      频道ID
     * @param channelType    频道列席
     * @param oldestOrderSeq 最后一次消息大orderSeq
     * @param limit          每次获取数量
     * @param contentTypes   消息内容类型
     * @return List<WKMsg>
     */
  Future<List<WKMsg>> searchMsgWithChannelAndContentTypes(String channelID,
      int channelType, int oldestOrderSeq, int limit, List<int> contentTypes) {
    return MessageDB.shared.searchMsgWithChannelAndContentTypes(
        channelID, channelType, oldestOrderSeq, limit, contentTypes);
  }

  Future<WKMsg?> getWithClientMsgNo(String clientMsgNo) {
    return MessageDB.shared.queryWithClientMsgNo(clientMsgNo);
  }

  Future<int> saveMsg(WKMsg msg, {DatabaseExecutor? database}) async {
    return await MessageDB.shared.insert(msg, database: database);
  }

  String generateClientMsgNo() {
    return "${const Uuid().v4().toString().replaceAll("-", "")}5";
  }

  Future<int> getMessageOrderSeq(
      int messageSeq, String channelID, int channelType) async {
    if (messageSeq == 0) {
      int tempOrderSeq =
          await MessageDB.shared.queryMaxOrderSeq(channelID, channelType);
      return tempOrderSeq + 1;
    }
    return messageSeq * wkOrderSeqFactor;
  }

  Future<int> updateViewedAt(int viewedAt, String clientMsgNO) async {
    dynamic json = <String, Object>{};
    json['viewed'] = 1;
    json['viewed_at'] = viewedAt;
    return MessageDB.shared.updateMsgWithFieldAndClientMsgNo(json, clientMsgNO);
  }

  Future<int> getMaxExtraVersionWithChannel(
      String channelID, int channelType) async {
    return MessageDB.shared
        .queryMaxExtraVersionWithChannel(channelID, channelType);
  }

  Future<void> saveRemoteExtraMsg(List<WKMsgExtra> list) async {
    MessageDB.shared.insertMsgExtras(list);
    List<String> msgIds = [];
    List<String> deletedMsgIds = [];

    // 创建 Map 用于快速查找，避免嵌套循环
    Map<String, WKMsgExtra> extraMap = {};
    for (var extra in list) {
      msgIds.add(extra.messageID);
      extraMap[extra.messageID] = extra;
      if (extra.isMutualDeleted == 1) {
        deletedMsgIds.add(extra.messageID);
      }
    }

    var msgList = await MessageDB.shared.queryWithMessageIds(msgIds);
    for (var msg in msgList) {
      msg.wkMsgExtra ??= WKMsgExtra();
      var extra = extraMap[msg.messageID];
      if (extra != null) {
        msg.wkMsgExtra!.readed = extra.readed;
        msg.wkMsgExtra!.readedCount = extra.readedCount;
        msg.wkMsgExtra!.unreadCount = extra.unreadCount;
        msg.wkMsgExtra!.revoke = extra.revoke;
        msg.wkMsgExtra!.revoker = extra.revoker;
        msg.wkMsgExtra!.isMutualDeleted = extra.isMutualDeleted;
        msg.wkMsgExtra!.editedAt = extra.editedAt;
        msg.wkMsgExtra!.contentEdit = extra.contentEdit;
        msg.wkMsgExtra!.extraVersion = extra.extraVersion;
        if (extra.contentEdit != '') {
          try {
            dynamic contentJson = jsonDecode(extra.contentEdit);
            msg.wkMsgExtra!.messageContent = WKIM.shared.messageManager
                .getMessageModel(WkMessageContentType.text, contentJson);
          } catch (e) {
            Logs.error('saveRemoteExtraMsg jsonDecode error: $e');
          }
        }
      }
      setRefreshMsg(msg);
    }
    if (deletedMsgIds.isNotEmpty) {
      MessageDB.shared.deleteWithMessageIDs(deletedMsgIds);
    }
  }

  void setSyncChannelMsgListener(
      String channelID,
      int channelType,
      int startMessageSeq,
      int endMessageSeq,
      int limit,
      int pullMode,
      Function(WKSyncChannelMsg?) back) async {
    if (_syncChannelMsgBack != null) {
      _syncChannelMsgBack!(channelID, channelType, startMessageSeq,
          endMessageSeq, limit, pullMode, (result) async {
        if (result != null && result.messages != null) {
          _saveSyncChannelMSGs(result.messages!).then((value) => back(result));
        } else {
          back(result);
        }
      });
    } else {
      Logs.error('未提供同步频道消息事件');
      back(null);
    }
  }

  Future<bool> _saveSyncChannelMSGs(List<WKSyncMsg> list) async {
    List<WKMsg> msgList = [];
    List<WKMsgExtra> msgExtraList = [];
    List<WKMsgReaction> msgReactionList = [];
    for (int j = 0, len = list.length; j < len; j++) {
      WKMsg wkMsg = list[j].getWKMsg();
      msgList.add(wkMsg);
      if (list[j].messageExtra != null) {
        WKMsgExtra extra = wkSyncExtraMsg2WKMsgExtra(
            wkMsg.channelID, wkMsg.channelType, list[j].messageExtra!);
        msgExtraList.add(extra);
      }
      if (wkMsg.reactionList != null && wkMsg.reactionList!.isNotEmpty) {
        msgReactionList.addAll(wkMsg.reactionList!);
      }
    }
    bool isSuccess = true;
    if (msgExtraList.isNotEmpty) {
      isSuccess = await MessageDB.shared.insertMsgExtras(msgExtraList);
    }
    if (msgList.isNotEmpty) {
      isSuccess = await MessageDB.shared.insertMsgList(msgList);
    }
    if (msgReactionList.isNotEmpty) {
      isSuccess =
          await ReactionDB.shared.insertOrUpdateReactionList(msgReactionList);
    }
    return isSuccess;
  }

  WKMsgExtra wkSyncExtraMsg2WKMsgExtra(
      String channelID, int channelType, WKSyncExtraMsg extraMsg) {
    WKMsgExtra extra = WKMsgExtra();
    extra.channelID = channelID;
    extra.channelType = channelType;
    extra.unreadCount = extraMsg.unreadCount;
    extra.readedCount = extraMsg.readedCount;
    extra.readed = extraMsg.readed;
    extra.messageID = extraMsg.messageIdStr;
    extra.isMutualDeleted = extraMsg.isMutualDeleted;
    extra.extraVersion = extraMsg.extraVersion;
    extra.revoke = extraMsg.revoke;
    extra.revoker = extraMsg.revoker;
    extra.needUpload = 0;
    if (extraMsg.contentEdit != null) {
      extra.contentEdit = jsonEncode(extraMsg.contentEdit);
    }

    extra.editedAt = extraMsg.editedAt;
    return extra;
  }

  saveMessageReactions(List<WKSyncMsgReaction> list) async {
    if (list.isEmpty) return;
    List<WKMsgReaction> reactionList = [];
    List<String> msgIds = [];
    for (int i = 0, size = list.length; i < size; i++) {
      WKMsgReaction reaction = WKMsgReaction();
      reaction.messageID = list[i].messageID;
      reaction.channelID = list[i].channelID;
      reaction.channelType = list[i].channelType;
      reaction.uid = list[i].uid;
      reaction.name = list[i].name;
      reaction.seq = list[i].seq;
      reaction.emoji = list[i].emoji;
      reaction.isDeleted = list[i].isDeleted;
      reaction.createdAt = list[i].createdAt;
      msgIds.add(reaction.messageID);
      reactionList.add(reaction);
    }
    await ReactionDB.shared.insertOrUpdateReactionList(reactionList);
    List<WKMsg> msgList = await MessageDB.shared.queryWithMessageIds(msgIds);
    getMsgReactionsAndRefreshMsg(msgIds, msgList);
  }

  getMsgReactionsAndRefreshMsg(
      List<String> messageIds, List<WKMsg> updatedMsgList) async {
    List<WKMsgReaction> reactionList =
        await ReactionDB.shared.queryWithMessageIds(messageIds);
    for (int i = 0, size = updatedMsgList.length; i < size; i++) {
      for (int j = 0, len = reactionList.length; j < len; j++) {
        if (updatedMsgList[i].messageID == reactionList[j].messageID) {
          if (updatedMsgList[i].reactionList == null) {
            updatedMsgList[i].reactionList = [];
          }
          updatedMsgList[i].reactionList!.add(reactionList[j]);
        }
      }
      setRefreshMsg(updatedMsgList[i]);
    }
  }

  /*
     * 查询或同步某个频道消息
     *
     * @param channelId                频道ID
     * @param channelType              频道类型
     * @param oldestOrderSeq           最后一次消息大orderSeq 第一次进入聊天传入0
     * @param contain                  是否包含 oldestOrderSeq 这条消息
     * @param pullMode                 拉取模式 0:向下拉取 1:向上拉取
     * @param aroundMsgOrderSeq        查询此消息附近消息
     * @param limit                    每次获取数量
     * @param iGetOrSyncHistoryMsgBack 请求返还
     */
  getOrSyncHistoryMessages(
      String channelId,
      int channelType,
      int oldestOrderSeq,
      bool contain,
      int pullMode,
      int limit,
      int aroundMsgOrderSeq,
      final Function(List<WKMsg>) iGetOrSyncHistoryMsgBack,
      final Function() syncBack) async {
    if (aroundMsgOrderSeq != 0) {
      int maxMsgSeq = await getMaxMessageSeq(channelId, channelType);
      int aroundMsgSeq = getOrNearbyMsgSeq(aroundMsgOrderSeq);

      if (maxMsgSeq >= aroundMsgSeq && maxMsgSeq - aroundMsgSeq <= limit) {
        // 显示最后一页数据
//                oldestOrderSeq = 0;
        oldestOrderSeq =
            await getMessageOrderSeq(maxMsgSeq, channelId, channelType);
        contain = true;
        pullMode = 0;
      } else {
        int minOrderSeq = await MessageDB.shared
            .getOrderSeq(channelId, channelType, aroundMsgOrderSeq, 3);
        if (minOrderSeq == 0) {
          oldestOrderSeq = aroundMsgOrderSeq;
        } else {
          if (minOrderSeq + limit < aroundMsgOrderSeq) {
            if (aroundMsgOrderSeq % wkOrderSeqFactor == 0) {
              oldestOrderSeq = ((aroundMsgOrderSeq / wkOrderSeqFactor - 3) *
                      wkOrderSeqFactor)
                  .toInt();
            } else {
              oldestOrderSeq = aroundMsgOrderSeq - 3;
            }
          } else {
            // todo 这里只会查询3条数据  oldestOrderSeq = minOrderSeq
            int startOrderSeq = await MessageDB.shared
                .getOrderSeq(channelId, channelType, aroundMsgOrderSeq, limit);
            if (startOrderSeq == 0) {
              oldestOrderSeq = aroundMsgOrderSeq;
            } else {
              oldestOrderSeq = startOrderSeq;
            }
          }
        }
        pullMode = 1;
        contain = true;
      }
    }
    MessageDB.shared.getOrSyncHistoryMessages(
        channelId,
        channelType,
        oldestOrderSeq,
        contain,
        pullMode,
        limit,
        iGetOrSyncHistoryMsgBack,
        syncBack);
  }

  int getOrNearbyMsgSeq(int orderSeq) {
    if (orderSeq % wkOrderSeqFactor == 0) {
      return orderSeq ~/ wkOrderSeqFactor;
    }
    return (orderSeq - orderSeq % wkOrderSeqFactor) ~/ wkOrderSeqFactor;
  }

  Future<int> getMaxMessageSeq(String channelID, int channelType) {
    return MessageDB.shared.getMaxMessageSeq(channelID, channelType);
  }

  pushNewMsg(List<WKMsg> list) {
    if (_newMsgBack != null) {
      _newMsgBack!.forEach((key, back) {
        back(list);
      });
    }
  }

  addOnNewMsgListener(String key, Function(List<WKMsg>) newMsgListener) {
    _newMsgBack ??= HashMap();
    if (key != '') {
      _newMsgBack![key] = newMsgListener;
    }
  }

  removeNewMsgListener(String key) {
    if (_newMsgBack != null) {
      _newMsgBack!.remove(key);
    }
  }

  addOnClearChannelMsgListener(String key, Function(String, int) back) {
    _clearChannelMsgBack ??= HashMap();
    if (key != '') {
      _clearChannelMsgBack![key] = back;
    }
  }

  removeClearChannelMsgListener(String key) {
    if (_clearChannelMsgBack != null) {
      _clearChannelMsgBack!.remove(key);
    }
  }

  _setClearChannelMsg(String channelID, int channelType) {
    if (_clearChannelMsgBack != null) {
      _clearChannelMsgBack!.forEach((key, back) {
        back(channelID, channelType);
      });
    }
  }

  addOnDeleteMsgListener(String key, Function(String) back) {
    _deleteMsgBack ??= HashMap();
    if (key != '') {
      _deleteMsgBack![key] = back;
    }
  }

  removeDeleteMsgListener(String key) {
    if (_deleteMsgBack != null) {
      _deleteMsgBack!.remove(key);
    }
  }

  _setDeleteMsg(String clientMsgNo) {
    if (_deleteMsgBack != null) {
      _deleteMsgBack!.forEach((key, back) {
        back(clientMsgNo);
      });
    }
  }

  _setUploadMsgExtra(WKMsgExtra extra) {
    if (_iUploadMsgExtraListener != null) {
      _iUploadMsgExtraListener!(extra);
    }
    Future.delayed(const Duration(seconds: 5), () {
      _startCheckTimer();
    });
  }

  Timer? checkMsgNeedUploadTimer;
  _startCheckTimer() {
    _stopCheckMsgNeedUploadTimer();
    checkMsgNeedUploadTimer =
        Timer.periodic(const Duration(seconds: 5), (timer) async {
      var list = await MessageDB.shared.queryMsgExtraWithNeedUpload(1);
      if (list.isNotEmpty) {
        for (var extra in list) {
          if (_iUploadMsgExtraListener != null) {
            _iUploadMsgExtraListener!(extra);
          }
        }
      } else {
        _stopCheckMsgNeedUploadTimer();
      }
    });
  }

  _stopCheckMsgNeedUploadTimer() {
    if (checkMsgNeedUploadTimer != null) {
      checkMsgNeedUploadTimer!.cancel();
      checkMsgNeedUploadTimer = null;
    }
  }

  addOnUploadMsgExtra(Function(WKMsgExtra) back) {
    _iUploadMsgExtraListener = back;
  }

  addOnRefreshMsgListener(String key, Function(WKMsg) back) {
    _refreshMsgBack ??= HashMap();
    if (key != '') {
      _refreshMsgBack![key] = back;
    }
  }

  removeOnRefreshMsgListener(String key) {
    if (_refreshMsgBack != null) {
      _refreshMsgBack!.remove(key);
    }
  }

  setRefreshMsg(WKMsg wkMsg) {
    if (_refreshMsgBack != null) {
      _refreshMsgBack!.forEach((key, back) {
        back(wkMsg);
      });
    }
  }

  addOnSyncChannelMsgListener(
      Function(
              String channelID,
              int channelType,
              int startMessageSeq,
              int endMessageSeq,
              int limit,
              int pullMode,
              Function(WKSyncChannelMsg?) back)?
          syncChannelMsgListener) {
    _syncChannelMsgBack = syncChannelMsgListener;
  }

  setOnMsgInserted(WKMsg wkMsg) {
    if (_msgInsertedBack != null) {
      _msgInsertedBack!(wkMsg);
    }
  }

  addOnMsgInsertedListener(Function(WKMsg) insertListener) {
    _msgInsertedBack = insertListener;
  }

  addOnUploadAttachmentListener(Function(WKMsg, Function(bool, WKMsg)) back) {
    _uploadAttachmentBack = back;
  }

  Future<void> sendMessage(WKMessageContent messageContent, WKChannel channel) {
    return sendWithOption(messageContent, channel, WKSendOptions());
  }

  /// Persists a newly authored message and its conversation as one operation.
  /// The caller owns envelope encoding, UI publication, and transport admission.
  Future<WKUIConversationMsg?> saveOutgoingMessage(
    WKMsg message, {
    bool Function()? isCurrent,
  }) async {
    final owner = _MessageOwner();
    void ensureCurrent() {
      owner.ensureCurrent();
      if (!(isCurrent?.call() ?? true)) {
        throw StateError('The outgoing message operation was cancelled.');
      }
    }

    ensureCurrent();
    final database = owner.database;
    if (database == null) throw StateError('Message database is not open.');
    if (message.clientSeq != 0) {
      throw StateError('An outgoing message must not already be persisted.');
    }
    final saved = await database.transaction((transaction) async {
      ensureCurrent();
      final orderSeq = await MessageDB.shared.queryMaxOrderSeq(
        message.channelID, message.channelType, database: transaction) + 1;
      ensureCurrent();
      final values = MessageDB.shared.getMap(message) as Map<String, dynamic>;
      values['order_seq'] = orderSeq;
      // A duplicate idempotency key is an error, never a renamed tombstone.
      final clientSeq = await transaction.insert(
        WKDBConst.tableMessage, values,
        conflictAlgorithm: ConflictAlgorithm.abort);
      ensureCurrent();
      final conversation = await WKIM.shared.conversationManager.saveWithWKMsg(
        message, 0, database: transaction);
      ensureCurrent();
      return (clientSeq, orderSeq, conversation);
    });
    ensureCurrent();
    message.clientSeq = saved.$1;
    message.orderSeq = saved.$2;
    return saved.$3;
  }

  Future<void> sendWithOption(
    WKMessageContent messageContent,
    WKChannel channel,
    WKSendOptions options,
  ) async {
    final owner = _MessageOwner();
    if (owner.uid == null || owner.uid!.isEmpty) {
      throw StateError('A message requires an authenticated owner.');
    }
    if (!options.header.noPersist && owner.database == null) {
      throw StateError('Message database is not open.');
    }
    WKMsg wkMsg = WKMsg();
    wkMsg.setting = options.setting;
    wkMsg.header = options.header;
    wkMsg.messageContent = messageContent;
    wkMsg.topicID = options.topicID;
    wkMsg.expireTime = options.expire;
    if (wkMsg.expireTime > 0) {
      wkMsg.expireTimestamp = wkMsg.timestamp + wkMsg.expireTime;
    }
    wkMsg.channelID = channel.channelID;
    wkMsg.channelType = channel.channelType;
    wkMsg.fromUID = owner.uid!;
    wkMsg.contentType = messageContent.contentType;

    wkMsg.content = _getSendPayload(wkMsg);
    wkMsg.setChannelInfo(channel);
    WKChannel? from = await WKIM.shared.channelManager.getChannel(
      wkMsg.fromUID,
      WKChannelType.personal,
    );
    owner.ensureCurrent();
    if (from != null) {
      wkMsg.setFrom(from);
    }
    if (!options.header.noPersist) {
      final uiMsg = await saveOutgoingMessage(wkMsg,
        isCurrent: () => owner.isCurrent);
      owner.ensureCurrent();
      setOnMsgInserted(wkMsg);
      owner.ensureCurrent();
      if (uiMsg != null) {
        WKIM.shared.conversationManager.setRefreshUIMsgs([uiMsg]);
      }
    }

    owner.ensureCurrent();
    if (wkMsg.messageContent is WKMediaMessageContent) {
      final upload = _uploadAttachmentBack;
      if (upload == null) {
        await updateMsgStatusFail(wkMsg.clientSeq);
        throw StateError('An attachment upload listener is required.');
      }
      final completion = Completer<(bool, WKMsg)>();
      final messageIdentity = (wkMsg.clientMsgNO, wkMsg.clientSeq,
        wkMsg.fromUID, wkMsg.channelID, wkMsg.channelType);
      upload(wkMsg, (success, uploaded) {
        if (!completion.isCompleted) completion.complete((success, uploaded));
      });
      final result = await completion.future;
      owner.ensureCurrent();
      if (!result.$1) {
        await updateMsgStatusFail(wkMsg.clientSeq);
        throw StateError('Attachment upload failed.');
      }
      final uploadedMsg = result.$2;
      if ((uploadedMsg.clientMsgNO, uploadedMsg.clientSeq, uploadedMsg.fromUID,
          uploadedMsg.channelID, uploadedMsg.channelType) != messageIdentity) {
        throw StateError('Attachment upload changed the message identity.');
      }
      final payload = _getSendPayload(uploadedMsg);
      if (!options.header.noPersist) {
        await MessageDB.shared.updateMsgWithFieldAndClientMsgNo(
          {'content': payload},
          wkMsg.clientMsgNO,
          database: owner.database,
        );
        owner.ensureCurrent();
      }
      final sendJson = jsonDecode(payload) as Map<String, dynamic>;
      sendJson.remove('localPath');
      sendJson.remove('coverLocalPath');
      uploadedMsg.content = jsonEncode(sendJson);
      await WKIM.shared.connectionManager.sendMessage(uploadedMsg);
    } else {
      await WKIM.shared.connectionManager.sendMessage(wkMsg);
    }
  }

  @Deprecated('use sendWithOption')
  sendMessageWithSetting(
    WKMessageContent messageContent,
    WKChannel channel,
    Setting setting,
  ) {
    var header = MessageHeader();
    header.redDot = true;
    return sendMessageWithSettingAndHeader(
      messageContent,
      channel,
      setting,
      header,
    );
  }

  @Deprecated('use sendWithOption')
  sendMessageWithSettingAndHeader(
    WKMessageContent messageContent,
    WKChannel channel,
    Setting setting,
    MessageHeader header,
  ) async {
    var options = WKSendOptions();
    options.setting = setting;
    options.header = header;
    return sendWithOption(messageContent, channel, options);
  }

  String _getSendPayload(WKMsg wkMsg) {
    dynamic json = wkMsg.messageContent!.encodeJson();
    json['type'] = wkMsg.contentType;
    if (wkMsg.messageContent!.reply != null) {
      json['reply'] = wkMsg.messageContent!.reply!.encode();
    }

    if (wkMsg.messageContent!.entities != null &&
        wkMsg.messageContent!.entities!.isNotEmpty) {
      var jsonArray = [];
      for (WKMsgEntity entity in wkMsg.messageContent!.entities!) {
        var jo = <String, dynamic>{};
        jo['offset'] = entity.offset;
        jo['length'] = entity.length;
        jo['type'] = entity.type;
        jo['value'] = entity.value;
        jsonArray.add(jo);
      }
      json['entities'] = jsonArray;
    }
    // 解析艾特
    if (wkMsg.messageContent!.mentionInfo != null) {
      var mentionJson = {};
      if (wkMsg.messageContent!.mentionInfo!.mentionAll) {
        mentionJson['all'] = 1;
      }
      if (wkMsg.messageContent!.mentionInfo!.uids != null &&
          wkMsg.messageContent!.mentionInfo!.uids!.isNotEmpty) {
        var jsonArray = [];
        for (String uid in wkMsg.messageContent!.mentionInfo!.uids!) {
          jsonArray.add(uid);
        }
        mentionJson['uids'] = jsonArray;
      }
      json['mention'] = mentionJson;
    }
    return jsonEncode(json);
  }

  Future<void> updateSendResult(
    String messageID,
    int clientSeq,
    int messageSeq,
    int reasonCode, {
    String applicationMessageID = '',
    bool Function()? isCurrent,
  }) async {
    final owner = _MessageOwner();
    bool current() => owner.isCurrent && (isCurrent?.call() ?? true);
    if (owner.database == null || !current()) return;
    try {
      final acknowledged = await owner.database!.transaction((
        transaction,
      ) async {
        void ensureCurrent() {
          if (!current()) throw const _StaleMessageOperation();
        }

        ensureCurrent();
        // Read inside the same transaction as the ACK update: a source RECV
        // may already have replaced the request body while this ACK was queued.
        final wkMsg = await MessageDB.shared.queryWithClientSeq(
          clientSeq,
          database: transaction,
        );
        ensureCurrent();
        if (wkMsg == null) return null;
        // Reliable source delivery is already positive commit evidence. A late
        // failed ACK must not erase its native identity or regress its status.
        if (wkMsg.payloadCommitted) {
          if (reasonCode != WKSendMsgResult.sendSuccess) return null;
          if (wkMsg.messageID != messageID || wkMsg.messageSeq != messageSeq) {
            throw StateError('SENDACK conflicts with committed source identity.');
          }
        }
        wkMsg.messageID = messageID;
        wkMsg.applicationMessageID = applicationMessageID;
        wkMsg.messageSeq = messageSeq;
        wkMsg.status = reasonCode;
        wkMsg.orderSeq = messageSeq == 0
            ? wkMsg.orderSeq
            : messageSeq * wkOrderSeqFactor;
        final map = <String, Object>{
          'message_id': messageID,
          'message_seq': messageSeq,
          'status': reasonCode,
          'order_seq': wkMsg.orderSeq,
        };
        await MessageDB.shared.updateMsgWithField(
          map,
          clientSeq,
          database: transaction,
        );
        ensureCurrent();
        final last = await ConversationDB.shared.queryMsgByMsgChannelId(
          wkMsg.channelID,
          wkMsg.channelType,
          database: transaction,
        );
        ensureCurrent();
        // An older SENDACK must not replace a newer conversation preview.
        if (wkMsg.isDeleted == 0 &&
            (last == null || last.lastClientMsgNO == wkMsg.clientMsgNO)) {
          await WKIM.shared.conversationManager.saveWithWKMsg(
            wkMsg,
            0,
            database: transaction,
          );
        }
        ensureCurrent();
        return wkMsg;
      });
      if (current() && acknowledged != null) {
        setRefreshMsg(acknowledged);
      }
    } on _StaleMessageOperation {
      return;
    }
  }

  Future<void> updateMsgStatusFail(int clientMsgSeq) async {
    final owner = _MessageOwner();
    if (owner.database == null) return;
    var map = <String, Object>{};
    map['status'] = WKSendMsgResult.sendFail;
    int row = await owner.database!.update(
      WKDBConst.tableMessage,
      map,
      where: 'client_seq = ? AND status = ? AND payload_committed = 0',
      whereArgs: [clientMsgSeq, WKSendMsgResult.sendLoading],
    );
    if (row > 0 && owner.isCurrent) {
      final wkMsg = await MessageDB.shared.queryWithClientSeq(
        clientMsgSeq,
        database: owner.database,
      );
      if (wkMsg != null && owner.isCurrent) setRefreshMsg(wkMsg);
    }
  }

  updateContent(String clientMsgNO, WKMessageContent messageContent,
      bool isRefreshUI) async {
    WKMsg? wkMsg = await MessageDB.shared.queryWithClientMsgNo(clientMsgNO);
    if (wkMsg != null) {
      var map = <String, Object>{};
      dynamic json = messageContent.encodeJson();
      json['type'] = wkMsg.contentType;

      map['content'] = jsonEncode(json);
      int result = await MessageDB.shared
          .updateMsgWithFieldAndClientMsgNo(map, clientMsgNO);
      if (isRefreshUI && result > 0) {
        wkMsg.messageContent = messageContent;
        wkMsg.content = _getSendPayload(wkMsg);
        setRefreshMsg(wkMsg);
      }
    }
  }

  Future<void> updateSendingMsgFail() {
    return MessageDB.shared.updateSendingMsgFail();
  }

  updateLocalExtraWithClientMsgNo(
      String clientMsgNO, Map<String, dynamic>? data) async {
    WKMsg? wkMsg = await MessageDB.shared.queryWithClientMsgNo(clientMsgNO);
    if (wkMsg != null) {
      var map = <String, Object>{};
      map['extra'] = WKDBConst.safeJsonEncode(data);
      int result = await MessageDB.shared
          .updateMsgWithFieldAndClientMsgNo(map, clientMsgNO);
      if (result > 0) {
        wkMsg.localExtraMap = data;
        setRefreshMsg(wkMsg);
      }
    }
  }

  updateMsgEdit(String messageID, String channelID, int channelType,
      Map<String, dynamic> content) async {
    var msgExtra = await MessageDB.shared.queryMsgExtraWithMsgID(messageID);
    msgExtra ??= WKMsgExtra();
    msgExtra.messageID = messageID;
    msgExtra.channelID = channelID;
    msgExtra.channelType = channelType;
    msgExtra.editedAt =
        (DateTime.now().millisecondsSinceEpoch / 1000).truncate();
    msgExtra.contentEdit = jsonEncode(content);
    msgExtra.needUpload = 1;
    List<WKMsgExtra> list = [];
    list.add(msgExtra);
    List<String> messageIds = [];
    messageIds.add(messageID);
    var result = await MessageDB.shared.insertMsgExtras(list);
    if (result) {
      var wkMsgs = await MessageDB.shared.queryWithMessageIds(messageIds);
      getMsgReactionsAndRefreshMsg(messageIds, wkMsgs);
      _setUploadMsgExtra(msgExtra);
    }
  }

  clearWithChannel(String channelId, int channelType) async {
    int row = await MessageDB.shared.deleteWithChannel(channelId, channelType);
    if (row > 0) {
      _setClearChannelMsg(channelId, channelType);
    }
  }

  deleteWithClientMsgNo(String clientMsgNo) async {
    var map = <String, Object>{};
    map['is_deleted'] = 1;

    var result = await MessageDB.shared
        .updateMsgWithFieldAndClientMsgNo(map, clientMsgNo);
    if (result > 0) {
      _setDeleteMsg(clientMsgNo);
      var wkMsg = await getWithClientMsgNo(clientMsgNo);
      if (wkMsg != null) {
        var coverMsg = await ConversationDB.shared
            .queryMsgByMsgChannelId(wkMsg.channelID, wkMsg.channelType);
        if (coverMsg != null && coverMsg.lastClientMsgNO == clientMsgNo) {
          var tempMsg = await MessageDB.shared.queryMaxOrderSeqMsgWithChannel(
              wkMsg.channelID, wkMsg.channelType);
          if (tempMsg != null) {
            var uiMsg =
                await WKIM.shared.conversationManager.saveWithWKMsg(tempMsg, 0);
            if (uiMsg != null) {
              List<WKUIConversationMsg> uiMsgs = [];
              uiMsgs.add(uiMsg);
              WKIM.shared.conversationManager.setRefreshUIMsgs(uiMsgs);
            }
          }
        }
      }
    }
  }

  Future<int> getMaxReactionSeqWithChannel(String channelID, int channelType) {
    return ReactionDB.shared.queryMaxSeqWithChannel(channelID, channelType);
  }
}
