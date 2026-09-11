import 'package:wukongimfluttersdk/common/mode.dart';
import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/manager/channel_manager.dart';
import 'package:wukongimfluttersdk/manager/channel_member_manager.dart';
import 'package:wukongimfluttersdk/manager/cmd_manager.dart';
import 'package:wukongimfluttersdk/manager/conversation_manager.dart';
import 'package:wukongimfluttersdk/manager/message_manager.dart';
import 'package:wukongimfluttersdk/manager/reminder_manager.dart';
import 'package:wukongimfluttersdk/model/wk_image_content.dart';
import 'package:wukongimfluttersdk/model/wk_text_content.dart';
import 'package:wukongimfluttersdk/model/wk_video_content.dart';
import 'package:wukongimfluttersdk/model/wk_voice_content.dart';
import 'package:wukongimfluttersdk/type/const.dart';

import 'common/options.dart';
import 'manager/connect_manager.dart';
import 'manager/event_manager.dart';
import 'model/wk_card_content.dart';
import 'proto/proto.dart';

class WKIM {
  WKIM._privateConstructor();
  int deviceFlagApp = 0;
  static final WKIM _instance = WKIM._privateConstructor();

  static WKIM get shared => _instance;
  Model runMode = Model.app;
  Options options = Options();
  int _setupGeneration = 0;

  Future<bool> setup(Options opts) async {
    if (opts.protoVersion != currentProtocolVersion ||
        !opts.hasExactSessionIdentity) {
      return false;
    }
    final generation = ++_setupGeneration;
    final identity = opts.sessionIdentity;
    options = opts;
    deviceFlagApp = opts.deviceFlag;
    eventManager.reset();
    _initNormalMsgContent();
    if (isApp()) {
      final result = await WKDBHelper.shared.init();
      if (!result ||
          generation != _setupGeneration ||
          !identical(options, opts) ||
          options.sessionIdentity != identity) {
        return false;
      }
      final database = WKDBHelper.shared.getDB();
      await messageManager.updateSendingMsgFail();
      return generation == _setupGeneration &&
          identical(options, opts) &&
          options.sessionIdentity == identity &&
          database != null &&
          database.isOpen &&
          identical(database, WKDBHelper.shared.getDB());
    }
    return true;
  }

  _initNormalMsgContent() {
    messageManager.registerMsgContent(WkMessageContentType.text, (
      dynamic data,
    ) {
      return WKTextContent('').decodeJson(data);
    });
    messageManager.registerMsgContent(WkMessageContentType.card, (
      dynamic data,
    ) {
      return WKCardContent('', '').decodeJson(data);
    });
    messageManager.registerMsgContent(WkMessageContentType.image, (
      dynamic data,
    ) {
      return WKImageContent(0, 0).decodeJson(data);
    });
    messageManager.registerMsgContent(WkMessageContentType.voice, (
      dynamic data,
    ) {
      return WKVoiceContent(0).decodeJson(data);
    });
    messageManager.registerMsgContent(WkMessageContentType.video, (
      dynamic data,
    ) {
      return WKVideoContent().decodeJson(data);
    });
  }

  @Deprecated('Use Options deviceFlag')
  void setDeviceFlag(int deviceFlag) {
    deviceFlagApp = deviceFlag;
  }

  bool isApp() {
    return runMode == Model.app;
  }

  WKConnectionManager connectionManager = WKConnectionManager.shared;
  WKEventManager eventManager = WKEventManager.shared;
  WKMessageManager messageManager = WKMessageManager.shared;
  WKConversationManager conversationManager = WKConversationManager.shared;
  WKChannelManager channelManager = WKChannelManager.shared;
  WKChannelMemberManager channelMemberManager = WKChannelMemberManager.shared;
  WKReminderManager reminderManager = WKReminderManager.shared;
  WKCMDManager cmdManager = WKCMDManager.shared;
}
