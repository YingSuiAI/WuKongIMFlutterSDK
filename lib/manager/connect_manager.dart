import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:uuid/uuid.dart';
import 'package:wukongimfluttersdk/db/const.dart';

import 'package:wukongimfluttersdk/db/wk_db_helper.dart';
import 'package:wukongimfluttersdk/entity/channel.dart';
import 'package:wukongimfluttersdk/entity/channel_member.dart';
import 'package:wukongimfluttersdk/entity/msg.dart';
import 'package:wukongimfluttersdk/proto/write_read.dart';
import 'package:wukongimfluttersdk/wkim.dart';
import 'package:wukongimfluttersdk/common/crypto_utils.dart';
import '../common/logs.dart';
import '../entity/conversation.dart';
import 'event_manager.dart';
import '../proto/packet.dart';
import '../proto/proto.dart';
import '../type/const.dart';

class _WKSocket {
  Socket? _socket;
  StreamSubscription<Uint8List>? _subscription;
  bool _isListening = false;
  bool _closed = false;
  bool _closeNotified = false;
  Future<void> _writeTail = Future<void>.value();
  Future<void>? _closeFuture;
  _WKSocket._internal(this._socket);

  factory _WKSocket.newSocket(Socket socket) {
    return _WKSocket._internal(socket);
  }

  Future<void> close() {
    final existing = _closeFuture;
    if (existing != null) {
      return existing;
    }
    _isListening = false;
    _closed = true;
    final subscription = _subscription;
    _subscription = null;
    final socket = _socket;
    _socket = null;
    if (subscription != null) {
      unawaited(
          subscription.cancel().then<void>((_) {}, onError: (error, stack) {
        Logs.debug('取消socket监听错误: $error');
      }));
    }
    socket?.destroy();
    final closing = () async {
      try {
        await _writeTail;
      } catch (e) {
        Logs.debug('发送消息时关闭socket错误: $e');
      }
    }();
    _closeFuture = closing;
    return closing;
  }

  Future<void> send(Uint8List data) {
    final operation = _writeTail.then((_) async {
      final socket = _socket;
      if (_closed || socket == null) {
        return;
      }
      try {
        socket.add(data);
        await socket.flush();
      } catch (e) {
        Logs.debug('发送消息错误$e');
      }
    });
    // Keep the chain alive even when a previous operation failed.
    _writeTail = operation.catchError((_) {});
    return operation;
  }

  void listen(void Function(Uint8List data) onData, void Function() onClosed) {
    if (!_isListening && _socket != null) {
      _subscription = _socket!.listen(onData, onError: (err) {
        Logs.debug('socket断开了${err.toString()}');
        _notifyClosed(onClosed);
      }, onDone: () {
        _notifyClosed(onClosed);
      });
      _isListening = true;
    }
  }

  void _notifyClosed(void Function() onClosed) {
    if (_closeNotified || _closed) {
      return;
    }
    _closeNotified = true;
    onClosed();
  }
}

class WKConnectionManager {
  WKConnectionManager._privateConstructor();
  static final WKConnectionManager _instance =
      WKConnectionManager._privateConstructor();
  static WKConnectionManager get shared => _instance;
  // bool _isLogout = false;
  bool isDisconnection = false;
  bool isReconnection = false;
  bool isNetworkUnavailable = false;
  final int reconnMilliseconds = 1500;
  Timer? heartTimer;
  Timer? checkNetworkTimer;
  final heartIntervalSecond = const Duration(seconds: 60);
  final checkNetworkSecond = const Duration(seconds: 1);
  int unReceivePongCount = 0;
  final LinkedHashMap<int, SendingMsg> _sendingMsgMap = LinkedHashMap();
  HashMap<String, Function(int, int?, ConnectionInfo?)>? _connectionListenerMap;
  _WKSocket? _socket;
  Timer? _reconnectTimer;
  int _lifecycleGeneration = 0;
  bool _wantsConnection = false;
  ConnectivityResult? lastConnectivityResult;
  final Connectivity _connectivity = Connectivity();

  addOnConnectionStatus(String key, Function(int, int?, ConnectionInfo?) back) {
    _connectionListenerMap ??= HashMap();
    _connectionListenerMap![key] = back;
  }

  removeOnConnectionStatus(String key) {
    if (_connectionListenerMap != null) {
      _connectionListenerMap!.remove(key);
    }
  }

  void addOnEventListener(String key, void Function(EventPacket) listener) {
    WKEventManager.shared.addListener(key, listener);
  }

  void removeOnEventListener(String key) {
    WKEventManager.shared.removeListener(key);
  }

  void setOnEventGapListener(void Function(WKEventGap)? listener) {
    WKEventManager.shared.setGapListener(listener);
  }

  setConnectionStatus(int status, {int? reasoncode, ConnectionInfo? info}) {
    if (_connectionListenerMap != null) {
      _connectionListenerMap!.forEach((key, back) {
        back(status, reasoncode, info);
      });
    }
  }

  connect() {
    var addr = WKIM.shared.options.addr;
    if ((addr == null || addr == "") && WKIM.shared.options.getAddr == null) {
      Logs.info("没有配置addr！");
      return;
    }
    if (WKIM.shared.options.uid == "" ||
        WKIM.shared.options.uid == null ||
        WKIM.shared.options.token == "" ||
        WKIM.shared.options.token == null) {
      Logs.error("没有初始化uid或token");
      return;
    }
    if (isNetworkUnavailable) {
      return;
    }
    _wantsConnection = true;
    isDisconnection = false;
    final generation = ++_lifecycleGeneration;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _cacheData = null;
    _closeAll();
    if (WKIM.shared.options.getAddr != null) {
      WKIM.shared.options.getAddr!((String addr) {
        if (_isCurrent(generation)) {
          _socketConnect(addr, generation);
        }
      });
    } else {
      _socketConnect(addr!, generation);
    }
  }

  disconnect(bool isLogout) {
    _wantsConnection = false;
    isDisconnection = true;
    ++_lifecycleGeneration;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    isNetworkUnavailable = false;
    isReconnection = false;
    lastConnectivityResult = null;
    try {
      if (isLogout) {
        WKIM.shared.options.uid = '';
        WKIM.shared.options.token = '';
        WKIM.shared.messageManager.updateSendingMsgFail();
        WKDBHelper.shared.close();
      }
    } finally {
      _closeAll();
      setConnectionStatus(WKConnectStatus.fail);
    }
  }

  bool _isCurrent(int generation) =>
      _wantsConnection &&
      !isDisconnection &&
      generation == _lifecycleGeneration;

  bool _isCurrentSocket(int? generation, _WKSocket? connectedSocket) {
    return (generation == null || _isCurrent(generation)) &&
        (connectedSocket == null || identical(_socket, connectedSocket));
  }

  _socketConnect(String addr, int generation) {
    if (!_isCurrent(generation)) {
      return;
    }
    Logs.info("连接地址--->$addr");
    if (addr == '') {
      _connectFail('连接地址为空', generation);
      return;
    }
    () async {
      try {
        var addrs = addr.split(":");
        if (addrs.length != 2) {
          throw const FormatException('连接地址格式错误');
        }
        var host = addrs[0];
        var port = int.parse(addrs[1]);
        setConnectionStatus(WKConnectStatus.connecting);
        final socket = await Socket.connect(host, port,
            timeout: const Duration(seconds: 5));
        if (!_isCurrent(generation)) {
          unawaited(socket.close().then<void>((_) {}, onError: (error, stack) {
            Logs.debug('关闭过期socket错误: $error');
          }));
          return;
        }
        _closeAllTransport();
        _socket = _WKSocket.newSocket(socket);
        _connectSuccess(generation);
      } catch (e) {
        Logs.error(e.toString());
        _connectFail(e, generation);
      }
    }();
  }

  // socket 连接成功
  _connectSuccess(int generation) {
    if (!_isCurrent(generation)) {
      return;
    }
    final connectedSocket = _socket;
    if (connectedSocket == null) {
      return;
    }
    // 监听消息
    connectedSocket.listen((Uint8List data) {
      if (!_isCurrent(generation) || !identical(_socket, connectedSocket)) {
        return;
      }
      try {
        _cutDatas(data,
            generation: generation, connectedSocket: connectedSocket);
      } catch (e) {
        Logs.debug('解析socket数据错误: $e');
        _scheduleReconnect(generation);
      }
      // _decodePacket(data);
    }, () {
      if (!_isCurrent(generation) || !identical(_socket, connectedSocket)) {
        Logs.debug("登出了");
        return;
      }
      _scheduleReconnect(generation);
    });
    // 发送连接包
    _sendConnectPacket(generation, connectedSocket);
  }

  _connectFail(error, int generation) {
    if (_isCurrent(generation)) {
      _scheduleReconnect(generation);
    }
  }

  void _scheduleReconnect(int generation) {
    if (!_isCurrent(generation) || _reconnectTimer != null) {
      return;
    }
    _reconnectTimer = Timer(Duration(milliseconds: reconnMilliseconds), () {
      _reconnectTimer = null;
      if (_isCurrent(generation)) {
        connect();
      }
    });
  }

  testCutData(Uint8List data) {
    _cutDatas(data);
  }

  Uint8List? _cacheData;
  _cutDatas(Uint8List data, {int? generation, _WKSocket? connectedSocket}) {
    if (_cacheData == null || _cacheData!.isEmpty) {
      _cacheData = data;
    } else {
      // 上次存在未解析完的消息
      Uint8List temp = Uint8List(_cacheData!.length + data.length);
      for (var i = 0; i < _cacheData!.length; i++) {
        temp[i] = _cacheData![i];
      }
      for (var i = 0; i < data.length; i++) {
        temp[i + _cacheData!.length] = data[i];
      }
      _cacheData = temp;
    }
    Uint8List lastMsgBytes = _cacheData!;
    int readLength = 0;
    while (lastMsgBytes.isNotEmpty && readLength != lastMsgBytes.length) {
      readLength = lastMsgBytes.length;
      ReadData readData = ReadData(lastMsgBytes);
      var b = readData.readUint8();
      var packetType = b >> 4;
      if (packetType == PacketType.pong.index) {
        Logs.debug('pong');
        unReceivePongCount = 0;
        Uint8List bytes = lastMsgBytes.sublist(1, lastMsgBytes.length);
        _cacheData = lastMsgBytes = bytes;
      } else {
        if (packetType <= 0x0f) {
          if (lastMsgBytes.length < 2) {
            _cacheData = lastMsgBytes;
            break;
          }
          int remainingLength = readData.readVariableLength();
          if (remainingLength == -1) {
            //剩余长度被分包
            _cacheData = lastMsgBytes;
            break;
          }
          if (remainingLength > 1 << 21) {
            _cacheData = null;
            break;
          }
          List<int> bytes = encodeVariableLength(remainingLength);

          if (remainingLength + 1 + bytes.length > lastMsgBytes.length) {
            //半包情况
            _cacheData = lastMsgBytes;
          } else {
            Uint8List msg =
                lastMsgBytes.sublist(0, remainingLength + 1 + bytes.length);
            _decodePacket(msg,
                generation: generation, connectedSocket: connectedSocket);
            if (!_isCurrentSocket(generation, connectedSocket)) {
              _cacheData = null;
              break;
            }
            Uint8List temps =
                lastMsgBytes.sublist(msg.length, lastMsgBytes.length);
            _cacheData = lastMsgBytes = temps;
          }
        } else {
          _cacheData = null;
          // 数据包错误，重连
          if (generation == null) {
            connect();
          } else if (_isCurrentSocket(generation, connectedSocket)) {
            _scheduleReconnect(generation);
          }
          break;
        }
      }
    }
  }

  _decodePacket(Uint8List data, {int? generation, _WKSocket? connectedSocket}) {
    if (!_isCurrentSocket(generation, connectedSocket)) {
      return;
    }
    var packet = WKIM.shared.options.proto.decode(data);
    Logs.debug('解码出包->$packet');
    unReceivePongCount = 0;
    if (packet.header.packetType == PacketType.connack) {
      var connackPacket = packet as ConnackPacket;
      if (connackPacket.reasonCode == 1) {
        Logs.debug('连接成功！');
        WKIM.shared.options.protoVersion = connackPacket.serviceProtoVersion;
        CryptoUtils.setServerKeyAndSalt(
            connackPacket.serverKey, connackPacket.salt);
        setConnectionStatus(WKConnectStatus.success,
            reasoncode: connackPacket.reasonCode,
            info: ConnectionInfo(connackPacket.nodeId));
        if (!_isCurrentSocket(generation, connectedSocket)) {
          return;
        }
        // Future.delayed(Duration(seconds: 1), () {

        // });
        try {
          WKIM.shared.conversationManager.setSyncConversation(() {
            if (!_isCurrentSocket(generation, connectedSocket)) {
              return;
            }
            setConnectionStatus(WKConnectStatus.syncCompleted);
            _resendMsg(
                generation: generation, connectedSocket: connectedSocket);
          });
        } catch (e) {
          Logs.error(e.toString());
        }

        if (!_isCurrentSocket(generation, connectedSocket)) {
          return;
        }
        _startHeartTimer(
            generation: generation, connectedSocket: connectedSocket);
        _startCheckNetworkTimer(
            generation: generation, connectedSocket: connectedSocket);
      } else {
        setConnectionStatus(WKConnectStatus.fail,
            reasoncode: connackPacket.reasonCode);
        Logs.debug('连接失败！错误->${connackPacket.reasonCode}');
      }
    } else if (packet.header.packetType == PacketType.recv) {
      Logs.debug('收到消息');
      var recvPacket = packet as RecvPacket;
      _verifyRecvMsg(recvPacket);
      if (!_isCurrentSocket(generation, connectedSocket)) {
        return;
      }
      if (!recvPacket.header.noPersist) {
        _sendReceAckPacket(
            recvPacket.messageID, recvPacket.messageSeq, recvPacket.header,
            generation: generation, connectedSocket: connectedSocket);
      }
    } else if (packet.header.packetType == PacketType.sendack) {
      var sendack = packet as SendAckPacket;
      Logs.debug('发送结果：${sendack.reasonCode}');
      WKIM.shared.messageManager.updateSendResult(sendack.messageID,
          sendack.clientSeq, sendack.messageSeq, sendack.reasonCode);
      if (_sendingMsgMap.containsKey(sendack.clientSeq)) {
        _sendingMsgMap[sendack.clientSeq]!.isCanResend = false;
      }
    } else if (packet.header.packetType == PacketType.event) {
      WKEventManager.shared.handle(packet as EventPacket);
    } else if (packet.header.packetType == PacketType.disconnect) {
      disconnect(true);
      if (!_isCurrentSocket(generation, connectedSocket)) {
        return;
      }
      // _closeAll();
      setConnectionStatus(WKConnectStatus.kicked);
    } else if (packet.header.packetType == PacketType.pong) {
      Logs.info('pong...');
    }
  }

  _closeAll() {
    // _isLogout = true;
    // WKIM.shared.options.uid = '';
    // WKIM.shared.options.token = '';
    // WKIM.shared.messageManager.updateSendingMsgFail();
    _stopCheckNetworkTimer();
    _stopHeartTimer();
    _closeAllTransport();
  }

  void _closeAllTransport() {
    _cacheData = null;
    if (_socket != null) {
      final socket = _socket!;
      _socket = null;
      unawaited(socket.close());
    }
  }

  _sendReceAckPacket(BigInt messageID, int messageSeq, PacketHeader header,
      {int? generation, _WKSocket? connectedSocket}) {
    RecvAckPacket ackPacket = RecvAckPacket();
    ackPacket.header.noPersist = header.noPersist;
    ackPacket.header.syncOnce = header.syncOnce;
    ackPacket.header.showUnread = header.showUnread;
    ackPacket.messageID = messageID;
    ackPacket.messageSeq = messageSeq;
    _sendPacket(ackPacket,
        generation: generation, connectedSocket: connectedSocket);
  }

  Future<void> _sendConnectPacket(
      int generation, _WKSocket connectedSocket) async {
    try {
      CryptoUtils.init();
      var deviceID = WKIM.shared.options.installationID;
      if (deviceID == null || deviceID.isEmpty) {
        deviceID = await _getDeviceID();
      }
      if (!_isCurrentSocket(generation, connectedSocket)) {
        return;
      }
      var connectPacket = ConnectPacket(
          uid: WKIM.shared.options.uid!,
          token: WKIM.shared.options.token!,
          version: WKIM.shared.options.protoVersion,
          clientKey: base64Encode(CryptoUtils.dhPublicKey!),
          deviceID: deviceID,
          clientTimestamp: DateTime.now().millisecondsSinceEpoch);
      connectPacket.deviceFlag = WKIM.shared.deviceFlagApp;
      await _sendPacket(connectPacket,
          generation: generation, connectedSocket: connectedSocket);
    } catch (e) {
      Logs.debug('发送连接包错误: $e');
      if (_isCurrentSocket(generation, connectedSocket)) {
        _scheduleReconnect(generation);
      }
    }
  }

  Future<void> _sendPacket(Packet packet,
      {int? generation, _WKSocket? connectedSocket}) async {
    final target = _socket;
    if (isReconnection || !_isCurrentSocket(generation, connectedSocket)) {
      return;
    }
    try {
      var data = WKIM.shared.options.proto.encode(packet);
      if (!_isCurrentSocket(generation, connectedSocket) ||
          !identical(_socket, target)) {
        return;
      }
      await target?.send(data);
    } catch (e) {
      Logs.debug('发送数据错误: $e');
    }
  }

  _startCheckNetworkTimer({int? generation, _WKSocket? connectedSocket}) {
    _stopCheckNetworkTimer();
    checkNetworkTimer = Timer.periodic(checkNetworkSecond, (timer) {
      final generation = _lifecycleGeneration;
      if (!_isCurrentSocket(generation, connectedSocket)) {
        return;
      }
      var connectivityResult = _connectivity.checkConnectivity();
      connectivityResult.then((value) {
        if (!_isCurrentSocket(generation, connectedSocket)) {
          return;
        }
        /**
         * 经过查阅 connectivity_plus 官方文档和源码确认：                                                                                                   
          checkConnectivity() 返回的 List<ConnectivityResult> 中，ConnectivityResult.none 只会单独出现，不会和其他连接类型（如 wifi、mobile）混合在同一个列表中。官方文档原文：               
          "The returned list is never empty. In case of no connectivity, the list contains a single element of [ConnectivityResult.none]. Note also that this is the only case where
          ConnectivityResult.none is present."
          参考链接：
          - https://pub.dev/documentation/connectivity_plus_platform_interface/latest/connectivity_plus_platform_interface/ConnectivityResult.html
          - https://github.com/fluttercommunity/plus_plugins/blob/main/packages/connectivity_plus/connectivity_plus/lib/connectivity_plus.dart
          所以 value.contains(ConnectivityResult.none) 在真机上的判断是可靠的，不会出现混合值误触发的情况。
          如果你是在模拟器上遇到反复触发"网络断开了"的问题，这通常是模拟器本身网络状态不稳定导致的，建议在真机上验证一下。
        */
        if (value.contains(ConnectivityResult.none)) {
          isReconnection = true;
          isNetworkUnavailable = true;
          Logs.debug('网络断开了');
          _checkSedingMsg(
              generation: generation, connectedSocket: connectedSocket);
          setConnectionStatus(WKConnectStatus.noNetwork);
          lastConnectivityResult = ConnectivityResult.none;
        } else {
          isNetworkUnavailable = false;
          if (lastConnectivityResult != null &&
              !value.contains(lastConnectivityResult)) {
            isReconnection = true;
          }
          if (isReconnection) {
            isReconnection = false;
            connect();
          }
        }
        if (value.isNotEmpty) {
          lastConnectivityResult = value[0];
        }
      }).catchError((error) {
        if (_isCurrentSocket(generation, connectedSocket)) {
          Logs.debug('检查网络状态错误: $error');
        }
        return null;
      });
    });
  }

  _stopCheckNetworkTimer() {
    // if (_connectivitySubscription != null) {
    // _connectivitySubscription?.cancel();
    // }
    if (checkNetworkTimer != null) {
      checkNetworkTimer!.cancel();
      checkNetworkTimer = null;
    }
  }

  _startHeartTimer({int? generation, _WKSocket? connectedSocket}) {
    _stopHeartTimer();
    heartTimer = Timer.periodic(heartIntervalSecond, (timer) {
      if (unReceivePongCount > 0) {
        Logs.debug('心跳包未收到pong，重连中...');
        isReconnection = false;
        connect();
        return;
      }
      Logs.info('ping...');
      unReceivePongCount++;
      if (_isCurrentSocket(generation, connectedSocket)) {
        _sendPacket(PingPacket(),
            generation: generation, connectedSocket: connectedSocket);
      }
    });
  }

  _stopHeartTimer() {
    if (heartTimer != null) {
      heartTimer!.cancel();
      heartTimer = null;
    }
  }

  sendMessage(WKMsg wkMsg) {
    SendPacket packet = SendPacket();
    packet.setting = wkMsg.setting;
    packet.header.noPersist = wkMsg.header.noPersist;
    packet.header.showUnread = wkMsg.header.redDot;
    packet.header.syncOnce = wkMsg.header.syncOnce;
    packet.channelID = wkMsg.channelID;
    packet.channelType = wkMsg.channelType;
    packet.clientSeq = wkMsg.clientSeq;
    packet.clientMsgNO = wkMsg.clientMsgNO;
    packet.topic = wkMsg.topicID;
    packet.expire = wkMsg.expireTime;
    packet.payload = wkMsg.content;
    _addSendingMsg(packet);
    _sendPacket(packet);
  }

  _verifyRecvMsg(RecvPacket recvMsg) {
    StringBuffer sb = StringBuffer();
    sb.writeAll([
      recvMsg.messageID,
      recvMsg.messageSeq,
      recvMsg.clientMsgNO,
      recvMsg.messageTime,
      recvMsg.fromUID,
      recvMsg.channelID,
      recvMsg.channelType,
      recvMsg.payload
    ]);
    var encryptContent = sb.toString();
    var result = CryptoUtils.aesEncrypt(encryptContent);
    String localMsgKey = CryptoUtils.generateMD5(result);
    if (recvMsg.msgKey != localMsgKey) {
      Logs.error('非法消息-->期望msgKey：$localMsgKey，实际msgKey：${recvMsg.msgKey}');
      return;
    } else {
      recvMsg.payload = CryptoUtils.aesDecrypt(recvMsg.payload);
      Logs.debug(recvMsg.toString());
      _saveRecvMsg(recvMsg);
    }
  }

  _saveRecvMsg(RecvPacket recvMsg) async {
    WKMsg msg = WKMsg();
    msg.header.redDot = recvMsg.header.showUnread;
    msg.header.noPersist = recvMsg.header.noPersist;
    msg.header.syncOnce = recvMsg.header.syncOnce;
    msg.setting = recvMsg.setting;
    msg.channelType = recvMsg.channelType;
    msg.channelID = recvMsg.channelID;
    msg.content = recvMsg.payload;
    msg.messageID = recvMsg.messageID.toString();
    msg.messageSeq = recvMsg.messageSeq;
    msg.timestamp = recvMsg.messageTime;
    msg.fromUID = recvMsg.fromUID;
    msg.clientMsgNO = recvMsg.clientMsgNO;
    msg.expireTime = recvMsg.expire;
    if (msg.expireTime > 0) {
      msg.expireTimestamp = msg.expireTime + msg.timestamp;
    }
    msg.status = WKSendMsgResult.sendSuccess;
    msg.topicID = recvMsg.topic;
    msg.orderSeq = await WKIM.shared.messageManager
        .getMessageOrderSeq(msg.messageSeq, msg.channelID, msg.channelType);
    dynamic contentJson = jsonDecode(msg.content);
    msg.contentType = WKDBConst.resolvePayloadContentType(contentJson);
    msg.isDeleted = _isDeletedMsg(contentJson);
    msg.messageContent = WKIM.shared.messageManager
        .getMessageModel(msg.contentType, contentJson);
    WKChannel? fromChannel = await WKIM.shared.channelManager
        .getChannel(msg.fromUID, WKChannelType.personal);
    if (fromChannel != null) {
      msg.setFrom(fromChannel);
    }
    if (msg.channelType == WKChannelType.group) {
      WKChannelMember? memberChannel = await WKIM.shared.channelMemberManager
          .getMember(msg.channelID, WKChannelType.group, msg.fromUID);
      if (memberChannel != null) {
        msg.setMemberOfFrom(memberChannel);
      }
    }
    WKIM.shared.messageManager.parsingMsg(msg);
    if (msg.isDeleted == 0 &&
        !msg.header.noPersist &&
        msg.contentType != WkMessageContentType.insideMsg) {
      int row = await WKIM.shared.messageManager.saveMsg(msg);
      msg.clientSeq = row;
      WKUIConversationMsg? uiMsg = await WKIM.shared.conversationManager
          .saveWithWKMsg(msg, msg.header.redDot ? 1 : 0);
      if (uiMsg != null) {
        List<WKUIConversationMsg> list = [];
        list.add(uiMsg);
        WKIM.shared.conversationManager.setRefreshUIMsgs(list);
      }
    } else {
      Logs.debug(
          '消息不能存库:is_deleted=${msg.isDeleted},no_persist=${msg.header.noPersist},content_type:${msg.contentType}');
    }
    if (msg.contentType != WkMessageContentType.insideMsg) {
      List<WKMsg> list = [];
      list.add(msg);
      WKIM.shared.messageManager.pushNewMsg(list);
    }
  }

  int _isDeletedMsg(dynamic jsonObject) {
    int isDelete = 0;
    if (jsonObject != null) {
      var visibles = jsonObject['visibles'];
      if (visibles != null && visibles is List) {
        bool isIncludeLoginUser = false;
        for (int i = 0, size = visibles.length; i < size; i++) {
          if (visibles[i] == WKIM.shared.options.uid) {
            isIncludeLoginUser = true;
            break;
          }
        }
        isDelete = isIncludeLoginUser ? 0 : 1;
      }
    }
    return isDelete;
  }

  _resendMsg({int? generation, _WKSocket? connectedSocket}) async {
    _removeSendingMsg();
    if (_sendingMsgMap.isNotEmpty) {
      for (var entry in _sendingMsgMap.entries) {
        if (entry.value.isCanResend) {
          Logs.debug("重发消息：${entry.value.sendPacket.clientSeq}");
          await _sendPacket(entry.value.sendPacket,
              generation: generation, connectedSocket: connectedSocket);
        }
      }
    }
  }

  _addSendingMsg(SendPacket sendPacket) {
    _removeSendingMsg();
    _sendingMsgMap[sendPacket.clientSeq] = SendingMsg(sendPacket);
  }

  _removeSendingMsg() {
    if (_sendingMsgMap.isNotEmpty) {
      List<int> ids = [];
      _sendingMsgMap.forEach((key, sendingMsg) {
        if (!sendingMsg.isCanResend) {
          ids.add(key);
        }
      });
      if (ids.isNotEmpty) {
        for (var i = 0; i < ids.length; i++) {
          _sendingMsgMap.remove(ids[i]);
        }
      }
    }
  }

  _checkSedingMsg({int? generation, _WKSocket? connectedSocket}) {
    if (_sendingMsgMap.isNotEmpty) {
      final it = _sendingMsgMap.entries.iterator;
      while (it.moveNext()) {
        var key = it.current.key;
        var wkSendingMsg = it.current.value;
        if (wkSendingMsg.sendCount == 5 && wkSendingMsg.isCanResend) {
          WKIM.shared.messageManager.updateMsgStatusFail(key);
          wkSendingMsg.isCanResend = false;
        } else {
          var nowTime =
              (DateTime.now().millisecondsSinceEpoch / 1000).truncate();
          if (nowTime - wkSendingMsg.sendTime > 10) {
            wkSendingMsg.sendTime =
                (DateTime.now().millisecondsSinceEpoch / 1000).truncate();
            wkSendingMsg.sendCount++;
            _sendingMsgMap[key] = wkSendingMsg;
            _sendPacket(wkSendingMsg.sendPacket,
                generation: generation, connectedSocket: connectedSocket);
            Logs.debug("消息发送失败，尝试重发中...");
          }
        }
      }

      _removeSendingMsg();
    }
  }
}

Future<String> _getDeviceID() async {
  SharedPreferences preferences = await SharedPreferences.getInstance();
  String wkUid = WKIM.shared.options.uid!;
  String key = "${wkUid}_device_id";
  var deviceID = preferences.getString(key);
  if (deviceID == null || deviceID == "") {
    deviceID = const Uuid().v4().toString().replaceAll("-", "");
    preferences.setString(key, deviceID);
  }
  return "${deviceID}F";
}

class SendingMsg {
  SendPacket sendPacket;
  int sendCount = 0;
  int sendTime = 0;
  bool isCanResend = true;
  SendingMsg(this.sendPacket) {
    sendTime = (DateTime.now().millisecondsSinceEpoch / 1000).truncate();
  }
}

class ConnectionInfo {
  int nodeId;
  ConnectionInfo(this.nodeId);
}
