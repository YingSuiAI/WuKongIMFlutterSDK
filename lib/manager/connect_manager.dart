import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:wukongimfluttersdk/db/const.dart';
import 'package:wukongimfluttersdk/db/message.dart';

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

// A reconnect may retain pending messages only within this authenticated session.
typedef _SessionIdentity = Object;

_SessionIdentity _currentSessionIdentity() =>
    WKIM.shared.options.sessionIdentity;

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
        subscription.cancel().then<void>(
          (_) {},
          onError: (error, stack) {
            Logs.debug('取消socket监听错误: $error');
          },
        ),
      );
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
        throw StateError('The socket was closed before the send was written.');
      }
      try {
        socket.add(data);
        await socket.flush();
      } catch (e) {
        Logs.debug('发送消息错误$e');
        rethrow;
      }
    });
    // Keep the chain alive even when a previous operation failed.
    _writeTail = operation.catchError((_) {});
    return operation;
  }

  void listen(void Function(Uint8List data) onData, void Function() onClosed) {
    if (!_isListening && _socket != null) {
      _subscription = _socket!.listen(
        onData,
        onError: (err) {
          Logs.debug('socket断开了${err.toString()}');
          _notifyClosed(onClosed);
        },
        onDone: () {
          _notifyClosed(onClosed);
        },
      );
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
  final heartIntervalSecond = const Duration(seconds: 30);
  final checkNetworkSecond = const Duration(seconds: 1);
  int unReceivePongCount = 0;
  final LinkedHashMap<int, SendingMsg> _sendingMsgMap = LinkedHashMap();
  final Map<(Object, int, _WKSocket?, int, String), Future<void>>
  _incomingTails = {};
  HashMap<String, Function(int, int?, ConnectionInfo?)>? _connectionListenerMap;
  _WKSocket? _socket;
  _WKSocket? _authenticatedSocket;
  Timer? _reconnectTimer;
  int _lifecycleGeneration = 0;
  bool _wantsConnection = false;
  _SessionIdentity? _connectionIdentity;
  _SessionIdentity? _sendingIdentity;
  int _nextWireClientSeq = 0;
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
    if (WKIM.shared.options.protoVersion != currentProtocolVersion ||
        !WKIM.shared.options.hasExactSessionIdentity) {
      Logs.error(
        "WKProto v7 requires installationID, appInstanceID, and positive installation/session generations",
      );
      return;
    }
    if (isNetworkUnavailable) {
      return;
    }
    _connectionIdentity = _currentSessionIdentity();
    _selectSendingSession(_connectionIdentity!);
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

  void disconnect(bool isLogout) => _disconnect(isLogout, WKConnectStatus.fail);

  void _disconnect(bool isLogout, int status) {
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
        _sendingMsgMap.clear();
        _sendingIdentity = null;
        _connectionIdentity = null;
        WKIM.shared.options.uid = '';
        WKIM.shared.options.token = '';
        unawaited(
          WKIM.shared.messageManager.updateSendingMsgFail().catchError((
            Object error,
            StackTrace stack,
          ) {
            Logs.debug('关闭会话时更新发送状态失败: ${error.runtimeType}');
          }),
        );
        unawaited(WKDBHelper.shared.close());
      }
    } finally {
      _closeAll();
      setConnectionStatus(status);
    }
  }

  bool _isCurrent(int generation) =>
      _wantsConnection &&
      !isDisconnection &&
      generation == _lifecycleGeneration &&
      _connectionIdentity == _currentSessionIdentity();

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
        final socket = await Socket.connect(
          host,
          port,
          timeout: const Duration(seconds: 5),
        );
        if (!_isCurrent(generation)) {
          unawaited(
            socket.close().then<void>(
              (_) {},
              onError: (error, stack) {
                Logs.debug('关闭过期socket错误: $error');
              },
            ),
          );
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
    connectedSocket.listen(
      (Uint8List data) {
        if (!_isCurrent(generation) || !identical(_socket, connectedSocket)) {
          return;
        }
        try {
          _cutDatas(
            data,
            generation: generation,
            connectedSocket: connectedSocket,
          );
        } catch (e) {
          Logs.debug('解析socket数据错误: $e');
          _scheduleReconnect(generation);
        }
        // _decodePacket(data);
      },
      () {
        if (!_isCurrent(generation) || !identical(_socket, connectedSocket)) {
          Logs.debug("登出了");
          return;
        }
        _closeAllTransport();
        _scheduleReconnect(generation);
      },
    );
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
    try {
      _cutDataFrames(
        data,
        generation: generation,
        connectedSocket: connectedSocket,
      );
    } on FormatException {
      _cacheData = null;
      if (generation != null && _isCurrentSocket(generation, connectedSocket)) {
        _closeAllTransport();
        _scheduleReconnect(generation);
      }
    }
  }

  void _cutDataFrames(
    Uint8List data, {
    int? generation,
    _WKSocket? connectedSocket,
  }) {
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
            throw const FormatException('Frame exceeds the receive limit.');
          }
          final frameLength = readData.offset + remainingLength;

          if (frameLength > lastMsgBytes.length) {
            //半包情况
            _cacheData = lastMsgBytes;
          } else {
            Uint8List msg = lastMsgBytes.sublist(0, frameLength);
            _decodePacket(
              msg,
              generation: generation,
              connectedSocket: connectedSocket,
            );
            if (!_isCurrentSocket(generation, connectedSocket)) {
              _cacheData = null;
              break;
            }
            Uint8List temps = lastMsgBytes.sublist(
              msg.length,
              lastMsgBytes.length,
            );
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
    Logs.debug('解码出包: ${packet.header.packetType.name}');
    unReceivePongCount = 0;
    if (packet.header.packetType == PacketType.connack) {
      var connackPacket = packet as ConnackPacket;
      if (connackPacket.reasonCode == 1) {
        Logs.debug('连接成功！');
        CryptoUtils.setServerKeyAndSalt(
          connackPacket.serverKey,
          connackPacket.salt,
        );
        _authenticatedSocket = _socket;
        setConnectionStatus(
          WKConnectStatus.success,
          reasoncode: connackPacket.reasonCode,
          info: ConnectionInfo(connackPacket.nodeId),
        );
        if (!_isCurrentSocket(generation, connectedSocket)) {
          return;
        }
        // Pending messages use the newly negotiated key, independently of the
        // optional conversation sync integration.
        unawaited(
          _resendMsg(generation: generation, connectedSocket: connectedSocket),
        );
        try {
          WKIM.shared.conversationManager.setSyncConversation(() {
            if (!_isCurrentSocket(generation, connectedSocket)) {
              return;
            }
            setConnectionStatus(WKConnectStatus.syncCompleted);
          });
        } catch (e) {
          Logs.error(e.toString());
        }

        if (!_isCurrentSocket(generation, connectedSocket)) {
          return;
        }
        _startHeartTimer(
          generation: generation,
          connectedSocket: connectedSocket,
        );
        _startCheckNetworkTimer(
          generation: generation,
          connectedSocket: connectedSocket,
        );
      } else {
        _authenticatedSocket = null;
        setConnectionStatus(
          WKConnectStatus.fail,
          reasoncode: connackPacket.reasonCode,
        );
        Logs.debug('连接失败！错误->${connackPacket.reasonCode}');
      }
    } else if (packet.header.packetType == PacketType.recv) {
      final recv = packet as RecvPacket;
      _enqueueIncoming(
        recv.channelID,
        recv.channelType,
        () => _receiveMessage(
          recv,
          generation: generation,
          connectedSocket: connectedSocket,
        ),
        generation: generation,
        connectedSocket: connectedSocket,
      );
    } else if (packet.header.packetType == PacketType.sendack) {
      var sendack = packet as SendAckPacket;
      final pending = _sendingMsgMap[sendack.clientSeq];
      if (_sendingIdentity != _currentSessionIdentity() ||
          pending == null ||
          pending.isAcknowledging ||
          pending.sendPacket.clientMsgNO != sendack.clientMsgNO) {
        return;
      }
      final identity = _currentSessionIdentity();
      pending.isAcknowledging = true;
      unawaited(() async {
        try {
          await WKIM.shared.messageManager.updateSendResult(
            sendack.messageID,
            pending.databaseClientSeq,
            sendack.messageSeq,
            sendack.reasonCode,
            applicationMessageID: sendack.applicationMessageID,
            isCurrent: () =>
                identity == _currentSessionIdentity() &&
                _isCurrentSocket(generation, connectedSocket),
          );
          if (identity == _currentSessionIdentity() &&
              _isCurrentSocket(generation, connectedSocket) &&
              identical(_sendingMsgMap[sendack.clientSeq], pending)) {
            _sendingMsgMap.remove(sendack.clientSeq);
          }
        } catch (error) {
          Logs.debug('更新发送结果失败: ${error.runtimeType}');
        } finally {
          pending.isAcknowledging = false;
        }
      }());
    } else if (packet.header.packetType == PacketType.event) {
      final event = packet as EventPacket;
      final envelope = event.decodeJsonData();
      final channelID = envelope?['channel_id'];
      final channelType = envelope?['channel_type'];
      if (channelID is! String || channelType is! int) return;
      _enqueueIncoming(
        channelID,
        channelType,
        () => WKEventManager.shared.handle(event),
        generation: generation,
        connectedSocket: connectedSocket,
      );
    } else if (packet.header.packetType == PacketType.disconnect) {
      _disconnect(true, WKConnectStatus.kicked);
    } else if (packet.header.packetType == PacketType.pong) {
      Logs.info('pong...');
    }
  }

  void _enqueueIncoming(
    String channelID,
    int channelType,
    FutureOr<void> Function() operation, {
    int? generation,
    _WKSocket? connectedSocket,
  }) {
    if (channelID.trim().isEmpty || channelType < 1 || channelType > 255) {
      return;
    }
    // Capture ownership at wire arrival, before a preceding RECV's SQLite
    // awaits. App callbacks cannot reconstruct ordering once EVENT overtakes it.
    final identity = _currentSessionIdentity();
    final lifecycle = _lifecycleGeneration;
    final socket = connectedSocket ?? _socket;
    final database = WKDBHelper.shared.getDB();
    final key = (identity, lifecycle, socket, channelType, channelID);
    late final Future<void> tail;
    tail = (_incomingTails[key] ?? Future<void>.value())
        .then<void>((_) async {
          if (identity != _currentSessionIdentity() ||
              lifecycle != _lifecycleGeneration ||
              !identical(socket, _socket) ||
              !identical(database, WKDBHelper.shared.getDB()) ||
              !_isCurrentSocket(generation, connectedSocket)) {
            return;
          }
          await operation();
        })
        .catchError((Object error, StackTrace stack) {
          Logs.debug('接收队列处理失败: ${error.runtimeType}');
        })
        .whenComplete(() {
          if (identical(_incomingTails[key], tail)) _incomingTails.remove(key);
        });
    _incomingTails[key] = tail;
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
    _incomingTails.clear();
    _cacheData = null;
    _authenticatedSocket = null;
    if (_socket != null) {
      final socket = _socket!;
      _socket = null;
      unawaited(socket.close());
    }
  }

  _sendReceAckPacket(
    BigInt messageID,
    int messageSeq,
    PacketHeader header, {
    int? generation,
    _WKSocket? connectedSocket,
  }) {
    RecvAckPacket ackPacket = RecvAckPacket();
    ackPacket.header.noPersist = header.noPersist;
    ackPacket.header.syncOnce = header.syncOnce;
    ackPacket.header.showUnread = header.showUnread;
    ackPacket.messageID = messageID;
    ackPacket.messageSeq = messageSeq;
    _sendPacket(
      ackPacket,
      generation: generation,
      connectedSocket: connectedSocket,
    );
  }

  Future<void> _sendConnectPacket(
    int generation,
    _WKSocket connectedSocket,
  ) async {
    try {
      CryptoUtils.init();
      final deviceID = WKIM.shared.options.installationID!;
      if (!_isCurrentSocket(generation, connectedSocket)) {
        return;
      }
      var connectPacket = ConnectPacket(
        uid: WKIM.shared.options.uid!,
        token: WKIM.shared.options.token!,
        version: WKIM.shared.options.protoVersion,
        clientKey: base64Encode(CryptoUtils.dhPublicKey!),
        deviceID: deviceID,
        appInstanceID: WKIM.shared.options.appInstanceID ?? '',
        installationGeneration: WKIM.shared.options.installationGeneration,
        sessionGeneration: WKIM.shared.options.sessionGeneration,
        clientTimestamp: DateTime.now().millisecondsSinceEpoch,
      );
      connectPacket.deviceFlag = WKIM.shared.deviceFlagApp;
      await _sendPacket(
        connectPacket,
        generation: generation,
        connectedSocket: connectedSocket,
      );
    } catch (e) {
      Logs.debug('发送连接包错误: $e');
      if (_isCurrentSocket(generation, connectedSocket)) {
        _scheduleReconnect(generation);
      }
    }
  }

  Future<void> _sendPacket(
    Packet packet, {
    int? generation,
    _WKSocket? connectedSocket,
    bool propagateError = false,
  }) async {
    generation ??= _lifecycleGeneration;
    final target = _socket;
    if (isReconnection || !_isCurrentSocket(generation, connectedSocket)) {
      return;
    }
    if (packet is SendPacket &&
        (target == null || !identical(_authenticatedSocket, target))) {
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
      if (_isCurrentSocket(generation, connectedSocket)) {
        _scheduleReconnect(generation);
      }
      if (propagateError) rethrow;
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
      connectivityResult
          .then((value) {
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
                generation: generation,
                connectedSocket: connectedSocket,
              );
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
          })
          .catchError((error) {
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
        _sendPacket(
          PingPacket(),
          generation: generation,
          connectedSocket: connectedSocket,
        );
      }
    });
  }

  _stopHeartTimer() {
    if (heartTimer != null) {
      heartTimer!.cancel();
      heartTimer = null;
    }
  }

  /// Admits a message into this session's outbox. When authenticated, also waits
  /// for the socket write; before CONNACK it remains queued for authenticated send.
  Future<void> sendMessage(WKMsg wkMsg) async {
    final identity = _currentSessionIdentity();
    final uid = WKIM.shared.options.uid;
    if (uid == null || uid.isEmpty) {
      throw StateError('Sending requires an authenticated session identity.');
    }
    _selectSendingSession(identity);
    SendPacket packet = SendPacket();
    packet.setting = wkMsg.setting;
    packet.header.noPersist = wkMsg.header.noPersist;
    packet.header.showUnread = wkMsg.header.redDot;
    packet.header.syncOnce = wkMsg.header.syncOnce;
    packet.channelID = wkMsg.channelID;
    packet.channelType = wkMsg.channelType;
    // A database row is not a wire attempt: late ACKs must not complete retries.
    if (_nextWireClientSeq == 0xffffffff) {
      throw StateError('The connection has exhausted its send sequence space.');
    }
    packet.clientSeq = ++_nextWireClientSeq;
    packet.clientMsgNO = wkMsg.clientMsgNO;
    packet.topic = wkMsg.topicID;
    packet.expire = wkMsg.expireTime;
    packet.payload = wkMsg.content;
    _addSendingMsg(packet, wkMsg.clientSeq);
    await _sendPacket(packet, propagateError: true);
  }

  void _selectSendingSession(_SessionIdentity identity) {
    if (_sendingIdentity != identity) {
      _sendingMsgMap.clear();
      _sendingIdentity = identity;
    }
  }

  Future<void> _receiveMessage(
    RecvPacket packet, {
    int? generation,
    _WKSocket? connectedSocket,
  }) async {
    final identity = _currentSessionIdentity();
    final database = WKDBHelper.shared.getDB();
    bool isCurrent() =>
        identity == _currentSessionIdentity() &&
        identical(database, WKDBHelper.shared.getDB()) &&
        _isCurrentSocket(generation, connectedSocket);
    try {
      _verifyRecvMsg(packet);
      if (await _saveRecvMsg(packet, isCurrent) &&
          isCurrent() &&
          !packet.header.noPersist) {
        _sendReceAckPacket(
          packet.messageID,
          packet.messageSeq,
          packet.header,
          generation: generation,
          connectedSocket: connectedSocket,
        );
      }
    } catch (error) {
      // Failed integrity, persistence, or obsolete owners must never ACK a drop.
      Logs.debug('接收消息未完成: ${error.runtimeType}');
    }
  }

  void _verifyRecvMsg(RecvPacket recvMsg) {
    if (recvMsg.setting.noEncrypt == 1) return;
    StringBuffer sb = StringBuffer();
    sb.writeAll([
      recvMsg.messageID,
      recvMsg.messageSeq,
      recvMsg.clientMsgNO,
      recvMsg.messageTime,
      recvMsg.fromUID,
      recvMsg.channelID,
      recvMsg.channelType,
      recvMsg.payload,
    ]);
    var encryptContent = sb.toString();
    var result = CryptoUtils.aesEncrypt(encryptContent);
    String localMsgKey = CryptoUtils.generateMD5(result);
    if (recvMsg.msgKey != localMsgKey) {
      throw const FormatException('Received message integrity failed.');
    }
    recvMsg.payload = CryptoUtils.aesDecrypt(recvMsg.payload);
  }

  Future<bool> _saveRecvMsg(
    RecvPacket recvMsg,
    bool Function() isCurrent,
  ) async {
    if (!isCurrent()) return false;
    final database = WKDBHelper.shared.getDB();
    final isSelfMessage = recvMsg.fromUID == WKIM.shared.options.uid;
    WKMsg msg = WKMsg();
    msg.header.redDot = recvMsg.header.showUnread;
    msg.header.noPersist = recvMsg.header.noPersist;
    msg.header.syncOnce = recvMsg.header.syncOnce;
    msg.setting = recvMsg.setting;
    msg.channelType = recvMsg.channelType;
    msg.channelID = recvMsg.channelID;
    msg.content = recvMsg.payload;
    msg.messageID = recvMsg.messageID.toString();
    msg.payloadCommitted = true;
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
    msg.orderSeq = await WKIM.shared.messageManager.getMessageOrderSeq(
      msg.messageSeq,
      msg.channelID,
      msg.channelType,
    );
    if (!isCurrent()) return false;
    dynamic contentJson = jsonDecode(msg.content);
    msg.contentType = WKDBConst.resolvePayloadContentType(contentJson);
    msg.isDeleted = _isDeletedMsg(contentJson);
    msg.messageContent = WKIM.shared.messageManager.getMessageModel(
      msg.contentType,
      contentJson,
    );
    WKChannel? fromChannel = await WKIM.shared.channelManager.getChannel(
      msg.fromUID,
      WKChannelType.personal,
    );
    if (!isCurrent()) return false;
    if (fromChannel != null) {
      msg.setFrom(fromChannel);
    }
    if (msg.channelType == WKChannelType.group) {
      WKChannelMember? memberChannel = await WKIM.shared.channelMemberManager
          .getMember(msg.channelID, WKChannelType.group, msg.fromUID);
      if (!isCurrent()) return false;
      if (memberChannel != null) {
        msg.setMemberOfFrom(memberChannel);
      }
    }
    WKIM.shared.messageManager.parsingMsg(
      msg,
      transportFromUID: recvMsg.fromUID,
      transportChannelID: recvMsg.channelID,
      transportChannelType: recvMsg.channelType,
    );
    if (!isCurrent()) return false;
    if (msg.isDeleted == 0 &&
        !msg.header.noPersist &&
        msg.contentType != WkMessageContentType.insideMsg) {
      if (database == null) {
        throw StateError(
          'A received persistent message requires an open database.',
        );
      }
      // The ACK promises durable admission, including its conversation projection.
      var duplicate = false;
      final uiMsg = await database.transaction((transaction) async {
        if (!isCurrent()) throw StateError('The receive session was replaced.');
        final existing = await transaction.query(
          WKDBConst.tableMessage,
          where:
              'channel_id = ? AND channel_type = ? AND '
              '(message_id = ? OR (client_msg_no = ? AND (from_uid = ? OR ? = 1)))',
          whereArgs: [
            msg.channelID,
            msg.channelType,
            msg.messageID,
            msg.clientMsgNO,
            msg.fromUID,
            isSelfMessage ? 1 : 0,
          ],
          limit: 1,
        );
        if (!isCurrent()) throw StateError('The receive session was replaced.');
        if (existing.isNotEmpty) {
          final row = existing.single;
          msg.clientSeq = row['client_seq'] as int;
          final existingMessageID = row['message_id'];
          final sameMessage = existingMessageID == msg.messageID;
          // SENDACK can arrive before the server's authoritative source echo.
          // An ACK only commits identity/status; it does not replace the local
          // request body with the canonical payload. Keep that payload opaque.
          if (sameMessage && WKDBConst.readInt(row, 'payload_committed') == 1) {
            if (row['content'] != msg.content ||
                row['client_msg_no'] != msg.clientMsgNO) {
              throw StateError('Conflicting committed message payload.');
            }
            duplicate = true;
            return null;
          }
          if (!isSelfMessage ||
              row['client_msg_no'] != msg.clientMsgNO ||
              (!sameMessage &&
                  existingMessageID != '' &&
                  existingMessageID != '0')) {
            throw StateError('Conflicting received message identity.');
          }
          // Local presentation identity can differ from the opaque wire UID.
          msg.fromUID = row['from_uid'] as String;
          // A source echo must not resurrect a locally deleted message or reset
          // device-local read/media state while replacing the submitted body.
          msg.isDeleted = WKDBConst.readInt(row, 'is_deleted');
          msg.voiceStatus = WKDBConst.readInt(row, 'voice_status');
          msg.viewed = WKDBConst.readInt(row, 'viewed');
          msg.viewedAt = WKDBConst.readInt(row, 'viewed_at');
          msg.localExtraMap = WKDBConst.readJsonValue(row, 'extra');
        }
        msg.clientSeq = await MessageDB.shared.insert(
          msg,
          database: transaction,
        );
        if (!isCurrent()) throw StateError('The receive session was replaced.');
        if (msg.isDeleted != 0) return null;
        final conversation = await WKIM.shared.conversationManager
            .saveWithWKMsg(
              msg,
              msg.header.redDot && !isSelfMessage ? 1 : 0,
              database: transaction,
            );
        if (!isCurrent()) throw StateError('The receive session was replaced.');
        return conversation;
      });
      if (!isCurrent()) return false;
      if (isSelfMessage && _sendingIdentity == _currentSessionIdentity()) {
        // Reliable source delivery is completion evidence even if SENDACK was
        // lost. Retire only the exact local attempt whose body was committed.
        _sendingMsgMap.removeWhere((_, pending) =>
            pending.databaseClientSeq == msg.clientSeq &&
            pending.sendPacket.clientMsgNO == msg.clientMsgNO &&
            pending.sendPacket.channelID == msg.channelID &&
            pending.sendPacket.channelType == msg.channelType);
      }
      if (duplicate) return true;
      if (uiMsg != null) {
        List<WKUIConversationMsg> list = [];
        list.add(uiMsg);
        WKIM.shared.conversationManager.setRefreshUIMsgs(list);
      }
    } else {
      Logs.debug(
        '消息不能存库:is_deleted=${msg.isDeleted},no_persist=${msg.header.noPersist},content_type:${msg.contentType}',
      );
    }
    if (msg.isDeleted == 0 &&
        msg.contentType != WkMessageContentType.insideMsg) {
      List<WKMsg> list = [];
      list.add(msg);
      WKIM.shared.messageManager.pushNewMsg(list);
    }
    return isCurrent();
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

  Future<void> _resendMsg({int? generation, _WKSocket? connectedSocket}) async {
    _removeSendingMsg();
    if (_sendingMsgMap.isNotEmpty) {
      for (var entry in _sendingMsgMap.entries.toList()) {
        if (!_isCurrentSocket(generation, connectedSocket) ||
            _sendingIdentity != _currentSessionIdentity()) {
          return;
        }
        if (entry.value.isCanResend &&
            !entry.value.isAcknowledging &&
            identical(_sendingMsgMap[entry.key], entry.value)) {
          Logs.debug("重发消息：${entry.value.sendPacket.clientSeq}");
          await _sendPacket(
            entry.value.sendPacket,
            generation: generation,
            connectedSocket: connectedSocket,
          );
        }
      }
    }
  }

  _addSendingMsg(SendPacket sendPacket, int databaseClientSeq) {
    _removeSendingMsg();
    _sendingMsgMap.removeWhere(
      (_, pending) => pending.sendPacket.clientMsgNO == sendPacket.clientMsgNO,
    );
    _sendingMsgMap[sendPacket.clientSeq] = SendingMsg(
      sendPacket,
      databaseClientSeq,
    );
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
        if (!wkSendingMsg.isCanResend || wkSendingMsg.isAcknowledging) continue;
        if (wkSendingMsg.sendCount == 5 && wkSendingMsg.isCanResend) {
          WKIM.shared.messageManager.updateMsgStatusFail(
            wkSendingMsg.databaseClientSeq,
          );
          wkSendingMsg.isCanResend = false;
        } else {
          var nowTime = (DateTime.now().millisecondsSinceEpoch / 1000)
              .truncate();
          if (nowTime - wkSendingMsg.sendTime > 10) {
            wkSendingMsg.sendTime =
                (DateTime.now().millisecondsSinceEpoch / 1000).truncate();
            wkSendingMsg.sendCount++;
            _sendingMsgMap[key] = wkSendingMsg;
            _sendPacket(
              wkSendingMsg.sendPacket,
              generation: generation,
              connectedSocket: connectedSocket,
            );
            Logs.debug("消息发送失败，尝试重发中...");
          }
        }
      }

      _removeSendingMsg();
    }
  }
}

class SendingMsg {
  SendPacket sendPacket;
  final int databaseClientSeq;
  int sendCount = 0;
  int sendTime = 0;
  bool isCanResend = true;
  bool isAcknowledging = false;
  SendingMsg(this.sendPacket, this.databaseClientSeq) {
    sendTime = (DateTime.now().millisecondsSinceEpoch / 1000).truncate();
  }
}

class ConnectionInfo {
  int nodeId;
  ConnectionInfo(this.nodeId);
}
