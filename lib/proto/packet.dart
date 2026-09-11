import 'dart:convert';

import 'package:wukongimfluttersdk/common/crypto_utils.dart';

import 'proto.dart';

class PacketHeader {
  PacketType packetType = PacketType.reserved; // 数据包类型
  int packetTypeValue = PacketType.reserved.index;
  bool showUnread = false; // 是否显示未读红点
  bool noPersist = false; // 是否不存储
  bool syncOnce = false; // 是否只同步一次
  bool dup = false; // 服务端重投或客户端重发标志
  int remainingLength = 0;
  bool hasServerVersion = false; // 是否有服务端版本
}

class Packet {
  PacketHeader header = PacketHeader();
}

class ConnectPacket extends Packet {
  int version;
  String clientKey;
  String deviceID;
  String appInstanceID;
  int installationGeneration;
  int sessionGeneration;
  int deviceFlag;
  int clientTimestamp;
  String uid;
  String token;
  ConnectPacket({
    this.version = currentProtocolVersion,
    this.clientKey = "",
    this.deviceID = "",
    this.appInstanceID = "",
    this.installationGeneration = 0,
    this.sessionGeneration = 0,
    this.clientTimestamp = 0,
    this.deviceFlag = 0,
    this.uid = "",
    this.token = "",
  }) {
    header.packetType = PacketType.connect;
  }
  @override
  String toString() {
    return "version:$version，deviceFlag:$deviceFlag，clientTimestamp:$clientTimestamp，installationGeneration:$installationGeneration，sessionGeneration:$sessionGeneration";
  }
}

class ConnackPacket extends Packet {
  String serverKey;
  String salt;
  int timeDiff;
  int reasonCode;
  int serviceProtoVersion = currentProtocolVersion;
  int nodeId = 0;
  ConnackPacket({
    this.serverKey = "",
    this.salt = "",
    this.timeDiff = 0,
    this.reasonCode = 0,
  });
}

class SendPacket extends Packet {
  Setting setting = Setting();
  int clientSeq;
  String clientMsgNO;
  String channelID;
  int channelType;
  String? topic;
  String payload = '';
  int expire = 0;
  SendPacket({
    this.clientSeq = 0,
    this.clientMsgNO = "",
    this.channelID = "",
    this.channelType = 1,
    this.topic = "",
  }) {
    header.packetType = PacketType.send;
  }

  String encodeMsgKey({String? encodedContent}) {
    if (setting.noEncrypt == 1) return '';
    String content = encodedContent ?? encodeMsgContent();
    StringBuffer sb = StringBuffer();
    sb.write(clientSeq);
    sb.write(clientMsgNO);
    sb.write(channelID);
    sb.write(channelType);
    sb.write(content);
    String msgKey = CryptoUtils.aesEncrypt(sb.toString());
    return CryptoUtils.generateMD5(msgKey);
  }

  String encodeMsgContent() {
    if (setting.noEncrypt == 1) return payload;
    return CryptoUtils.aesEncrypt(payload);
  }
}

class SendAckPacket extends Packet {
  String messageID = "";

  /// Public application identity, distinct from the native transport messageID.
  String applicationMessageID = "";
  int clientSeq = 0;
  String clientMsgNO = "";
  int messageSeq = 0;
  int reasonCode = 0;
  SendAckPacket() {
    header.packetType = PacketType.sendack;
  }
}

class RecvAckPacket extends Packet {
  BigInt messageID = BigInt.from(0);
  int messageSeq;
  RecvAckPacket({this.messageSeq = 0}) {
    header.packetType = PacketType.recvack;
  }
}

class RecvPacket extends Packet {
  Setting setting = Setting();
  String msgKey = "";
  String fromUID = "";
  String channelID = "";
  int channelType = 0;
  String clientMsgNO = "";
  BigInt messageID = BigInt.from(0);
  int messageSeq = 0;
  int messageTime = 0;
  String topic = "";
  String payload = "";
  int expire = 0;
  @override
  String toString() {
    return 'RecvPacket(messageID: $messageID, messageSeq: $messageSeq, '
        'messageTime: $messageTime, channelType: $channelType)';
  }
}

class EventPacket extends Packet {
  String eventID = "";
  String eventType = "";
  int timestamp = 0;
  List<int> data = const [];

  Map<String, dynamic>? decodeJsonData() {
    try {
      final value = jsonDecode(utf8.decode(data));
      return value is Map<String, dynamic> ? value : null;
    } on FormatException {
      return null;
    }
  }

  EventPacket() {
    header.packetType = PacketType.event;
    header.packetTypeValue = PacketType.event.index;
  }
}

class UnknownPacket extends Packet {
  final List<int> data;

  UnknownPacket(PacketHeader packetHeader, this.data) {
    header = packetHeader;
  }
}

class DisconnectPacket extends Packet {
  int reasonCode = 0;
  String reason = "";
  DisconnectPacket() {
    header.packetType = PacketType.disconnect;
  }
}

class PingPacket extends Packet {
  PingPacket() {
    header.packetType = PacketType.ping;
  }
}

class PongPacket extends Packet {
  PongPacket() {
    header.packetType = PacketType.pong;
  }
}
