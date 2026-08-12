import 'dart:math';
import 'dart:typed_data';

import 'package:wukongimfluttersdk/wkim.dart';

import '../common/logs.dart';
import 'packet.dart';
import 'write_read.dart';

enum PacketType {
  reserved, // 保留位
  connect, // 客户端请求连接到服务器(c2s)
  connack,
  send,
  sendack,
  recv,
  recvack,
  ping,
  pong,
  disconnect,
  sub,
  suback,
  event,
  unknown,
}

class Setting {
  int receipt = 0;
  int topic = 0;
  Setting decode(int v) {
    receipt = (v >> 7 & 0x01);
    topic = (v >> 3 & 0x01);
    return this;
  }

  int encode() {
    return receipt << 7 | topic << 3;
  }
}

class Proto {
  Map<PacketType, Function> packetEncodeMap = {
    PacketType.connect: encodeConnect,
    PacketType.send: encodeSend,
    PacketType.recvack: encodeRecvAck,
  };
  Map packetDecodeMap = {
    PacketType.connack: decodeConnack,
    PacketType.recv: decodeRecv,
    PacketType.sendack: decodeSendAck,
    PacketType.disconnect: decodeDisconnect,
    PacketType.event: decodeEvent,
  };

  Uint8List encode(Packet packet) {
    var write = WriteData();
    if (packet.header.packetType != PacketType.ping &&
        packet.header.packetType != PacketType.pong) {
      var packetEncodeFunc = packetEncodeMap[packet.header.packetType];
      var body = packetEncodeFunc!(packet);
      var header = encodeHeader(packet, body.length);
      write.writeBytes(header);
      write.writeBytes(body);
    } else {
      var header = encodeHeader(packet, 0);
      write.writeBytes(header);
    }
    return write.toUint8List();
  }

  Packet decode(Uint8List data) {
    var reader = ReadData(data);
    var header = decodeHeader(reader);
    if (header.packetType == PacketType.ping) {
      return PingPacket();
    }
    if (header.packetType == PacketType.pong) {
      return PongPacket();
    }
    var packetDecodeFunc = packetDecodeMap[header.packetType];
    if (packetDecodeFunc == null) {
      return UnknownPacket(header, reader.readRemaining());
    }
    return packetDecodeFunc(header, reader);
  }
}

Uint8List encodeConnect(ConnectPacket packet) {
  WriteData write = WriteData();
  write.writeUint8(packet.version);
  write.writeUint8(packet.deviceFlag);
  write.writeString(packet.deviceID);
  write.writeString(packet.uid);
  write.writeString(packet.token);
  write.writeUint64(BigInt.from(packet.clientTimestamp));
  write.writeString(packet.clientKey);
  if (packet.version == 6) {
    write.writeString(packet.appInstanceID);
    write.writeUint64(BigInt.from(packet.sessionGeneration));
  }
  return write.toUint8List();
}

decodeConnack(PacketHeader header, ReadData reader) {
  var connAck = ConnackPacket();
  connAck.header = header;
  if (header.hasServerVersion) {
    var version = reader.readByte();
    Logs.debug("server protocol version: $version");
    connAck.serviceProtoVersion =
        min(version, WKIM.shared.options.protoVersion);
  }
  connAck.timeDiff = reader.readUint64().toInt();
  connAck.reasonCode = reader.readUint8();
  connAck.serverKey = reader.readString();
  connAck.salt = reader.readString();
  if (connAck.serviceProtoVersion >= 4) {
    connAck.nodeId = reader.readUint64().toInt();
  }
  return connAck;
}

PacketHeader decodeHeader(ReadData reader) {
  var b = reader.readByte();
  var header = PacketHeader();
  header.noPersist = (b & 0x01) > 0;
  header.showUnread = ((b >> 1) & 0x01) > 0;
  header.syncOnce = ((b >> 2) & 0x01) > 0;
  header.packetTypeValue = b >> 4;
  header.packetType = packetTypeFromValue(header.packetTypeValue);
  if (header.packetType != PacketType.ping &&
      header.packetType != PacketType.pong) {
    header.remainingLength = reader.readVariableLength();
  }
  if (header.packetType == PacketType.connack) {
    header.hasServerVersion = (b & 0x01) > 0;
  }
  return header;
}

PacketType packetTypeFromValue(int value) {
  if (value >= PacketType.reserved.index && value <= PacketType.event.index) {
    return PacketType.values[value];
  }
  return PacketType.unknown;
}

encodeHeader(Packet packet, int remainingLength) {
  if (packet.header.packetType == PacketType.ping ||
      packet.header.packetType == PacketType.pong) {
    return [(packet.header.packetType.index << 4) | 0];
  }
  List<int> headers = [];

  var typeAndFlags = (encodeBool(false) << 3) |
      (encodeBool(packet.header.syncOnce) << 2) |
      (encodeBool(packet.header.showUnread) << 1) |
      encodeBool(packet.header.noPersist);

  headers.add(packet.header.packetType.index << 4 | 0 | typeAndFlags);
  var vLen = encodeVariableLength(remainingLength);
  headers.addAll(vLen);

  return headers;
}

encodeBool(bool b) {
  return b ? 1 : 0;
}

List<int> encodeVariableLength(int len) {
  List<int> ret = [];
  while (len > 0) {
    var digit = len % 0x80;
    len = (len / 0x80).floor();
    if (len > 0) {
      digit |= 0x80;
    }
    ret.add(digit);
  }
  return ret;
}

Uint8List encodeSend(SendPacket packet) {
  WriteData write = WriteData();
  write.writeUint8(packet.setting.encode());
  write.writeUint32(packet.clientSeq);
  write.writeString(packet.clientMsgNO);
  write.writeString(packet.channelID);
  write.writeUint8(packet.channelType);
  if (WKIM.shared.options.protoVersion >= 3) {
    write.writeUint32(packet.expire);
  }
  write.writeString(packet.encodeMsgKey());
  if (packet.setting.topic == 1) {
    write.writeString(packet.topic == null ? "" : packet.topic!);
  }
  write.writeBytes(packet.encodeMsgContent().codeUnits);
  return write.toUint8List();
}

Uint8List encodeRecvAck(RecvAckPacket packet) {
  WriteData write = WriteData();
  write.writeUint64(packet.messageID);
  if (WKIM.shared.options.protoVersion >= 6) {
    write.writeUint64(BigInt.from(packet.messageSeq));
  } else {
    write.writeUint32(packet.messageSeq);
  }
  return write.toUint8List();
}

SendAckPacket decodeSendAck(PacketHeader header, ReadData reader) {
  var sendack = SendAckPacket();
  sendack.messageID = reader.readUint64().toString();
  sendack.clientSeq = reader.readUint32();
  sendack.messageSeq = WKIM.shared.options.protoVersion >= 6
      ? reader.readUint64().toInt()
      : reader.readUint32();
  sendack.reasonCode = reader.readUint8();
  if (reader.remainingLength > 0) {
    sendack.clientMsgNO = reader.readString();
  }
  return sendack;
}

RecvPacket decodeRecv(PacketHeader header, ReadData reader) {
  var recv = RecvPacket();
  recv.header = header;
  int setting = reader.readUint8();
  recv.setting = Setting().decode(setting);
  recv.msgKey = reader.readString();
  recv.fromUID = reader.readString();
  recv.channelID = reader.readString();
  recv.channelType = reader.readUint8().toInt();
  if (WKIM.shared.options.protoVersion >= 3) {
    recv.expire = reader.readUint32().toInt();
  }
  recv.clientMsgNO = reader.readString();
  recv.messageID = reader.readUint64();
  recv.messageSeq = WKIM.shared.options.protoVersion >= 6
      ? reader.readUint64().toInt()
      : reader.readUint32().toInt();
  recv.messageTime = reader.readUint32().toInt();
  if (recv.setting.topic == 1) {
    recv.topic = reader.readString();
  }
  var payload = reader.readRemaining();
  recv.payload = String.fromCharCodes(payload);
  return recv;
}

EventPacket decodeEvent(PacketHeader header, ReadData reader) {
  var event = EventPacket();
  event.header = header;
  event.eventID = reader.readString();
  event.eventType = reader.readString();
  event.timestamp = reader.readUint64().toInt();
  event.data = reader.readRemaining();
  return event;
}

DisconnectPacket decodeDisconnect(PacketHeader header, ReadData reader) {
  var disconnect = DisconnectPacket();
  disconnect.reasonCode = reader.readUint8();
  disconnect.reason = reader.readString();
  return disconnect;
}
