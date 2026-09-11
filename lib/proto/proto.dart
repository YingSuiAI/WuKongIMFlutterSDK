import 'dart:convert';
import 'dart:typed_data';

import 'packet.dart';
import 'write_read.dart';

const currentProtocolVersion = 7;

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
  int noEncrypt = 0;
  int _otherBits = 0;
  Setting decode(int v) {
    receipt = (v >> 7 & 0x01);
    topic = (v >> 3 & 0x01);
    noEncrypt = (v >> 4 & 0x01);
    _otherBits = v & 0x67;
    return this;
  }

  int encode() {
    return _otherBits | receipt << 7 | topic << 3 | noEncrypt << 4;
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
      if (packetEncodeFunc == null) {
        throw UnsupportedError('Unsupported WKProto packet encoding');
      }
      var body = packetEncodeFunc(packet);
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
    if (header.remainingLength != reader.remainingLength) {
      throw const FormatException('WKProto frame length mismatch');
    }
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
    final packet = packetDecodeFunc(header, reader) as Packet;
    if (reader.remainingLength != 0) {
      throw const FormatException('Unexpected WKProto trailing fields');
    }
    return packet;
  }
}

Uint8List encodeConnect(ConnectPacket packet) {
  if (packet.version != currentProtocolVersion) {
    throw const FormatException('WKProto requires protocol version 7');
  }
  WriteData write = WriteData();
  write.writeUint8(packet.version);
  write.writeUint8(packet.deviceFlag);
  write.writeString(packet.deviceID);
  write.writeString(packet.uid);
  write.writeString(packet.token);
  write.writeUint64(BigInt.from(packet.clientTimestamp));
  write.writeString(packet.clientKey);
  write.writeString(packet.appInstanceID);
  write.writeUint64(BigInt.from(packet.installationGeneration));
  write.writeUint64(BigInt.from(packet.sessionGeneration));
  return write.toUint8List();
}

decodeConnack(PacketHeader header, ReadData reader) {
  var connAck = ConnackPacket();
  connAck.header = header;
  if (header.hasServerVersion) {
    var version = reader.readByte();
    if (version != currentProtocolVersion) {
      throw const FormatException('WKProto requires server protocol version 7');
    }
  }
  connAck.timeDiff = reader.readInt64();
  connAck.reasonCode = reader.readUint8();
  if (connAck.reasonCode == 1 && !header.hasServerVersion) {
    throw const FormatException('Successful CONNACK requires server version 7');
  }
  connAck.serverKey = reader.readString();
  connAck.salt = reader.readString();
  connAck.nodeId = reader.readUint64AsInt();
  return connAck;
}

PacketHeader decodeHeader(ReadData reader) {
  var b = reader.readByte();
  var header = PacketHeader();
  header.noPersist = (b & 0x01) > 0;
  header.showUnread = ((b >> 1) & 0x01) > 0;
  header.syncOnce = ((b >> 2) & 0x01) > 0;
  header.dup = ((b >> 3) & 0x01) > 0;
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

  var typeAndFlags =
      (encodeBool(packet.header.dup) << 3) |
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
  final content = packet.encodeMsgContent();
  WriteData write = WriteData();
  write.writeUint8(packet.setting.encode());
  write.writeUint32(packet.clientSeq);
  write.writeString(packet.clientMsgNO);
  write.writeString(packet.channelID);
  write.writeUint8(packet.channelType);
  write.writeUint32(packet.expire);
  write.writeString(packet.encodeMsgKey(encodedContent: content));
  if (packet.setting.topic == 1) {
    write.writeString(packet.topic == null ? "" : packet.topic!);
  }
  write.writeBytes(utf8.encode(content));
  return write.toUint8List();
}

Uint8List encodeRecvAck(RecvAckPacket packet) {
  WriteData write = WriteData();
  write.writeUint64(packet.messageID);
  write.writeUint64(BigInt.from(packet.messageSeq));
  return write.toUint8List();
}

SendAckPacket decodeSendAck(PacketHeader header, ReadData reader) {
  var sendack = SendAckPacket();
  sendack.header = header;
  sendack.messageID = reader.readUint64().toSigned(64).toString();
  sendack.clientSeq = reader.readUint32();
  sendack.messageSeq = reader.readUint64AsInt();
  sendack.reasonCode = reader.readUint8();
  sendack.clientMsgNO = reader.readString();
  sendack.applicationMessageID = reader.readString();
  if (sendack.reasonCode == 1 && sendack.applicationMessageID.isEmpty) {
    throw const FormatException(
      'Successful SENDACK requires application identity',
    );
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
  recv.expire = reader.readUint32();
  recv.clientMsgNO = reader.readString();
  recv.messageID = reader.readUint64();
  recv.messageSeq = reader.readUint64AsInt();
  recv.messageTime = reader.readUint32().toInt();
  if (recv.setting.topic == 1) {
    recv.topic = reader.readString();
  }
  var payload = reader.readRemaining();
  recv.payload = utf8.decode(payload);
  return recv;
}

EventPacket decodeEvent(PacketHeader header, ReadData reader) {
  var event = EventPacket();
  event.header = header;
  event.eventID = reader.readString();
  event.eventType = reader.readString();
  event.timestamp = reader.readUint64AsInt();
  event.data = reader.readRemaining();
  return event;
}

DisconnectPacket decodeDisconnect(PacketHeader header, ReadData reader) {
  var disconnect = DisconnectPacket();
  disconnect.header = header;
  disconnect.reasonCode = reader.readUint8();
  disconnect.reason = reader.readString();
  return disconnect;
}
