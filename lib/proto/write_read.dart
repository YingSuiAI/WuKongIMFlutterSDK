import 'dart:convert';
import 'dart:typed_data';

class ReadData {
  final Uint8List _data;
  late final ByteData _byteData;
  int offset = 0;

  int get remainingLength => _data.length - offset;

  ReadData(this._data) {
    _byteData = ByteData.sublistView(_data);
  }

  void _requireBytes(int count) {
    if (count > remainingLength) {
      throw const FormatException('Truncated WKProto field');
    }
  }

  int readByte() {
    _requireBytes(1);
    var d = _data[offset];
    offset++;
    return d;
  }

  int readUint8() {
    _requireBytes(1);
    var v = _byteData.getUint8(offset);
    offset++;
    return v;
  }

  int readUint16() {
    _requireBytes(2);
    var v = _byteData.getUint16(offset);
    offset += 2;
    return v;
  }

  Uint8List readRemaining() {
    var data = _data.sublist(offset);
    offset = _data.length;
    return data;
  }

  String readString() {
    var len = readUint16();
    if (len <= 0) {
      return "";
    }
    _requireBytes(len);
    var d = _data.sublist(offset, offset + len);
    offset += len;
    return utf8.decode(d);
  }

  int readUint32() {
    _requireBytes(4);
    var v = _byteData.getUint32(offset);
    offset += 4;
    return v;
  }

  BigInt readUint64() {
    _requireBytes(8);
    var data = _data.sublist(offset, offset + 8);
    offset += 8;
    var n = BigInt.from(0);
    for (var i = 0; i < data.length; i++) {
      var d = BigInt.from(2).pow((data.length - i - 1) * 8);
      n = n + BigInt.from(data[i]) * d;
    }
    return n;
  }

  int readInt64() {
    return _exactInt(readUint64().toSigned(64));
  }

  int readUint64AsInt() => _exactInt(readUint64());

  int _exactInt(BigInt value) {
    if (!value.isValidInt) {
      throw const FormatException('WKProto integer exceeds runtime int range');
    }
    return value.toInt();
  }

  int readVariableLength() {
    var multiplier = 0;
    var rLength = 0;
    while (multiplier < 27) {
      if (remainingLength == 0) {
        return -1;
      }
      var b = readUint8();
      /* tslint:disable */
      rLength = rLength | ((b & 127) << multiplier);
      if ((b & 128) == 0) {
        return rLength;
      }
      multiplier += 7;
    }
    throw const FormatException('WKProto remaining length exceeds four bytes');
  }
}

class WriteData {
  List<int> data = [];
  writeUint8(int v) {
    RangeError.checkValueInInterval(v, 0, 0xff, 'uint8');
    data.add(v & 0xff);
  }

  writeUint16(int v) {
    RangeError.checkValueInInterval(v, 0, 0xffff, 'uint16');
    data.add((v >> 8) & 0xff);
    data.add(v & 0xff);
  }

  writeUint32(int v) {
    RangeError.checkValueInInterval(v, 0, 0xffffffff, 'uint32');
    data.add((v >> 24) & 0xff);
    data.add((v >> 16) & 0xff);
    data.add((v >> 8) & 0xff);
    data.add((v) & 0xff);
  }

  var d32 = BigInt.from(4294967296);
  writeUint64(BigInt b) {
    if (b.isNegative || b.bitLength > 64) {
      throw RangeError('WKProto uint64 is outside the wire range');
    }
    var b1 = (b ~/ d32).toInt();
    var b2 = (b % d32).toInt();
    writeUint32(b1);
    writeUint32(b2);
  }

  writeBytes(List<int> bytes) {
    data.addAll(bytes);
  }

  writeString(String v) {
    if (v.isNotEmpty) {
      // var wdata = v.codeUnits;
      var wdata = utf8.encode(v);
      RangeError.checkValueInInterval(wdata.length, 0, 0xffff, 'UTF-8 length');
      writeUint16(wdata.length);
      data.addAll(wdata);
    } else {
      writeUint16(0x00);
    }
  }

  toUint8List() {
    return Uint8List.fromList(data);
  }
}
