import '../proto/proto.dart';

class Options {
  String? uid, token;
  String? addr; // connect address IP:PORT
  int protoVersion = 0x06; // protocol version
  int deviceFlag = 0;
  String? installationID;
  String? appInstanceID;
  int sessionGeneration = 0;
  bool debug = true;
  Function(Function(String addr) complete)?
      getAddr; // async get connect address
  Proto proto = Proto();
  Options();

  Options.newDefault(this.uid, this.token, {this.addr});
}
