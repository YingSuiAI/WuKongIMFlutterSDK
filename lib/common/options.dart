import '../proto/proto.dart';

class Options {
  String? uid, token;
  String? addr; // connect address IP:PORT
  int protoVersion = currentProtocolVersion;
  int deviceFlag = 0;
  String? installationID;
  String? appInstanceID;
  int installationGeneration = 0;
  int sessionGeneration = 0;
  bool debug = true;
  Function(Function(String addr) complete)?
  getAddr; // async get connect address
  Proto proto = Proto();
  Options();

  Options.newDefault(this.uid, this.token, {this.addr});

  /// Immutable identity captured before asynchronous work starts. Endpoint and
  /// debug settings do not change the authenticated owner of that work.
  Object get sessionIdentity => (
    uid,
    token,
    protoVersion,
    deviceFlag,
    installationID,
    appInstanceID,
    installationGeneration,
    sessionGeneration,
  );

  bool get hasExactSessionIdentity =>
      installationID != null &&
      installationID!.trim().isNotEmpty &&
      appInstanceID != null &&
      appInstanceID!.trim().isNotEmpty &&
      installationGeneration > 0 &&
      sessionGeneration > 0;
}
