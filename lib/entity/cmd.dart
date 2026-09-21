class WkSyncCMD {
  String cmd = '';
  dynamic param;
}

class WKCMD {
  String cmd = '';
  dynamic param;

  /// Trusted transport origin, separate from any sender/channel in [param].
  /// Empty for commands not delivered by a verified realtime RECV frame.
  String fromUID = '';
  String channelID = '';
  int channelType = 0;
}
