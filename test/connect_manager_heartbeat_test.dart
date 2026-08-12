import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:wukongimfluttersdk/manager/connect_manager.dart';

void main() {
  test('heartbeat interval emits every thirty seconds', () {
    fakeAsync((async) {
      var heartbeats = 0;
      final timer = Timer.periodic(
        WKConnectionManager.shared.heartIntervalSecond,
        (_) => heartbeats++,
      );

      async.elapse(const Duration(seconds: 29));
      expect(heartbeats, 0);

      async.elapse(const Duration(seconds: 1));
      expect(heartbeats, 1);

      async.elapse(const Duration(seconds: 30));
      expect(heartbeats, 2);
      timer.cancel();
    });
  });
}
