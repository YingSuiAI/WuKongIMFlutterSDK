## 悟空IM Flutter SDK

 ![](https://img.shields.io/static/v1?label=platform&message=flutter&color=green) ![](https://img.shields.io/hexpm/l/plug.svg)

[悟空IM](https://github.com/WuKongIM/WuKongIM "文档") flutter sdk 源码 [详细文档](http://githubim.com/sdk/flutter.html "文档")

## 快速入门

#### 安装
[![pub package](https://img.shields.io/pub/v/wukongimfluttersdk.svg)](https://pub.dartlang.org/packages/wukongimfluttersdk)

```
dependencies:
  wukongimfluttersdk: ^version // 版本号看上面
```
#### 引入
```dart
import 'package:wukongimfluttersdk/wkim.dart';
```

**初始化sdk**
```dart
// All identity values come from the current server-issued IM session.
final options = Options.newDefault(uid, token)
  ..installationID = installationId
  ..appInstanceID = appInstanceId
  ..installationGeneration = installationGeneration
  ..sessionGeneration = sessionGeneration;
final ready = await WKIM.shared.setup(options);
if (!ready) return; // This setup was rejected or superseded by another session.
```
**初始化IP**
```dart
WKIM.shared.options.getAddr = (Function(String address) complete) async {
    // 可通过接口获取后返回
      String ip = await HttpUtils.getIP();
      complete(ip);
    };
```
**连接**
```dart
WKIM.shared.connectionManager.connect();
```
**断开**
```dart
// isLogout true：退出并不再重连 false：退出保持重连
WKIM.shared.connectionManager.disconnect(isLogout)
```

**发消息**
```dart
await WKIM.shared.messageManager.sendMessage(
  WKTextContent('我是文本消息'),
  WKChannel(channelID, channelType),
);
```

### 当前协议与生命周期

- 只支持协议 v6 和完整的服务端会话身份，不降级到旧协议。初始化和发送的
  Future 必须等待并处理失败；发送 Future 完成不等于服务端已提交，最终结果由
  与当前发送尝试匹配的 SENDACK 驱动。
- 异步消息操作绑定发起时的会话和数据库。同会话断线重连保留待确认发送；登出或
  更换身份不能重发旧会话消息，迟到 ACK 不能更新新账号数据库。
- SQLite 内的 `wk_schema_migrations` 是唯一迁移完成依据。当前数据库可原位重开，
  新数据库按事务初始化；无此 ledger 的旧业务数据库不受支持，不读取旧偏好水位，
  也不会自动清库或改路径。初始化失败必须交回调用方处理。

## 监听
**连接监听**
```dart
WKIM.shared.connectionManager.addOnConnectionStatus('home',
        (status, reason,connectInfo) {
      if (status == WKConnectStatus.connecting) {
        // 连接中
      } else if (status == WKConnectStatus.success) {
        var nodeId = connectInfo?.nodeId; // 节点id
        // 成功
      } else if (status == WKConnectStatus.noNetwork) {
        // 网络异常
      } else if (status == WKConnectStatus.syncMsg) {
        //同步消息中
      } else if (status == WKConnectStatus.syncCompleted) {
        //同步完成
      }
    });
```
**消息入库**
```dart
WKIM.shared.messageManager.addOnMsgInsertedListener((wkMsg) {
      // todo 展示在UI上
    });
```
**收到新消息**
```dart
WKIM.shared.messageManager.addOnNewMsgListener('chat', (msgs) {
      // todo 展示在UI上
    });
```
**刷新某条消息**
```dart
WKIM.shared.messageManager.addOnRefreshMsgListener('chat', (wkMsg) {
      // todo 刷新消息
    });
```

**命令消息(cmd)监听**
```dart
WKIM.shared.cmdManager.addOnCmdListener('chat', (cmdMsg) {
    // todo 按需处理cmd消息
});
```
- 包含`key`的事件监听均有移除监听的方法，为了避免重复收到事件回掉，在退出或销毁页面时通过传入的`key`移除事件

### 许可证
悟空IM 使用 Apache 2.0 许可证。有关详情，请参阅 LICENSE 文件。
