// Run from the matching WuKongIM server checkout:
// GOWORK=off go run /absolute/sdk/test/fixtures/generate_protocol_golden.go
// The server's production v7 codec and encryption, not Dart, are the oracle.
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/codec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkprotoenc"
)

func main() {
	printFrame := func(name string, packet frame.Frame) {
		encoded, err := codec.New().EncodeFrame(packet, frame.ApplicationMessageIDVersion)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s=%s\n", name, hex.EncodeToString(encoded))
	}
	printFrame("connect", &frame.ConnectPacket{
		Version: 7, DeviceFlag: 1, DeviceID: "install-1", UID: "u1", Token: "token-1",
		ClientTimestamp: 1786521600000, ClientKey: "client-key", AppInstanceID: "app-1",
		InstallationGeneration: 3, SessionGeneration: 7,
	})
	printFrame("connack", &frame.ConnackPacket{
		Framer: frame.Framer{HasServerVersion: true}, ServerVersion: 7,
		TimeDiff: -1000, ReasonCode: frame.ReasonSuccess,
		ServerKey: "server-key", Salt: "fedcba9876543210", NodeId: 42,
	})
	printFrame("sendack", &frame.SendackPacket{
		MessageID: 9007199254740993, ClientSeq: 0x1020304, MessageSeq: 0x100000003,
		ReasonCode: frame.ReasonSuccess, ClientMsgNo: "client-42",
		ApplicationMessageID: "019c0000-0000-7000-8000-000000000001",
	})
	keys := wkprotoenc.SessionKeys{AESKey: []byte("0123456789abcdef"), AESIV: []byte("fedcba9876543210")}
	recv, err := wkprotoenc.SealRecvPacket(&frame.RecvPacket{
		Framer: frame.Framer{RedDot: true, DUP: true}, Setting: 0x88,
		MessageID: 9007199254740993, MessageSeq: 0x100000003,
		ClientMsgNo: "client-42", FromUID: "sender", ChannelID: "会话", ChannelType: 2,
		Expire: 3600, Topic: "topic-1", Timestamp: 1788998400,
		Payload: []byte(`{"type":1,"content":"你好"}`),
	}, keys)
	if err != nil {
		panic(err)
	}
	printFrame("recv", recv)
}
