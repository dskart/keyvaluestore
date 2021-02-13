# lessdbstore

This is a backend implementation for [LessDB](https://github.com/ccbrown/lessdb).

## Regenerating Protobuf Files

If you need to regenerate the protobuf files, you can use this command:

```
rm -rf lessdbstore/protos && mkdir lessdbstore/protos && protoc --go_opt=Mclient/client.proto=github.com/ccbrown/keyvaluestore/lessdbstore/protos/client --go_opt=Mcommon/rustproto.proto=github.com/ccbrown/keyvaluestore/lessdbstore/protos/common --go_out=lessdbstore/protos --go-grpc_opt=Mclient/client.proto=github.com/ccbrown/keyvaluestore/lessdbstore/protos/client --go-grpc_opt=Mcommon/rustproto.proto=github.com/ccbrown/keyvaluestore/lessdbstore/protos/common --go-grpc_out=lessdbstore/protos ../lessdb/protos/client/client.proto ../lessdb/protos/common/rustproto.proto --proto_path=../lessdb/protos
```
