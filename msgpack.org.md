High Performance and Feature-Rich Idiomatic Go Library providing
encode/decode support for msgpack, *and [binc](http://github.com/ugorji/binc)* .

To install:

    go get github.com/ugorji/go/codec

Online documentation: [http://godoc.org/github.com/ugorji/go/codec]

The idiomatic Go support is as seen in other encoding packages in
the standard library (ie json, xml, gob, etc).

Rich Feature Set includes:

  - Simple but extremely powerful and feature-rich API
  - Very High Performance (outperforming Gob, Json and Bson by 2-4X)
  - Efficient zero-copying into temporary byte buffers  
    when encoding into or decoding from a byte slice.
  - Standard field renaming via tags
  - Supports extension functions to handle the encode/decode of custom types
  - Schema-less decoding  
  - Provides a RPC Server and Client Codec for net/rpc communication protocol.
  - Msgpack Specific:
      - Provides extension functions to handle spec-defined extensions (binary, timestamp)
      - Options to resolve ambiguities in handling raw bytes (as string or []byte)  
      - RPC Server/Client Codec for msgpack-rpc protocol
      
Typical usage model:

    var (
      mapStrIntfTyp = reflect.TypeOf(map[string]interface{}(nil))
      sliceByteTyp = reflect.TypeOf([]byte(nil))
      timeTyp = reflect.TypeOf(time.Time{})
    )
    
    // create and configure Handle
    var (
      bh codec.BincHandle
      mh codec.MsgpackHandle
    )

    mh.MapType = mapStrIntfTyp
    // Configure these two below accordingly if supporting new msgpack spec or not
    // mh.RawToString = true
    // mh.WriteExt = true
    
    // configure extensions for msgpack, to enable Binary and Time support for tags 0 and 1
    mh.AddExt(sliceByteTyp, 0, mh.BinaryEncodeExt, mh.BinaryDecodeExt)
    mh.AddExt(timeTyp, 1, mh.TimeEncodeExt, mh.TimeDecodeExt)

    // create and use decoder/encoder
    var (
      r io.Reader
      w io.Writer
      b []byte
      h = &mh // or bh to use binc
    )
    
    dec = codec.NewDecoder(r, h)
    dec = codec.NewDecoderBytes(b, h)
    err = dec.Decode(&v) 
    
    enc = codec.NewEncoder(w, h)
    enc = codec.NewEncoderBytes(&b, h)
    err = enc.Encode(v)
    
    //RPC Server
    go func() {
        for {
            conn, err := listener.Accept()
            rpcCodec := codec.GoRpc.ServerCodec(conn, h)
            //OR rpcCodec := codec.MsgpackSpecRpc.ServerCodec(conn, h)
            rpc.ServeCodec(rpcCodec)
        }
    }()
    
    //RPC Communication (client side)
    conn, err = net.Dial("tcp", "localhost:5555")  
    rpcCodec := rpcH.ClientCodec(conn, h)  
    client := rpc.NewClientWithCodec(rpcCodec)

