/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_RPC_COMPRESS_STRUCT_H_
#define _OB_RPC_COMPRESS_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace obrpc
{

enum ObRpcCompressMode
{
  RPC_STREAM_COMPRESS_NONE = 0,
  RPC_STREAM_COMPRESS_LZ4 = 1,
  RPC_STREAM_COMPRESS_ZSTD = 2,
  RPC_STREAM_COMPRESS_ZSTD_138 = 3,
};


struct ObRpcCompressCtx
{
public:
  ObRpcCompressCtx() : is_inited_(false),
    compress_mode_(RPC_STREAM_COMPRESS_NONE),
    block_size_(0),
    ring_buffer_pos_(0),
    ring_buffer_size_(0),
    ring_buffer_(NULL),
    compressor_(NULL) {}
  virtual ~ObRpcCompressCtx(){}

  virtual int init(ObRpcCompressMode mode, int16_t block_size, char *ring_buffer, int64_t ring_buffer_size)
  {
    UNUSED(mode);
    UNUSED(block_size);
    UNUSED(ring_buffer);
    UNUSED(ring_buffer_size);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int free_ctx_mem() = 0;
  virtual int reset_mode(ObRpcCompressMode new_mode)
  {
    UNUSED(new_mode);
    return common::OB_NOT_SUPPORTED;
  }

  void set_compress_mode(oceanbase::common::ObCompressorType type)
  {
    compress_mode_ = get_compress_mode_from_type(type);
  }

  static bool is_valid_compress_mode(ObRpcCompressMode mode)
  {
    return (RPC_STREAM_COMPRESS_LZ4 == mode
            || RPC_STREAM_COMPRESS_ZSTD == mode
            || RPC_STREAM_COMPRESS_ZSTD_138 == mode);
  }

  static ObRpcCompressMode get_compress_mode_from_type(oceanbase::common::ObCompressorType type) 
  {
    ObRpcCompressMode compress_mode = RPC_STREAM_COMPRESS_NONE;
    if (common::STREAM_LZ4_COMPRESSOR == type) {
      compress_mode = RPC_STREAM_COMPRESS_LZ4;
    } else if (common::STREAM_ZSTD_COMPRESSOR == type) {
      compress_mode = RPC_STREAM_COMPRESS_ZSTD;
    } else if (common::STREAM_ZSTD_1_3_8_COMPRESSOR == type) {
      compress_mode = RPC_STREAM_COMPRESS_ZSTD_138;
    } else {
      compress_mode = RPC_STREAM_COMPRESS_NONE;
    }
    return compress_mode;
  }

  common::ObStreamCompressor *get_stream_compressor() const {return compressor_;}
  oceanbase::common::ObCompressorType get_compress_type(ObRpcCompressMode mode) const;
  char *get_ring_buffer() {return ring_buffer_;}

  bool is_compress_mode_changed(ObRpcCompressMode mode) const
  {
    return compress_mode_ != mode;
  }
  inline bool is_normal_mode() const { return RPC_STREAM_COMPRESS_NONE == compress_mode_; }
  bool need_compress() { return RPC_STREAM_COMPRESS_NONE != compress_mode_;}

  VIRTUAL_TO_STRING_KV(K(is_inited_), K(compress_mode_), K(block_size_), K(ring_buffer_pos_),
                       K(ring_buffer_size_), KP(ring_buffer_), KP(compressor_));

public:
  bool is_inited_;
  ObRpcCompressMode compress_mode_;
  int16_t block_size_;
  int64_t ring_buffer_pos_;
  int64_t ring_buffer_size_;
  char *ring_buffer_;
  common::ObStreamCompressor *compressor_;
};

struct ObRpcCompressCCtx : public ObRpcCompressCtx
{
public:
  ObRpcCompressCCtx() : total_data_size_before_compress_(0), cctx_(NULL) {}
  ~ObRpcCompressCCtx(){}
  int init(ObRpcCompressMode mode, int16_t block_size, char *ring_buffer, int64_t ring_buffer_size);
  int reset_mode(ObRpcCompressMode new_mode);
  int free_ctx_mem();
  TO_STRING_KV(K(is_inited_), K(compress_mode_), K(block_size_), K(ring_buffer_pos_),
               K(ring_buffer_size_), KP(ring_buffer_), K(total_data_size_before_compress_),
               KP(compressor_), KP(cctx_));
public:
  int64_t total_data_size_before_compress_;
  void *cctx_;
};

struct ObRpcCompressDCtx : public ObRpcCompressCtx
{
public:
  ObRpcCompressDCtx() : dctx_(NULL) {}
  ~ObRpcCompressDCtx(){}
  int init(ObRpcCompressMode mode, int16_t block_size, char *ring_buffer, int64_t ring_buffer_size);
  int reset_mode(ObRpcCompressMode new_mode);
  int free_ctx_mem();
public:
  TO_STRING_KV(K(is_inited_), K(compress_mode_), K(block_size_), K(ring_buffer_pos_),
               K(ring_buffer_size_), KP(ring_buffer_),
               KP(compressor_), KP(dctx_));
  void *dctx_;
};

struct ObRpcCompressCtxSet
{
public:
  ObRpcCompressCtxSet(){}
  ~ObRpcCompressCtxSet(){}
  void free_ctx_memory();
  TO_STRING_KV(K(compress_ctx_), K(decompress_ctx_));
public:
  ObRpcCompressCCtx compress_ctx_;
  ObRpcCompressDCtx decompress_ctx_;
};

struct ObCompressPacketHeader
{
public:
  ObCompressPacketHeader() : magic_(0x00), full_size_(0){}
  ~ObCompressPacketHeader(){}
  static int64_t get_decode_size()
  {
    return sizeof(magic_) + sizeof(full_size_);
  }
  int init_magic(ObRpcCompressMode compress_mode, bool is_data_compressed);
  int encode(char *buf, int64_t buf_len, int64_t &pos);
  int decode(char *buf, int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;
  virtual bool is_data_compressed() const { return 0 != (magic_ & COMPRESS_RESULT_MASK); }
public:
  //The second and third digits from the bottom of magic_num indicate the compression algorithm, currently 00 means lz4, 01 means zstd
  const int8_t COMPRESS_METHOD_MASK = 0x6;
  const int8_t COMPRESS_LZ4_MASK = 0x00;
  const int8_t COMPRESS_ZSTD_MASK = 0x02;
  const int8_t COMPRESS_ZSTD_138_MASK = 0x04;
  //The lowest bit of magic_num is 0 means the data is not compressed, and the lowest bit is 1 means the data is compressed
  const int8_t COMPRESS_RESULT_MASK = 0x01;
  const int8_t COMPRESS_PACKET_TYPE_MASK = (int8_t)0x80;

  //Do not add or delete fields at the very beginning of the packet sent in compressed state
  int8_t magic_;
  int32_t full_size_;//include PacketHeader and following segment packet
};

//Do not modify this structure, do not add or delete fields at will
struct ObCompressHeadPacketHeader : public ObCompressPacketHeader
{
public:
  //The highest bit of Magic HeadPacket is 0
  ObCompressHeadPacketHeader() :
      total_data_len_before_compress_(0),
      total_data_len_after_compress_(0),
      compressed_size_(0),
      origin_size_(0) {}

  ~ObCompressHeadPacketHeader() {}
  int init(ObRpcCompressMode compress_mode,
           int32_t full_len,
           int32_t data_len_before_compress,
           int32_t data_len_after_compress,
           int16_t compressed_size,
           int16_t origin_size,
           bool is_data_compressed);
  int encode(char *buf, int64_t buf_len, int64_t &pos);
  int decode(char *buf, int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;

  TO_STRING_KV(K(magic_), K(full_size_), K(total_data_len_before_compress_),
               K(total_data_len_after_compress_), K(compressed_size_), K(origin_size_));

public:
  int32_t total_data_len_before_compress_;//The size of the entire ObRpcPacket before compression
  int32_t total_data_len_after_compress_;//The compressed size of the entire ObRpcPacket
  int16_t compressed_size_;//The size of compressed data in this package
  int16_t origin_size_;//Original size of compressed data in this package
};

//Do not modify this structure, do not add or delete fields at will
struct ObCompressSegmentPacketHeader : public ObCompressPacketHeader
{
public:
  ObCompressSegmentPacketHeader() : origin_size_(0) {}
  ~ObCompressSegmentPacketHeader(){}
  int init(ObRpcCompressMode compress_mode,
           int16_t compressed_size,
           int16_t origin_size,
           bool is_data_compressed);
  int encode(char *buf, int64_t buf_len, int64_t &pos);
  int decode(char *buf, int64_t data_len, int64_t &pos);
  virtual int64_t get_encode_size() const;
  TO_STRING_KV(K(magic_), K(full_size_), K(origin_size_));
public:
  int16_t origin_size_;
};

//If you want to add or delete members, you need to pay attention to the compatibility: according to the value of the payload, determine how many members can be deserialized in the end
struct ObCmdPacketInCompress : public ObCompressPacketHeader
{
  //cmd packet post when in compress mode
public:
  enum CmdType {
    RPC_CMD_INVALID = 0,
    RPC_CMD_MODIFY_COMPRESS_MODE = 1,
    RPC_CMD_RESET_COMPRESS_CTX = 2,
  };
  explicit ObCmdPacketInCompress(CmdType cmd_type);
  ObCmdPacketInCompress();
  ~ObCmdPacketInCompress(){}
  void set_compress_mode(ObRpcCompressMode mode) {compress_mode_ = mode;}
  int encode(char *buf, int64_t buf_len, int64_t &pos);
  int decode(char *buf, int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;
  int64_t get_decode_size() const;
  bool is_modify_compress_mode() const {return RPC_CMD_MODIFY_COMPRESS_MODE == cmd_type_;}
  bool is_reset_compress_ctx() const {return RPC_CMD_RESET_COMPRESS_CTX == cmd_type_;}

public:
  const int16_t CMD_PAY_LOAD = sizeof(cmd_type_) + sizeof(compress_mode_);
  const int8_t MAGIC_NUM = (int8_t)0xff;
  TO_STRING_KV(K(magic_), K(full_size_), K(payload_), K(cmd_type_), K(compress_mode_));
  int16_t payload_;//data length after payload
  int16_t cmd_type_;
  int16_t compress_mode_;
};

//If you want to add or delete members, you need to pay attention to compatibility
struct ObCmdPacketInNormal
{
  //cmd packet post when in normal mode
public:
  ObCmdPacketInNormal()
      : full_size_(0), payload_(CMD_PAY_LOAD), compress_mode_(RPC_STREAM_COMPRESS_NONE), block_size_(0), ring_buffer_size_(0) {}
  ObCmdPacketInNormal(ObRpcCompressMode mode, int16_t block_size, int32_t ring_buffer_size)
      : full_size_(0), payload_(CMD_PAY_LOAD), compress_mode_(mode), block_size_(block_size), ring_buffer_size_(ring_buffer_size) {}
  ~ObCmdPacketInNormal(){}
  int encode(char *buf, int64_t buf_len, int64_t &pos);
  //If you want to add or delete members, you need to pay attention to the compatibility: according to the value of the payload, determine how many members can be deserialized in the end
  int decode(char *buf, int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;
  ObRpcCompressMode get_compress_mode() const {return static_cast<ObRpcCompressMode>(compress_mode_);}
  int64_t get_decode_size() const;
  TO_STRING_KV(K(full_size_), K(payload_), K(compress_mode_), K(block_size_), K(ring_buffer_size_));
public:
  //4 bytes magic_num
  const int16_t CMD_PAY_LOAD = sizeof(compress_mode_) + sizeof(block_size_) + sizeof(ring_buffer_size_);
  int32_t full_size_;//include following compressed packet
  int16_t payload_;//data length after payload
  int16_t compress_mode_;
  int16_t block_size_;
  int32_t ring_buffer_size_;
};

}//namespace obrpc
}//namespace oceanbase
#endif
