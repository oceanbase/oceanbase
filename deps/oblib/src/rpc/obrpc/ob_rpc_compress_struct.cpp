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

#define USING_LOG_PREFIX RPC_OBRPC
#include "ob_rpc_compress_struct.h"
#include "ob_rpc_net_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc::frame;
namespace oceanbase
{
namespace obrpc
{

oceanbase::common::ObCompressorType ObRpcCompressCtx::get_compress_type(ObRpcCompressMode mode) const
{
  oceanbase::common::ObCompressorType type = NONE_COMPRESSOR;
  if (RPC_STREAM_COMPRESS_LZ4 == mode) {
    type = STREAM_LZ4_COMPRESSOR;
  } else if (RPC_STREAM_COMPRESS_ZSTD == mode) {
    type = STREAM_ZSTD_COMPRESSOR;
  } else if (RPC_STREAM_COMPRESS_ZSTD_138 == mode) {
    type = STREAM_ZSTD_1_3_8_COMPRESSOR;
  } else {
    type = NONE_COMPRESSOR;
  }
  return type;
}


int ObRpcCompressCCtx::init(ObRpcCompressMode mode,
                            int16_t block_size,
                            char *ring_buffer,
                            int64_t ring_buffer_size)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObCompressorType type = get_compress_type(mode);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(mode), K(block_size), K(ret));
  } else if (OB_UNLIKELY(!is_valid_compress_mode(mode))
             || OB_UNLIKELY(block_size <= 0)
             || OB_ISNULL(ring_buffer)
             || OB_UNLIKELY(ring_buffer_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compress mode", K(mode), K(ret));
  } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_stream_compressor(type, compressor_))) {
    LOG_WARN("get_compressor failed", K(type), K(ret));
  } else if (OB_ISNULL(compressor_)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("got compressor is NULL", K(ret));
  } else if (OB_FAIL(compressor_->create_compress_ctx(cctx_))) {
    LOG_WARN("failed to create compress ctx", K(type), K(ret));
  } else if (OB_ISNULL(cctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compress_ctx_ is NULL", KP(cctx_), K(ret));
  } else {
    compress_mode_ = mode;
    block_size_ = block_size;
    ring_buffer_ = ring_buffer;
    ring_buffer_size_ = ring_buffer_size;
    ring_buffer_pos_ = 0;
    total_data_size_before_compress_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObRpcCompressCCtx::reset_mode(ObRpcCompressMode new_mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("compress ctx is not inited", K(ret));
  } else if (compress_mode_ == new_mode) {
    //no change, do nothing
  } else {
    if (RPC_STREAM_COMPRESS_NONE != compress_mode_) {
      //free old ctx, and set ctx to NULL
      if (OB_ISNULL(compressor_) || OB_ISNULL(cctx_) || OB_ISNULL(ring_buffer_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ctx member is NULL", KP(compressor_), KP(cctx_), KP(ring_buffer_),
                 K(compress_mode_), K(ret));
      } else if (OB_FAIL(compressor_->free_compress_ctx(cctx_))) {
        LOG_WARN("failed to free compress ctx", K(ret));
      } else {
        MEMSET(ring_buffer_, 0, ring_buffer_size_);
        compressor_ = NULL;
        cctx_ = NULL;
        ring_buffer_pos_ = 0;
        total_data_size_before_compress_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      compress_mode_ = new_mode;
      ObCompressorType type = get_compress_type(new_mode);

      if (RPC_STREAM_COMPRESS_NONE == compress_mode_) {
        compressor_ = NULL;
      } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_stream_compressor(type, compressor_))) {
        LOG_WARN("get_stream_compressor failed", K(type), K(ret));
      } else if (OB_ISNULL(compressor_)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("got compressor is NULL", K(ret));
      } else if (OB_FAIL(compressor_->create_compress_ctx(cctx_))) {
        LOG_WARN("failed to create compress ctx", K(type), K(ret));
      } else if (OB_ISNULL(cctx_)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("got compress_ctx is NULL", K(ret));
      } else {
        ring_buffer_pos_ = 0;
        total_data_size_before_compress_ = 0;
      }
    }
  }
  return ret;
}

int ObRpcCompressCCtx::free_ctx_mem()
{
  int ret = OB_SUCCESS;
  if (NULL != cctx_) {
    if (OB_ISNULL(compressor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compressor_ is NULL when ctx is not NULL", K(compress_mode_), K(ret));
    } else if (OB_FAIL(compressor_->free_compress_ctx(cctx_))) {
      LOG_ERROR("failed to free compress ctx", K(compress_mode_), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObRpcCompressDCtx::init(ObRpcCompressMode mode,
                            int16_t block_size,
                            char *ring_buffer,
                            int64_t ring_buffer_size)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObCompressorType type = get_compress_type(mode);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(mode), K(block_size), K(ret));
  } else if (OB_UNLIKELY(!is_valid_compress_mode(mode))
             || OB_UNLIKELY(block_size <= 0)
             || OB_ISNULL(ring_buffer)
             || OB_UNLIKELY(ring_buffer_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compress mode", K(mode), K(ret));
  } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_stream_compressor(type, compressor_))) {
    LOG_WARN("get_compressor failed", K(type), K(ret));
  } else if (OB_ISNULL(compressor_)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("got compressor is NULL", K(ret));
  } else if (OB_FAIL(compressor_->create_decompress_ctx(dctx_))) {
    LOG_WARN("failed to create compress ctx", K(type), K(ret));
  } else if (OB_ISNULL(dctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("decompress_ctx_ is NULL", KP(dctx_), K(ret));
  } else {
    compress_mode_ = mode;
    block_size_ = block_size;
    ring_buffer_ = ring_buffer;
    ring_buffer_size_ = ring_buffer_size;
    ring_buffer_pos_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObRpcCompressDCtx::reset_mode(ObRpcCompressMode new_mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("decompress ctx is not inited", K(ret));
  } else if (compress_mode_ == new_mode) {
    //no change, do nothing
  } else {
    if (RPC_STREAM_COMPRESS_NONE != compress_mode_) {
      //free old ctx, and set ctx to NULL
      if (OB_ISNULL(compressor_) || OB_ISNULL(dctx_) || OB_ISNULL(ring_buffer_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ctx member is NULL", KP(compressor_), KP(dctx_), KP(ring_buffer_),
                 K(compress_mode_), K(ret));
      } else if (OB_FAIL(compressor_->free_decompress_ctx(dctx_))) {
        LOG_WARN("failed to free compress ctx", K(ret));
      } else {
        MEMSET(ring_buffer_, 0, ring_buffer_size_);
        compressor_ = NULL;
        dctx_ = NULL;
        ring_buffer_pos_ = 0;
      }
    }

    if (OB_SUCC(ret)) {
      compress_mode_ = new_mode;
      ObCompressorType type = get_compress_type(new_mode);

      if (RPC_STREAM_COMPRESS_NONE == compress_mode_) {
        compressor_ = NULL;
      } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_stream_compressor(type, compressor_))) {
        LOG_WARN("get_stream_compressor failed", K(type), K(ret));
      } else if (OB_ISNULL(compressor_)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("got compressor is NULL", K(ret));
      } else if (OB_FAIL(compressor_->create_decompress_ctx(dctx_))) {
        LOG_WARN("failed to create compress ctx", K(type), K(ret));
      } else if (OB_ISNULL(dctx_)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("got compress_ctx is NULL", K(ret));
      } else {
        ring_buffer_pos_ = 0;
      }
    }
  }
  return ret;
}

int ObRpcCompressDCtx::free_ctx_mem()
{
  int ret = OB_SUCCESS;
  if (NULL != dctx_) {
    if (OB_ISNULL(compressor_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("compressor_ is NULL when ctx is not NULL", K(compress_mode_), K(ret));
    } else if (OB_FAIL(compressor_->free_decompress_ctx(dctx_))) {
      LOG_ERROR("failed to free decompress ctx", K(compress_mode_), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

void ObRpcCompressCtxSet::free_ctx_memory()
{
  compress_ctx_.free_ctx_mem();
  decompress_ctx_.free_ctx_mem();
}

int ObCompressPacketHeader::init_magic(ObRpcCompressMode compress_mode, bool is_data_compressed)
{
  int ret = OB_SUCCESS;
  if (is_data_compressed) {
    magic_ |= COMPRESS_RESULT_MASK;
  }

  if (RPC_STREAM_COMPRESS_LZ4 == compress_mode) {
    magic_ |= COMPRESS_LZ4_MASK;
  } else if (RPC_STREAM_COMPRESS_ZSTD == compress_mode) {
    magic_ |= COMPRESS_ZSTD_MASK;
  } else if (RPC_STREAM_COMPRESS_ZSTD_138 == compress_mode) {
    magic_ |= COMPRESS_ZSTD_138_MASK;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid compress_mode", K(compress_mode), K(ret));
  }
  return ret;
}

int ObCompressPacketHeader::encode(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, magic_))) {
    LOG_WARN("encode failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, full_size_))) {
    LOG_WARN("encode failed", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObCompressPacketHeader::decode(char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &magic_))) {
    LOG_WARN("decode failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &full_size_))) {
    LOG_WARN("decode failed", K(ret));
  } else {/*do nothing*/}

  return ret;
}

int64_t ObCompressPacketHeader::get_encode_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i8(magic_);
  size += serialization::encoded_length_i32(full_size_);
  return size;
}

int ObCompressHeadPacketHeader::init(ObRpcCompressMode compress_mode,
                                     int32_t full_size,
                                     int32_t data_len_before_compress,
                                     int32_t data_len_after_compress,
                                     int16_t compressed_len,
                                     int16_t origin_len,
                                     bool is_data_compressed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObRpcCompressCtx::is_valid_compress_mode(compress_mode))
                || OB_UNLIKELY(full_size <= 0)
                || OB_UNLIKELY(data_len_before_compress <= 0)
                || OB_UNLIKELY(data_len_after_compress <= 0)
                || OB_UNLIKELY(compressed_len <= 0)
                || OB_UNLIKELY(origin_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(compress_mode),
             K(data_len_before_compress), K(data_len_after_compress),
             K(compressed_len), K(origin_len), K(is_data_compressed), K(ret));

  } else {
    magic_ = (int8_t)0x00;

    if (OB_FAIL(init_magic(compress_mode, is_data_compressed))) {
      LOG_WARN("failed to init magic", K(compress_mode), K(is_data_compressed), K(ret));
    } else {
      full_size_ = full_size;
      total_data_len_before_compress_ = data_len_before_compress;
      total_data_len_after_compress_ = data_len_after_compress;
      compressed_size_ = compressed_len;
      origin_size_ = origin_len;
    }
  }
    return ret;
}

int ObCompressHeadPacketHeader::encode(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(ObCompressPacketHeader::encode(buf, buf_len, pos))) {
    LOG_WARN("encode header failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, total_data_len_before_compress_))) {
    LOG_WARN("encode total_data_len_before_compress_ failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, total_data_len_after_compress_))) {
    LOG_WARN("encode total_data_len_after_compress_ failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, compressed_size_))) {
    LOG_WARN("encode compressed_size_ failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, origin_size_))) {
    LOG_WARN("encode origin_size_ failed", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObCompressHeadPacketHeader::decode(char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(ObCompressPacketHeader::decode(buf, data_len, pos))) {
    LOG_WARN("decode header failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &total_data_len_before_compress_))) {
    LOG_WARN("decode total_data_len_before_compress_ failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &total_data_len_after_compress_))) {
    LOG_WARN("decode total_data_len_after_compress_ failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &compressed_size_))) {
    LOG_WARN("decode compressed_size_ failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &origin_size_))) {
    LOG_WARN("decode origin_size_ failed", K(ret));
  } else {/*do nothing*/}

  return ret;
}

int64_t ObCompressHeadPacketHeader::get_encode_size() const
{
  int64_t size = 0;
  size += ObCompressPacketHeader::get_encode_size();
  size += serialization::encoded_length_i32(total_data_len_before_compress_);
  size += serialization::encoded_length_i32(total_data_len_after_compress_);
  size += serialization::encoded_length_i16(compressed_size_);
  size += serialization::encoded_length_i16(origin_size_);
  return size;
}

int ObCompressSegmentPacketHeader::init(ObRpcCompressMode compress_mode,
                                        int16_t compressed_len,
                                        int16_t origin_len,
                                        bool is_data_compressed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObRpcCompressCtx::is_valid_compress_mode(compress_mode))
      || OB_UNLIKELY(compressed_len <= 0) || OB_UNLIKELY(origin_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(compress_mode), K(compressed_len),
             K(origin_len), K(is_data_compressed), K(ret));
  } else {
    magic_ = int8_t(0x80);

    if (OB_FAIL(init_magic(compress_mode, is_data_compressed))) {
      LOG_WARN("failed to init magic", K(compress_mode), K(is_data_compressed), K(ret));
    } else {
      full_size_ = compressed_len + static_cast<int32_t>(get_encode_size());
      origin_size_ = origin_len;
    }
  }
    return ret;
}


int ObCompressSegmentPacketHeader::encode(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(ObCompressPacketHeader::encode(buf, buf_len, pos))) {
    LOG_WARN("encode header failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, origin_size_))) {
    LOG_WARN("encode origin_size_ failed", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObCompressSegmentPacketHeader::decode(char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(ObCompressPacketHeader::decode(buf, data_len, pos))) {
    LOG_WARN("encode header failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &origin_size_))) {
    LOG_WARN("decode origin_size_ failed", K(ret));
  } else {/*do nothing*/}

  return ret;
}

int64_t ObCompressSegmentPacketHeader::get_encode_size() const
{
  int64_t size = 0;
  size += ObCompressPacketHeader::get_encode_size();
  size += serialization::encoded_length_i16(origin_size_);
  return size;
}

ObCmdPacketInCompress::ObCmdPacketInCompress(CmdType cmd_type)
{
  magic_ = MAGIC_NUM;
  full_size_ = 0;
  payload_ = CMD_PAY_LOAD;
  cmd_type_ = static_cast<int16_t>(cmd_type);
  compress_mode_ = static_cast<int16_t>(RPC_STREAM_COMPRESS_NONE);
}

ObCmdPacketInCompress::ObCmdPacketInCompress()
{
  magic_ = MAGIC_NUM;
  full_size_ = 0;
  payload_ = CMD_PAY_LOAD;
  cmd_type_ = static_cast<int16_t>(RPC_CMD_INVALID);
  compress_mode_ = static_cast<int16_t>(RPC_STREAM_COMPRESS_NONE);
}

int ObCmdPacketInCompress::encode(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(ObCompressPacketHeader::encode(buf, buf_len, pos))) {
    LOG_WARN("encode header failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, payload_))) { // next 4 for packet content length
    LOG_WARN("Encode payload_ error", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, cmd_type_))) {
    LOG_WARN("Encode cmd_type_ error", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, compress_mode_))) {
    LOG_WARN("Encode compress_mode_ error", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObCmdPacketInCompress::decode(char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  //maybe need to cmp first 4 byte
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len <= 4)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(ObCompressPacketHeader::decode(buf, data_len, pos))) {
    LOG_WARN("decode header failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &payload_))) {
    LOG_WARN("decode payload_ failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &cmd_type_))) {
    LOG_WARN("decode cmd_type_ failed", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &compress_mode_))) {
    LOG_WARN("decode compress_mode_ failed", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int64_t ObCmdPacketInCompress::get_encode_size() const
{
  int64_t size = ObCompressPacketHeader::get_encode_size()
      + serialization::encoded_length_i16(payload_)
      + serialization::encoded_length_i16(cmd_type_)
      + serialization::encoded_length_i16(compress_mode_);
  return size;
}

int64_t ObCmdPacketInCompress::get_decode_size() const
{
  return  ObCompressPacketHeader::get_decode_size() + serialization::encoded_length_i16(payload_) + payload_;
}

int ObCmdPacketInNormal::encode(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY((buf_len - pos) < sizeof(ObRpcPacket::MAGIC_COMPRESS_HEADER_FLAG))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len), K(pos), K(ret));
  } else {
    MEMCPY(buf, ObRpcPacket::MAGIC_COMPRESS_HEADER_FLAG, 4);                    // first 4 bytes store magic flag
    pos += 4;
    if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, full_size_))) { // next 4 for packet content length
      LOG_WARN("Encode full_size_ error", K(ret));
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, payload_))) { // next 4 for packet content length
      LOG_WARN("Encode payload_ error", K(ret));
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, compress_mode_))) {
      LOG_WARN("Encode compress_mode_ error", K(ret));
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, block_size_))) {//skip 4 bytes for reserved
      LOG_WARN("Encode block_size_ error", K(ret));
    } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, ring_buffer_size_))) {//skip 4 bytes for reserved
      LOG_WARN("Encode ring_buffer_size_ error", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObCmdPacketInNormal::decode(char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  //maybe need to cmp first 4 byte
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len <= 4)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len));
  } else {
    uint8_t mflag[4] = {static_cast<uint8_t> (buf[0]),
      static_cast<uint8_t> (buf[1]),
      static_cast<uint8_t> (buf[2]),
      static_cast<uint8_t> (buf[3])};
     pos += 4;
    if (OB_UNLIKELY(0 != MEMCMP(&mflag[0], ObRpcPacket::MAGIC_COMPRESS_HEADER_FLAG, sizeof(mflag)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid mflag", K(buf[0]), K(buf[1]), K(buf[2]), K(buf[3]), K(ret));
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &full_size_))) {
      LOG_WARN("decode full_size_ failed", K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &payload_))) {
      LOG_WARN("decode payload_ failed", K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &compress_mode_))) {
      LOG_WARN("decode compress_mode_ failed", K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &block_size_))) {
      LOG_WARN("decode block_size_ failed", K(ret));
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &ring_buffer_size_))) {
      LOG_WARN("decode ring_buffer_size_ failed", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int64_t ObCmdPacketInNormal::get_encode_size() const
{
  return  4 + serialization::encoded_length_i32(full_size_)
      + serialization::encoded_length_i16(payload_)
      + serialization::encoded_length_i16(compress_mode_)
      + serialization::encoded_length_i16(block_size_)
      + serialization::encoded_length_i32(ring_buffer_size_);
}

int64_t ObCmdPacketInNormal::get_decode_size() const
{
  return  4 + serialization::encoded_length_i32(full_size_)
      + serialization::encoded_length_i16(payload_) + payload_;
}

}//namespace obrpc
}//namespace oceanbase
