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
#define USING_LOG_PREFIX STORAGE

#include "ob_string_stream_encoder.h"
#include "ob_integer_stream_encoder.h"
#include "ob_cs_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace oceanbase::common;

int ObStringStreamEncoder::encode(
    ObStringStreamEncoderCtx &ctx,
    ObIDatumIter &iter,
    ObMicroBufferWriter &writer,
    ObMicroBufferWriter *all_string_writer,
    ObIArray<uint32_t> &stream_offset_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(iter.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("datums is empty", KR(ret), K(ctx));
  } else {
    ctx_ = &ctx;
    writer_ = &writer;
    all_string_writer_ = all_string_writer;
    int64_t orig_pos = writer.length();
    if (OB_FAIL(convert_datum_to_stream_(iter))) {
      LOG_WARN("fail to convert to stream", KR(ret));
    } else if (OB_FAIL(encode_byte_stream_(stream_offset_arr))) {
      LOG_WARN("fail to encode_byte_stream", KR(ret));
    } else if (OB_FAIL(encode_offset_stream_(stream_offset_arr))) {
      LOG_WARN("fail to compress offset stream", KR(ret));
    }
    if (OB_FAIL(ret)) {
      // if failed, reset writer buffer's pos
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = writer_->set_length(orig_pos))) {
        LOG_WARN("fail to set pos", KR(ret), KR(tmp_ret), K(orig_pos));
      }
    }
  }

  return ret;
}

int ObStringStreamEncoder::convert_datum_to_stream_(ObIDatumIter &iter)
{
  int ret = OB_SUCCESS;
  const int64_t byte_size = get_byte_packed_int_size(ctx_->meta_.uncompressed_len_);
  switch(byte_size) {
  case 1 : {
    if (OB_FAIL(do_convert_datum_to_stream_<uint8_t>(iter))) {
      LOG_WARN("fail to do_convert_datum_to_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  case 2 : {
    if (OB_FAIL(do_convert_datum_to_stream_<uint16_t>(iter))) {
      LOG_WARN("fail to do_convert_datum_to_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  case 4 : {
    if (OB_FAIL(do_convert_datum_to_stream_<uint32_t>(iter))) {
      LOG_WARN("fail to do_convert_datum_to_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  case 8 : {
    if (OB_FAIL(do_convert_datum_to_stream_<uint64_t>(iter))) {
      LOG_WARN("fail to do_convert_datum_to_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "uint byte width size not invalid", K(ret), K(byte_size));
    break;
  }

  return ret;
}


int ObStringStreamEncoder::encode_byte_stream_(ObIArray<uint32_t> &stream_offset_arr)
{
  int ret = OB_SUCCESS;
  char *buf = writer_->current();
  int64_t pos = 0;
  if (OB_FAIL(ctx_->meta_.serialize(buf, writer_->remain_buffer_size(), pos))) {
    LOG_WARN("fail to serialize string stream meta", K(ret), KPC_(ctx));
  } else if (OB_FAIL(writer_->advance(pos))) {
    LOG_WARN("fail to advance", K(ret), K(pos));
  } else if (OB_FAIL(stream_offset_arr.push_back(writer_->length()))) {
    LOG_WARN("fail to push back", KPC(writer_), KR(ret));
  }
  return ret;
}

int ObStringStreamEncoder::encode_offset_stream_(ObIArray<uint32_t> &stream_offset_arr)
{
  int ret = OB_SUCCESS;
  const int64_t byte_size = get_byte_packed_int_size(ctx_->meta_.uncompressed_len_);
  switch(byte_size) {
  case 1 : {
    if (OB_FAIL(do_encode_offset_stream_<uint8_t>(stream_offset_arr))) {
      LOG_WARN("fail to do_encode_offset_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  case 2 : {
    if (OB_FAIL(do_encode_offset_stream_<uint16_t>(stream_offset_arr))) {
      LOG_WARN("fail to do_encode_offset_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  case 4 : {
    if (OB_FAIL(do_encode_offset_stream_<uint32_t>(stream_offset_arr))) {
      LOG_WARN("fail to do_encode_offset_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  case 8 : {
    if (OB_FAIL(do_encode_offset_stream_<uint64_t>(stream_offset_arr))) {
      LOG_WARN("fail to do_encode_offset_stream_", K(ret), KPC_(ctx));
    }
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "uint byte width size not invalid", K(ret), K(byte_size));
    break;
  }
  return ret;

}


} // end namespace blocksstable
} // end namespace oceanbase
