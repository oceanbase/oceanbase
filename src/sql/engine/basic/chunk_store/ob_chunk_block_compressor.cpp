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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/chunk_store/ob_chunk_block_compressor.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

ObChunkBlockCompressor::ObChunkBlockCompressor()
  : compressor_type_(NONE_COMPRESSOR),
    compressor_(NULL)
{
}

ObChunkBlockCompressor::~ObChunkBlockCompressor()
{
  reset();
}

void ObChunkBlockCompressor::reset()
{
  compressor_type_ = NONE_COMPRESSOR;
  if (compressor_ != nullptr) {
    compressor_ = nullptr;
  }
}

int ObChunkBlockCompressor::init(const ObCompressorType comptype)
{
  int ret = OB_SUCCESS;
  reset();

  if (comptype == NONE_COMPRESSOR) {
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(comptype, compressor_))) {
    LOG_WARN("Fail to get compressor, ", K(ret), K(comptype));
  } else {
    compressor_type_ = comptype;
  }
  return ret;
}

int ObChunkBlockCompressor::calc_need_size(int64_t in_size, int64_t &need_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  need_size = 0;
  if (compressor_type_ == NONE_COMPRESSOR) {
    need_size = in_size;
  } else if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compressor is unexpected null", K(ret), K_(compressor));
  } else if (OB_FAIL(compressor_->get_max_overflow_size(in_size, max_overflow_size))) {
    LOG_WARN("fail to get max_overflow_size, ", K(ret), K(in_size));
  } else {
    need_size = max_overflow_size + in_size;
  }

  return ret;
}

int ObChunkBlockCompressor::compress(const char *in, const int64_t in_size, const int64_t max_comp_size,
                                     char *out, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (compressor_type_ == NONE_COMPRESSOR) {
    MEMCPY(out, in, in_size);
    out_size = in_size;
  } else if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compressor is unexpected null", K(ret), K_(compressor));
  } else {
    int64_t comp_size = 0;
    if (OB_FAIL(compressor_->compress(in, in_size, out, max_comp_size, comp_size))) {
      LOG_WARN("compressor fail to compress.", K(in), K(in_size),
                  "comp_ptr", out, K(max_comp_size), K(comp_size));
    } else if (comp_size >= in_size) {
      LOG_TRACE("compressed_size is larger than origin_size",
                  K(comp_size), K(in_size));
      MEMCPY(out, in, in_size);
      out_size = in_size;
    } else {
      out_size = comp_size;
    }
  }
  return ret;
}

int ObChunkBlockCompressor::decompress(const char *in, const int64_t in_size,
                                       const int64_t uncomp_size,
                                       char *out, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  int64_t decomp_size = 0;
  if ((compressor_type_ == NONE_COMPRESSOR) || in_size == uncomp_size) {
    MEMCPY(out, in, in_size);
    out_size = in_size;
  } else if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compressor is unexpected null", K(ret), K_(compressor));
  } else if (OB_FAIL(compressor_->decompress(in, in_size, out, uncomp_size,
                                             decomp_size))) {
    LOG_WARN("failed to decompress data", K(ret), K(in_size), K(uncomp_size));
  } else {
    out_size = decomp_size;
  }
  return ret;
}

}
}
