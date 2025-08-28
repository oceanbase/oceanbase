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

#include "storage/backup/ob_backup_index_compressor.h"
#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace backup
{

ObBackupIndexBlockCompressor::ObBackupIndexBlockCompressor()
  : is_inited_(false),
    is_none_(false),
    block_size_(0),
    compressor_(NULL),
    comp_buf_(ObModIds::BACKUP),
    decomp_buf_(ObModIds::BACKUP)
{
}

ObBackupIndexBlockCompressor::~ObBackupIndexBlockCompressor()
{
  
}

void ObBackupIndexBlockCompressor::reuse()
{
  is_none_ = false;
  block_size_ = 0;
  compressor_ = nullptr;
  comp_buf_.reuse();
  decomp_buf_.reuse();
}

void ObBackupIndexBlockCompressor::reset()
{
  is_inited_ = false;
  reuse();
}

int ObBackupIndexBlockCompressor::init(const int64_t block_size, const ObCompressorType comp_type)
{
  int ret = OB_SUCCESS;
  reset();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "backup index block compressor init twice", K(ret));
  } else if (OB_UNLIKELY(block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(block_size), K(comp_type));
  } else if (comp_type == NONE_COMPRESSOR) {
    is_none_ = true;
    block_size_ = block_size;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(comp_type, compressor_))) {
    STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), K(comp_type));
  } else {
    is_none_ = false;
    block_size_ = block_size;
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupIndexBlockCompressor::compress(
    const char *in, const int64_t in_size,
    const char *&out, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "compressor is not init", K(ret));
  } else if (is_none_) {
    out = in;
    out_size = in_size;
  } else if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "compressor is unexpected null", K(ret), K_(compressor));
  } else if (OB_FAIL(compressor_->get_max_overflow_size(in_size, max_overflow_size))) {
    STORAGE_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(in_size));
  } else {
    int64_t comp_size = 0;
    int64_t max_comp_size = max_overflow_size + in_size;
    int64_t need_size = std::max(max_comp_size, block_size_ * 2);
    if (OB_FAIL(comp_buf_.ensure_space(need_size))) {
      STORAGE_LOG(WARN, "macro block writer fail to allocate memory for comp_buf_.", K(ret),
                  K(need_size));
    } else if (OB_FAIL(compressor_->compress(in, in_size, comp_buf_.data(), max_comp_size, comp_size))) {
      STORAGE_LOG(WARN, "compressor fail to compress.", K(in), K(in_size),
                  "comp_ptr", comp_buf_.data(), K(max_comp_size), K(comp_size));
    } else if (comp_size >= in_size) {
      STORAGE_LOG(TRACE, "compressed_size is larger than origin_size",
                  K(comp_size), K(in_size));
      out = in;
      out_size = in_size;
    } else {
      out = comp_buf_.data();
      out_size = comp_size;
      comp_buf_.reuse();
    }
  }
  return ret;
}

int ObBackupIndexBlockCompressor::decompress(
    const char *in, const int64_t in_size, const int64_t uncomp_size,
    const char *&out, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  int64_t decomp_size = 0;
  decomp_buf_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "compressor is not init", K(ret));
  } else if (is_none_ || in_size == uncomp_size) {
    out = in;
    out_size = in_size;
  } else if (OB_FAIL(decomp_buf_.ensure_space(uncomp_size))) {
    STORAGE_LOG(WARN, "failed to ensure decomp space", K(ret), K(uncomp_size));
  } else if (OB_FAIL(compressor_->decompress(in, in_size, decomp_buf_.data(), uncomp_size, decomp_size))) {
    STORAGE_LOG(WARN, "failed to decompress data", K(ret), K(in_size), K(uncomp_size));
  } else {
    out = decomp_buf_.data();
    out_size = decomp_size;
  }
  return ret;
}

}
}