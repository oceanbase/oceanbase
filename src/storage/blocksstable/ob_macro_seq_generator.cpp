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

#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/ob_server_struct.h"
#endif
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

void ObMacroSeqParam::reset()
{
  seq_type_ = SEQ_TYPE_MAX;
  start_ = 0;
  interval_ = 0;
  step_ = 0;
}

bool ObMacroSeqParam::is_valid() const
{
  bool bret = seq_type_ < SEQ_TYPE_MAX && start_ >= 0;
  if (bret && SEQ_TYPE_SKIP == seq_type_) {
    bret = interval_ > 0 && step_ > 0;
  }
  return bret;
}

void ObMacroIncSeqGenerator::reset()
{
  is_inited_ = false;
  start_ = 0;
  current_ = -1;
  seq_threshold_ = 0;
}

int ObMacroIncSeqGenerator::init(const ObMacroSeqParam &seq_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!seq_param.is_valid() || seq_param.seq_type_ != ObMacroSeqParam::SEQ_TYPE_INC)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(seq_param));
  } else {
    start_ = seq_param.start_;
    current_ = -1;
#ifdef OB_BUILD_SHARED_STORAGE
    seq_threshold_ = start_ + (GCTX.is_shared_storage_mode() ? compaction::MACRO_STEP_SIZE : blocksstable::ObMacroDataSeq::MAX_MACRO_SEQ);
#else
    seq_threshold_ = start_ + blocksstable::ObMacroDataSeq::MAX_MACRO_SEQ;
#endif
    is_inited_ = true;
  }
  return ret;
}

int ObMacroIncSeqGenerator::get_next(int64_t &seq_val)
{
  int ret = OB_SUCCESS;
  seq_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(preview_next(current_, seq_val))) {
    LOG_WARN("preview next value failed", K(ret));
  } else if (OB_UNLIKELY(seq_val >= seq_threshold_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("seq is larger than threshold", K(ret), K(seq_val), K_(seq_threshold), K_(start));
  } else {
    current_ = seq_val;
  }
  return ret;
}

int ObMacroIncSeqGenerator::preview_next(const int64_t current_val, int64_t &next_val) const
{
  int ret = OB_SUCCESS;
  next_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("init twice", K(ret));
  } else {
    if (current_val < 0) {
      next_val = start_;
    } else {
      next_val = current_val + 1;
    }
  }
  return ret;
}

void ObMacroSkipSeqGenerator::reset()
{
  ddl_seq_generator_.reset();
}

int ObMacroSkipSeqGenerator::init(const ObMacroSeqParam &seq_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!seq_param.is_valid() || seq_param.seq_type_ != ObMacroSeqParam::SEQ_TYPE_SKIP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(seq_param));
  } else if (OB_FAIL(ddl_seq_generator_.init(seq_param.start_, seq_param.interval_, seq_param.step_))) {
    LOG_WARN("init ddl sequence generator failed", K(ret), K(seq_param));
  }
  return ret;
}

int ObMacroSkipSeqGenerator::get_next(int64_t &seq_val)
{
  bool is_step_over = false;
  return ddl_seq_generator_.get_next(seq_val, is_step_over);
}

int ObMacroSkipSeqGenerator::preview_next(const int64_t current_val, int64_t &next_val) const
{
  return ddl_seq_generator_.preview_next(current_val, next_val);
}
