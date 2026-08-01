/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

void ObMacroSeqParam::reset()
{
  seq_type_ = SEQ_TYPE_MAX;
  start_ = 0;
}

bool ObMacroSeqParam::is_valid() const
{
  return seq_type_ < SEQ_TYPE_MAX && start_ >= 0;
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
    seq_threshold_ = start_ + blocksstable::ObMacroDataSeq::MAX_MACRO_SEQ;
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
