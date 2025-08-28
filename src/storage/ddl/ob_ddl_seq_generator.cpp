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

#include "storage/ddl/ob_ddl_seq_generator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObDDLSeqGenerator::ObDDLSeqGenerator()
  : is_inited_(false), start_(0), interval_(0), step_(0), current_(-1)
{

}

ObDDLSeqGenerator::~ObDDLSeqGenerator()
{
  reset();
}

int ObDDLSeqGenerator::init(const int64_t start, const int64_t interval, const int64_t step)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(start < 0 || interval <= 0 || step <= 0 || step < interval)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start), K(interval), K(step));
  } else {
    start_ = start;
    interval_ = interval;
    step_ = step;
    current_ = -1;
    is_inited_ = true;
  }
  return ret;

}

void ObDDLSeqGenerator::reset()
{
  is_inited_ = false;
  start_ = 0;
  interval_ = 0;
  step_ = 0;
  current_ = -1;
}

int ObDDLSeqGenerator::get_next(int64_t &seq_val, bool &is_step_over)
{
  int ret = OB_SUCCESS;
  seq_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(*this));
  } else if (OB_FAIL(preview_next(current_, seq_val))) {
    LOG_WARN("preview next failed", K(ret));
  } else {
    current_ = seq_val;
    is_step_over = this->is_step_over(current_);
  }
  return ret;
}

int ObDDLSeqGenerator::get_next_interval(int64_t &start, int64_t &end)
{
  int ret = OB_SUCCESS;
  start = -1;
  end = -1;
  int64_t seq_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t next_round_idx = current_ < 0 ? start_ / step_ : current_ / step_ + 1;
    start = next_round_idx * step_ + start_ % step_;
    end = next_round_idx * step_ + start_ % step_ + interval_ - 1;
    current_ = end;
  }
  return ret;
}

int ObDDLSeqGenerator::preview_next(const int64_t current, int64_t &next_val) const
{
  int ret = OB_SUCCESS;
  next_val = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(*this));
  } else {
    if (current < 0) {
      next_val = start_;
    } else if (is_step_over(current)) {
      next_val = current + step_ - interval_ + 1;
    } else {
      next_val = current + 1;
    }
  }
  return ret;
}

int64_t ObDDLSeqGenerator::get_current() const
{
  return current_;
}

int64_t ObDDLSeqGenerator::get_interval_size() const
{
  return interval_;
}

bool ObDDLSeqGenerator::is_step_over(const int64_t val) const
{ 
  return is_inited_
    && val % step_ == (start_ % step_ + interval_ - 1);
}

