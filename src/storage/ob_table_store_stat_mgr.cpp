/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_table_store_stat_mgr.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
// ------------------ Statistic ------------------ //
bool ObMergeIterStat::is_valid() const
{
  return call_cnt_ >= 0 && output_row_cnt_ >= 0;
}

int ObMergeIterStat::add(const ObMergeIterStat& other)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is invalid", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else {
    call_cnt_ += other.call_cnt_;
    output_row_cnt_ += other.output_row_cnt_;
  }
  return ret;
}

ObMergeIterStat & ObMergeIterStat::operator=(const ObMergeIterStat &other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObMergeIterStat));
  }
  return *this;
}

bool ObBlockAccessStat::is_valid() const
{
  return effect_read_cnt_ >= 0 && empty_read_cnt_ >= 0;
}

int ObBlockAccessStat::add(const ObBlockAccessStat& other)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is invalid", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else {
    effect_read_cnt_ += other.effect_read_cnt_;
    empty_read_cnt_ += other.empty_read_cnt_;
  }
  return ret;
}

ObBlockAccessStat & ObBlockAccessStat::operator=(const ObBlockAccessStat &other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObBlockAccessStat));
  }
  return *this;
}

ObTableStoreStat::ObTableStoreStat()
{
  reset();
}

void ObTableStoreStat::reset()
{
  MEMSET(this, 0, sizeof(ObTableStoreStat));
}

void ObTableStoreStat::reuse()
{
  share::ObLSID ls_id = ls_id_;
  common::ObTabletID tablet_id = tablet_id_;
  common::ObTableID table_id = table_id_;
  MEMSET(this, 0, sizeof(ObTableStoreStat));
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
  table_id_ = table_id;
}

bool ObTableStoreStat::is_valid() const
{
  bool valid = true;
  if (row_cache_hit_cnt_ < 0 || row_cache_miss_cnt_ < 0 || row_cache_put_cnt_ < 0
      || bf_filter_cnt_ < 0 || bf_empty_read_cnt_ < 0 || bf_access_cnt_ < 0
      || block_cache_hit_cnt_ < 0 || block_cache_miss_cnt_ < 0
      || access_row_cnt_ < 0 || output_row_cnt_ < 0 || fuse_row_cache_hit_cnt_ < 0
      || fuse_row_cache_miss_cnt_ < 0 || fuse_row_cache_put_cnt_ < 0
      || macro_access_cnt_ < 0 || micro_access_cnt_ < 0 || pushdown_micro_access_cnt_ < 0
      || pushdown_row_access_cnt_ < 0 || pushdown_row_select_cnt_ < 0
      || !single_get_stat_.is_valid() || !multi_get_stat_.is_valid() || !index_back_stat_.is_valid()
      || !single_scan_stat_.is_valid() || !multi_scan_stat_.is_valid()
      || !exist_row_.is_valid() ||!get_row_.is_valid() || !scan_row_.is_valid()) {
    valid = false;
  }
  return valid;
}

int ObTableStoreStat::add(const ObTableStoreStat& other)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is invalid", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else if (other.ls_id_ != ls_id_ || other.tablet_id_ != tablet_id_ || other.table_id_ != table_id_) {
    ret = OB_NOT_THE_OBJECT;
    LOG_DEBUG("not the same table store", K(ret), K(other));
  } else {
    row_cache_hit_cnt_ += other.row_cache_hit_cnt_;
    row_cache_miss_cnt_ += other.row_cache_miss_cnt_;
    row_cache_put_cnt_ += other.row_cache_put_cnt_;
    bf_filter_cnt_ += other.bf_filter_cnt_;
    bf_empty_read_cnt_ += other.bf_empty_read_cnt_;
    bf_access_cnt_ += other.bf_access_cnt_;
    block_cache_hit_cnt_ += other.block_cache_hit_cnt_;
    block_cache_miss_cnt_ += other.block_cache_miss_cnt_;
    access_row_cnt_ += other.access_row_cnt_;
    output_row_cnt_ += other.output_row_cnt_;
    fuse_row_cache_hit_cnt_ += other.fuse_row_cache_hit_cnt_;
    fuse_row_cache_miss_cnt_ += other.fuse_row_cache_miss_cnt_;
    fuse_row_cache_put_cnt_ += other.fuse_row_cache_put_cnt_;
    macro_access_cnt_ += other.macro_access_cnt_;
    micro_access_cnt_ += other.micro_access_cnt_;
    pushdown_micro_access_cnt_ += other.pushdown_micro_access_cnt_;
    pushdown_row_access_cnt_ += other.pushdown_row_access_cnt_;
    pushdown_row_select_cnt_ += other.pushdown_row_select_cnt_;
    //ignore ret
    single_get_stat_.add(other.single_get_stat_);
    multi_get_stat_.add(other.multi_get_stat_);
    index_back_stat_.add(other.index_back_stat_);
    single_scan_stat_.add(other.single_scan_stat_);
    multi_scan_stat_.add(other.multi_scan_stat_);
    exist_row_.add(other.exist_row_);
    get_row_.add(other.get_row_);
    scan_row_.add(other.scan_row_);
  }
  return ret;
}

ObTableStoreStat &ObTableStoreStat::operator=(const ObTableStoreStat& other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObTableStoreStat));
  }
  return *this;
}

// ------------------ Iterator ------------------ //
ObTableStoreStatIterator::ObTableStoreStatIterator()
  : cur_idx_(0),
    is_opened_(false)
{
}

ObTableStoreStatIterator::~ObTableStoreStatIterator()
{
}

void ObTableStoreStatIterator::reset()
{
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObTableStoreStatIterator::open()
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableStoreStatIterator has been opened", K(ret));
  } else {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObTableStoreStatIterator::get_next_stat(ObTableStoreStat &stat)
{
  int ret = OB_SUCCESS;
  UNUSED(stat);
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStoreStatIterator has not been opened", K(ret));
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}
} // namespace oceanbase
} // namespace storage
