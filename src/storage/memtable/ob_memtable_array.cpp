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

#include "storage/memtable/ob_memtable_array.h"

#include "ob_memtable.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace transaction;

namespace memtable {
ObMemtableArrWrap::ObMemtableArrWrap() : mt_start_(0), mt_end_(0)
{
  MEMSET(mt_, 0, sizeof(mt_));
}

ObMemtableArrWrap::~ObMemtableArrWrap()
{}

void ObMemtableArrWrap::reset()
{
  MEMSET(mt_, 0, sizeof(mt_));
  mt_start_ = 0;
  mt_end_ = 0;
}

int ObMemtableArrWrap::dec_active_trx_count_at_active_mt()
{
  int ret = OB_SUCCESS;
  int64_t count = get_count_();
  int64_t pos = (mt_end_ - 1) % common::MAX_MEMSTORE_CNT_IN_STORAGE;

  if (pos < 0 || count == 0 || mt_end_ <= 0 || mt_start_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "dec_active_trx_count_at_active_mt invalid argument", K(pos), K(ret), K(mt_start_), K(mt_end_));
  } else {
    if (NULL != mt_[pos]) {
      mt_[pos]->dec_active_trx_count();
      mt_[pos] = NULL;
      mt_end_--;
    }
  }

  return ret;
}

int ObMemtableArrWrap::remove_mem_ctx_for_trans_ctx(ObMemtable* mt)
{
  int ret = OB_SUCCESS;
  int64_t count = get_count_();

  if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "dec_active_trx_count_at_active_mt invalid argument", K(mt), K(ret));
  } else if (mt_end_ < 0 || mt_start_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "remove mem ctx for trans ctx", K(ret), K(mt_start_), K(mt_end_));
  } else {
    if (0 != count) {
      int64_t pos = mt_start_ % common::MAX_MEMSTORE_CNT_IN_STORAGE;
      if (mt == mt_[pos]) {
        mt_[pos]->dec_ref();
        mt_[pos] = NULL;
        mt_start_++;
      }
    }
  }

  return ret;
}

int ObMemtableArrWrap::clear_mt_arr()
{
  int ret = OB_SUCCESS;

  for (int64_t i = mt_start_; OB_SUCCESS == ret && i < mt_end_; i++) {
    int64_t pos = i % common::MAX_MEMSTORE_CNT_IN_STORAGE;
    if (NULL != mt_[pos]) {
      mt_[pos]->dec_active_trx_count();
      mt_[pos] = NULL;
    }
  }

  if (OB_SUCCESS == ret) {
    mt_start_ = 0;
    mt_end_ = 0;
  }

  return ret;
}

int ObMemtableArrWrap::add_memtable(ObMemtable* memtable)
{
  int ret = OB_SUCCESS;
  int64_t count = get_count_();

  if (NULL == memtable) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "add memtable get nullptr", K(memtable), K(ret), K(mt_start_), K(mt_end_));
  } else if (count >= common::MAX_MEMSTORE_CNT_IN_STORAGE) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "add memtable upper than allowed", K(memtable), K(ret), K(mt_start_), K(mt_end_));
  } else {
    int64_t pos = mt_end_ % common::MAX_MEMSTORE_CNT_IN_STORAGE;
    mt_[pos] = memtable;
    mt_end_++;
  }

  return ret;
}

ObMemtable* ObMemtableArrWrap::get_active_mt() const
{
  ObMemtable* ret = NULL;
  int64_t count = get_count_();
  int64_t pos = (mt_end_ - 1) % common::MAX_MEMSTORE_CNT_IN_STORAGE;
  if (count > 0 && pos >= 0) {
    ret = mt_[pos];
  }
  return ret;
}

int ObMemtableArrWrap::inc_active_trx_count_at_active_mt(ObMemtable*& active_memtable)
{
  int ret = OB_SUCCESS;
  int64_t pos = (mt_end_ - 1) % common::MAX_MEMSTORE_CNT_IN_STORAGE;
  int64_t count = get_count_();

  if (pos < 0 || count < 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get active memtable fail", K(ret), K(pos), K(mt_start_), K(mt_end_));
  } else if (0 == count || NULL == mt_[pos]) {
    // me_count_ might be zero before start writing or after trans clear
    // overwrite error code later
    ret = OB_EAGAIN;
  } else if (OB_FAIL(mt_[pos]->inc_active_trx_count())) {
    // return OB_STATE_NOT_MATCH means inc ref on non-active memtable
    TRANS_LOG(WARN, " memtable inc trx count fail", K(ret), K(pos));
  } else {
    active_memtable = mt_[pos];
  }

  return ret;
}

int ObMemtableArrWrap::update_max_trans_version_(const int64_t pos, const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  if (pos < 0 || pos >= common::MAX_MEMSTORE_CNT_IN_STORAGE) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "update_max_trans_version invalid argument", K(ret), K(pos));
  } else {
    if (NULL != mt_[pos]) {
      if (OB_FAIL(mt_[pos]->update_max_trans_version(trans_version))) {
        TRANS_LOG(WARN, "update max trans version error", K(ret), K(trans_version), K(pos));
      }
    }
  }

  return ret;
}

int ObMemtableArrWrap::update_max_schema_version_(const int64_t pos, const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (pos < 0 || pos >= common::MAX_MEMSTORE_CNT_IN_STORAGE) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "update_max_schema_version invalid argument", K(ret), K(pos));
  } else {
    if (NULL != mt_[pos]) {
      mt_[pos]->set_max_schema_version(schema_version);
    }
  }

  return ret;
}

int ObMemtableArrWrap::update_all_mt_max_trans_version(const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  for (int64_t i = mt_start_; i < mt_end_; i++) {
    int64_t pos = i % common::MAX_MEMSTORE_CNT_IN_STORAGE;
    update_max_trans_version_(pos, trans_version);
  }

  return ret;
}

int ObMemtableArrWrap::update_all_mt_max_schema_version(const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  for (int64_t i = mt_start_; i < mt_end_; i++) {
    int64_t pos = i % common::MAX_MEMSTORE_CNT_IN_STORAGE;
    update_max_schema_version_(pos, schema_version);
  }

  return ret;
}

bool ObMemtableArrWrap::is_contain_this_memtable(ObMemtable* memtable)
{
  int bool_ret = false;

  for (int64_t i = mt_start_; !bool_ret && i < mt_end_; i++) {
    int64_t pos = i % common::MAX_MEMSTORE_CNT_IN_STORAGE;
    if (mt_[pos] == memtable) {
      bool_ret = true;
    }
  }

  return bool_ret;
}

int ObMemtableArrWrap::check_memtable_count(int64_t& count)
{
  int ret = OB_SUCCESS;

  for (int64_t i = mt_start_; i < mt_end_; ++i) {
    int64_t pos = i % common::MAX_MEMSTORE_CNT_IN_STORAGE;
    if (NULL != mt_[pos]) {
      ATOMIC_AAF(&count, 1);
    }
  }
  if (count != get_count_()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected memtable count", K(ret), K(count), "arr_cnt", get_count_());
  }

  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
