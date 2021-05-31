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

#include "ob_range_purger.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/mvcc/ob_query_engine.h"

namespace oceanbase {
namespace storage {
void ObRangePurger::init(const Stores& stores, ObMultipleMerge::MergeIterators& iters)
{
  has_partial_read_ = false;
  int64_t store_cnt = stores.count();
  int64_t i = 0;
  for (i = 0; i < store_cnt && i < common::MAX_MEMSTORE_CNT_IN_STORAGE && stores.at(store_cnt - i - 1)->is_memtable();
       i++) {
    purger_[i].reset();
    purger_[i].set_iter((ObRangePurgerBase::Iter*)iters[i]);
  }
  memstore_cnt_ = (int)i;
  STORAGE_LOG(TRACE, "Purger.init", K(memstore_cnt_), K(stores));
}

void ObRangePurger::on_push(int idx, uint8_t iter_flag)
{
  if (!has_partial_read_ && idx > 0 && (iter_flag & STORE_ITER_ROW_PARTIAL)) {
    STORAGE_LOG(TRACE, "partial read happen");
    has_partial_read_ = true;
  }
}

void ObRangePurgerBase::try_purge(int64_t range_pos, bool is_inmem, bool is_del, bool read_elr_data)
{
  if (NULL == iter_) {
    // pass
  } else {
    if (range_pos != range_pos_) {
      range_pos_ = range_pos;
      is_last_inmem_ = false;
      key_.reset();
      val_ = NULL;
    }
    if (is_inmem) {
      if (NULL != key_.get_rowkey()) {
        int ret = OB_SUCCESS;
        bool purged = false;
        memtable::ObMemtable* memtable = NULL;
        if (NULL == (memtable = (memtable::ObMemtable*)iter_->get_memtable())) {
          STORAGE_LOG(WARN, "memtable is NULL");
        } else if (read_elr_data) {
          if (EXECUTE_COUNT_PER_SEC(1)) {
            STORAGE_LOG(INFO,
                "current trans read early lock release data, can not purge",
                K(range_pos),
                K(is_del),
                K(read_elr_data));
          }
        } else if (OB_FAIL(memtable->get_query_engine().check_and_purge(
                       (const memtable::ObMemtableKey*)&key_, val_, iter_->get_read_snapshot(), purged))) {
          STORAGE_LOG(ERROR, "check_and_purge", K(ret), K(key_), K(val_), "version", iter_->get_read_snapshot());
        } else if (purged) {
          STORAGE_LOG(TRACE, "RangePurger: purge", K(*memtable), K(key_), K(*val_));
          ATOMIC_FAA(&memtable->get_mt_stat().purge_row_count_, 1);
          EVENT_INC(MEMSTORE_ROW_PURGE_COUNT);
        }
      }
      if (is_last_inmem_) {
        const memtable::ObMemtableKey* key = nullptr;
        iter_->get_key_val(key, val_);
        key_.encode(key->get_table_id(), key->get_rowkey());
      } else {
        is_last_inmem_ = true;
      }
    } else if (is_del) {
      // ignore
    } else {
      is_last_inmem_ = false;
      key_.reset();
      val_ = NULL;
    }
  }
}

void ObRangePurger::try_purge(int64_t range_pos, int iter_idx, bool is_del, bool read_elr_data)
{
  for (int i = 0; !has_partial_read_ && i < memstore_cnt_ && i <= iter_idx; i++) {
    uint32_t& sleep_interval = get_writing_throttling_sleep_interval();
    sleep_interval = 0;
    purger_[i].try_purge(range_pos, i == iter_idx, is_del, read_elr_data);
    if (sleep_interval > 0) {
      usleep(sleep_interval);
      sleep_interval = 0;
    }
  }
}

void ObRangePurger::skip_range(int idx)
{
  for (int i = idx + 1; !has_partial_read_ && i < memstore_cnt_; i++) {
    purger_[i].reset_state();
  }
}
};  // end namespace storage
};  // end namespace oceanbase
