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

#ifndef OCEANBASE_STORAGE_OB_RANGE_PURGER_H_
#define OCEANBASE_STORAGE_OB_RANGE_PURGER_H_
#include "share/ob_define.h"
#include "ob_multiple_merge.h"

namespace oceanbase {
namespace memtable {
class ObMemtableScanIterator;
class ObMemtableKey;
class ObMvccRow;
};  // namespace memtable
namespace storage {

class ObRangePurgerBase {
public:
  typedef memtable::ObMemtableScanIterator Iter;
  typedef memtable::ObMemtableKey Key;
  typedef memtable::ObMvccRow Value;
  ObRangePurgerBase() : iter_(NULL), range_pos_(-1), is_last_inmem_(false), val_(NULL)
  {}
  ~ObRangePurgerBase()
  {}
  void set_iter(Iter* iter)
  {
    iter_ = iter;
  }
  void reset()
  {
    iter_ = NULL;
    range_pos_ = -1;
    is_last_inmem_ = false;
    key_.reset();
    val_ = NULL;
  }
  void try_purge(int64_t range_pos, bool is_inmem, bool is_del, bool read_elr_data);
  void reset_state()
  {
    range_pos_ = -1;
    is_last_inmem_ = false;
    key_.reset();
    val_ = NULL;
  }

private:
  Iter* iter_;
  int64_t range_pos_;
  bool is_last_inmem_;
  Key key_;
  Value* val_;
};

class ObRangePurger {
public:
  typedef common::ObIArray<ObITable*> Stores;
  ObRangePurger() : has_partial_read_(false), memstore_cnt_(0)
  {}
  ~ObRangePurger()
  {}
  void init(const Stores& stores, ObMultipleMerge::MergeIterators& iters);
  void reset()
  {
    has_partial_read_ = false;
    for (int i = 0; i < memstore_cnt_; i++) {
      purger_[i].reset();
    }
    memstore_cnt_ = 0;
  }
  void on_push(int idx, uint8_t iter_flag);
  void try_purge(int64_t range_pos, int iter_idx, bool is_del, bool read_elr_data);
  void skip_range(int idx);

private:
  bool has_partial_read_;
  int memstore_cnt_;
  ObRangePurgerBase purger_[common::MAX_MEMSTORE_CNT_IN_STORAGE];
};
};  // namespace storage
};  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_RANGE_PURGER_H_ */
