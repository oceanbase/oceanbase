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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_ARRAY_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_ARRAY_

#include "lib/lock/ob_spin_lock.h"

namespace oceanbase {
namespace memtable {
class ObMemtable;
// wrap memtable array interface
class ObMemtableArrWrap {
public:
  ObMemtableArrWrap();
  ~ObMemtableArrWrap();

public:
  void reset();
  int inc_active_trx_count_at_active_mt(ObMemtable*& active_memtable);
  int dec_active_trx_count_at_active_mt();
  int remove_mem_ctx_for_trans_ctx(ObMemtable* mt);
  int clear_mt_arr();
  int add_memtable(ObMemtable* memtable);
  ObMemtable* get_active_mt() const;
  int update_all_mt_max_trans_version(const int64_t trans_version);
  int update_all_mt_max_schema_version(const int64_t schema_version);
  bool has_memtable()
  {
    return get_count_() > 0;
  };
  bool is_reach_max_memtable_cnt()
  {
    return get_count_() >= 16;
  };
  bool is_contain_this_memtable(ObMemtable* memtable);
  int check_memtable_count(int64_t& count);

private:
  int64_t get_count_() const
  {
    return mt_end_ - mt_start_;
  }
  int update_max_trans_version_(const int64_t pos, const int64_t trans_version);
  int update_max_schema_version_(const int64_t pos, const int64_t schema_version);

private:
  ObMemtable* mt_[common::MAX_MEMSTORE_CNT_IN_STORAGE];
  int64_t mt_start_;
  int64_t mt_end_;
};

}  // namespace memtable
}  // namespace oceanbase
#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_ARRAY_
