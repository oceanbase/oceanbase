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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_
#define OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_

#include "sql/executor/ob_interm_result_item_pool.h"
#include "sql/executor/ob_slice_id.h"
#include "share/ob_scanner.h"
#include "lib/list/ob_list.h"
#include "share/ob_define.h"
#include "sql/engine/ob_phy_operator.h"
#include "storage/blocksstable/ob_tmp_file.h"

namespace oceanbase {
namespace sql {
class ObIntermResultItem;
class ObTaskSmallResult;

union AtomicCntAndState {
  volatile uint64_t atomic_;
  struct {
    int32_t cnt_;
    int32_t state_;
  };
};

class ObIntermResultInfo {
public:
  ObIntermResultInfo() : slice_id_()
  {}
  virtual ~ObIntermResultInfo()
  {}

  ObSliceID slice_id_;

  inline void reset()
  {
    slice_id_.reset();
  }
  inline void init(const ObSliceID& slice_id)
  {
    slice_id_ = slice_id;
  }
  inline bool is_init() const
  {
    return slice_id_.is_valid();
  }
  inline int64_t hash() const
  {
    return slice_id_.hash();
  }
  inline bool operator==(const ObIntermResultInfo& other) const
  {
    return slice_id_.equal(other.slice_id_);
  }
  TO_STRING_KV(K_(slice_id));
};

class ObIntermResult {
public:
  friend class ObIntermResultIterator;

  static const int32_t STATE_NORMAL = 0;
  static const int32_t STATE_RECYCLE = 1;

  ObIntermResult();
  virtual ~ObIntermResult();

  void reset();
  int try_inc_cnt();
  int try_dec_cnt();
  int try_begin_recycle();
  int try_end_recycle();
  int add_row(uint64_t tenant_id, const common::ObNewRow& row);
  int get_all_row_count(int64_t& all_row_count);
  int get_all_data_size(int64_t& size) const;
  int get_data_size_detail(int64_t& mem, int64_t& disk) const;
  bool rows_is_completed() const
  {
    return rows_is_completed_;
  }
  int complete_add_rows(uint64_t tenant_id);

  inline void set_expire_time(int64_t expire_time)
  {
    expire_time_ = expire_time;
  }
  inline int64_t get_expire_time() const
  {
    return expire_time_;
  }
  inline int32_t get_state() const
  {
    return cnt_and_state_.state_;
  }
  void set_found_rows(const int64_t count)
  {
    found_rows_ = count;
  }
  int64_t get_found_rows() const
  {
    return found_rows_;
  }
  void set_affected_rows(const int64_t count)
  {
    affected_rows_ = count;
  }
  int64_t get_affected_rows() const
  {
    return affected_rows_;
  }
  int64_t get_scanner_count() const
  {
    return data_.count();
  }
  void set_last_insert_id_session(const int64_t last_insert_id)
  {
    last_insert_id_session_ = last_insert_id;
  }
  int64_t get_last_insert_id_session() const
  {
    return last_insert_id_session_;
  }
  int try_fetch_single_scanner(ObTaskSmallResult& small_result) const;
  inline void set_is_result_accurate(bool is_accurate)
  {
    is_result_accurate_ = is_accurate;
  }
  inline bool is_result_accurate() const
  {
    return is_result_accurate_;
  }
  inline void set_matched_rows(int64_t matched_rows)
  {
    matched_rows_ = matched_rows;
  }
  inline int64_t get_matched_rows() const
  {
    return matched_rows_;
  }
  inline void set_duplicated_rows(int64_t duplicated_rows)
  {
    duplicated_rows_ = duplicated_rows;
  }
  inline int64_t get_duplicated_rows() const
  {
    return duplicated_rows_;
  }
  TO_STRING_EMPTY();
  inline bool is_disk_store_opened(void) const
  {
    return fd_ >= 0;
  }
  int choose_store(bool& disk, const uint64_t tenant_id, const bool force_disk_store) const;
  void set_row_reclaim_func(ObPhyOperator::reclaim_row_t func)
  {
    row_reclaim_func_ = func;
  }

  int get_item(const int64_t index, ObIIntermResultItem*& item);

private:
  int alloc_scanner();
  void free_scanner();

  // reclaim with %row_reclaim_func_ when free interm result
  void reclaim_rows();

  int save_cur_scanner(uint64_t tenant_id);
  int alloc_ir_item(ObIIntermResultItem*& item, const uint64_t tenant_id, const bool force_disk_store);
  int reset_and_init_cur_scanner();
  int check_and_init_cur_scanner();

private:
  static const int64_t DEFAULT_INTERM_RESULT_ITEM_NUM = 2;
  common::ObScanner* cur_scanner_;
  ObIntermResultItemPool* ir_item_pool_;
  common::ObSEArray<ObIIntermResultItem*, DEFAULT_INTERM_RESULT_ITEM_NUM> data_;
  bool rows_is_completed_;
  int64_t expire_time_;
  int64_t found_rows_;
  uint64_t last_insert_id_session_;
  bool is_result_accurate_;
  AtomicCntAndState cnt_and_state_;
  int64_t affected_rows_;
  int64_t matched_rows_;
  int64_t duplicated_rows_;
  int64_t fd_;
  int64_t dir_id_;
  int64_t offset_;
  ObPhyOperator::reclaim_row_t row_reclaim_func_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIntermResult);
};

class ObIntermResultIterator {
public:
  friend class ObIntermResultManager;
  ObIntermResultIterator();
  virtual ~ObIntermResultIterator();

  void reset();
  // get scanner directly is used in old rpc, will be removed after upgrade to 1.4
  // @deprecated
  int get_next_scanner(common::ObScanner& scanner);
  int get_next_interm_result_item(ObIIntermResultItem*& ir_item);
  int64_t get_scanner_count();
  ObIntermResult* get_interm_result()
  {
    return ir_;
  }
  // int64_t get_col_count();
private:
  int set_interm_result(const ObIntermResultInfo& ir_info, ObIntermResult* ir, bool has_inc_cnt);
  int get_interm_result_info(ObIntermResultInfo& ir_info);

  int32_t cur_pos_;
  // current iter scanner
  common::ObScanner* cur_scanner_;
  common::ObRowStore::Iterator row_store_it_;
  ObIntermResult* ir_;
  ObIntermResultInfo ir_info_;
  bool has_inc_cnt_;
  DISALLOW_COPY_AND_ASSIGN(
      ObIntermResultIterator);  // Copy must be prohibited, otherwise the reference count will go wrong
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_INTERM_RESULT_ */
