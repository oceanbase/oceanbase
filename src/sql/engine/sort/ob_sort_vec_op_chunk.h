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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_CHUNK_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_CHUNK_H_

#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/sort/ob_sort_row_store_mgr.h"

namespace oceanbase {
namespace sql {
template <typename Store_Row, bool has_addon>
struct ObSortVecOpChunk : public common::ObDLinkBase<ObSortVecOpChunk<Store_Row, has_addon>>
{
  explicit ObSortVecOpChunk(const int64_t level, common::ObIAllocator &allocator) :
    level_(level), sort_row_store_mgr_(allocator), sk_row_iter_(), addon_row_iter_(), sk_row_(nullptr), addon_row_(nullptr),
    use_inmem_data_(false), inmem_rows_(), row_idx_(0), slice_id_(0)
  {}
  void reset_row_iter()
  {
    sk_row_iter_.reset();
    addon_row_iter_.reset();
    row_idx_ = 0;
  }
  int init_row_iter()
  {
    int ret = common::OB_SUCCESS;
    if (use_inmem_data_) {
      row_idx_ = 0;
    } else if (OB_FAIL(sk_row_iter_.init(&sort_row_store_mgr_.get_sk_store()))) {
      SQL_ENG_LOG(WARN, "init iterator failed", K(ret));
    } else if (has_addon && OB_FAIL(addon_row_iter_.init(&sort_row_store_mgr_.get_addon_store()))) {
      SQL_ENG_LOG(WARN, "init iterator failed", K(ret));
    }
    return ret;
  }
  int get_next_row()
  {
    int ret = common::OB_SUCCESS;
    int64_t read_rows = 0;
    const int64_t max_rows = 1;
    if (use_inmem_data_) {
      if (row_idx_ >= inmem_rows_.count()) {
        ret = OB_ITER_END;
      } else {
        sk_row_ = inmem_rows_.at(row_idx_);
        addon_row_ = sk_row_->get_addon_ptr(*sort_row_store_mgr_.get_sk_row_meta());
        row_idx_++;
      }
    } else if (OB_FAIL(sk_row_iter_.get_next_batch(max_rows, read_rows,
                                            reinterpret_cast<const ObCompactRow **>(&sk_row_)))) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "get next row failed", K(ret));
      }
    } else if (has_addon) {
      if (OB_FAIL(addon_row_iter_.get_next_batch(
            max_rows, read_rows, reinterpret_cast<const ObCompactRow **>(&addon_row_)))) {
        if (OB_ITER_END != ret) {
          SQL_ENG_LOG(WARN, "get next row failed", K(ret));
        }
      } else {
        const_cast<Store_Row *>(sk_row_)->set_addon_ptr(addon_row_, *sort_row_store_mgr_.get_sk_row_meta());
      }
    }
    return ret;
  }

  ObTempRowStore &get_sk_store() { return sort_row_store_mgr_.get_sk_store(); }
  ObTempRowStore &get_addon_store() { return sort_row_store_mgr_.get_addon_store(); }
  int64_t get_file_size() const { return sort_row_store_mgr_.get_file_size(); }
  // Row count abstraction for both in-memory and temp-store modes.
  int64_t get_row_count() const
  {
    return use_inmem_data_ ? inmem_rows_.count() : sort_row_store_mgr_.get_row_cnt();
  }

public:
  int64_t level_;
  ObSortRowStoreMgr<Store_Row, has_addon> sort_row_store_mgr_;
  ObTempRowStore::Iterator sk_row_iter_;
  ObTempRowStore::Iterator addon_row_iter_;
  const Store_Row *sk_row_;
  const Store_Row *addon_row_;
  bool use_inmem_data_;
  common::ObArray<Store_Row *> inmem_rows_;
  int64_t row_idx_;
  int64_t slice_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSortVecOpChunk);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_CHUNK_H_ */
