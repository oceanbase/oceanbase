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

namespace oceanbase {
namespace sql {
template <typename Store_Row, bool has_addon>
struct ObSortVecOpChunk : public common::ObDLinkBase<ObSortVecOpChunk<Store_Row, has_addon>>
{
  explicit ObSortVecOpChunk(const int64_t level) :
    level_(level), sk_row_iter_(), addon_row_iter_(), sk_row_(nullptr), addon_row_(nullptr)
  {}
  void reset_row_iter()
  {
    sk_row_iter_.reset();
    addon_row_iter_.reset();
  }
  int init_row_iter()
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(sk_row_iter_.init(&sk_store_))) {
      SQL_ENG_LOG(WARN, "init iterator failed", K(ret));
    } else if (has_addon && OB_FAIL(addon_row_iter_.init(&addon_store_))) {
      SQL_ENG_LOG(WARN, "init iterator failed", K(ret));
    }
    return ret;
  }
  int get_next_row()
  {
    int ret = common::OB_SUCCESS;
    int64_t read_rows = 0;
    const int64_t max_rows = 1;
    if (OB_FAIL(sk_row_iter_.get_next_batch(max_rows, read_rows,
                                            reinterpret_cast<const ObCompactRow **>(&sk_row_)))) {
      SQL_ENG_LOG(WARN, "get next row failed", K(ret));
    } else if (has_addon) {
      if (OB_FAIL(addon_row_iter_.get_next_batch(
            max_rows, read_rows, reinterpret_cast<const ObCompactRow **>(&addon_row_)))) {
        SQL_ENG_LOG(WARN, "get next row failed", K(ret));
      } else {
        const_cast<Store_Row *>(sk_row_)->set_addon_ptr(addon_row_, sk_store_.get_row_meta());
      }
    }
    return ret;
  }

public:
  int64_t level_;
  ObTempRowStore sk_store_;
  ObTempRowStore addon_store_;
  ObTempRowStore::Iterator sk_row_iter_;
  ObTempRowStore::Iterator addon_row_iter_;
  const Store_Row *sk_row_;
  const Store_Row *addon_row_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSortVecOpChunk);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_CHUNK_H_ */
