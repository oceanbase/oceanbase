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

#ifndef SQL_ENGINE_AGGREGATE_OB_DISTINCT
#define SQL_ENGINE_AGGREGATE_OB_DISTINCT

#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "lib/container/ob_fixed_array.h"
namespace oceanbase {
namespace sql {
typedef common::ObColumnInfo ObDistinctColumn;
class ObDistinct : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObDistinct(common::ObIAllocator& alloc);
  // ObDistinct();
  virtual ~ObDistinct();
  virtual void reset();
  virtual void reuse();
  // not always whole row is compared
  // example: SELECT distinct c1 FROM t order by c2;
  int add_distinct_column(const int64_t column_index, common::ObCollationType cs_type);
  inline void set_block_mode(bool is_block_mode)
  {
    is_block_mode_ = is_block_mode;
  }
  // TODO distinct operator need re_est_cost function
  int init(int64_t distinct_count)
  {
    return ObPhyOperator::init_array_size<>(distinct_columns_, distinct_count);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistinct);

protected:
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  // data members
  common::ObFixedArray<ObDistinctColumn, common::ObIAllocator> distinct_columns_;
  bool is_block_mode_;
};
}  // namespace sql
}  // namespace oceanbase
#endif
