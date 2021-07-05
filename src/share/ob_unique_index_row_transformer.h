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

#ifndef OB_UNIQUE_INDEX_ROW_TRANSFORMER_H_
#define OB_UNIQUE_INDEX_ROW_TRANSFORMER_H_

#include "common/row/ob_row.h"
#include "share/ob_worker.h"

namespace oceanbase {
namespace share {

class ObUniqueIndexRowTransformer {
public:
  static int check_need_shadow_columns(const common::ObNewRow& row, const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt, const common::ObIArray<int64_t>* projector, bool& need_shadow_columns);
  static int convert_to_unique_index_row(const common::ObNewRow& row, const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt, const int64_t shadow_column_cnt, const common::ObIArray<int64_t>* projector,
      common::ObNewRow& result_row, const bool need_copy_cell = false);
  static int convert_to_unique_index_row(const common::ObNewRow& row, const common::ObCompatibilityMode sql_mode,
      const int64_t unique_key_cnt, const int64_t shadow_column_cnt, const common::ObIArray<int64_t>* projector,
      bool& need_shadow_columns, common::ObNewRow& result_row, const bool need_copy_cell = false);

private:
  static int check_oracle_need_shadow_columns(const common::ObNewRow& row, const int64_t unique_key_cnt,
      const common::ObIArray<int64_t>* projector, bool& need_shadow_columns);
  static int check_mysql_need_shadow_columns(const common::ObNewRow& row, const int64_t unique_key_cnt,
      const common::ObIArray<int64_t>* projector, bool& need_shadow_columns);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OB_UNIQUE_INDEX_ROW_TRANSFORMER_H_
