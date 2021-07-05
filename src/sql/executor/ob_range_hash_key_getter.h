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

#ifndef _OB_SQ_OB_RANGE_HASH_KEY_GETTER_H_
#define _OB_SQ_OB_RANGE_HASH_KEY_GETTER_H_

#include "common/ob_partition_key.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_task_event.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share

namespace sql {

class ObRangeHashKeyGetter {
public:
  ObRangeHashKeyGetter(const int64_t& repartition_table_id,
      const common::ObFixedArray<ObTransmitRepartColumn, common::ObIAllocator>& repart_columns,
      const common::ObFixedArray<ObTransmitRepartColumn, common::ObIAllocator>& repart_sub_columns)
      : repartition_table_id_(repartition_table_id),
        repart_columns_(repart_columns),
        repart_sub_columns_(repart_sub_columns)
  {}

  ~ObRangeHashKeyGetter() = default;

  int get_part_subpart_obj_idxs(int64_t& part_obj_idx, int64_t& subpart_obj_idx) const;
  //  int get_part_subpart_idx(const share::schema::ObTableSchema *table_schema,
  //                                  int64_t slice_idx,
  //                                  int64_t &part_idx,
  //                                  int64_t &subpart_idx) const;
private:
  const int64_t& repartition_table_id_;
  const common::ObFixedArray<ObTransmitRepartColumn, common::ObIAllocator>& repart_columns_;
  const common::ObFixedArray<ObTransmitRepartColumn, common::ObIAllocator>& repart_sub_columns_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
