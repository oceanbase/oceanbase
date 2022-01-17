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

#ifndef _OB_PART_MGR_H
#define _OB_PART_MGR_H 1

#include "common/ob_range.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_sql_define.h"

namespace oceanbase {
namespace common {
class ObPartMgr {
public:
  ObPartMgr()
  {}
  virtual ~ObPartMgr()
  {}

  /*
   * Get primary or secondary partition
   * @in param tenant_id tenant id
   * @in param table_id table id
   * @in param part_level finds the level of the partition. 1 means the primary partition, 2 means the secondary
   * partition. Other values are reported as errors
   * @in param part_id first-level sub-id, -1 means not given
   * @in param range Find the p partition range. If it is a list/hash partition, the upper and lower bounds of the range
   * must be equal, otherwise an error is reported. The range partition can be a range Whether the iterator returned by
   * @in param reverse is a reverse order query
   * @out param partition ids
   */
  virtual int get_part(const uint64_t table_id, const share::schema::ObPartitionLevel part_level, const int64_t part_id,
      const common::ObNewRange& range, bool reverse, ObIArray<int64_t>& part_ids) = 0;
  virtual int get_part(const uint64_t table_id, const share::schema::ObPartitionLevel part_level, const int64_t part_id,
      const common::ObNewRow& row, ObIArray<int64_t>& part_ids) = 0;
};
}  // namespace common
}  // namespace oceanbase

#endif /* _OB_PART_MGR_H */
