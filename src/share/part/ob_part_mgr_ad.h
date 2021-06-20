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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_PART_MGR_AD_
#define OCEANBASE_SQL_OPTIMIZER_OB_PART_MGR_AD_
#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_sql_define.h"

#include <stdlib.h>

namespace oceanbase {
namespace common {
class ObNewRange;
class ObPartMgr;
class ObNewRow;
}  // namespace common
namespace sql {

static const int64_t COUNT = 200;
class ObPartMgrAD {
public:
  // for hash/key/range_func part or insert statement
  static int get_part(common::ObPartMgr* part_mgr, const int64_t table_id,
      const share::schema::ObPartitionLevel part_level, const share::schema::ObPartitionFuncType part_type,
      const bool insert_or_replace, const int64_t p_id,
      const common::ObObj& value,  // part func result
      common::ObIArray<int64_t>& partition_ids, int64_t* part_idx);

  // For range columns
  static int get_part(common::ObPartMgr* part_mgr, const int64_t table_id,
      const share::schema::ObPartitionLevel part_level, const share::schema::ObPartitionFuncType part_type,
      const bool insert_or_replace, const int64_t p_id, const common::ObNewRow& row,
      common::ObIArray<int64_t>& partition_ids, int64_t* part_idx = NULL);

  /////////The for range range partition must only contain column. If there are multiple columns, range represents the
  /// column vector in the schema//////
  static int get_part(common::ObPartMgr* part_mgr, const int64_t table_id,
      const share::schema::ObPartitionLevel part_level, const int64_t p_id, ObOrderDirection direction,
      const common::ObIArray<common::ObNewRange*>& ranges, common::ObIArray<int64_t>& part_ids);

  static int get_all_part(common::ObPartMgr* part_mgr, const int64_t table_id,
      const share::schema::ObPartitionLevel part_level, const int64_t p_id, ObOrderDirection direction,
      common::ObIArray<int64_t>& part_ids);

private:
  // Get point values range partition id
  static int get_part(common::ObPartMgr* part_mgr, const int64_t table_id,
      const share::schema::ObPartitionLevel part_level, const share::schema::ObPartitionFuncType part_type,
      const bool insert_or_replace, const int64_t p_id, const common::ObNewRange& value,
      common::ObIArray<int64_t>& partition_ids, int64_t* part_idx = NULL);
};

}  // namespace sql
}  // namespace oceanbase

#endif
