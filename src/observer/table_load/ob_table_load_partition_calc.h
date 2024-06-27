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

#pragma once

#include "common/object/ob_object.h"
#include "share/schema/ob_column_schema.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_row_array.h"
#include "share/table/ob_table_load_define.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/table_load/ob_table_load_time_convert.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadSchema;


class ObTableLoadPartitionCalc
{
public:
  ObTableLoadPartitionCalc();
  int init(const ObTableLoadParam &param, sql::ObSQLSessionInfo *session_info);
  int get_part_key(const table::ObTableLoadObjRow &row, common::ObNewRow &part_key) const;
  int cast_part_key(common::ObNewRow &part_key, common::ObIAllocator &allocator) const;
  int get_partition_by_row(common::ObIArray<common::ObNewRow> &part_rows,
                           common::ObIArray<table::ObTableLoadPartitionId> &partition_ids);
  int64_t get_part_key_obj_count() const {return part_key_obj_index_.count();}
private:
  int init_part_key_index(const share::schema::ObTableSchema *table_schema,
                        common::ObIAllocator &allocator);
public:
  struct IndexAndType
  {
    IndexAndType() : index_(-1), column_schema_(nullptr) {}
    int64_t index_;
    const share::schema::ObColumnSchemaV2 *column_schema_;
    TO_STRING_KV(K_(index), KP_(column_schema));
  };
public:
  table::ObTableLoadArray<IndexAndType> part_key_obj_index_;
  sql::ObSQLSessionInfo *session_info_;
  ObTableLoadTimeConverter time_cvrt_;
  common::ObCastMode cast_mode_;
  bool is_partition_with_autoinc_;
  int64_t partition_with_autoinc_idx_;
private:
  const ObTableLoadParam *param_;
  // data members
  bool is_partitioned_;
  // 非分区表
  table::ObTableLoadPartitionId partition_id_;
  // 分区表
  common::ObArenaAllocator allocator_;
  sql::ObSqlCtx sql_ctx_;
  sql::ObExecContext exec_ctx_;
  sql::ObTableLocation table_location_;
  ObSchemaGetterGuard schema_guard_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadPartitionCalc);
};

}  // namespace observer
}  // namespace oceanbase
