// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

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

struct ObTableLoadPartitionCalcContext
{
  ObTableLoadPartitionCalcContext(const table::ObTableLoadObjRowArray &obj_rows,
                                  const ObTableLoadParam &param, common::ObIAllocator &allocator)
    : obj_rows_(obj_rows), param_(param), allocator_(allocator)
  {
    partition_ids_.set_block_allocator(common::ModulePageAllocator(allocator_));
  }
  const table::ObTableLoadObjRowArray &obj_rows_;
  const ObTableLoadParam &param_;
  common::ObIAllocator &allocator_;
  common::ObArray<table::ObTableLoadPartitionId> partition_ids_;
};

class ObTableLoadPartitionCalc
{
public:
  ObTableLoadPartitionCalc();
  int init(uint64_t tenant_id, uint64_t table_id, sql::ObSQLSessionInfo *session_info);
  int calc(ObTableLoadPartitionCalcContext &ctx);
private:
  int init_part_key_index(const share::schema::ObTableSchema *table_schema,
                        common::ObIAllocator &allocator);
  int get_row(ObTableLoadPartitionCalcContext &ctx, const table::ObTableLoadObjRow &obj_row, int32_t length, common::ObNewRow &part_row,
              common::ObIAllocator &allocator) const;
  int get_partition_by_row(common::ObIArray<common::ObNewRow> &part_rows,
                           common::ObIArray<table::ObTableLoadPartitionId> &partition_ids);
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
  bool is_partition_with_autoinc_;
  int64_t partition_with_autoinc_idx_;
private:
  // data members
  uint64_t tenant_id_;
  uint64_t table_id_;
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
