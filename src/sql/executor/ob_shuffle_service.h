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

#ifndef OB_SHUFFLE_UTIL_H_
#define OB_SHUFFLE_UTIL_H_
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_task_event.h"
#include "sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{
using namespace share::schema;

typedef common::ObColumnInfo ObTransmitRepartColumn;
class ObShuffleService
{
public:
  typedef common::hash::ObHashMap<int64_t, int64_t,
                  common::hash::NoPthreadDefendMode> PartIdx2PartIdMap;
  typedef common::hash::ObHashMap<int64_t, int64_t,
                  common::hash::NoPthreadDefendMode> SubPartIdx2SubPartIdMap;
public:
  ObShuffleService(ObIAllocator &allocator) :
    allocator_(allocator),
    row_cache_(),
    current_cell_count_(-1)
  {}

  ~ObShuffleService() = default;
  /*
   * shuffle 算值的接口目前不进行分区表达式的计算。
   * 举个例子，当分区条件为A表的（c1+1）时，分区为range类型
   * [5,10),[10,20)
   * 预计出现的重分区是c2 ＝c1+1，可能是c2+1 ＝c1+1。
   * (上面两种重分区目前优化器应该是没有支持)
   * c2 = c1+1这种肯定能将c2发到对应值的分区上。
   * 如果是c2+1=c1+1这种类型，如果在transmit算子之前已经计算了c2+1
   * 那么这个接口也是有效的。
   *
   * */


  int get_partition_ids(ObExecContext &exec_ctx,
                        const share::schema::ObTableSchema &table_schema,
                        const common::ObNewRow &row,
                        const ObSqlExpression &part_partition_func,
                        const ObSqlExpression &subpart_partition_func,
                        const ObIArray<ObTransmitRepartColumn> &repart_columns,
                        const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                        int64_t &part_idx,
                        int64_t &subpart_idx,
                        bool &no_match_partiton);

  // 这个接口仅在px框架下使用, 非px请使用上面接口.
  int get_partition_ids(ObExecContext &exec_ctx,
                        const share::schema::ObTableSchema &table_schema,
                        const common::ObNewRow &row,
                        const ObSqlExpression &part_partition_func,
                        const ObSqlExpression &subpart_partition_func,
                        const ObIArray<ObTransmitRepartColumn> &repart_columns,
                        const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                        const ObPxPartChMap &ch_map,
                        int64_t &part_id,
                        int64_t &subpart_id,
                        bool &no_match_partiton,
                        ObRepartitionType part_type);
private:
  int init_expr_ctx(ObExecContext &exec_ctx);
  int get_part_id(ObExecContext &exec_ctx,
                  const share::schema::ObTableSchema &table_schema,
                  const common::ObNewRow &row,
                  const ObSqlExpression &part_func,
                  const ObIArray<ObTransmitRepartColumn> &repart_columns,
                  int64_t &part_id);
  int get_key_part_id(ObExecContext &exec_ctx,
                      const share::schema::ObTableSchema &table_schema,
                      const common::ObNewRow &row,
                      const ObSqlExpression &part_func,
                      int64_t &part_id);
  int get_non_key_partition_part_id(ObExecContext &exec_ctx,
                                    const share::schema::ObTableSchema &table_schema,
                                    const ObNewRow &row,
                                    const ObIArray<ObTransmitRepartColumn> &repart_columns,
                                    int64_t &part_id);
  int get_subpart_id(ObExecContext &exec_ctx,
                     const share::schema::ObTableSchema &table_schema,
                     const common::ObNewRow &row,
                     int64_t part_id,
                     const ObSqlExpression &key_shuffle_func,
                     const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                     int64_t &subpart_id);
  int get_key_subpart_id(ObExecContext &exec_ctx,
                         const share::schema::ObTableSchema &table_schema,
                         const ObNewRow &row,
                         int64_t part_id,
                         const ObSqlExpression &subpart_partition_func,
                         int64_t &subpart_id);
  int get_non_key_subpart_id(ObExecContext &exec_ctx,
                             const share::schema::ObTableSchema &table_schema,
                             const common::ObNewRow &row,
                             int64_t part_id,
                             const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                             int64_t &subpart_id);
  int get_repart_row(ObExecContext &exec_ctx,
                     const common::ObNewRow &in_row,
                     const ObIArray<ObTransmitRepartColumn> &repart_columns,
                     common::ObNewRow &out_row,
                     bool hash_part);

public:
  constexpr static int64_t NO_MATCH_PARTITION = -2;

private:
  common::ObIAllocator &allocator_;
  common::ObNewRow row_cache_;
  int64_t current_cell_count_;
  // Expr calculation context
  ObExprCtx expr_ctx_;

};

}

}

#endif
