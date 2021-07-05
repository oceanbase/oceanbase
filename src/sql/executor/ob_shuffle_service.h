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
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_task_event.h"
#include "sql/ob_sql_define.h"

namespace oceanbase {
namespace sql {
using namespace share::schema;

class ObShuffleService {
public:
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> PartIdx2PartIdMap;
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> SubPartIdx2SubPartIdMap;

public:
  ObShuffleService(ObIAllocator& allocator)
      : allocator_(allocator),
        row_cache_(),
        current_cell_count_(-1),
        part_idx_to_part_id_(NULL),
        subpart_idx_to_subpart_id_(NULL)
  {}

  ~ObShuffleService() = default;

  int get_partition_ids(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const common::ObNewRow& row, const ObSqlExpression& part_partition_func,
      const ObSqlExpression& subpart_partition_func, const ObIArray<ObTransmitRepartColumn>& repart_columns,
      const ObIArray<ObTransmitRepartColumn>& repart_sub_columns, int64_t& part_idx, int64_t& subpart_idx,
      bool& no_match_partiton);

  inline void set_part_idx_to_part_id_map(PartIdx2PartIdMap* map)
  {
    part_idx_to_part_id_ = map;
  }

  inline void set_subpart_idx_to_subpart_id_map(SubPartIdx2SubPartIdMap* map)
  {
    subpart_idx_to_subpart_id_ = map;
  }

  // This interface is only used under the px framework,
  // non-px please use the above interface.
  int get_partition_ids(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const common::ObNewRow& row, const ObSqlExpression& part_partition_func,
      const ObSqlExpression& subpart_partition_func, const ObIArray<ObTransmitRepartColumn>& repart_columns,
      const ObIArray<ObTransmitRepartColumn>& repart_sub_columns, const ObPxPartChMap& ch_map, int64_t& part_id,
      int64_t& subpart_id, bool& no_match_partiton, ObRepartitionType part_type);
  // for px channel map
  template <typename T>
  static int get_part_id_by_ch_map(const T& ch_map, int64_t& part_id)
  {
    int ret = common::OB_SUCCESS;
    if (ch_map.size() > 0) {
      part_id = extract_part_idx(ch_map.begin()->first);
    } else {
      ret = common::OB_ERR_UNEXPECTED;
      // SQL_LOG(WARN, "part ch map size if unexpected", K(ret));
    }
    return ret;
  }

  template <typename T>
  static int get_sub_part_id_by_ch_map(const T& ch_map, const int64_t part_id, int64_t& sub_part_id)
  {
    int ret = common::OB_SUCCESS;
    if (ch_map.size() > 0) {
      int64_t t_part_id = common::OB_INVALID_INDEX;
      for (auto iter = ch_map.begin(); OB_SUCC(ret) && iter != ch_map.end(); iter++) {
        t_part_id = extract_part_idx(iter->first);
        if (part_id == t_part_id) {
          sub_part_id = extract_subpart_idx(iter->first);
          break;
        }
      }
    } else {
      ret = common::OB_ERR_UNEXPECTED;
      // SQL_LOG(WARN, "part ch map size if unexpected", K(ret));
    }
    return ret;
  }

private:
  int init_expr_ctx(ObExecContext& exec_ctx);
  int get_part_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const common::ObNewRow& row, const ObSqlExpression& part_func,
      const ObIArray<ObTransmitRepartColumn>& repart_columns, int64_t& part_id);
  int get_key_part_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const common::ObNewRow& row, const ObSqlExpression& part_func, int64_t& part_id);
  int get_non_key_partition_part_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const ObNewRow& row, const ObIArray<ObTransmitRepartColumn>& repart_columns, int64_t& part_id);
  int get_subpart_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const common::ObNewRow& row, int64_t part_id, const ObSqlExpression& key_shuffle_func,
      const ObIArray<ObTransmitRepartColumn>& repart_sub_columns, int64_t& subpart_id);
  int get_key_subpart_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema, const ObNewRow& row,
      int64_t part_id, const ObSqlExpression& subpart_partition_func, int64_t& subpart_id);
  int get_non_key_subpart_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
      const common::ObNewRow& row, int64_t part_id, const ObIArray<ObTransmitRepartColumn>& repart_sub_columns,
      int64_t& subpart_id);
  int get_repart_row(ObExecContext& exec_ctx, const common::ObNewRow& in_row,
      const ObIArray<ObTransmitRepartColumn>& repart_columns, common::ObNewRow& out_row, bool hash_part,
      bool is_hash_v2);

  int get_hash_part_id(const common::ObNewRow& row, const int64_t part_num, common::ObIArray<int64_t>& part_ids,
      const PartIdx2PartIdMap& part_map);
  int get_hash_subpart_id(const common::ObNewRow& row, const int64_t subpart_num,
      common::ObIArray<int64_t>& subpart_ids, const SubPartIdx2SubPartIdMap& subpart_map);

public:
  constexpr static int64_t NO_MATCH_PARTITION = -2;

private:
  common::ObIAllocator& allocator_;
  common::ObNewRow row_cache_;
  int64_t current_cell_count_;
  // Expr calculation context
  ObExprCtx expr_ctx_;

  PartIdx2PartIdMap* part_idx_to_part_id_;
  SubPartIdx2SubPartIdMap* subpart_idx_to_subpart_id_;
};

}  // namespace sql

}  // namespace oceanbase

#endif
