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

#ifndef OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_MERGE_OP_H_
#define OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_MERGE_OP_H_

#include "sql/engine/dml/ob_table_merge_op.h"

namespace oceanbase {
namespace sql {
class ObMultiTableMergeSpec : public ObTableMergeSpec, public ObMultiDMLInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObMultiTableMergeSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObTableMergeSpec(alloc, type), ObMultiDMLInfo(alloc)
  {}

  virtual ~ObMultiTableMergeSpec(){};
  virtual bool is_multi_dml() const
  {
    return true;
  }
};

class ObMultiTableMergeOp : public ObTableMergeOp, public ObMultiDMLCtx {
public:
  static const int64_t DELETE_OP = 0;
  static const int64_t UPDATE_INSERT_OP = 1;
  static const int64_t INSERT_OP = 2;
  static const int64_t DML_OP_CNT = 3;

public:
  ObMultiTableMergeOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableMergeOp(ctx, spec, input), ObMultiDMLCtx(ctx.get_allocator())
  {}
  virtual ~ObMultiTableMergeOp(){};

protected:
  virtual int inner_open() override;
  virtual int inner_close() override;

  int shuffle_merge_row(bool& got_row);
  int process_update(bool& got_row);
  int update_row(bool need_delete);
  int process_insert(bool& got_row);
  virtual void destroy() override
  {
    ObTableMergeOp::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_SQL_ENGINE_DML_OB_MULTI_TABLE_MERGE_OP_H_ */
