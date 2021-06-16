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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase {
namespace sql {

class ObMaterialSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObMaterialSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObOpSpec(alloc, type)
  {}
};

class ObMaterialOp : public ObOperator {
public:
  ObMaterialOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObOperator(exec_ctx, spec, input),
        mem_context_(nullptr),
        datum_store_(),
        datum_store_it_(),
        profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
        sql_mem_processor_(profile_),
        is_first_(false)
  {}

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;

  int get_material_row_count(int64_t& count) const
  {
    count = datum_store_.get_row_cnt();
    return common::OB_SUCCESS;
  }

private:
  int process_dump();
  int get_all_row_from_child(ObSQLSessionInfo& session);

  bool need_dump()
  {
    return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound();
  }
  void destroy_mem_context()
  {
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }

private:
  lib::MemoryContext* mem_context_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator datum_store_it_;
  friend class ObValues;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  bool is_first_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_OP_H_ */
