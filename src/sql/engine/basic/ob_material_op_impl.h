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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_MATERIAL_OP_IMPL_H_
#define OCEANBASE_SQL_ENGINE_BASIC_MATERIAL_OP_IMPL_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase
{
namespace sql
{

class ObMaterialOpImpl {
public:
  explicit ObMaterialOpImpl(ObMonitorNode &op_monitor_info, ObSqlWorkAreaProfile &profile);
  virtual ~ObMaterialOpImpl();

  void reset();
  void unregister_profile_if_necessary()
  {
    sql_mem_processor_.unregister_profile_if_necessary();
  }

  int init(const uint64_t tenant_id,
           ObEvalCtx *eval_ctx,
           ObExecContext *exec_ctx,
           ObIOEventObserver *observer,
           const int64_t default_block_size = ObChunkDatumStore::BLOCK_SIZE);
  inline void set_input_rows(int64_t input_rows) { input_rows_ = input_rows; }
  inline void set_input_width(int64_t input_width) { input_width_ = input_width; }
  inline void set_operator_type(ObPhyOperatorType op_type) { op_type_ = op_type; }
  inline void set_operator_id(uint64_t op_id) { op_id_ = op_id; }
  int64_t get_material_row_count() const { return datum_store_.get_row_cnt(); }

  // before add row process: update date used memory, try dump ...
  int before_add_row();

  int add_row(const common::ObIArray<ObExpr*> &exprs,
              const ObChunkDatumStore::StoredRow *&store_row);
  int add_row(const ObChunkDatumStore::StoredRow &src_sr,
              const ObChunkDatumStore::StoredRow *&store_row);
  int add_row(const ObDatum *src_datums,
              const int64_t datum_cnt,
              const int64_t extra_size,
              const ObChunkDatumStore::StoredRow *&store_row);
  int add_row(const ObChunkDatumStore::StoredRow &sr)
  {
    const ObChunkDatumStore::StoredRow *store_row = NULL;
    return add_row(sr, store_row);
  }
  int add_row(const common::ObIArray<ObExpr*> &exprs)
  {
    const ObChunkDatumStore::StoredRow *store_row = NULL;
    return add_row(exprs, store_row);
  }

  // add batch rows by selector
  int add_batch(const common::ObIArray<ObExpr *> &exprs,
                const ObBitVector &skip,
                const int64_t batch_size);
  int finish_add_row();

  int get_next_row(const common::ObIArray<ObExpr*> &exprs);
  int get_next_row(const ObChunkDatumStore::StoredRow *&sr);

  // get next batch rows, %max_cnt should equal or smaller than max batch size.
  // return OB_ITER_END for EOF
  int get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                     const int64_t max_rows,
                     int64_t &read_rows);

  int rescan();
  int reuse();
  // rewind get_next_row() iterator to begin.
  int rewind();


private:
  int process_dump();
  bool need_dump()
  { return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound(); }
  void destroy_mem_context()
  {
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
private:
  friend class ObValues;
  bool inited_;
  bool got_first_row_;
  int64_t tenant_id_;
  ObExecContext *exec_ctx_;
  lib::MemoryContext mem_context_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator datum_store_it_;
  ObEvalCtx *eval_ctx_;
  ObSqlWorkAreaProfile &profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObIOEventObserver *io_event_observer_;
  int64_t input_rows_;
  int64_t input_width_;
  ObPhyOperatorType op_type_;
  uint64_t op_id_;
};

}
}

#endif