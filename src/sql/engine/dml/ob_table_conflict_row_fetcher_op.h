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

#ifndef OCEANBASE_SQL_ENGINE_DML_TABLE_CONFLICT_ROW_FETCHER_H_
#define OCEANBASE_SQL_ENGINE_DML_TABLE_CONFLICT_ROW_FETCHER_H_

#include "sql/engine/ob_operator.h"
#include "common/ob_partition_key.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
namespace oceanbase {
namespace storage {
class ObDMLBaseParam;
}
namespace sql {

struct ObPartConflictDatumStore {
  OB_UNIS_VERSION(1);

public:
  ObPartConflictDatumStore() : part_key_(), conflict_datum_store_(NULL)
  {}
  TO_STRING_KV(K_(part_key), KPC_(conflict_datum_store));

  common::ObPartitionKey part_key_;
  sql::ObChunkDatumStore* conflict_datum_store_;
};

class ObTCRFetcherOpInput : public ObOpInput {
  friend class ObTableConflictRowFetcherOp;
  OB_UNIS_VERSION(1);

public:
  ObTCRFetcherOpInput(ObExecContext& ctx, const ObOpSpec& spec)
      : ObOpInput(ctx, spec), part_conflict_rows_(), alloc_(NULL)
  {}
  virtual ~ObTCRFetcherOpInput()
  {}
  virtual void reset() override
  {
    part_conflict_rows_.reset();
  }
  virtual int init(ObTaskInfo& task_info);
  void set_deserialize_allocator(common::ObIAllocator* alloc)
  {
    alloc_ = alloc;
  }

private:
  common::ObSEArray<ObPartConflictDatumStore, 4> part_conflict_rows_;
  common::ObIAllocator* alloc_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTCRFetcherOpInput);
};

class ObConflictDatumIterator : public common::ObNewRowIterator {
public:
  ObConflictDatumIterator(const ObExpr* const* exprs, int64_t col_cnt, ObIAllocator* alloc)
      : exprs_(exprs), col_cnt_(col_cnt), alloc_(alloc)
  {}
  virtual ~ObConflictDatumIterator()
  {
    reset();
  }
  virtual int get_next_row(common::ObNewRow*& row) override;
  virtual void reset() override
  {
    row_iter_.reset();
  }
  int init(sql::ObChunkDatumStore* conflict_datum_store);

private:
  const ObExpr* const* exprs_;
  int64_t col_cnt_;
  ObIAllocator* alloc_;
  sql::ObChunkDatumStore::Iterator row_iter_;
  ObNewRow checker_row_;
};

class ObTableConflictRowFetcherSpec : public ObOpSpec {
  OB_UNIS_VERSION(1);

public:
  ObTableConflictRowFetcherSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type),
        table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        conf_col_ids_(alloc),
        access_col_ids_(alloc),
        conflict_exprs_(alloc),
        access_exprs_(alloc),
        only_data_table_(false)
  {}

public:
  uint64_t table_id_;
  uint64_t index_tid_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> conf_col_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> access_col_ids_;
  ExprFixedArray conflict_exprs_;
  ExprFixedArray access_exprs_;
  bool only_data_table_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableConflictRowFetcherSpec);
};

class ObTableConflictRowFetcherOp : public ObOperator {
public:
  ObTableConflictRowFetcherOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObOperator(ctx, spec, input),
        dup_row_iter_arr_(),
        row2exprs_projector_(ctx.get_allocator()),
        cur_row_idx_(0),
        cur_rowkey_id_(0)
  {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override
  {
    ObOperator::destroy();
    dup_row_iter_arr_.reset();
  }

private:
  int fetch_conflict_rows(storage::ObDMLBaseParam& dml_para);

  DISALLOW_COPY_AND_ASSIGN(ObTableConflictRowFetcherOp);

  common::ObSEArray<common::ObNewRowIterator*, 4> dup_row_iter_arr_;
  storage::ObRow2ExprsProjector row2exprs_projector_;
  int64_t cur_row_idx_;
  int64_t cur_rowkey_id_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
