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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_INSERT_OP_H_
#define DEV_SRC_SQL_DAS_OB_DAS_INSERT_OP_H_
#include "sql/das/ob_das_task.h"
#include "storage/access/ob_dml_param.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{
class ObDASInsertResult;

typedef common::ObList<blocksstable::ObDatumRowIterator *, common::ObIAllocator> ObDuplicatedIterList;
class ObDASConflictIterator : public blocksstable::ObDatumRowIterator
{
public:
  ObDASConflictIterator(const ObjMetaFixedArray &output_types,
                        common::ObIAllocator &alloc)
    : output_types_(output_types),
      duplicated_iter_list_(alloc),
      curr_iter_(duplicated_iter_list_.begin())
  {
  }

  ~ObDASConflictIterator() {};

  void reset();
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;

  void init_curr_iter()
  { curr_iter_ = duplicated_iter_list_.begin(); }
  ObDuplicatedIterList &get_duplicated_iter_array()
  { return duplicated_iter_list_; }
private:
  const ObjMetaFixedArray &output_types_;
  ObDuplicatedIterList duplicated_iter_list_;
  ObDuplicatedIterList::iterator curr_iter_;
};

class ObDASInsertOp : public ObIDASTaskOp
{
  OB_UNIS_VERSION(1);
  friend class ObDASInsertResult;
public:
  ObDASInsertOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASInsertOp() = default;

  virtual int open_op() override;
  virtual int release_op() override;
  virtual int record_task_result_to_rtdef() override;
  virtual int assign_task_result(ObIDASTaskOp *other) override;
  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  virtual int init_task_info(uint32_t row_extend_size) override;
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) override;
  virtual const ObDASBaseCtDef *get_ctdef() const override { return ins_ctdef_; }
  virtual ObDASBaseRtDef *get_rtdef() override { return ins_rtdef_; }
  int write_row(const ExprFixedArray &row,
                ObEvalCtx &eval_ctx,
                ObChunkDatumStore::StoredRow *&stored_row);
  int64_t get_row_cnt() const { return insert_buffer_.get_row_cnt(); }
  void set_das_ctdef(const ObDASInsCtDef *ins_ctdef) { ins_ctdef_ = ins_ctdef; }
  void set_das_rtdef(ObDASInsRtDef *ins_rtdef) { ins_rtdef_ = ins_rtdef; }
  virtual int dump_data() const override
  {
    return insert_buffer_.dump_data(*ins_ctdef_);
  }

  blocksstable::ObDatumRowIterator *get_duplicated_result()
  { return result_; }

  int64_t get_affected_rows() { return affected_rows_; }
  bool get_is_duplicated() { return is_duplicated_; }

  INHERIT_TO_STRING_KV("parent", ObIDASTaskOp,
                       KPC_(ins_ctdef),
                       KPC_(ins_rtdef),
                       K_(insert_buffer));

private:
  int insert_rows();
  int insert_row_with_fetch();
  int store_conflict_row(ObDASInsertResult &ins_result);

  int insert_index_with_fetch(ObDMLBaseParam &dml_param,
                              ObAccessService *as,
                              blocksstable::ObDatumRowIterator &dml_iter,
                              ObDASConflictIterator *result_iter,
                              const ObDASInsCtDef *ins_ctdef,
                              ObDASInsRtDef *ins_rtdef,
                              storage::ObStoreCtxGuard &store_ctx_guard,
                              const UIntFixedArray *duplicated_column_ids,
                              common::ObTabletID tablet_id,
                              transaction::ObTxReadSnapshot *snapshot);

private:
  const ObDASInsCtDef *ins_ctdef_;
  ObDASInsRtDef *ins_rtdef_;
  ObDASWriteBuffer insert_buffer_;
  blocksstable::ObDatumRowIterator *result_;
  int64_t affected_rows_;  // local execute result, no need to serialize
  bool is_duplicated_;  // local execute result, no need to serialize
};

class ObDASInsertResult : public ObIDASTaskResult, public blocksstable::ObDatumRowIterator
{
  OB_UNIS_VERSION_V(1);
public:
  ObDASInsertResult();
  virtual ~ObDASInsertResult();
  virtual int init(const ObIDASTaskOp &op, common::ObIAllocator &alloc) override;
  virtual int reuse() override;
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
  virtual void reset();
  virtual int link_extra_result(ObDASExtraData &extra_result, ObIDASTaskOp *task_op) override;
  int init_result_newrow_iter(const ObjMetaFixedArray *output_types);
  ObDASWriteBuffer &get_result_buffer() { return result_buffer_; }
  int64_t get_affected_rows() const { return affected_rows_; }
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }

  bool is_duplicated() { return is_duplicated_; }
  void set_is_duplicated(bool is_duplicated) { is_duplicated_ = is_duplicated; }
  transaction::ObTxReadSnapshot &get_response_snapshot() { return response_snapshot_; }

  INHERIT_TO_STRING_KV("ObIDASTaskResult", ObIDASTaskResult,
                       K_(affected_rows),
                       K_(is_duplicated),
                       K_(response_snapshot));
private:
  int64_t affected_rows_;
  ObDASWriteBuffer result_buffer_;
  ObDASWriteBuffer::NewRowIterator result_newrow_iter_;
  const ObjMetaFixedArray *output_types_;
  bool is_duplicated_;
  transaction::ObTxReadSnapshot response_snapshot_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_INSERT_OP_H_ */
