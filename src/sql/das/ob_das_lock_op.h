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

#ifndef DEV_SRC_SQL_DAS_OB_DAS_LOCK_OP_H_
#define DEV_SRC_SQL_DAS_OB_DAS_LOCK_OP_H_
#include "sql/das/ob_das_task.h"
#include "storage/access/ob_dml_param.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{
class ObDASLockOp : public ObIDASTaskOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASLockOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASLockOp() = default;

  virtual int open_op() override;
  virtual int release_op() override;
  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  virtual int init_task_info(uint32_t row_extend_size) override;
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) override;
  virtual const ObDASBaseCtDef *get_ctdef() const override { return lock_ctdef_; }
  virtual ObDASBaseRtDef *get_rtdef() override { return lock_rtdef_; }
  int write_row(const ExprFixedArray &row,
                ObEvalCtx &eval_ctx,
                ObChunkDatumStore::StoredRow *&stored_row,
                bool &buffer_full);
  int64_t get_row_cnt() const { return lock_buffer_.get_row_cnt(); }
  void set_das_ctdef(const ObDASLockCtDef *del_ctdef) { lock_ctdef_ = del_ctdef; }
  void set_das_rtdef(ObDASLockRtDef *del_rtdef) { lock_rtdef_ = del_rtdef; }
  virtual int dump_data() const override
  {
    return lock_buffer_.dump_data(*lock_ctdef_);
  }

  INHERIT_TO_STRING_KV("parent", ObIDASTaskOp,
                       KPC_(lock_ctdef),
                       KPC_(lock_rtdef),
                       K_(lock_buffer));
private:
  const ObDASLockCtDef *lock_ctdef_;
  ObDASLockRtDef *lock_rtdef_;
  ObDASWriteBuffer lock_buffer_;
  int64_t affected_rows_;  // local execute result, no need to serialize
};

class ObDASLockResult : public ObIDASTaskResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObDASLockResult();
  virtual ~ObDASLockResult();
  virtual int init(const ObIDASTaskOp &op, common::ObIAllocator &alloc) override;
  virtual int reuse() override;
  int64_t get_affected_rows() const { return affected_rows_; }
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }
  INHERIT_TO_STRING_KV("ObIDASTaskResult", ObIDASTaskResult,
                        K_(affected_rows));
private:
  int64_t affected_rows_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_DAS_LOCK_OP_H_ */
