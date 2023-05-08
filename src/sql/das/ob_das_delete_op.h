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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_DELETE_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_DELETE_OP_H_
#include "sql/das/ob_das_task.h"
#include "storage/access/ob_dml_param.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_dml_ctx_define.h"
namespace oceanbase
{
namespace sql
{
class ObDASDeleteOp : public ObIDASTaskOp
{
  OB_UNIS_VERSION(1);
public:
  ObDASDeleteOp(common::ObIAllocator &op_alloc);
  virtual ~ObDASDeleteOp() = default;

  virtual int open_op() override;
  virtual int release_op() override;
  virtual int decode_task_result(ObIDASTaskResult *task_result) override;
  virtual int fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit) override;
  virtual int init_task_info(uint32_t row_extend_size) override;
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) override;
  virtual const ObDASBaseCtDef *get_ctdef() const override { return del_ctdef_; }
  virtual ObDASBaseRtDef *get_rtdef() override { return del_rtdef_; }
  int write_row(const ExprFixedArray &row,
                ObEvalCtx &eval_ctx,
                ObChunkDatumStore::StoredRow *&stored_row,
                bool &buffer_full);
  int64_t get_row_cnt() const { return write_buffer_.get_row_cnt(); }
  void set_das_ctdef(const ObDASDelCtDef *del_ctdef) { del_ctdef_ = del_ctdef; }
  void set_das_rtdef(ObDASDelRtDef *del_rtdef) { del_rtdef_ = del_rtdef; }
  virtual int dump_data() const override
  {
    return write_buffer_.dump_data(*del_ctdef_);
  }

  INHERIT_TO_STRING_KV("parent", ObIDASTaskOp,
                       KPC_(del_ctdef),
                       KPC_(del_rtdef),
                       K_(write_buffer));
private:
  const ObDASDelCtDef *del_ctdef_;
  ObDASDelRtDef *del_rtdef_;
  ObDASWriteBuffer write_buffer_;
  int64_t affected_rows_;  // local execute result, no need to serialize
};

class ObDASDeleteResult : public ObIDASTaskResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObDASDeleteResult();
  virtual ~ObDASDeleteResult();
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
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_DELETE_OP_H_ */
