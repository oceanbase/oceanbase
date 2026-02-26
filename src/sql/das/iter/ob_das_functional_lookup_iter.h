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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUNCTIONAL_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUNCTIONAL_LOOKUP_ITER_H_

#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASFuncLookupIterParam : public ObDASIterParam
{
public:
  ObDASFuncLookupIterParam()
    : ObDASIterParam(DAS_ITER_FUNC_LOOKUP),
      default_batch_row_count_(0),
      index_ctdef_(nullptr),
      index_rtdef_(nullptr),
      lookup_ctdef_(nullptr),
      lookup_rtdef_(nullptr),
      index_table_iter_(nullptr),
      data_table_iter_(nullptr),
      rowkey_exprs_(nullptr),
      doc_id_expr_(nullptr),
      trans_desc_(nullptr),
      snapshot_(nullptr)
  {}
  int64_t default_batch_row_count_;
  const ObDASBaseCtDef *index_ctdef_;
  ObDASBaseRtDef *index_rtdef_;
  const ObDASScanCtDef *lookup_ctdef_;
  ObDASScanRtDef *lookup_rtdef_;
  ObDASIter *index_table_iter_;
  ObDASIter *data_table_iter_;
  const ExprFixedArray *rowkey_exprs_;
  ObExpr *doc_id_expr_;
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid()
        && index_table_iter_ != nullptr && data_table_iter_ != nullptr
        && index_ctdef_ != nullptr && index_rtdef_ != nullptr && doc_id_expr_ != nullptr;
  }
};

class ObDASScanCtDef;
class ObDASScanRtDef;
class ObDASHNSWScanIter;

/**
 * Func Lookup Iter:
 *              Func Lookup Iter
 *                /        \
 *             /              \
 *          /                    \
 *       /                          \
 *  INDEX_ITER                  DATA_ITER = FTS_MERGE_ITER
 * (ROWKEY_DOCID)
 *
 * Func Lookup:
 *            Local Lookup Iter
 *             /              \
 *            /                \
 *           /                  \
 * Local Lookup/Das Scan    Func Lookup Iter
 **/

/*
 * In ObDASFuncLookupIter, the data iter is a fts merge iter which is just
 * a tool iter including main lookup iter and tr merge iters.
 */
class ObDASFuncLookupIter : public ObDASLocalLookupIter
{
public:
  ObDASFuncLookupIter()
    : ObDASLocalLookupIter(ObDASIterType::DAS_ITER_FUNC_LOOKUP),
      index_scan_rowsize_(0),
      data_scan_read_rowsize_(0),
      start_table_scan_(false)
  {}
  virtual ~ObDASFuncLookupIter() {}
  void set_index_scan_param(storage::ObTableScanParam &scan_param) { static_cast<ObDASScanIter *>(index_table_iter_)->set_scan_param(scan_param);}
  ObDASScanIter *get_index_scan_iter() { return static_cast<ObDASScanIter *>(index_table_iter_); }
  int64 get_group_id() const
  {
    const ExprFixedArray *exprs = &(static_cast<const ObDASScanCtDef *>(index_ctdef_))->pd_expr_spec_.access_exprs_;
    int64 group_id = 0;
    for (int i = 0; i < exprs->count(); i++) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    return group_id;
  }
  virtual void clear_evaluated_flag() override;
  virtual int set_scan_rowkey(ObEvalCtx *eval_ctx,
                              const ObIArray<ObExpr *> &rowkey_exprs,
                              const ObDASScanCtDef *lookup_ctdef,
                              ObIAllocator *alloc,
                              int64_t group_id) override;
  friend class ObDASHNSWScanIter;
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int add_rowkey() override;
  virtual int add_rowkeys(int64_t count) override;
  virtual int do_index_lookup() override;
  virtual int check_index_lookup() override;
  virtual void reset_lookup_state() override;
protected:
  int64_t index_scan_rowsize_;
  int64_t data_scan_read_rowsize_;
  bool start_table_scan_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUNCTIONAL_LOOKUP_ITER_H_ */
