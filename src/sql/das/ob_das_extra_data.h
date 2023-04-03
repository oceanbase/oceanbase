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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_EXTRA_DATA_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_EXTRA_DATA_H_
#include "sql/das/ob_das_define.h"
#include "sql/das/ob_das_rpc_proxy.h"
namespace oceanbase
{
namespace sql
{
class ObDASExtraData
{
public:
  ObDASExtraData();
  ~ObDASExtraData() { erase_task_result(); }
  int init(const int64_t task_id,
           const int64_t timeout_ts,
           const common::ObAddr &result_addr,
           rpc::frame::ObReqTransport *transport);
  void set_output_info(const ExprFixedArray *output_exprs, ObEvalCtx *eval_ctx)
  {
    output_exprs_ = output_exprs;
    eval_ctx_ = eval_ctx;
  }
  int get_next_row();
  int get_next_rows(int64_t &count, int64_t capacity);
  void erase_task_result();
  void set_has_more(const bool has_more) { has_more_ = has_more; }
  void set_need_check_output_datum(bool v) { need_check_output_datum_ = v; }
  TO_STRING_KV(KPC_(output_exprs));
private:
  int fetch_result();
private:
  const ExprFixedArray *output_exprs_;
  ObEvalCtx *eval_ctx_;
  int64_t task_id_;
  int64_t timeout_ts_;
  common::ObAddr result_addr_;
  obrpc::ObDASRpcProxy rpc_proxy_;
  ObDASDataFetchRes result_;
  ObChunkDatumStore::Iterator result_iter_;
  bool has_more_;
  bool need_check_output_datum_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_EXTRA_DATA_H_ */
