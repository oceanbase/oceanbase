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

#ifndef _OB_SQ_OB_PX_EXCHANGE_H_
#define _OB_SQ_OB_PX_EXCHANGE_H_

#include "lib/ob_define.h"
#include "sql/engine/ob_operator.h"
namespace oceanbase
{
namespace sql
{

class ObPxExchangeOpInput : public ObOpInput
{
public:
  ObPxExchangeOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObOpInput(ctx, spec),
    task_id_(common::OB_INVALID_ID),
    sqc_id_(common::OB_INVALID_ID),
    dfo_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObPxExchangeOpInput() = default;
  virtual void reset() override { /*@TODO fix reset member by xiaochu*/ }
  virtual void set_task_id(int64_t task_id) { task_id_ = task_id; }
  virtual void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  virtual void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  int64_t get_task_id() const { return task_id_; }
  int64_t get_sqc_id() const { return sqc_id_; }
  int64_t get_dfo_id() const { return dfo_id_; }
protected:
  int64_t task_id_; // 目前主要是用于从 ch sets 中找到属于自己的 ch set
  int64_t sqc_id_;
  int64_t dfo_id_;
};

}
}
#endif /* _OB_SQ_OB_PX_EXCHANGE_H_ */
//// end of header file
