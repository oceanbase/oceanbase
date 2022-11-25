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

#ifndef _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_OPERATOR_H_
#define _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_OPERATOR_H_

#include "lib/container/ob_array.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace omt
{
class ObTenant;

class ObMultiTenantOperator
{
public:
  ObMultiTenantOperator();
  virtual ~ObMultiTenantOperator();

  int init();
  // 处理当前租户
  virtual int process_curr_tenant(common::ObNewRow *&row) = 0;
  // 释放上一个租户的资源
  virtual void release_last_tenant() = 0;
  // 过滤租户
  virtual bool is_need_process(uint64_t tenant_id) { return true; }
  // 释放资源, 注意继承ObMultiTenantOperator的子类在销毁时必须首先调用ObMultiTenantOperator::reset()
  // 由ObMultiTenantOperator维护的租户状态释放子类上的租户对象
  void reset();

  int execute(common::ObNewRow *&row);
private:
  bool inited_;
  ObArray<uint64_t> tenant_ids_;
  int64_t tenant_idx_;
  ObTenant *tenant_;
  ObLDHandle handle_;
};


} // end of namespace omt
} // end of namespace oceanbase


#endif /* _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_OPERATOR_H_ */
