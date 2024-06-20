/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_OB_TENANT_RESOURCE_LIMIT_H_
#define OB_ALL_VIRTUAL_OB_TENANT_RESOURCE_LIMIT_H_

#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"

namespace oceanbase
{
namespace share
{
class ObResourceInfo;
}

namespace observer
{
class ObResourceLimitTable : public common::ObVirtualTableScannerIterator,
                             public omt::ObMultiTenantOperator
{
public:
  ObResourceLimitTable();
  virtual ~ObResourceLimitTable();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int set_addr(common::ObAddr &addr);
  enum COLUMN_NAME
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    ZONE,
    RESOURCE_NAME,
    CURRENT_UTILIZATION,
    MAX_UTILIZATION,
    RSERVED_VALUE,
    LIMIT_VALUE,
    EFFECTIVE_LIMIT_TYPE
  };
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int get_next_resource_info_(share::ObResourceInfo &info);
private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  share::ObLogicResourceStatIterator iter_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObResourceLimitTable);
};

} // end namespace observer
} // end namespace oceanbase

#endif
