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

#ifndef OB_ALL_VIRTUAL_LOGSERVICE_CLUSTER_INFO_H
#define OB_ALL_VIRTUAL_LOGSERVICE_CLUSTER_INFO_H

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualLogServiceClusterInfo : public common::ObVirtualTableScannerIterator
{
  enum Column : int64_t
  {
    CLUSTER_ID = common::OB_APP_MIN_COLUMN_ID,
    CLUSTER_VERSION,
    LM_RPC_ADDR,
    LM_HTTP_ADDR,
  };
public:
  ObAllVirtualLogServiceClusterInfo() {}
  virtual ~ObAllVirtualLogServiceClusterInfo() {}
public:
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  int fill_scanner_();
  int fill_row_();
private:
  uint64_t logservice_cluster_id_;
  char cluster_version_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char lm_rpc_addr_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
  char lm_http_addr_[common::OB_IP_PORT_STR_BUFF] = {'\0'};
};

} // end of namespace observer
} // end of namespace oceanbase

#endif