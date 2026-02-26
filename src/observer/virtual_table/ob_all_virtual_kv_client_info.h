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
#ifndef OB_ALL_VIRTUAL_KV_CLIENT_INFO_H_
#define OB_ALL_VIRTUAL_KV_CLIENT_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/table/ob_table_client_info_mgr.h"
namespace oceanbase
{
namespace observer
{
struct ObGetAllClientInfoOp {
  explicit ObGetAllClientInfoOp(common::ObIArray<table::ObTableClientInfo>& cli_infos)
    : allocator_(nullptr),
      cli_infos_(cli_infos)
  {}
  int operator()(common::hash::HashMapPair<uint64_t, table::ObTableClientInfo*> &entry);
  void set_allocator(ObIAllocator *allocator) { allocator_ = allocator; }
private:
  ObIAllocator *allocator_;
  common::ObIArray<table::ObTableClientInfo>& cli_infos_;
};

class ObAllVirtualKvClientInfo : public common::ObVirtualTableScannerIterator,
                                 public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualKvClientInfo()
    : ObVirtualTableScannerIterator(),
      cur_idx_(0),
      svr_addr_()
  {
    MEMSET(svr_ip_buf_, 0, common::OB_IP_STR_BUFF);
  }
  virtual ~ObAllVirtualKvClientInfo() { reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int set_svr_addr(common::ObAddr &addr);
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
private:
  enum CLI_INFO_COLUMN
  {
    CLIENT_ID = common::OB_APP_MIN_COLUMN_ID,
    CLIENT_IP,
    CLIENT_PORT,
    SVR_IP,
    SVR_PORT,
    TENANT_ID,
    USER_NAME,
    FIRST_LOGIN_TS,
    LAST_LOGIN_TS,
    CLIENT_INFO,
  };
  int64_t cur_idx_;
  ObAddr svr_addr_;
  char svr_ip_buf_[common::OB_IP_STR_BUFF];
  ObSEArray<table::ObTableClientInfo, 128> cli_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualKvClientInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif