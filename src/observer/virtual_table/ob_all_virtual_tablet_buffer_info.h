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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_BUFFER_INFO_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TABLET_BUFFER_INFO_H_

#include "common/row/ob_row.h"
#include "lib/guard/ob_shared_guard.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_multi_tenant.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_ls_id.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualTabletBufferInfo : public common::ObVirtualTableScannerIterator,
                                     public omt::ObMultiTenantOperator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLET_BUFFER_PTR,
    TABLET_OBJ_PTR,
    POOL_TYPE,
    LS_ID,
    TABLET_ID,
    IN_MAP,
    LAST_ACCESS_TIME
  };
public:
  ObAllVirtualTabletBufferInfo();
  virtual ~ObAllVirtualTabletBufferInfo();
  virtual void reset();
  int init(common::ObAddr &addr);
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int get_tablet_pool_infos();
  int gen_row(const ObTabletBufferInfo &buffer_info, common::ObNewRow *&row);
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

private:
  static const int64_t STR_LEN = 128;
private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  int64_t index_;
  ObTabletPoolType pool_type_;
  ObSArray<ObTabletBufferInfo> buffer_infos_;
  char tablet_pointer_[STR_LEN];
  char tablet_buffer_pointer_[STR_LEN];
};
} // observer
} // oceanbase
#endif