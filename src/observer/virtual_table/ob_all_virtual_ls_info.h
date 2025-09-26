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

#ifndef OB_ALL_VIRTUAL_OB_LS_INFO_H_
#define OB_ALL_VIRTUAL_OB_LS_INFO_H_

#include "storage/ls/ob_ls.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

// 虚拟表列ID枚举
enum class ObAllVirtualLSInfoColumnId : uint64_t
{
  SVR_IP = OB_APP_MIN_COLUMN_ID,
  SVR_PORT = OB_APP_MIN_COLUMN_ID + 1,
  TENANT_ID = OB_APP_MIN_COLUMN_ID + 2,
  LS_ID = OB_APP_MIN_COLUMN_ID + 3,
  REPLICA_TYPE = OB_APP_MIN_COLUMN_ID + 4,
  LS_STATE = OB_APP_MIN_COLUMN_ID + 5,
  TABLET_COUNT = OB_APP_MIN_COLUMN_ID + 6,
  WEAK_READ_TIMESTAMP = OB_APP_MIN_COLUMN_ID + 7,
  NEED_REBUILD = OB_APP_MIN_COLUMN_ID + 8,
  CLOG_CHECKPOINT_TS = OB_APP_MIN_COLUMN_ID + 9,
  CLOG_CHECKPOINT_LSN = OB_APP_MIN_COLUMN_ID + 10,
  MIGRATE_STATUS = OB_APP_MIN_COLUMN_ID + 11,
  REBUILD_SEQ = OB_APP_MIN_COLUMN_ID + 12,
  TABLET_CHANGE_CHECKPOINT_SCN = OB_APP_MIN_COLUMN_ID + 13,
  TRANSFER_SCN = OB_APP_MIN_COLUMN_ID + 14,
  TX_BLOCKED = OB_APP_MIN_COLUMN_ID + 15,
  REQUIRED_DATA_DISK_SIZE = OB_APP_MIN_COLUMN_ID + 16,
};

class ObAllVirtualLSInfo : public common::ObVirtualTableScannerIterator,
                           public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualLSInfo();
  virtual ~ObAllVirtualLSInfo();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }
private:
  // 过滤得到需要处理的租户
  virtual bool is_need_process(uint64_t tenant_id) override;
  // 处理当前迭代的租户
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  // 释放上一个租户的资源
  virtual void release_last_tenant() override;
private:
  int next_ls_info_(ObLSVTInfo &ls_info);
private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char state_name_[common::MAX_LS_STATE_LENGTH];
  char replica_type_name_[common::MAX_REPLICA_TYPE_LENGTH];
  /* 跨租户访问的资源必须由ObMultiTenantOperator来处理释放*/
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLSInfo);
};

} // observer
} // oceanbase
#endif
