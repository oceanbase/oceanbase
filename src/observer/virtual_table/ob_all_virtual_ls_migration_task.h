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

#ifndef OB_ALL_VIRTUAL_LS_MIGRATION_TASK_H_
#define OB_ALL_VIRTUAL_LS_MIGRATION_TASK_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "logservice/palf/log_meta_info.h"//CONFIG_VERSION_LEN
#include "storage/high_availability/ob_ls_migration_handler.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualLSMigrationTask : public common::ObVirtualTableScannerIterator,
                                    public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualLSMigrationTask();
  virtual ~ObAllVirtualLSMigrationTask();
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
  int next_migration_task_(
      ObLSMigrationTask &task,
      ObLSMigrationHandlerStatus &status);
private:
  common::ObAddr addr_;
  /* 跨租户访问的资源必须由ObMultiTenantOperator来处理释放*/
  int64_t ls_id_;
  ObSharedGuard<storage::ObLSIterator> ls_iter_guard_;

  char ip_buf_[common::OB_IP_STR_BUFF];
  char task_id_buf_[common::OB_TRACE_STAT_BUFFER_SIZE];
  char config_version_buf_[palf::LogConfigVersion::CONFIG_VERSION_LEN];
  char src_ip_port_buf_[common::MAX_IP_PORT_LENGTH];
  char dest_ip_port_buf_[common::MAX_IP_PORT_LENGTH];
  char data_src_ip_port_buf_[common::MAX_IP_PORT_LENGTH];

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLSMigrationTask);
};

} // observer
} // oceanbase
#endif /* OB_ALL_VIRTUAL_LS_MIGRATION_TASK_H_ */
