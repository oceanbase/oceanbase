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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SANDBOX_PROCESS_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SANDBOX_PROCESS_H_

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/ob_sandbox_manager.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualSandboxProcess : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    PID,
    SANDBOX_STATE,
    PROCESS_STATE,
    PROCESS_NAME,
    CPU_USAGE,
    CPU_TIME,
    MEMORY_USAGE,
    START_TIME,
    EXECUTE_PATH,
    ROOT_PATH
  };

  ObAllVirtualSandboxProcess();
  virtual ~ObAllVirtualSandboxProcess() override;
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  int fill_row_(const ObSandboxProcessSnapshot &snapshot, common::ObNewRow *&row);

private:
  bool is_inited_;
  int64_t next_idx_;
  common::ObSEArray<ObSandboxProcessSnapshot, 16> snapshots_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char process_state_buf_[2];

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSandboxProcess);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SANDBOX_PROCESS_H_
