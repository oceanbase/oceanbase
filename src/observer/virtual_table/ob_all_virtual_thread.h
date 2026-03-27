/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_THREAD_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_THREAD_H_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualThread : public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TID,
    TNAME,
    STATUS,
    WAIT_EVENT,
    LATCH_WAIT,
    LATCH_HOLD,
    TRACE_ID,
    LOOP_TS,
    CGROUP_PATH,
    NUMA_NODE
  };

public:
  ObAllVirtualThread();
  virtual ~ObAllVirtualThread() override;
  virtual int inner_open() override;
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  static const int32_t PATH_BUFSIZE = 512;
  bool is_inited_;
  bool is_config_cgroup_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char tname_[16];
  char wait_event_[96];
  char wait_addr_[16];
  char locks_addr_[256];
  char trace_id_buf_[40];
  char cgroup_path_buf_[PATH_BUFSIZE];
  int read_real_cgroup_path();

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualThread);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_THREAD_H_ */