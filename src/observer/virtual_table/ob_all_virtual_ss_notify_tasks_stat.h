/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_SS_NOTIFY_TASKS_STAT_H
#define OB_ALL_VIRTUAL_SS_NOTIFY_TASKS_STAT_H

#include "lib/container/ob_tuple.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace sslog
{
class ObSSLogNotifyTask;
class ObSSLogNotifyTaskQueue;
}
namespace observer
{

class ObAllVirtualSSNotifyTasksStat : public common::ObVirtualTableScannerIterator
{
  struct IterNodeOp {
    IterNodeOp(ObAllVirtualSSNotifyTasksStat *p_stat,
               char *temp_buffer)
    : this_(p_stat),
    temp_buffer_(temp_buffer),
    state_(nullptr) {}
    int operator()(sslog::ObSSLogNotifyTask &notify_task,
                   sslog::ObSSLogNotifyTaskQueue &queue);
    ObAllVirtualSSNotifyTasksStat* this_;
    char *temp_buffer_;
    const char *state_;
  };
  struct IterateTenantOp {
    IterateTenantOp(ObAllVirtualSSNotifyTasksStat *p_stat,
                    char *temp_buffer)
    : this_(p_stat),
    temp_buffer_(temp_buffer) {}
    int operator()();
    ObAllVirtualSSNotifyTasksStat* this_;
    char *temp_buffer_;
  };
  friend class IterNodeOp;
  friend class IterateTenantOp;
  static constexpr int64_t IP_BUFFER_SIZE = MAX_IP_ADDR_LENGTH;
public:
  static constexpr int64_t BUFFER_SIZE = 32_MB;
  explicit ObAllVirtualSSNotifyTasksStat(omt::ObMultiTenant *omt) : omt_(omt) {}
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  TO_STRING_KV(KP_(omt))
private:
  int convert_node_info_to_row_(const char *state,
                                const sslog::ObSSLogNotifyTask &notify_task,
                                char *buffer,
                                const int64_t buffer_size,
                                common::ObNewRow &row);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSNotifyTasksStat);
  omt::ObMultiTenant *omt_;
  char ip_buffer_[IP_BUFFER_SIZE];
};

} // observer
} // oceanbase
#endif
