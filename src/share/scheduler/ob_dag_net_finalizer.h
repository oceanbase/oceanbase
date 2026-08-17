/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEDULER_OB_DAG_NET_FINALIZER_H_
#define OCEANBASE_SHARE_SCHEDULER_OB_DAG_NET_FINALIZER_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace share
{

class ObDagNetFinalizerDag;

class ObDagNetFinalizerTask final : public ObITask
{
public:
  ObDagNetFinalizerTask();
  int init();
  int process() override;
};

class ObDagNetFinalizerDag final : public ObIDag
{
public:
  ObDagNetFinalizerDag();
  int init(ObIDagNet &dag_net);
  int create_first_task() override;
  int report_result() override;
  bool operator ==(const ObIDag &other) const override { return this == &other; }
  uint64_t hash() const override { return reinterpret_cast<uint64_t>(this); }
  int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      common::ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  uint64_t get_consumer_group_id() const override { return USER_RESOURCE_OTHER_GROUP_ID; }
  bool ignore_warning() override { return true; }
  int decide_retry_strategy(
      const int error_code,
      ObDagRetryStrategy &retry_status) override
  {
    UNUSED(error_code);
    retry_status = DAG_SKIP_RETRY;
    return OB_SUCCESS;
  }

  int process_dag_net_finalizer_();

private:
  // Payload only: the carrier is deliberately not attached to the owner via
  // ObIDag::set_dag_net(), so canceling the owner cannot cancel this DAG.
  ObIDagNet *owner_dag_net_;
  ObDagNetType::ObDagNetTypeEnum dag_net_type_;
  lib::Worker::CompatMode compat_mode_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEDULER_OB_DAG_NET_FINALIZER_H_
