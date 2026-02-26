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

#ifndef _STORAGE_DDL_OB_COLUMN_CLUSTERED_DAG_
#define _STORAGE_DDL_OB_COLUMN_CLUSTERED_DAG_

#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "src/share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{

namespace storage
{

struct ObColumnClusteredDagInitParam : public ObDDLIndependentDagInitParam
{
public:
  ObColumnClusteredDagInitParam() : px_thread_count_(0) {}
  virtual bool is_valid() const override { return ObDDLIndependentDagInitParam::is_valid() && px_thread_count_ > 0; }
  INHERIT_TO_STRING_KV("DDLDagInitParm", ObDDLIndependentDagInitParam, K(px_thread_count_));

public:
  int64_t px_thread_count_;
};

class ObColumnClusteredDag : public ObDDLIndependentDag
{
public:
  ObColumnClusteredDag();
  virtual ~ObColumnClusteredDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  int set_px_finished();
  int update_tablet_range_count();
  void set_vec_tablet_rebuild(const bool value) { is_vec_tablet_rebuild_ = value; }
  int64_t get_total_slice_count() const { return total_slice_count_; }
  virtual bool is_scan_finished() override { return px_thread_count_ > 0 && px_finished_count_ >= px_thread_count_; }

  INHERIT_TO_STRING_KV("DDLDag", ObDDLIndependentDag, K_(px_thread_count), K_(px_finished_count), K_(is_range_count_ready), K_(total_slice_count), K_(use_static_plan));

protected:
  int64_t px_thread_count_;
  int64_t px_finished_count_;
  lib::ObMutex mutex_;
  bool is_range_count_ready_; // update table total slice count and each tablet slice count
  int64_t total_slice_count_; // for idempotence of user autoinc column
  bool use_static_plan_;
  bool is_vec_tablet_rebuild_;
};


}// namespace storage
}// namespace oceanbase

#endif//_STORAGE_DDL_OB_COLUMN_CLUSTERED_DAG_
