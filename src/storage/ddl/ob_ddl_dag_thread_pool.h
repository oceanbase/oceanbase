/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _STORAGE_DDL_OB_DDL_DAG_THREAD_POOL_
#define _STORAGE_DDL_OB_DDL_DAG_THREAD_POOL_

#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}

namespace storage
{
class ObDDLIndependentDag;

class ObDDLDagThreadPool : public share::ObThreadPool
{
public:
  ObDDLDagThreadPool() : is_inited_(false), start_succeeded_(false), ddl_dag_(nullptr), session_info_(nullptr) {}
  int init(const int64_t thread_count, ObDDLIndependentDag *ddl_dag, sql::ObSQLSessionInfo *session_info);
  virtual int start() override;
  virtual void run1() override;

private:
  bool is_inited_;
  bool start_succeeded_;
  ObDDLIndependentDag *ddl_dag_;
  sql::ObSQLSessionInfo *session_info_;
};


}// namespace storage
}// namespace oceanbase

#endif//_STORAGE_DDL_OB_DDL_DAG_THREAD_POOL_
