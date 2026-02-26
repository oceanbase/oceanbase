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
  ObDDLDagThreadPool() : is_inited_(false), ddl_dag_(nullptr), session_info_(nullptr) {}
  int init(const int64_t thread_count, ObDDLIndependentDag *ddl_dag, sql::ObSQLSessionInfo *session_info);
  virtual void run1() override;

private:
  bool is_inited_;
  ObDDLIndependentDag *ddl_dag_;
  sql::ObSQLSessionInfo *session_info_;
};


}// namespace storage
}// namespace oceanbase

#endif//_STORAGE_DDL_OB_DDL_DAG_THREAD_POOL_
