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

#define USING_LOG_PREFIX SQL_EXE

#include "lib/allocator/ob_allocator.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/executor/ob_local_identity_task_spliter.h"
#include "sql/executor/ob_remote_identity_task_spliter.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTaskSpliterFactory::ObTaskSpliterFactory() : store_()
{
}

ObTaskSpliterFactory::~ObTaskSpliterFactory()
{
  for (int64_t i = 0; i < store_.count(); ++i) {
    ObTaskSpliter *ts = store_.at(i);
    if (OB_LIKELY(NULL != ts)) {
      ts->~ObTaskSpliter();
    }
  }
}

void ObTaskSpliterFactory::reset()
{
  for (int64_t i = 0; i < store_.count(); ++i) {
    ObTaskSpliter *ts = store_.at(i);
    if (OB_LIKELY(NULL != ts)) {
      ts->~ObTaskSpliter();
    }
  }
  store_.reset();
}

#define ALLOCATE_TASK_SPLITER(T) \
      void *ptr = allocator.alloc(sizeof(T)); \
      if (OB_ISNULL(ptr)) { \
        ret = OB_ALLOCATE_MEMORY_FAILED; \
      } else { \
        T *s = new(ptr)T(); \
        if (OB_FAIL(store_.push_back(s))) { \
          s->~T();                          \
          LOG_WARN("fail to store spliter", K(ret)); \
        } else if (OB_FAIL(s->init(exec_ctx.get_physical_plan_ctx(), \
                                   &exec_ctx, \
                                   job, \
                                   exec_ctx.get_sche_allocator()))) { \
          LOG_WARN("fail to init spliter", K(ret)); \
        } else { \
          spliter = s; \
        }\
      }

int ObTaskSpliterFactory::create(ObExecContext &exec_ctx,
                                 ObJob &job,
                                 int spliter_type,
                                 ObTaskSpliter *&spliter)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = exec_ctx.get_sche_allocator();
  switch (spliter_type) {
    case ObTaskSpliter::LOCAL_IDENTITY_SPLIT: {
      ALLOCATE_TASK_SPLITER(ObLocalIdentityTaskSpliter);
      break;
    }
    case ObTaskSpliter::REMOTE_IDENTITY_SPLIT: {
      ALLOCATE_TASK_SPLITER(ObRemoteIdentityTaskSpliter);
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected spliter type", K(spliter_type));
      break;
    }
  }
  return ret;
}

