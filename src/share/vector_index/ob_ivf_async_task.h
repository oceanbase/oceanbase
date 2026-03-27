/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_IVF_ASYNC_TASK_H_
#define OCEANBASE_SHARE_OB_IVF_ASYNC_TASK_H_

#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"

namespace oceanbase
{
namespace share
{
class ObPluginVectorIndexAdaptor;
class ObIvfAsyncTask : public ObVecIndexIAsyncTask
{
public:
  ObIvfAsyncTask() : ObVecIndexIAsyncTask(ObMemAttr(MTL_ID(), "IvfAsyTask")) {}
  virtual ~ObIvfAsyncTask() {}
  int do_work() override;

private:
  int delete_deprecated_cache(ObPluginVectorIndexService &vector_index_service);
  int write_cache(ObPluginVectorIndexService &vector_index_service);

  DISALLOW_COPY_AND_ASSIGN(ObIvfAsyncTask);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_IVF_ASYNC_TASK_H_