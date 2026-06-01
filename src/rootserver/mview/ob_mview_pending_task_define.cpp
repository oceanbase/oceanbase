/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_pending_task_define.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

ObMViewPendingTaskKey::ObMViewPendingTaskKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID),
    mview_id_(OB_INVALID_ID)
{
}

ObMViewPendingTaskKey::ObMViewPendingTaskKey(const uint64_t tenant_id,
                                             const int64_t refresh_id,
                                             const uint64_t mview_id)
  : tenant_id_(tenant_id),
    refresh_id_(refresh_id),
    mview_id_(mview_id)
{
}

ObMViewPendingTaskKey::~ObMViewPendingTaskKey()
{
}

bool ObMViewPendingTaskKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && refresh_id_ > 0
      && OB_INVALID_ID != mview_id_;
}

uint64_t ObMViewPendingTaskKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&refresh_id_, sizeof(refresh_id_), hash_val);
  hash_val = murmurhash(&mview_id_, sizeof(mview_id_), hash_val);
  return hash_val;
}

int ObMViewPendingTaskKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

bool ObMViewPendingTaskKey::operator==(const ObMViewPendingTaskKey &other) const
{
  return tenant_id_ == other.tenant_id_
      && refresh_id_ == other.refresh_id_
      && mview_id_ == other.mview_id_;
}

ObMViewPendingTaskSessionIdEntry::ObMViewPendingTaskSessionIdEntry()
  : key_(),
    info_idx_(OB_INVALID_INDEX_INT64),
    session_id_(0)
{
}

ObMViewPendingTaskSessionIdEntry::ObMViewPendingTaskSessionIdEntry(
    const ObMViewPendingTaskKey &key,
    const int64_t info_idx)
  : key_(key),
    info_idx_(info_idx),
    session_id_(0)
{
}

ObMViewRefreshKey::ObMViewRefreshKey()
  : tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID)
{
}

ObMViewRefreshKey::ObMViewRefreshKey(const uint64_t tenant_id, const int64_t refresh_id)
  : tenant_id_(tenant_id),
    refresh_id_(refresh_id)
{
}

ObMViewRefreshKey::~ObMViewRefreshKey()
{
}

bool ObMViewRefreshKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_ && refresh_id_ > 0;
}

uint64_t ObMViewRefreshKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&refresh_id_, sizeof(refresh_id_), hash_val);
  return hash_val;
}

int ObMViewRefreshKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = hash();
  return ret;
}

bool ObMViewRefreshKey::operator==(const ObMViewRefreshKey &other) const
{
  return tenant_id_ == other.tenant_id_
      && refresh_id_ == other.refresh_id_;
}

ObMViewPendingRunningJobInfo::ObMViewPendingRunningJobInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID),
    mview_id_(OB_INVALID_ID),
    target_data_sync_scn_(0),
    refresh_method_(share::schema::ObMVRefreshMethod::MAX),
    refresh_parallel_(0),
    gmt_create_(0),
    gmt_modified_(0),
    session_id_(0)
{
}

ObMViewPendingRunningJobInfo::~ObMViewPendingRunningJobInfo()
{
}

ObMViewPendingTask::ObMViewPendingTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID),
    mview_id_(OB_INVALID_ID),
    seq_(0),
    target_data_sync_scn_(0),
    refresh_method_(share::schema::ObMVRefreshMethod::MAX),
    refresh_parallel_(0),
    status_(MV_TASK_PENDING),
    skip_cnt_(0),
    retry_count_(0),
    next_retry_ts_(0),
    flags_(0),
    dep_mview_id_cnt_(0),
    dep_mview_ids_(NULL),
    gmt_create_(0),
    gmt_modified_(0),
    svr_addr_(),
    session_id_(0)
{
}

ObMViewPendingTask::~ObMViewPendingTask()
{
}

bool ObMViewPendingTask::is_valid() const
{
  return refresh_id_ > 0 && OB_INVALID_ID != mview_id_;
}

bool ObMViewPendingTask::is_root_task() const
{
  return 0 != (flags_ & ROOT_TASK_FLAG);
}

bool ObMViewPendingTask::is_nested_refresh() const
{
  return 0 != (flags_ & NESTED_REFRESH_FLAG);
}

int ObMViewPendingTask::assign(const ObMViewPendingTask &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  refresh_id_ = other.refresh_id_;
  mview_id_ = other.mview_id_;
  seq_ = other.seq_;
  target_data_sync_scn_ = other.target_data_sync_scn_;
  refresh_method_ = other.refresh_method_;
  refresh_parallel_ = other.refresh_parallel_;
  status_ = other.status_;
  skip_cnt_ = other.skip_cnt_;
  retry_count_ = other.retry_count_;
  next_retry_ts_ = other.next_retry_ts_;
  flags_ = other.flags_;
  gmt_create_ = other.gmt_create_;
  gmt_modified_ = other.gmt_modified_;
  svr_addr_ = other.svr_addr_;
  session_id_ = other.session_id_;
  // do not copy dep_mview_ids_
  dep_mview_id_cnt_ = 0;
  dep_mview_ids_ = NULL;
  return ret;
}

int ObMViewPendingTask::deep_copy(ObIAllocator &alloc,
                                  const ObMViewPendingTask &src,
                                  ObMViewPendingTask *&dst)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  int64_t alloc_size = 0;
  char *dep_buf = NULL;
  dst = NULL;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task", KR(ret), K(src));
  } else if (OB_UNLIKELY(src.dep_mview_id_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dep cnt", KR(ret), K(src.dep_mview_id_cnt_));
  } else if (OB_UNLIKELY(src.dep_mview_id_cnt_ > 0 && OB_ISNULL(src.dep_mview_ids_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dep mview ids is null with positive cnt", KR(ret), K(src.dep_mview_id_cnt_));
  } else if (OB_FALSE_IT(alloc_size = static_cast<int64_t>(sizeof(ObMViewPendingTask))
                                + src.dep_mview_id_cnt_ * static_cast<int64_t>(sizeof(uint64_t)))) {
  } else if (OB_ISNULL(buf = alloc.alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc pending task failed", KR(ret), K(alloc_size));
  } else if (OB_FALSE_IT(dst = new (buf) ObMViewPendingTask())) {
  } else if (OB_FAIL(dst->assign(src))) {
    LOG_WARN("assign task failed", KR(ret), K(src));
  } else if (src.dep_mview_id_cnt_ > 0) {
    dep_buf = reinterpret_cast<char *>(buf) + sizeof(ObMViewPendingTask);
    dst->dep_mview_ids_ = reinterpret_cast<uint64_t *>(dep_buf);
    dst->dep_mview_id_cnt_ = src.dep_mview_id_cnt_;
    MEMCPY(dst->dep_mview_ids_,
           src.dep_mview_ids_,
           src.dep_mview_id_cnt_ * static_cast<int64_t>(sizeof(uint64_t)));
  } else {
    dst->dep_mview_ids_ = NULL;
  }
  if (OB_FAIL(ret) && NULL != dst) {
    dst->~ObMViewPendingTask();
    alloc.free(buf);
    dst = NULL;
  }
  return ret;
}

ObMViewPendingRefreshCtx::ObMViewPendingRefreshCtx()
  : tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID),
    root_mview_id_(OB_INVALID_ID),
    unfinished_task_cnt_(0),
    running_task_cnt_(0),
    has_terminal_failure_(false),
    root_task_succeeded_(false),
    cancelled_(false),
    trace_id_()
{
}

ObMViewPendingRefreshCtx::~ObMViewPendingRefreshCtx()
{
}

bool ObMViewPendingRefreshCtx::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && refresh_id_ > 0;
}

} // namespace rootserver
} // namespace oceanbase
