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

#include "storage/ob_partition_service.h"
#include "storage/ob_partition_checkpoint.h"

namespace oceanbase {

using namespace common;
using namespace clog;

namespace storage {
int ObCheckPoingLogCb::init(ObPartitionService* ps, const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  if (NULL == ps || 0 >= checkpoint) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ps), K(checkpoint));
  } else {
    ps_ = ps;
    checkpoint_ = checkpoint;
  }
  return ret;
}

int ObCheckPoingLogCb::on_success(const ObPartitionKey& pkey, const ObLogType log_type, const uint64_t log_id,
    const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(log_type);
  UNUSED(log_id);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (NULL == ps_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, ps is NULL", K(ret), KP(ps_));
  } else {
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(ps_->get_partition(pkey, guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
      if (OB_PARTITION_NOT_EXIST == ret) {
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(partition = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, partition is null", K(ret), K(pkey));
    } else if (OB_FAIL(partition->update_last_checkpoint(checkpoint_))) {
      STORAGE_LOG(WARN, "update checkpoint failed", K(ret), K(pkey), K(checkpoint_));
    } else {
      // do nothing
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "checkpoint log callback failed", K(ret), K(pkey), K(log_id), K(*this));
  } else {
    STORAGE_LOG(TRACE, "checkpoint log callback success", K(pkey), K(log_id), K(*this));
  }
  ObCheckPoingLogCbFactory::release(this);
  return ret;
}

int ObCheckPoingLogCb::on_finished(const ObPartitionKey& pkey, const uint64_t log_id)
{
  UNUSED(pkey);
  UNUSED(log_id);
  ObCheckPoingLogCbFactory::release(this);
  return OB_SUCCESS;
}

ObCheckPoingLogCb* ObCheckPoingLogCbFactory::alloc()
{
  return op_reclaim_alloc(ObCheckPoingLogCb);
}

void ObCheckPoingLogCbFactory::release(ObCheckPoingLogCb* cb)
{
  if (NULL != cb) {
    op_reclaim_free(cb);
    cb = NULL;
  }
}

}  // namespace storage
}  // namespace oceanbase
