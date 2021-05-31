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

#include "ob_flashback_engine.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace storage {

// 1000 -> 500, 1300 -> 700
// data between 500 and 700 is visible
bool ObFlashBackEngine::can_skip_version(
    const int64_t snapshot_version, const int64_t trans_version, ObFlashBackInfoArray* flashback_infos)
{
  bool can_skip = false;
  if (NULL != flashback_infos) {
    for (int64_t i = flashback_infos->count() - 1; i >= 0; i--) {
      if (snapshot_version < flashback_infos->at(i).from_) {
        continue;
      } else {
        if (trans_version > flashback_infos->at(i).from_) {
          can_skip = false;
          break;
        } else if (trans_version > flashback_infos->at(i).to_) {
          can_skip = true;
          break;
        }
      }
    }
  }
  return can_skip;
}

int ObFlashBackEngine::push_flashback_info(ObFlashBackInfo& flashback_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flashback_infos_.push_back(flashback_info))) {
    STORAGE_LOG(WARN, "failed to assign flashback infos", K(ret), K(flashback_info));
  }
  STORAGE_LOG(INFO, "push flashback infos", K(flashback_info), K(flashback_infos_));
  return ret;
}

int ObFlashBackEngine::set_flashback_infos(ObFlashBackInfoArray& flashback_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flashback_infos_.assign(flashback_infos))) {
    STORAGE_LOG(WARN, "failed to assign flashback infos", K(ret), K(flashback_infos));
  }
  return ret;
}

int ObFlashBackEngine::get_flashback_infos(ObFlashBackInfoArray& flashback_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flashback_infos.assign(flashback_infos_))) {
    STORAGE_LOG(WARN, "failed to assign flashback infos", K(ret), K_(flashback_infos));
  }
  return ret;
}

ObFlashBackPartitionCb::ObFlashBackPartitionCb()
    : guard_(), flashback_info_(), write_clog_state_(CB_INIT), is_locking_(false)
{}

void ObFlashBackPartitionCb::reset()
{
  guard_.reset();
  write_clog_state_ = CB_INIT;
  is_locking_ = false;
}

int ObFlashBackPartitionCb::init(
    ObIPartitionGroup* partition_group, ObPartitionService& partition_service, const ObFlashBackInfo& flashback_info)
{
  int ret = OB_SUCCESS;
  guard_.set_partition_group(partition_service.get_pg_mgr(), *partition_group);
  flashback_info_ = flashback_info;
  return ret;
}

int ObFlashBackPartitionCb::on_success(const common::ObPartitionKey& pkey, const clog::ObLogType log_type,
    const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  int ret = OB_SUCCESS;
  UNUSED(pkey);
  UNUSED(log_type);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  if (FALSE_IT(flashback_info_.log_id_ = log_id)) {
  } else if (OB_FAIL(guard_.get_partition_group()->update_flashback_info(flashback_info_))) {
    STORAGE_LOG(WARN, "failed to flashback", K(ret), K(pkey), K_(flashback_info));
  } else {
    bool need_release = false;
    while (true) {
      if (false == ATOMIC_TAS(&is_locking_, true)) {
        if (CB_END != write_clog_state_) {
          write_clog_state_ = CB_SUCCESS;
        } else {
          // rs wait thread has exited, can release memory
          need_release = true;
        }
        ATOMIC_STORE(&is_locking_, false);
        break;
      }
    }
    if (need_release) {
      op_free(this);
    }
  }

  return ret;
}

int ObFlashBackPartitionCb::on_finished(const common::ObPartitionKey& pkey, const uint64_t log_id)
{
  UNUSED(pkey);
  UNUSED(log_id);
  int ret = OB_SUCCESS;
  // lock
  bool need_release = false;
  while (true) {
    if (false == ATOMIC_TAS(&is_locking_, true)) {
      if (CB_END != write_clog_state_) {
        write_clog_state_ = CB_FAIL;
      } else {
        // index_builder wait thread has exited, can release memory
        need_release = true;
      }
      ATOMIC_STORE(&is_locking_, false);
      break;
    }
  }
  if (need_release) {
    op_free(this);
  }

  return ret;
}

int ObFlashBackPartitionCb::check_can_release(bool& can_release)
{
  int ret = OB_SUCCESS;
  // lock
  while (true) {
    if (false == ATOMIC_TAS(&is_locking_, true)) {
      // clog is not callbacked
      if (CB_FAIL != write_clog_state_ && CB_SUCCESS != write_clog_state_) {
        write_clog_state_ = CB_END;
        can_release = false;
      } else {
        can_release = true;
      }
      ATOMIC_STORE(&is_locking_, false);
      break;
    }
  }

  return ret;
}

ObFlashBackPartitionCb* ObFlashBackPartitionCbFactory::alloc()
{
  return op_reclaim_alloc(ObFlashBackPartitionCb);
}

void ObFlashBackPartitionCbFactory::release(ObFlashBackPartitionCb* cb)
{
  if (NULL != cb) {
    cb->reset();
    op_reclaim_free(cb);
    cb = NULL;
  }
}

}  // namespace storage
}  // namespace oceanbase
