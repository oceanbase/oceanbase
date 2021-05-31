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

#include "ob_trans_log.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "common/ob_partition_key.h"
#include "ob_trans_factory.h"
#include "clog/ob_log_entry.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
using namespace clog;

namespace transaction {
OB_SERIALIZE_MEMBER(ObTransLog, log_type_, partition_, trans_id_, cluster_id_);
OB_SERIALIZE_MEMBER((ObTransRedoLog, ObTransLog), tenant_id_, log_no_, scheduler_, coordinator_, participants_,
    trans_param_, mutator_, active_memstore_version_, prev_trans_arr_, can_elr_, xid_, is_last_);
OB_SERIALIZE_MEMBER((ObTransPrepareLog, ObTransLog), tenant_id_, scheduler_, coordinator_, participants_, trans_param_,
    prepare_status_, prev_redo_log_ids_, local_trans_version_, active_memstore_version_, app_trace_id_str_,
    partition_log_info_arr_, checkpoint_, prev_trans_arr_, can_elr_, app_trace_info_, xid_);
OB_SERIALIZE_MEMBER(
    (ObTransCommitLog, ObTransLog), partition_log_info_arr_, global_trans_version_, checksum_, split_info_);
OB_SERIALIZE_MEMBER((ObTransPreCommitLog, ObTransLog), publish_version_);
OB_SERIALIZE_MEMBER((ObTransAbortLog, ObTransLog), partition_log_info_arr_, split_info_);
OB_SERIALIZE_MEMBER((ObTransClearLog, ObTransLog));
// for sp transaction log
OB_SERIALIZE_MEMBER((ObSpTransRedoLog, ObTransLog), tenant_id_, log_no_, trans_param_, mutator_,
    active_memstore_version_, prev_trans_arr_, can_elr_);
OB_SERIALIZE_MEMBER((ObSpTransCommitLog, ObSpTransRedoLog), global_trans_version_, checksum_, prev_redo_log_ids_,
    app_trace_id_str_, checkpoint_, app_trace_info_);
OB_SERIALIZE_MEMBER((ObSpTransAbortLog, ObTransLog));
OB_SERIALIZE_MEMBER(ObCheckpointLog, checkpoint_);

OB_SERIALIZE_MEMBER((ObTransStateLog, ObTransLog), create_ts_, tenant_id_, trans_expired_time_, scheduler_,
    trans_param_, is_readonly_, trans_type_, session_id_, proxy_session_id_, commit_task_count_, stmt_info_,
    app_trace_id_str_, schema_version_, prev_trans_arr_, can_elr_, proposal_leader_, cluster_version_,
    snapshot_version_, cur_query_start_time_, stmt_expired_time_, xid_);
OB_SERIALIZE_MEMBER((ObTransMutatorLog, ObTransLog), tenant_id_, trans_expired_time_, trans_param_, log_no_, mutator_,
    prev_trans_arr_, can_elr_, cluster_version_);
OB_SERIALIZE_MEMBER((ObTransMutatorAbortLog, ObTransLog));

int ObTransLog::init(
    const int64_t log_type, const ObPartitionKey& partition, const ObTransID& trans_id, const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (!ObTransLogType::is_valid(log_type) || !partition.is_valid() || !trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(log_type), K(partition), K(trans_id), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_type_ = log_type;
    partition_ = partition;
    trans_id_ = trans_id;
    cluster_id_ = cluster_id;
  }

  return ret;
}

bool ObTransLog::is_valid() const
{
  return ObTransLogType::is_valid(log_type_) && partition_.is_valid() && trans_id_.is_valid();
}

int ObTransLog::inner_replace_tenant_id(const uint64_t new_tenant_id)
{
  // The source and target tenant id may be different during physical backup and restore,
  // and they must be replaced as needed during recovery.
  int ret = OB_SUCCESS;
  ObPartitionKey new_pkey;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(partition_, new_tenant_id, new_pkey))) {
    TRANS_LOG(WARN, "replace_pkey_tenant_id failed", K(ret), K(new_tenant_id), K_(partition));
  } else {
    partition_ = new_pkey;
  }

  return ret;
}

int ObTransMutator::init(const bool use_mutator_buf)
{
  int ret = OB_SUCCESS;

  if (use_mutator_buf) {
    if (NULL != mutator_buf_) {
      TRANS_LOG(WARN, "alloc mutator buffer twice", KP_(mutator_buf));
      ret = OB_INIT_TWICE;
    } else if (NULL == (mutator_buf_ = MutatorBufFactory::alloc())) {
      TRANS_LOG(WARN, "alloc mutator buffer error");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      data_ = mutator_buf_->get_buf();
      capacity_ = mutator_buf_->get_size();
    }
  } else {
    // do nothing
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "ObTransMutator init error", KR(ret));
  } else {
    use_mutator_buf_ = use_mutator_buf;
  }

  return ret;
}

void ObTransMutator::reset()
{
  if (NULL != mutator_buf_) {
    MutatorBufFactory::release(mutator_buf_);
    mutator_buf_ = NULL;
  }
  // use MutatorBuf to store data by default
  use_mutator_buf_ = true;
  ObDataBuffer::reset();
}

int ObTransMutator::assign(const ObTransMutator& src)
{
  int ret = OB_SUCCESS;
  if (use_mutator_buf_) {
    if (OB_ISNULL(data_)) {
      TRANS_LOG(WARN, "ObTransMutator is not inited");
      ret = OB_NOT_INIT;
    } else if (src.position_ > capacity_) {
      TRANS_LOG(WARN, "size overflow", K(src.position_), K(capacity_));
      ret = OB_SIZE_OVERFLOW;
    } else if (src.position_ > 0) {
      MEMCPY(data_, src.data_, src.position_);
      position_ = src.position_;
    } else {
      position_ = 0;
    }
  } else {
    data_ = src.data_;
    position_ = src.position_;
  }
  return ret;
}

int ObTransMutator::assign(const char* data, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(data), K(size));
  } else if (use_mutator_buf_) {
    if (OB_ISNULL(data_)) {
      TRANS_LOG(WARN, "ObTransMutator is not inited");
      ret = OB_NOT_INIT;
    } else if (size > capacity_) {
      TRANS_LOG(WARN, "size overflow", K(size), K(capacity_));
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(data_, data, size);
      position_ = size;
    }
  } else {
    data_ = const_cast<char*>(data);
    position_ = size;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTransMutator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_) || position_ <= 0) {
    // If the data is invalid, only the data length field is serialized and is set to 0
    int64_t length = 0;
    LST_DO_CODE(OB_UNIS_ENCODE, length);
  } else {
    // If the data content is valid, then serialize the valid data
    LST_DO_CODE(OB_UNIS_ENCODE, position_);  // data length

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (pos + position_ > buf_len) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(buf + pos, data_, position_);
      pos += position_;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTransMutator)
{
  int ret = OB_SUCCESS;

  // deserialize data length
  LST_DO_CODE(OB_UNIS_DECODE, position_);

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "ObTransMutator deserialize position error", KR(ret), K_(position));
  } else if (pos + position_ > data_len) {
    TRANS_LOG(WARN, "ObTransMutator deserialize error", K(pos), K_(position), K(data_len));
    ret = OB_SERIALIZE_ERROR;
  } else {
    if (use_mutator_buf_) {
      if (OB_ISNULL(data_)) {
        TRANS_LOG(WARN, "ObTransMutator is not inited");
        ret = OB_NOT_INIT;
      } else if (position_ > 0) {
        // Copy the data if and only if the data length is valid
        MEMCPY(data_, buf + pos, position_);
        pos += position_;
      } else {
        // This situation indicates that the mutator buffer
        // of the redo log is empty and will not be processed
      }
    } else {
      // When MutatorBuf is not used to store data, the data is not copied,
      // and the data pointer directly points to the data address in the deserialized buffer
      data_ = const_cast<char*>(buf + pos);
      pos += position_;
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTransMutator)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, position_);

  // only when the data is valid, the serialized data length is not 0
  // Avoid the situation where position_ is less than 0
  if (position_ > 0) {
    len += position_;
  }

  return len;
}

int ObTransMutatorIterator::init(const char* ptr, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL != ptr_) {
    ret = OB_INIT_TWICE;
  } else {
    ptr_ = ptr;
    size_ = size;
  }
  return ret;
}

void ObTransMutatorIterator::reset()
{
  ptr_ = NULL;
  size_ = 0;
  pos_ = 0;
}

int ObTransMutatorIterator::get_next(ObPartitionKey& pkey, const char*& ptr, int64_t& size)
{
  int ret = OB_SUCCESS;
  if (NULL == ptr_) {
    ret = OB_NOT_INIT;
  } else if (size_ < pos_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected error", KR(ret), K_(size), K_(pos));
  } else if (size_ == pos_) {
    ret = OB_ITER_END;
  } else {
    int64_t tmp_pos = pos_;
    if (OB_FAIL(pkey.deserialize(ptr_, size_, tmp_pos))) {
      TRANS_LOG(WARN, "deserialize partition key failed", KR(ret));
    } else if (OB_FAIL(serialization::decode_i64(ptr_, size_, tmp_pos, &size))) {
      TRANS_LOG(WARN, "decode size failed", KR(ret));
    } else {
      ptr = ptr_ + tmp_pos;
      pos_ = tmp_pos + size;
    }
  }
  return ret;
}

int ObTransMutatorIterator::get_next(ObPartitionKey& pkey, ObTransMutator& mutator, int64_t& size)
{
  int ret = OB_SUCCESS;
  if (NULL == ptr_) {
    ret = OB_NOT_INIT;
  } else if (size_ < pos_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected error", KR(ret), K_(size), K_(pos));
  } else if (size_ == pos_) {
    ret = OB_ITER_END;
  } else {
    mutator.reset();
    int64_t tmp_pos = pos_;
    if (OB_FAIL(pkey.deserialize(ptr_, size_, tmp_pos))) {
      TRANS_LOG(WARN, "deserialize partition key failed", KR(ret));
    } else if (OB_FAIL(serialization::decode_i64(ptr_, size_, tmp_pos, &size))) {
      TRANS_LOG(WARN, "decode size failed", KR(ret));
    } else if (OB_FAIL(mutator.init())) {
      TRANS_LOG(WARN, "mutator init failed", KR(ret));
    } else if (OB_FAIL(mutator.assign(ptr_ + tmp_pos, size))) {
      TRANS_LOG(WARN, "mutator assign failed", KR(ret));
    } else {
      pos_ = tmp_pos + size;
    }
  }
  return ret;
}

int ObTransRedoLog::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mutator_.init())) {
    TRANS_LOG(WARN, "mutator init error", K(ret));
  }
  return ret;
}

bool ObTransRedoLog::is_valid() const
{
  return ObTransLog::is_valid() && log_no_ >= 0 && scheduler_.is_valid() && coordinator_.is_valid() &&
         participants_.count() > 0 && trans_param_.is_valid();
}

int ObTransRedoLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else if (new_tenant_id != tenant_id_ && OB_SYS_TENANT_ID != tenant_id_) {
    uint64_t old_id = tenant_id_;
    tenant_id_ = new_tenant_id;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      TRANS_LOG(INFO, "replace_tenant_id", K(new_tenant_id), K(old_id));
    }
    const ObPartitionKey old_coordinator = coordinator_;
    if (old_coordinator.is_valid() &&
        OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(old_coordinator, new_tenant_id, coordinator_))) {
      TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
    } else {
      const int64_t part_cnt = participants_.count();
      for (int64_t i = 0; i < part_cnt && OB_SUCC(ret); ++i) {
        const ObPartitionKey tmp_pkey = participants_.at(i);
        if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(tmp_pkey, new_tenant_id, participants_[i]))) {
          TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
        }
      }
    }
  } else {
  }

  return ret;
}

bool ObTransRedoLog::is_xa_trans() const
{
  return xid_.is_valid() && !xid_.empty();
}

// prepare log generated by observers before version 2.0 is replayed on observers of version 2.0
// checkpoint has not been deserialized, no need to check at this time
bool ObTransPrepareLog::is_valid() const
{
  return ObTransLog::is_valid() && scheduler_.is_valid() && coordinator_.is_valid() && participants_.count() > 0 &&
         trans_param_.is_valid();
}

int64_t ObTransPrepareLog::get_commit_version() const
{
  int64_t commit_version = OB_INVALID_VERSION;

  for (int64_t idx = 0; idx < partition_log_info_arr_.count(); ++idx) {
    const int64_t ts = partition_log_info_arr_.at(idx).get_log_timestamp();

    if (OB_INVALID_VERSION == commit_version) {
      commit_version = ts;
    } else {
      commit_version = std::max(commit_version, ts);
    }
  }

  return commit_version;
}

int ObTransPrepareLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "replace_tenant_id failed", K(ret));
  } else {
    const ObPartitionKey old_coordinator = coordinator_;
    if (old_coordinator.is_valid() &&
        OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(old_coordinator, new_tenant_id, coordinator_))) {
      TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
    } else {
      const int64_t part_cnt = participants_.count();
      for (int64_t i = 0; i < part_cnt && OB_SUCC(ret); ++i) {
        const ObPartitionKey tmp_pkey = participants_.at(i);
        if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(tmp_pkey, new_tenant_id, participants_[i]))) {
          TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t log_info_cnt = partition_log_info_arr_.count();
      for (int64_t i = 0; i < log_info_cnt && OB_SUCC(ret); ++i) {
        const ObPartitionKey tmp_pkey = partition_log_info_arr_.at(i).get_partition();
        ObPartitionKey tmp_new_pkey;
        if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(tmp_pkey, new_tenant_id, tmp_new_pkey))) {
          TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
        } else if (OB_FAIL(partition_log_info_arr_[i].set_partition(tmp_new_pkey))) {
          TRANS_LOG(ERROR, "set_partition failed", K(ret));
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCC(ret) && new_tenant_id != tenant_id_ && OB_SYS_TENANT_ID != tenant_id_) {
      uint64_t old_id = tenant_id_;
      tenant_id_ = new_tenant_id;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(INFO, "replace_tenant_id", K(new_tenant_id), K(old_id));
      }
    }
  }

  return ret;
}

bool ObTransCommitLog::is_valid() const
{
  return ObTransLog::is_valid() && global_trans_version_ > 0;
}

int ObTransCommitLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
    const int64_t log_info_cnt = partition_log_info_arr_.count();
    for (int64_t i = 0; i < log_info_cnt && OB_SUCC(ret); ++i) {
      const ObPartitionKey tmp_pkey = partition_log_info_arr_.at(i).get_partition();
      ObPartitionKey tmp_new_pkey;
      if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(tmp_pkey, new_tenant_id, tmp_new_pkey))) {
        TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
      } else if (OB_FAIL(partition_log_info_arr_[i].set_partition(tmp_new_pkey))) {
        TRANS_LOG(ERROR, "set_partition failed", K(ret));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObTransPreCommitLog::init(const int64_t log_type, const common::ObPartitionKey& partition,
    const ObTransID& trans_id, const uint64_t cluster_id, const int64_t publish_version)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !trans_id.is_valid() || OB_LOG_TRANS_PRE_COMMIT != log_type || publish_version <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id), K(log_type), K(publish_version), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransLog::init(log_type, partition, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "ObTransLog init error", KR(ret), K(partition), K(trans_id));
  } else {
    publish_version_ = publish_version;
  }

  return ret;
}

bool ObTransPreCommitLog::is_valid() const
{
  return ObTransLog::is_valid() && publish_version_ > 0;
}

int ObTransPreCommitLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
  }

  return ret;
}

int ObTransAbortLog::init(const int64_t log_type, const ObPartitionKey& partition, const ObTransID& trans_id,
    const PartitionLogInfoArray& partition_log_info_arr, const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !trans_id.is_valid() || OB_LOG_TRANS_ABORT != log_type) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id), K(log_type), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransLog::init(log_type, partition, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "ObTransLog init error", KR(ret), K(partition), K(trans_id));
  } else if (OB_FAIL(partition_log_info_arr_.assign(partition_log_info_arr))) {
    TRANS_LOG(WARN, "partition_log_info_arr assign error", KR(ret), K(partition), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

bool ObTransAbortLog::is_valid() const
{
  // do not check partition_log_info_arr_ for compatible
  return ObTransLog::is_valid();
}

int ObTransAbortLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
    const int64_t log_info_cnt = partition_log_info_arr_.count();
    for (int64_t i = 0; i < log_info_cnt && OB_SUCC(ret); ++i) {
      const ObPartitionKey tmp_pkey = partition_log_info_arr_.at(i).get_partition();
      ObPartitionKey tmp_new_pkey;
      if (OB_FAIL(ObPartitionKey::replace_pkey_tenant_id(tmp_pkey, new_tenant_id, tmp_new_pkey))) {
        TRANS_LOG(ERROR, "replace_pkey_tenant_id failed", K(ret));
      } else if (OB_FAIL(partition_log_info_arr_[i].set_partition(tmp_new_pkey))) {
        TRANS_LOG(ERROR, "set_partition failed", K(ret));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObTransClearLog::init(
    const int64_t log_type, const ObPartitionKey& partition, const ObTransID& trans_id, const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !trans_id.is_valid() || OB_LOG_TRANS_CLEAR != log_type) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id), K(log_type), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransLog::init(log_type, partition, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "ObTransLog init error", KR(ret), K(partition), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

bool ObTransClearLog::is_valid() const
{
  return ObTransLog::is_valid();
}

int ObTransClearLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
  }

  return ret;
}

int ObSpTransRedoLog::init(const int64_t log_type, const ObPartitionKey& partition, const ObTransID& trans_id,
    const uint64_t tenant_id, const int64_t log_no, const ObStartTransParam& trans_param, const uint64_t cluster_id,
    const ObElrTransInfoArray& prev_trans_arr, const bool can_elr)
{
  int ret = OB_SUCCESS;

  if ((OB_LOG_SP_TRANS_REDO != log_type && OB_LOG_SP_TRANS_COMMIT != log_type &&
          OB_LOG_SP_ELR_TRANS_COMMIT != log_type) ||
      !partition.is_valid() || !trans_id.is_valid() || log_no < 0 || !trans_param.is_valid()) {
    TRANS_LOG(
        WARN, "invalid argument", K(log_type), K(partition), K(trans_id), K(log_no), K(trans_param), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransLog::init(log_type, partition, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "ObTransLog init error", KR(ret), K(partition), K(trans_id), K(log_no));
  } else if (OB_FAIL(prev_trans_arr_.assign(prev_trans_arr))) {
    TRANS_LOG(WARN, "prev transaction arr assign error", KR(ret), K(prev_trans_arr));
  } else if (OB_FAIL(mutator_.init())) {
    TRANS_LOG(WARN, "mutator init error", KR(ret), K(partition), K(trans_id), K(log_no));
  } else {
    tenant_id_ = tenant_id;
    log_no_ = log_no;
    trans_param_ = trans_param;
    active_memstore_version_ = ObVersion(2);
    can_elr_ = can_elr;
  }

  return ret;
}

int ObSpTransRedoLog::set_log_type(const int64_t log_type)
{
  int ret = OB_SUCCESS;

  if (OB_LOG_SP_TRANS_REDO != log_type && OB_LOG_SP_TRANS_COMMIT != log_type) {
    TRANS_LOG(WARN, "invalid argument", K(log_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_type_ = log_type;
  }

  return ret;
}

int ObSpTransRedoLog::set_prev_trans_arr(const ObElrTransInfoArray& elr_arr)
{
  int ret = OB_SUCCESS;

  // It is possible that elr_arr is empty, so no need to check here
  if (OB_FAIL(prev_trans_arr_.assign(elr_arr))) {
    TRANS_LOG(WARN, "prev trans arr assign error", KR(ret), K(elr_arr));
  }

  return ret;
}

bool ObSpTransRedoLog::is_valid() const
{
  return ObTransLog::is_valid() && log_no_ >= 0 && trans_param_.is_valid();
}

int ObSpTransRedoLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else if (new_tenant_id != tenant_id_ && OB_SYS_TENANT_ID != tenant_id_) {
    uint64_t old_id = tenant_id_;
    tenant_id_ = new_tenant_id;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      TRANS_LOG(INFO, "replace_tenant_id", K(new_tenant_id), K(old_id));
    }
  } else {
  }

  return ret;
}

int ObSpTransCommitLog::init(const int64_t log_type, const ObPartitionKey& partition, const uint64_t tenant_id,
    const ObTransID& trans_id, const uint64_t checksum, const uint64_t cluster_id, const ObRedoLogIdArray& redo_log_ids,
    const ObStartTransParam& trans_param, const int64_t log_no, const ObString& app_trace_id_str,
    const int64_t checkpoint, const ObElrTransInfoArray& prev_trans_arr, const bool can_elr,
    const common::ObString& app_trace_info)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !trans_id.is_valid() ||
      (OB_LOG_SP_TRANS_COMMIT != log_type && OB_LOG_SP_ELR_TRANS_COMMIT != log_type) || redo_log_ids.count() < 0 ||
      !is_valid_tenant_id(tenant_id) || !trans_param.is_valid() || log_no < 0 || checkpoint < 0) {
    TRANS_LOG(WARN,
        "invalid argument",
        K(partition),
        K(trans_id),
        K(log_type),
        K(cluster_id),
        K(redo_log_ids),
        K(tenant_id),
        K(trans_param),
        K(log_no),
        K(checkpoint));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSpTransRedoLog::init(
                 log_type, partition, trans_id, tenant_id, log_no, trans_param, cluster_id, prev_trans_arr, can_elr))) {
    TRANS_LOG(WARN, "redo log init error", KR(ret), K(trans_id), K(log_no));
  } else if (OB_FAIL(prev_redo_log_ids_.assign(redo_log_ids))) {
    TRANS_LOG(WARN, "assign redo log ids error", KR(ret), K(redo_log_ids));
  } else {
    (void)app_trace_id_str_.assign_ptr(app_trace_id_str.ptr(), app_trace_id_str.length());
    (void)app_trace_info_.assign_ptr(app_trace_info.ptr(), app_trace_info.length());
    checksum_ = checksum;
    checkpoint_ = checkpoint;
  }

  return ret;
}

bool ObSpTransCommitLog::is_valid() const
{
  return ObTransLog::is_valid() && prev_redo_log_ids_.count() >= 0 && checkpoint_ >= 0;
}

int ObSpTransCommitLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
  }

  return ret;
}

int ObSpTransAbortLog::init(
    const int64_t log_type, const ObPartitionKey& partition, const ObTransID& trans_id, const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !trans_id.is_valid() || OB_LOG_SP_TRANS_ABORT != log_type) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id), K(log_type), K(cluster_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransLog::init(log_type, partition, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "ObTransLog init error", KR(ret), K(partition), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

bool ObSpTransAbortLog::is_valid() const
{
  return ObTransLog::is_valid();
}

int ObSpTransAbortLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
  }

  return ret;
}

int ObCheckpointLog::init(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  if (0 >= checkpoint) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(checkpoint));
  } else {
    checkpoint_ = checkpoint;
  }
  return ret;
}

bool ObCheckpointLog::is_valid() const
{
  return checkpoint_ > 0;
}

int ObTransStateLog::init(const int64_t log_type, const ObPartitionKey& pkey, const ObTransID& trans_id,
    const ObAddr& scheduler, const uint64_t cluster_id, const int64_t tenant_id, const int64_t trans_expired_time,
    const ObStartTransParam& trans_param, const bool is_readonly, const int trans_type, const int session_id,
    const int64_t proxy_session_id, const int64_t commit_task_count, const ObTransStmtInfo& stmt_info,
    const ObString app_trace_id_str, const int64_t schema_version, const ObElrTransInfoArray& prev_trans_arr,
    const bool can_elr, const ObAddr& proposal_leader, const uint64_t cluster_version, const int64_t snapshot_version,
    const int64_t cur_query_start_time, const int64_t stmt_expired_time, const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransLog::init(log_type, pkey, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "trans log init failed", KR(ret));
  } else if (OB_FAIL(prev_trans_arr_.assign(prev_trans_arr))) {
    TRANS_LOG(WARN, "prev trans arr assign error", KR(ret), K(prev_trans_arr));
  } else {
    create_ts_ = ObTimeUtility::current_time();
    tenant_id_ = tenant_id;
    trans_expired_time_ = trans_expired_time;
    scheduler_ = scheduler;
    trans_param_ = trans_param;
    is_readonly_ = is_readonly;
    trans_type_ = trans_type;
    session_id_ = session_id;
    proxy_session_id_ = proxy_session_id;
    commit_task_count_ = commit_task_count;
    stmt_info_ = stmt_info;
    schema_version_ = schema_version;
    can_elr_ = can_elr;
    proposal_leader_ = proposal_leader;
    cluster_version_ = cluster_version;
    snapshot_version_ = snapshot_version;
    cur_query_start_time_ = cur_query_start_time;
    stmt_expired_time_ = stmt_expired_time;
    app_trace_id_str_ = app_trace_id_str;
    xid_ = xid;
  }
  return ret;
}

int ObTransStateLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else if (new_tenant_id != tenant_id_ && OB_SYS_TENANT_ID != tenant_id_) {
    uint64_t old_id = tenant_id_;
    tenant_id_ = new_tenant_id;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      TRANS_LOG(INFO, "replace_tenant_id", K(new_tenant_id), K(old_id));
    }
  } else {
  }

  return ret;
}

int ObTransMutatorLog::init(const int64_t log_type, const ObPartitionKey& pkey, const ObTransID& trans_id,
    const uint64_t cluster_id, const int64_t tenant_id, const int64_t trans_expired_time,
    const ObStartTransParam& trans_param, const int64_t log_no, const ObElrTransInfoArray& prev_trans_arr,
    const bool can_elr, const uint64_t cluster_version, const bool use_mutator_buf)
{
  int ret = OB_SUCCESS;
  if (0 > log_no) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(log_no));
  } else if (OB_FAIL(ObTransLog::init(log_type, pkey, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "trans log init failed", KR(ret));
  } else if (OB_FAIL(prev_trans_arr_.assign(prev_trans_arr))) {
    TRANS_LOG(WARN, "prev trans arr assign error", KR(ret), K(prev_trans_arr));
  } else if (OB_FAIL(mutator_.init(use_mutator_buf))) {
    TRANS_LOG(WARN, "mutator init failed", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    trans_expired_time_ = trans_expired_time;
    trans_param_ = trans_param;
    log_no_ = log_no;
    can_elr_ = can_elr;
    cluster_version_ = cluster_version;
  }
  return ret;
}

int ObTransMutatorLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else if (new_tenant_id != tenant_id_ && OB_SYS_TENANT_ID != tenant_id_) {
    uint64_t old_id = tenant_id_;
    tenant_id_ = new_tenant_id;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      TRANS_LOG(INFO, "replace_tenant_id", K(new_tenant_id), K(old_id));
    }
  } else {
  }

  return ret;
}

int ObTransMutatorAbortLog::init(
    const int64_t log_type, const ObPartitionKey& pkey, const ObTransID& trans_id, const uint64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTransLog::init(log_type, pkey, trans_id, cluster_id))) {
    TRANS_LOG(WARN, "trans log init failed", KR(ret));
  }
  return ret;
}

int ObTransMutatorAbortLog::replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (!is_valid_tenant_id(new_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(new_tenant_id), K_(partition));
  } else if (new_tenant_id == partition_.get_tenant_id()) {
    // no need update tenant_id, skip
  } else if (OB_FAIL(ObTransLog::inner_replace_tenant_id(new_tenant_id))) {
    TRANS_LOG(WARN, "inner_replace_tenant_id failed", K(ret));
  } else {
  }

  return ret;
}

int ObTransLogParseUtils::parse_redo_prepare_log(
    const clog::ObLogEntry& log_entry, uint64_t& log_id, int64_t& log_timestamp, ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  const char* buf = log_entry.get_buf();
  const int64_t len = log_entry.get_header().get_data_len();
  int64_t log_type = storage::OB_LOG_UNKNOWN;
  int64_t pos = 0;
  int64_t inc_num = 0;

  if (NULL == buf || len <= 0) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(len), K(log_entry));
    ret = OB_INVALID_ARGUMENT;
  } else if (clog::OB_LOG_SUBMIT != log_entry.get_header().get_log_type()) {
    log_id = log_entry.get_header().get_log_id();
    log_timestamp = log_entry.get_header().get_submit_timestamp();
    trans_id.reset();
    ret = OB_SUCCESS;
    TRANS_LOG(INFO, "this is not OB_LOG_SUBMIT log", KR(ret), K(log_entry));
    // Parse log_type and determine whether it is trans log
  } else if (OB_FAIL(serialization::decode_i64(buf, len, pos, &log_type))) {
    TRANS_LOG(ERROR, "decode log type fail", KR(ret), K(buf), K(len), K(pos), K(log_entry));
  } else if (!storage::ObStorageLogTypeChecker::is_trans_log(log_type)) {
    log_id = log_entry.get_header().get_log_id();
    log_timestamp = log_entry.get_header().get_submit_timestamp();
    trans_id.reset();
    ret = OB_SUCCESS;
    TRANS_LOG(INFO, "this is not trans_log", KR(ret), K(log_type), K(log_entry));
    // Check whether the log type is redo/prepare log
  } else if (((log_type & storage::OB_LOG_TRANS_REDO) == 0) && ((log_type & storage::OB_LOG_TRANS_PREPARE) == 0)) {
    log_id = log_entry.get_header().get_log_id();
    log_timestamp = log_entry.get_header().get_submit_timestamp();
    trans_id.reset();
    ret = OB_SUCCESS;
    TRANS_LOG(INFO, "this is not redo or prepare log", KR(ret), K(log_type), K(log_entry));
    // parse inc_num
  } else if (OB_FAIL(serialization::decode_i64(buf, len, pos, &inc_num))) {
    TRANS_LOG(ERROR, "decode reserve field fail", KR(ret), K(buf), K(len), K(pos), K(log_type), K(log_entry));
  } else {
    log_id = log_entry.get_header().get_log_id();
    log_timestamp = log_entry.get_header().get_submit_timestamp();
    // If it contains redo log, only need to parse redo log
    if ((log_type & storage::OB_LOG_TRANS_REDO) != 0) {
      transaction::ObTransRedoLogHelper helper;
      transaction::ObTransRedoLog log(helper);
      const uint64_t real_tenant_id = log_entry.get_header().get_partition_key().get_tenant_id();
      // Initialize the mutator in the redo log, do not copy data when deserializing
      if (OB_FAIL(log.get_mutator().init(false))) {
        TRANS_LOG(WARN, "log init for deserialize failed", KR(ret), K(log_entry));
      } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
        TRANS_LOG(ERROR, "deserialize redo log fail", KR(ret), K(buf), K(len), K(pos), K(log), K(log_entry));
      } else {
        trans_id = log.get_trans_id();
      }
    } else {
      // there is no redo log, deserialize prepare log
      ObTransPrepareLogHelper helper;
      ObTransPrepareLog log(helper);
      if (OB_FAIL(log.deserialize(buf, len, pos))) {
        TRANS_LOG(ERROR, "deserialize redo log fail", KR(ret), K(buf), K(len), K(pos), K(log), K(log_entry));
      } else {
        trans_id = log.get_trans_id();
      }
    }
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
