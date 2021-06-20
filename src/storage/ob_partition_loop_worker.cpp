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

#include "storage/ob_partition_loop_worker.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "clog/ob_i_submit_log_cb.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_log.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/ob_partition_checkpoint.h"
#include "storage/ob_partition_group.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace clog;

namespace storage {
int ObPartitionLoopWorker::init(ObPartitionGroup* partition)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition) || OB_ISNULL(pls_ = partition->get_log_service()) ||
      OB_ISNULL(replay_status_ = partition->get_replay_status()) || OB_ISNULL(txs_ = partition->get_trans_service()) ||
      OB_ISNULL(ps_ = partition->get_partition_service()) || !partition->get_partition_key().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP_(pls), KP_(replay_status), KP_(txs));
  } else {
    pkey_ = partition->get_partition_key();
    partition_ = partition;
    is_inited_ = true;
    STORAGE_LOG(INFO, "partition loop worker init success", K_(pkey));
  }

  return ret;
}

void ObPartitionLoopWorker::reset()
{
  is_inited_ = false;
  pkey_.reset();
  pls_ = NULL;
  replay_status_ = NULL;
  txs_ = NULL;
  partition_ = NULL;
  ps_ = NULL;
  is_migrating_flag_ = false;
  force_write_checkpoint_ = false;
  last_checkpoint_ = 0;
  replay_pending_checkpoint_ = 0;
  last_max_trans_version_ = 0;
  next_gene_checkpoint_log_ts_ = 0;
  memstore_info_record_.reset();
  save_data_info_ts_ = 0;
  check_scheduler_status_ts_ = 0;
  check_dup_table_partition_ts_ = 0;
  safe_slave_read_timestamp_ = 0;
  last_checkpoint_value_ = 0;
}

int ObPartitionLoopWorker::check_init(void* cp, const char* cp_name) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == cp || NULL == cp_name) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "component does not exist", "component name", cp_name);
  }
  return ret;
}

int ObPartitionLoopWorker::set_partition_key(const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionLoopWorker not init", K(ret), K(pkey));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
  }

  return ret;
}

int ObPartitionLoopWorker::gen_readable_info_with_sstable_(ObPartitionReadableInfo& readable_info)
{
  int ret = OB_SUCCESS;

  ObPartitionGroupMeta pg_meta;
  if (OB_FAIL(partition_->get_pg_storage().get_pg_meta(pg_meta))) {
    STORAGE_LOG(WARN, "fail to get pg meta", K(ret), K(pkey_));
  } else {
    const int64_t sstable_snapshot_version = pg_meta.storage_info_.get_data_info().get_publish_version();
    readable_info.min_log_service_ts_ = sstable_snapshot_version + 1;
    readable_info.min_replay_engine_ts_ = INT64_MAX;
    readable_info.min_trans_service_ts_ = INT64_MAX;
    readable_info.calc_readable_ts();
  }

  return ret;
}

int ObPartitionLoopWorker::gen_readable_info_with_memtable_(ObPartitionReadableInfo& readable_info)
{
  int ret = OB_SUCCESS;
  uint64_t next_replay_log_id = OB_INVALID_ID;

  // the order of pls, replay_engine, trx should not be changed
  if (OB_ISNULL(pls_) || OB_ISNULL(txs_) || OB_ISNULL(replay_status_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "not inited", K(ret), K(pkey_), KP_(pls), KP_(txs), KP_(replay_status));
  } else if (OB_FAIL(pls_->get_next_replay_log_info(next_replay_log_id, readable_info.min_log_service_ts_))) {
    if (OB_STATE_NOT_MATCH == ret) {
      // print one log per minute
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        STORAGE_LOG(WARN, "get_next_replay_log_info error", K(ret), K_(pkey));
      }
    } else {
      STORAGE_LOG(WARN, "get_next_replay_log_info error", K(ret), K_(pkey));
    }
  } else {
    readable_info.min_replay_engine_ts_ = replay_status_->get_min_unreplay_log_timestamp();
    if (OB_FAIL(txs_->get_min_uncommit_prepare_version(pkey_, readable_info.min_trans_service_ts_))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          STORAGE_LOG(WARN, "get_min_uncommit_prepare_version error", K(ret), K_(pkey), KP_(txs));
        }
      } else {
        STORAGE_LOG(WARN, "get_min_uncommit_prepare_version error", K(ret), K_(pkey), KP_(txs));
      }
    } else {
      readable_info.calc_readable_ts();
    }
  }

  return ret;
}

int ObPartitionLoopWorker::gen_readable_info_(ObPartitionReadableInfo& readable_info)
{
  int ret = OB_SUCCESS;

  // is_inited_ has been checked outside
  const ObPartitionState partition_state = partition_->get_partition_state();
  const ObReplicaProperty& property = partition_->get_replica_property();

  if (0 == property.get_memstore_percent()) {
    if (F_WORKING == partition_state) {
      if (partition_->get_pg_storage().is_empty_pg()) {
        if (OB_FAIL(gen_readable_info_with_memtable_(readable_info))) {
          STORAGE_LOG(WARN, "fail to gen readable info with memtable", K(ret), K(pkey_));
        } else if (!readable_info.is_valid()) {
          ret = OB_STATE_NOT_MATCH;
        }
      } else if (OB_FAIL(gen_readable_info_with_sstable_(readable_info))) {
        STORAGE_LOG(WARN, "fail to gen readable info with sstable", K(ret), K(pkey_));
      }
    } else if ((L_TAKEOVER == partition_state || L_WORKING == partition_state) &&
               pls_->has_valid_next_replay_log_ts()) {
      if (OB_FAIL(gen_readable_info_with_memtable_(readable_info))) {
        STORAGE_LOG(WARN, "fail to gen readable info with memtable", K(ret), K(pkey_));
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
    }
    readable_info.force_ = true;
  } else if (OB_FAIL(gen_readable_info_with_memtable_(readable_info))) {
    STORAGE_LOG(WARN, "fail to gen readable info with memtable", K(ret), K(pkey_));
  }

  return ret;
}

int ObPartitionLoopWorker::generate_weak_read_timestamp(const int64_t max_stale_time, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  ObPartitionReadableInfo readable_info;

  DEBUG_SYNC(BLOCK_WEAK_READ_TIMESTAMP);
  DEBUG_SYNC(SYNC_PG_AND_REPLAY_ENGINE_DEADLOCK);

  bool is_restore = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (FALSE_IT(is_restore = (partition_->get_pg_storage().is_restore()))) {
  } else if (OB_FAIL(gen_readable_info_(readable_info))) {
    // no need to caculate timestamp when partition is rebuilding
    if (OB_STATE_NOT_MATCH != ret && OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to gen readble info", K(ret), K(pkey_));
    }
  } else if (!readable_info.is_valid()) {
    if (is_restore) {
      // ignore pg in restore
      ret = OB_STATE_NOT_MATCH;
      if (REACH_TIME_INTERVAL(2 * 1000 * 1000L)) {
        STORAGE_LOG(WARN, "partition is in restore, just ignore", K(ret), K_(pkey), K(readable_info));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid timestamp, unexpected error", K(ret), K_(pkey), K(readable_info));
    }
  } else if (OB_FAIL(partition_->get_pg_storage().update_readable_info(readable_info))) {
    STORAGE_LOG(WARN, "failed to update readable info", K(ret), K(readable_info), K(pkey_));
  } else {
    timestamp = readable_info.max_readable_ts_;
  }

  const int64_t delta_ts = ObTimeUtility::current_time() - readable_info.max_readable_ts_;
  // no need to caculate timestamp when partition is migrating
  if (delta_ts >= max_stale_time && !is_migrating_flag_ && !partition_->is_replica_using_remote_memstore()) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      // do not set ret, just print log
      STORAGE_LOG(WARN, "slave read timestamp too old", K(pkey_), K(delta_ts), K(readable_info));
    }
  }
  return ret;
}

int ObPartitionLoopWorker::write_checkpoint_(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  ObCheckPoingLogCb* cb = NULL;
  uint64_t log_id = OB_INVALID_ID;
  if (NULL == (cb = ObCheckPoingLogCbFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "alloc memory failed", K(ret));
  } else {
    if (OB_FAIL(cb->init(ps_, checkpoint))) {
      STORAGE_LOG(WARN, "checkpoint log callback init failed", K(ret), K(checkpoint));
    } else if (OB_FAIL(write_checkpoint(checkpoint, cb, log_id))) {
      STORAGE_LOG(WARN, "submit checkpoint log failed", K(ret), K(log));
    } else {
      // do nothing
    }

    if (OB_SUCCESS != ret) {
      ObCheckPoingLogCbFactory::release(cb);
      cb = NULL;
    }
  }
  return ret;
}

int ObPartitionLoopWorker::update_last_checkpoint(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;

  if (0 == ATOMIC_LOAD(&last_checkpoint_) && 0 < checkpoint) {
    STORAGE_LOG(INFO, "last checkpoint updated", K_(pkey), K(checkpoint));
  }
  (void)inc_update(&last_checkpoint_, checkpoint);

  return ret;
}

int ObPartitionLoopWorker::get_checkpoint_info(int64_t& checkpoint_version, int64_t& safe_slave_read_ts)
{
  safe_slave_read_ts = ATOMIC_LOAD(&safe_slave_read_timestamp_);
  checkpoint_version = last_checkpoint_;
  return OB_SUCCESS;
}

int ObPartitionLoopWorker::write_checkpoint(const int64_t checkpoint, ObISubmitLogCb* cb, uint64_t& log_id)

{
  int ret = OB_SUCCESS;
  ObCheckpointLog log;
  char buf[1024];
  int64_t pos = 0;
  int64_t log_timestamp = 0;
  const int64_t base_timestamp = 0;
  const bool is_trans_log = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (OB_ISNULL(pls_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition member is NULL", K(ret), K(pkey_));
  } else if (0 >= checkpoint) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(checkpoint));
  } else if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (OB_FAIL(log.init(checkpoint))) {
    STORAGE_LOG(WARN, "checkpoint log init failed", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, sizeof(buf), pos, OB_LOG_TRANS_CHECKPOINT))) {
    STORAGE_LOG(WARN, "serialize log type failed", K(ret));
  } else if (OB_FAIL(log.serialize(buf, sizeof(buf), pos))) {
    STORAGE_LOG(WARN, "serialize checkpoint log failed", K(ret));
  } else if (OB_FAIL(pls_->submit_log(buf, pos, base_timestamp, cb, is_trans_log, log_id, log_timestamp))) {
    STORAGE_LOG(WARN, "submit checkpoint log failed", K(ret), K(log));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLoopWorker::gene_checkpoint_()
{
  int ret = OB_SUCCESS;
  int64_t cur_checkpoint = OB_INVALID_TIMESTAMP;
  common::ObRole role = INVALID_ROLE;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K_(pkey));
  } else if (OB_FAIL(partition_->get_pg_storage().get_weak_read_timestamp(cur_checkpoint))) {
    STORAGE_LOG(WARN, "fail to get slave read ts", K(ret), K_(pkey));
  } else if (OB_FAIL(partition_->get_log_service()->get_role(role))) {
    STORAGE_LOG(WARN, "fail to get_role", K(ret), K_(pkey));
  } else {
    const bool is_leader = is_strong_leader(role);
    (void)inc_update(&safe_slave_read_timestamp_, cur_checkpoint);
    const int64_t now = ObTimeUtility::current_time();
    const ObPartitionState state = partition_->get_partition_state();
    const int64_t last_checkpoint = ATOMIC_LOAD(&last_checkpoint_);
    const int64_t last_max_trans_version = ATOMIC_LOAD(&last_max_trans_version_);
    int64_t max_trans_version = 0;
    if (is_leader && is_leader_state(state) && cur_checkpoint > last_checkpoint) {
      // Only partition leader can write checkponit log and update last_checkpoint_
      if (OB_FAIL(txs_->get_max_trans_version(pkey_, max_trans_version))) {
        STORAGE_LOG(WARN, "get max trans version failed", K(ret), K_(pkey));
      } else if (last_max_trans_version < max_trans_version) {
        ATOMIC_STORE(&last_max_trans_version_, max_trans_version);
      } else if (last_max_trans_version == max_trans_version) {
        if (last_checkpoint <= max_trans_version ||
            ((cur_checkpoint - last_checkpoint_value_) > COLD_PARTITION_CHECKPOINT_INTERVAL &&
                REACH_COUNT_PER_SEC(COLD_PARTITION_CHECKPOINT_PS_LIMIT))) {
          if (OB_FAIL(write_checkpoint_(cur_checkpoint))) {
            STORAGE_LOG(WARN, "write checkpoint failed", K(ret), K_(pkey), K(cur_checkpoint));
          } else {
            last_checkpoint_value_ = cur_checkpoint;
            if (EXECUTE_COUNT_PER_SEC(16)) {
              STORAGE_LOG(INFO, "write checkpoint success", K_(pkey), K(cur_checkpoint));
            }
          }
        } else {
          update_last_checkpoint(cur_checkpoint);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected error", K(ret), K(last_max_trans_version), K(max_trans_version));
        ATOMIC_STORE(&last_max_trans_version_, max_trans_version);
      }
    }

    if (is_leader && is_leader_working(state) && ATOMIC_LOAD(&force_write_checkpoint_) &&
        cur_checkpoint > last_checkpoint_value_) {
      if (OB_FAIL(write_checkpoint_(cur_checkpoint))) {
        STORAGE_LOG(WARN, "write checkpoint failed", K(ret), K_(pkey), K(cur_checkpoint));
      } else {
        last_checkpoint_value_ = cur_checkpoint;
        set_force_write_checkpoint(false);
        if (EXECUTE_COUNT_PER_SEC(16)) {
          STORAGE_LOG(INFO, "write checkpoint success", K_(pkey), K(cur_checkpoint));
        }
      }
    }

    if (now >= next_gene_checkpoint_log_ts_) {
      STORAGE_LOG(TRACE,
          "gene checkpoint",
          K_(pkey),
          K(state),
          K_(last_checkpoint),
          K(cur_checkpoint),
          K(last_max_trans_version),
          K(max_trans_version));
      next_gene_checkpoint_log_ts_ = now + ObRandom::rand(100000000, 400000000);
    }
  }

  return ret;
}

int ObPartitionLoopWorker::replay_checkpoint_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K_(pkey));
  } else if (NULL == txs_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "txs is NULL", K(ret));
  } else {
    ObPartitionState state = partition_->get_partition_state();
    if (is_working_state(state)) {
      const int64_t checkpoint_version = ATOMIC_LOAD(&last_checkpoint_);
      // call checkpoint method when checkpoint version changed
      if (0 < checkpoint_version && OB_FAIL(txs_->checkpoint(pkey_, checkpoint_version, this))) {
        STORAGE_LOG(WARN, "checkpoint failed", K(ret), K_(pkey), K(checkpoint_version));
      }
    }
  }
  return ret;
}

int ObPartitionLoopWorker::update_publish_version_()
{
  int ret = OB_SUCCESS;
  int64_t gts = 0;
  const uint64_t tenant_id = pkey_.get_tenant_id();
  if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, NULL, gts))) {
    STORAGE_LOG(WARN, "get gts failed", K(ret), K(tenant_id), K_(pkey));
  } else if (OB_FAIL(OB_TS_MGR.update_publish_version(tenant_id, gts, false))) {
    STORAGE_LOG(WARN, "update publish version failed", K(ret), K(tenant_id), K_(pkey), K(gts));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionLoopWorker::do_partition_loop_work()
{
  int64_t now = ObTimeUtility::current_time();
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2000) {
    // write checkpoint log every 50ms
    (void)gene_checkpoint_();
  }

  (void)update_publish_version_();

  // gc trans_result_info for elr every 15s
  if (now - save_data_info_ts_ > 15000000) {
    (void)txs_->gc_trans_result_info(pkey_, safe_slave_read_timestamp_);
  }

  // gc participant context every 15s
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100 && now - check_scheduler_status_ts_ > 15000000) {
    (void)txs_->check_scheduler_status(pkey_);
    check_scheduler_status_ts_ = ObTimeUtility::current_time();
  }

  // check duplicate partition state every 15s
  if (now - check_dup_table_partition_ts_ > 15000000 && !pkey_.is_pg() &&
      !partition_->is_replica_using_remote_memstore()) {
    if (OB_EAGAIN != check_dup_table_partition_()) {
      check_dup_table_partition_ts_ = ObTimeUtility::current_time();
    }
  }

  return OB_SUCCESS;
}

int ObPartitionLoopWorker::check_dup_table_partition_()
{
  int ret = OB_SUCCESS;
  DupReplicaType dup_replica_type = DupReplicaType::INVALID_TYPE;
  const common::ObAddr& self = ps_->get_self_addr();
  if (OB_FAIL(ps_->get_dup_replica_type(pkey_, self, dup_replica_type))) {
    if (OB_NOT_INIT != ret && OB_INVALID_ARGUMENT != ret && OB_ERR_UNEXPECTED != ret) {
      ret = OB_EAGAIN;
    } else {
      TRANS_LOG(WARN, "get dup replica type failed", K(ret), K_(pkey), K(self));
    }
  } else if (OB_FAIL(txs_->update_dup_table_partition_info(pkey_, dup_replica_type == DupReplicaType::DUP_REPLICA))) {
    TRANS_LOG(WARN, "update dup replica type failed", K(ret), K_(pkey), K(self));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLoopWorker::get_readable_info_(ObPartitionReadableInfo& readable_info)
{
  int ret = OB_SUCCESS;
  ObPartitionArray pkeys;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition loop worker is not inited", K(ret));
  } else if (OB_FAIL(partition_->get_all_pg_partition_keys(pkeys))) {
    STORAGE_LOG(WARN, "get all pg partition keys error", K(ret), K(pkeys), K(*this));
  } else if (pkeys.count() <= 0) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "empty pg, no need to calculate slave read timestamp", K(*this), K(pkeys));
    }
  } else {
    readable_info = partition_->get_pg_storage().get_readable_info();
  }

  return ret;
}

int64_t ObPartitionLoopWorker::get_cur_min_log_service_ts()
{
  int tmp_ret = OB_SUCCESS;
  int64_t int_ret = OB_SUCCESS;

  ObPartitionReadableInfo readable_info;
  if (OB_SUCCESS != (tmp_ret = get_readable_info_(readable_info))) {
    STORAGE_LOG(WARN, "get readable info error", K(tmp_ret), K_(pkey));
  } else {
    int_ret = readable_info.min_log_service_ts_;
  }

  return int_ret;
}

int64_t ObPartitionLoopWorker::get_cur_min_trans_service_ts()
{
  int tmp_ret = OB_SUCCESS;
  int64_t int_ret = OB_SUCCESS;

  ObPartitionReadableInfo readable_info;
  if (OB_SUCCESS != (tmp_ret = get_readable_info_(readable_info))) {
    STORAGE_LOG(WARN, "get readable info error", K(tmp_ret), K_(pkey));
  } else {
    int_ret = readable_info.min_trans_service_ts_;
  }

  return int_ret;
}

int64_t ObPartitionLoopWorker::get_cur_min_replay_engine_ts()
{
  int tmp_ret = OB_SUCCESS;
  int64_t int_ret = OB_SUCCESS;

  ObPartitionReadableInfo readable_info;
  if (OB_SUCCESS != (tmp_ret = get_readable_info_(readable_info))) {
    STORAGE_LOG(WARN, "get readable info error", K(tmp_ret), K_(pkey));
  } else {
    int_ret = readable_info.min_replay_engine_ts_;
  }

  return int_ret;
}

int ObPartitionLoopWorker::set_replay_checkpoint(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not inited, replay status is NULL", K(ret), K(this));
  } else if (0 > checkpoint || INT64_MAX == checkpoint) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(checkpoint));
  } else if (!replay_status_->has_pending_abort_task(pkey_)) {
    const int64_t last_checkpoint = ATOMIC_LOAD(&last_checkpoint_);
    const int64_t NOTICE_THRESHOLD = 3 * 1000 * 1000;
    if (last_checkpoint > 0 && checkpoint - last_checkpoint > NOTICE_THRESHOLD) {
      STORAGE_LOG(INFO, "replay checkpoint updated", K_(pkey), K(checkpoint), K(*this));
    }
    inc_update(&last_checkpoint_, checkpoint);
  } else {
    inc_update(&replay_pending_checkpoint_, checkpoint);
  }
  return ret;
}

int ObPartitionLoopWorker::get_replay_checkpoint(int64_t& checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not inited, replay status is NULL", K(ret));
  } else {
    const int64_t pending_checkpoint = ATOMIC_LOAD(&replay_pending_checkpoint_);
    if (!replay_status_->has_pending_abort_task(pkey_)) {
      const int64_t last_checkpoint = ATOMIC_LOAD(&last_checkpoint_);
      const int64_t NOTICE_THRESHOLD = 1 * 1000 * 1000;
      if (last_checkpoint > 0 && pending_checkpoint - last_checkpoint > NOTICE_THRESHOLD) {
        STORAGE_LOG(INFO, "replay checkpoint updated", K_(pkey), K(last_checkpoint), K(pending_checkpoint));
      }
      inc_update(&last_checkpoint_, pending_checkpoint);
    }
    checkpoint = ATOMIC_LOAD(&last_checkpoint_);
  }
  return ret;
}

void ObPartitionLoopWorker::reset_curr_data_info()
{
  memstore_info_record_.reset();
  save_data_info_ts_ = 0;
}

}  // namespace storage
}  // namespace oceanbase
