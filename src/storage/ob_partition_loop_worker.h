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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_LOOP_WORKER
#define OCEANBASE_STORAGE_OB_PARTITION_LOOP_WORKER

#include "storage/ob_partition_memstore_info_record.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_replay_status.h"

namespace oceanbase {
namespace clog {
class ObIPartitionLogService;
class ObISubmitLogCb;
}  // namespace clog

namespace storage {
class ObPartitionGroup;
class ObPartitionLoopWorker {
public:
  ObPartitionLoopWorker()
  {
    reset();
  }
  virtual ~ObPartitionLoopWorker()
  {}
  int init(ObPartitionGroup* partition);
  void reset();
  int check_init(void* cp, const char* cp_name) const;
  int set_partition_key(const common::ObPartitionKey& pkey);

  int do_partition_loop_work();
  int generate_weak_read_timestamp(const int64_t max_stale_time, int64_t& timestamp);

  // for __all_virtual_partition_info
  int64_t get_cur_min_log_service_ts();
  int64_t get_cur_min_trans_service_ts();
  int64_t get_cur_min_replay_engine_ts();
  void set_migrating_flag(const bool flag)
  {
    is_migrating_flag_ = flag;
  }
  bool get_migrating_flag() const
  {
    return is_migrating_flag_;
  }

  int update_last_checkpoint(const int64_t checkpoint);
  void reset_last_checkpoint()
  {
    ATOMIC_STORE(&last_checkpoint_, 0);
  };
  int64_t get_last_checkpoint() const
  {
    return ATOMIC_LOAD(&last_checkpoint_);
  }
  int set_replay_checkpoint(const int64_t checkpoint);
  int get_replay_checkpoint(int64_t& checkpoint);
  void reset_memstore_info_record()
  {
    memstore_info_record_.reset();
  };
  void reset_curr_data_info();
  const ObMemstoreInfoRecord& get_memstore_info_record() const
  {
    return memstore_info_record_;
  }
  int get_checkpoint_info(int64_t& checkpoint_version, int64_t& safe_slave_read_ts);
  int write_checkpoint(const int64_t checkpoint, clog::ObISubmitLogCb* cb, uint64_t& log_id);
  void set_force_write_checkpoint(const bool force)
  {
    ATOMIC_SET(&force_write_checkpoint_, force);
  }

  TO_STRING_KV(K_(pkey), K_(replay_status), K_(is_migrating_flag), K_(last_checkpoint), K_(replay_pending_checkpoint),
      K_(last_max_trans_version), K_(next_gene_checkpoint_log_ts), K_(safe_slave_read_timestamp),
      K_(last_checkpoint_value));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionLoopWorker);
  static const int64_t COLD_PARTITION_CHECKPOINT_INTERVAL = 3600LL * 1000 * 1000;
  static const int64_t COLD_PARTITION_CHECKPOINT_PS_LIMIT = 1000;

  int gen_readable_info_with_sstable_(ObPartitionReadableInfo& readable_info);
  int gen_readable_info_with_memtable_(ObPartitionReadableInfo& readable_info);
  int gen_readable_info_(ObPartitionReadableInfo& readable_info);
  int gene_checkpoint_();
  int write_checkpoint_(const int64_t checkpoint);
  // replay checkpoint log, checkpoint active transaction
  int replay_checkpoint_();
  // update publish version when ts source type is lts
  int update_publish_version_();
  int check_dup_table_partition_();
  int get_readable_info_(ObPartitionReadableInfo& readable_info);

protected:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  clog::ObIPartitionLogService* pls_;
  ObReplayStatus* replay_status_;
  transaction::ObTransService* txs_;
  ObPartitionGroup* partition_;
  ObPartitionService* ps_;

  bool is_migrating_flag_;
  bool force_write_checkpoint_;

  int64_t last_checkpoint_;
  int64_t replay_pending_checkpoint_;
  int64_t last_max_trans_version_;
  int64_t next_gene_checkpoint_log_ts_;

  ObMemstoreInfoRecord memstore_info_record_;
  int64_t save_data_info_ts_;
  int64_t check_scheduler_status_ts_;
  int64_t check_dup_table_partition_ts_;
  int64_t safe_slave_read_timestamp_;
  int64_t last_checkpoint_value_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_LOOP_WORKER
