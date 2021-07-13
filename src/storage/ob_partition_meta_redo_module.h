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

#ifndef OB_PARTITION_META_REDO_MODULE_H_
#define OB_PARTITION_META_REDO_MODULE_H_

#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_pg_partition.h"
#include "ob_i_partition_group.h"
#include "ob_i_partition_component_factory.h"
#include "ob_pg_index.h"
#include "ob_pg_mgr.h"
#include "ob_partition_log.h"

namespace oceanbase {
namespace storage {
class ObIPartitionArrayGuard;
class PGKeyHashSet;

// used for unit test mock
#define VIRTUAL_FOR_UNITTEST virtual
class ObPartitionMetaRedoModule : public blocksstable::ObIRedoModule {
public:
  ObPartitionMetaRedoModule();
  virtual ~ObPartitionMetaRedoModule();

  int init(ObPartitionComponentFactory* cp_fty, share::schema::ObMultiVersionSchemaService* schema_service,
      ObBaseFileMgr* file_mgr);
  virtual int destroy();

public:
  // replay
  virtual int replay(const blocksstable::ObRedoModuleReplayParam& param) override;
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) override;
  virtual int enable_write_log() override;

  // read and write checkpoint
  VIRTUAL_FOR_UNITTEST int load_partition(
      const char* buf, const int64_t buf_len, int64_t& pos, blocksstable::ObStorageFileHandle& file_handle);
  VIRTUAL_FOR_UNITTEST int get_all_partitions(ObIPartitionArrayGuard& partitions);

  // others
  VIRTUAL_FOR_UNITTEST const ObPGMgr& get_pg_mgr() const
  {
    return pg_mgr_;
  }
  VIRTUAL_FOR_UNITTEST int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const;
  VIRTUAL_FOR_UNITTEST ObIPartitionGroupIterator* alloc_pg_iter();
  VIRTUAL_FOR_UNITTEST void revert_pg_iter(ObIPartitionGroupIterator* iter);
  VIRTUAL_FOR_UNITTEST void set_replay_old(const bool is_replay_old)
  {
    is_replay_old_ = is_replay_old;
  }
  VIRTUAL_FOR_UNITTEST bool is_replay_old() const
  {
    return is_replay_old_;
  }
  VIRTUAL_FOR_UNITTEST int get_partitions_by_file_key(
      const ObTenantFileKey& file_key, ObIPartitionArrayGuard& partitions);
  inline ObPGPartitionMap& get_partition_map()
  {
    return partition_map_;
  }

protected:
  virtual int inner_add_partition(
      ObIPartitionGroup& partition, const bool need_check_tenant, const bool is_replay, const bool allow_multi_value);
  virtual int inner_del_partition_for_replay(const common::ObPartitionKey& pkey, const int64_t file_id);
  virtual int inner_del_partition(const common::ObPartitionKey& pkey);
  int inner_del_partition_impl(const common::ObPartitionKey& pkey, const int64_t* file_id);
  virtual int init_partition_group(ObIPartitionGroup& pg, const common::ObPartitionKey& pkey);
  virtual int post_replay_remove_pg_partition(const ObChangePartitionLogEntry& log_entry)
  {
    UNUSED(log_entry);
    return OB_SUCCESS;
  }
  virtual int get_pg_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key) const;

  int replay_add_partition_slog(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_create_partition_group(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_remove_partition(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_replica_type(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_modify_table_store(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_drop_index_of_store(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_set_split_state(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_set_split_info(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_update_partition_group_meta(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_create_pg_partition_store(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_update_pg_partition_meta(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_add_sstable(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_remove_sstable(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_macro_meta(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_update_tenant_file_info(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_add_recovery_point_data(const blocksstable::ObRedoModuleReplayParam& param);
  int replay_remove_recovery_point_data(const blocksstable::ObRedoModuleReplayParam& param);

private:
  int get_create_pg_param(const ObCreatePartitionGroupLogEntry& log_entry, const bool write_pg_slog,
      blocksstable::ObStorageFileHandle* file_handle, ObBaseFileMgr* file_mgr, ObCreatePGParam& param);
  int get_partition(const common::ObPartitionKey& pkey, const int64_t* file_id, ObIPartitionGroupGuard& guard) const;
  int get_replay_partition(
      const common::ObPartitionKey& pkey, const int64_t file_id, ObIPartitionGroupGuard& guard) const;
  int handle_pg_conflict(const char* buf, const int64_t buf_len, int64_t& pos,
      blocksstable::ObStorageFileHandle& file_handle, ObIPartitionGroup*& ptt);

protected:
  ObIPartitionComponentFactory* cp_fty_;
  transaction::ObTransService* txs_;
  replayengine::ObILogReplayEngine* rp_eg_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObBaseFileMgr* file_mgr_;
  ObPartitionGroupIndex pg_index_;
  ObPGMgr pg_mgr_;
  common::ObConcurrentFIFOAllocator iter_allocator_;
  lib::ObMutex structure_change_mutex_;
  // Maintain the mapping relation between pkey and ObPGPartition
  ObPGPartitionMap partition_map_;
  bool is_replay_old_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMetaRedoModule);
};

class ObIPartitionArrayGuard {
public:
  ObIPartitionArrayGuard() : pg_mgr_(nullptr), partitions_()
  {}
  virtual ~ObIPartitionArrayGuard()
  {
    reuse();
  }
  void set_pg_mgr(const ObPGMgr& pg_mgr)
  {
    pg_mgr_ = &pg_mgr;
  }
  int push_back(ObIPartitionGroup* partition)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(pg_mgr_)) {
      ret = common::OB_NOT_INIT;
    } else if (OB_SUCC(partitions_.push_back(partition))) {
      partition->inc_ref();
    }
    return ret;
  }
  ObIPartitionGroup* at(int64_t i)
  {
    return partitions_.at(i);
  }
  int64_t count() const
  {
    return partitions_.count();
  }
  void reuse()
  {
    if (partitions_.count() > 0 && nullptr != pg_mgr_) {
      for (int64_t i = 0; i < partitions_.count(); ++i) {
        pg_mgr_->revert_pg(partitions_.at(i));
      }
      partitions_.reset();
    }
  }
  int reserve(const int64_t count)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(partitions_.reserve(count))) {
      STORAGE_LOG(WARN, "failed to reserve partitions", K(ret), K(count));
    }
    return ret;
  }

private:
  const ObPGMgr* pg_mgr_;
  common::ObSEArray<ObIPartitionGroup*, OB_DEFAULT_PARTITION_KEY_COUNT> partitions_;
  DISALLOW_COPY_AND_ASSIGN(ObIPartitionArrayGuard);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_PARTITION_META_REDO_MODULE_H_ */
