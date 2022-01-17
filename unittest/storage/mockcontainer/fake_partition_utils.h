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

#ifndef OCEANBASE_FAKE_PARTITION_UTILS_H_
#define OCEANBASE_FAKE_PARTITION_UTILS_H_

#include "mock_ob_partition_service.h"
#include "mock_ob_partition.h"
#include "mock_ob_partition_storage.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_component_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

class FakePartition;
class FakePartitionStorage : public storage::MockObIPartitionStorage {
public:
  FakePartitionStorage(FakePartition& holder) : holder_(holder)
  {}
  virtual ~FakePartitionStorage()
  {}
  virtual int get_macro_block_list(blocksstable::ObSelfBufferWriter& list);
  virtual int halt_prewarm() override
  {
    return OB_SUCCESS;
  }
  virtual int purge_retire_stores(uint64_t& last_replay_log_id)
  {
    UNUSED(last_replay_log_id);
    return OB_SUCCESS;
  }
  virtual int get_all_version_stores(ObTablesHandle& stores_handle)
  {
    UNUSED(stores_handle);
    return OB_SUCCESS;
  }
  virtual int query_range_to_macros(ObIAllocator& allocator, const ObIArray<ObStoreRange>& ranges, const int64_t type,
      uint64_t* macros_count, const int64_t* total_task_count, ObIArray<ObStoreRange>* splitted_ranges,
      ObIArray<int64_t>* split_index)
  {
    UNUSED(allocator);
    UNUSED(ranges);
    UNUSED(type);
    UNUSED(macros_count);
    UNUSED(total_task_count);
    UNUSED(splitted_ranges);
    UNUSED(split_index);
    return common::OB_SUCCESS;
  }
  int get_saved_storage_info(ObSavedStorageInfo& info, common::ObVersion& version)
  {
    UNUSED(info);
    UNUSED(version);
    return common::OB_SUCCESS;
  }
  int retire_warmup_store(const bool is_disk_full)
  {
    UNUSED(is_disk_full);
    return common::OB_SUCCESS;
  }
  int halt_prewarm_store()
  {
    return common::OB_SUCCESS;
  }
  int append_sstable(const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter)
  {
    UNUSED(param);
    UNUSED(iter);
    return common::OB_SUCCESS;
  }

private:
  FakePartition& holder_;
};

class FakePartition : public storage::MockObIPartition, public blocksstable::ObIBaseStorageLogEntry {
public:
  FakePartition() : pmeta_(), smeta_(), arena_(ObModIds::OB_PARTITION_SERVICE), storage_(*this)
  {}
  virtual ~FakePartition()
  {}
  int set(const blocksstable::ObPartitionMeta& meta);
  int add_macro_block(MacroBlockId macro_id);
  virtual bool is_valid() const
  {
    return pmeta_.is_valid() && smeta_.is_valid();
  }
  // write ssstore objects @version tree to data file , used by write_check_point
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int64_t get_serialize_size() const;
  virtual ObIPartitionStorage* get_storage()
  {
    return &storage_;
  }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_KV(K_(pmeta), K_(smeta));
    return pos;
  }

public:
  blocksstable::ObPartitionMeta pmeta_;
  blocksstable::ObSSTableMeta smeta_;
  ObArenaAllocator arena_;
  FakePartitionStorage storage_;
};

class FakePartitionService : public MockObIPartitionService {
public:
  FakePartitionService() : partition_list_(), cp_fty_(NULL), arena_(ObModIds::OB_PARTITION_SERVICE)
  {
    init();
  }
  virtual ~FakePartitionService()
  {}
  ObPartitionComponentFactory* get_component_factory() const
  {
    return cp_fty_;
  }
  int init();
  int destroy();
  int add_partition(storage::ObIPartition* partition);
  FakePartition* create_partition(const blocksstable::ObPartitionMeta& meta);
  virtual int get_all_partitions(ObIPartitionArrayGuard& partitions);
  virtual int revert_partition(storage::ObIPartitionGroup* partition);
  virtual int load_partition(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos);

private:
  ObSEArray<ObIPartitionGroup*, 4096> partition_list_;
  ObPartitionComponentFactory* cp_fty_;
  ObArenaAllocator arena_;
};

}  // namespace storage
}  // namespace oceanbase

#endif
