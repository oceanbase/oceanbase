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

#ifndef OCEANBASE_OBSERVER_FAKE_PARTITION_MGR_H_
#define OCEANBASE_OBSERVER_FAKE_PARTITION_MGR_H_

#include "lib/container/ob_array.h"
#include "lib/thread/ob_async_task_queue.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_i_partition_mgr.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_major_freeze_status.h"
#define private public
#include "storage/ob_replay_status.h"

namespace oceanbase {
namespace storage {

class ObFakePartitionStorage : public ObIPartitionStorage {
public:
  ObFakePartitionStorage();
  virtual ~ObFakePartitionStorage();

  virtual int init(const common::ObPartitionKey& pkey, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, ObIPartitionReport* report,
      blocksstable::ObStorageCacheSuite& cache_suite);

  virtual void destroy();

  //
  // scan table partition
  //
  // @param ctx [in] transaction context
  // @param param [in] query param
  // @param result [out] iterator to get the result set
  //
  // @return result iterator
  //
  virtual int table_scan(const ObStoreCtx& ctx, ObTableScanParam& param, common::ObNewRowIterator*& result);
  //
  // release scan iterator
  //
  // @param iter [in] iterator to be reverted
  //
  virtual int revert_scan_iter(common::ObNewRowIterator* iter);
  //
  // delete rows
  //     delete table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param index_included [in] need to delete index too
  // @param column_ids [in] all column referenced.rowkey first
  // @param row_iter [in] primary keys to be deleted
  // @param affected_rows [out]
  //
  // @retval OB_TRANSACTION_SET_VIOLATION
  // @retval OB_SNAPSHOT_DISCARDED
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  //
  virtual int delete_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  //
  // insert rows
  //     insert table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param column_ids [in] insert columns
  // @param row_iter [in] insert values
  // @param affected_rows [out]
  //
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  // @retval OB_ERR_PRIMARY_KEY_DUPLICATE
  //
  virtual int insert_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  //
  // update rows
  //     update table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param index_included [in] if index need to be updated
  // @param column_ids [in] all columns related
  // @param column_ids [in] updated columns
  // @param row_iter [in] odd rows are old and even rows are new ones
  // @param affected_rows [out]
  //
  // @retval OB_TRANSACTION_SET_VIOLATION
  // @retval OB_SNAPSHOT_DISCARDED
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  // @retval OB_ERR_PRIMARY_KEY_DUPLICATE
  //
  virtual int update_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  //
  // lock rows
  //     lock table rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table id
  // @param row_iter [in] rowkey iterator
  // @param lock_flag [in] lock flags: LF_WRITE or LF_NONE
  // @param affected_rows [out]
  //
  // @retval OB_TRANSACTION_SET_VIOLATION
  // @retval OB_SNAPSHOT_DISCARDED
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  //
  virtual int lock_rows(const ObStoreCtx& ctx, const int64_t timeout, common::ObNewRowIterator* row_iter,
      ObLockFlag lock_flag, int64_t& affected_rows);
  //
  // do freeze
  //
  // @param freeze_type [in] major/minor freeze
  // @param frozen_version [out]
  //
  // @return
  //
  virtual int freeze_memtable(const FreezeType freeze_type, uint64_t frozen_version);

  //
  // migrate the static and increment data for a given version partition
  //
  // @param migrate_version [in] target migrate version for a partition
  //
  // @return
  // SUCC / FAILED
  virtual int migrate(uint64_t migrate_version);

  virtual int merge(uint64_t frozen_version);

  // NOT thread safe, add store to the partition storage.
  // Without lock protection, only should be invoked while initializing or log replaying
  //@param store [in] the store may be a ssstore or memstore.
  //@param write_redo_log [in] true if need write redo log, only used for ssstore
  //@return OB_SUCCESS or other error code
  virtual int add_store(ObIStore& store, const bool write_redo_log);

  // NOT thread safe, remove store from the partition storage.
  // Without lock protection, only should be invoked while initializing or log replaying
  //@param version [in] the version of remove store
  //@param write_redo_log [in] true if need write redo log, only used for ssstore
  //@return OB_SUCCESS, OB_ENTRY_NOT_EXIST or other error code
  virtual int remove_store(const common::ObVersion& version, const bool write_redo_log);

  //
  // replay log
  //
  // @param mutator [in] new replayed data
  //
  // @return
  //
  virtual int replay_mutator(common::ObMutator& mutator);

  virtual enum ObPartitionStorageStatus get_status(const uint64_t frozen_version);

  virtual int save_base_storage_info(const common::ObBaseStorageInfo& info);
  virtual int get_base_storage_info(common::ObBaseStorageInfo& info);
  virtual int get_saved_storage_info(ObSavedStorageInfo& info);

  virtual int replay(const ObStoreCtx& ctx, const char* data, const int64_t data_len);
  virtual int lock(const ObStoreCtx& ctx);
  //
  // return cost of doing real scan described in param.
  //
  virtual int64_t get_scan_cost(const ObTableScanParam& param);

  // write ssstore objects @version tree to data file , used by write_check_point
  virtual int serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size);
  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int get_serialize_size() const;

  virtual int64_t get_storage_log_seq_num();

  virtual const common::ObPartitionKey& get_partition_key() const;
  virtual common::ObIAllocator& get_memstore_allocator()
  {
    return memstore_allocator_;
  }
  virtual const share::schema::ObMultiVersionSchemaService* get_schema_service() const
  {
    return NULL;
  }

private:
  common::ObPartitionKey partition_key_;
  uint64_t merged_version_;
  enum ObPartitionStorageStatus state_;
  common::DefaultPageAllocator memstore_allocator_;
  lib::ObMutex mutex_;
};

class ObFakePartition : public ObIPartition {
public:
  ObFakePartition();
  virtual ~ObFakePartition()
  {}

  virtual int init(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, clog::ObIPartitionLogService* pls);
  virtual void destroy();
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos);
  // get partition log service
  virtual const clog::ObIPartitionLogService* get_log_service() const;
  virtual clog::ObIPartitionLogService* get_log_service();

  // get partition storage
  virtual ObIPartitionStorage* get_storage();
  virtual const common::ObPartitionKey& get_partition_key() const;
  virtual void set_partition_key(const common::ObPartitionKey& pkey);

  virtual int table_scan(const ObStoreCtx& ctx, ObTableScanParam& param, common::ObNewRowIterator*& result);
  virtual int revert_scan_iter(common::ObNewRowIterator* iter);
  virtual int delete_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int insert_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int update_rows(const ObStoreCtx& ctx, const int64_t timeout, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows);
  virtual int lock_rows(const ObStoreCtx& ctx, const int64_t timeout, common::ObNewRowIterator* row_iter,
      const ObLockFlag lock_flag, int64_t& affected_rows);
  virtual ObReplayStatus& get_replay_status();
  virtual void set_valid(bool valid)
  {
    UNUSED(valid);
  }
  virtual bool is_valid() const
  {
    return true;
  }
  virtual int rdlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int wrlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int try_rdlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int try_wrlock()
  {
    return common::OB_SUCCESS;
  }
  virtual int unlock()
  {
    return common::OB_SUCCESS;
  }

  virtual int save_base_storage_info(const common::ObBaseStorageInfo& info);
  virtual int save_frozen_storage_info(const common::ObBaseStorageInfo& info);
  virtual int get_base_storage_info(common::ObBaseStorageInfo& info);
  virtual int get_frozen_storage_info(common::ObBaseStorageInfo& info);
  virtual int get_saved_storage_info(ObSavedStorageInfo& info);

  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int64_t get_serialize_size() const;

  virtual common::ObRole get_role() const
  {
    return role_;
  }
  virtual void set_role(const common::ObRole role)
  {
    role_ = role;
  }
  // major freeze related
  virtual int set_major_freeze_status(const int64_t frozen_version, const int64_t major_freeze_status);
  virtual int get_major_freeze_status(int64_t& frozen_version, int64_t& major_freeze_status) const;
  virtual void set_memtable_version(const int64_t memtable_version);
  virtual int64_t get_memtable_version() const;
  virtual int64_t get_c2r_timestamp()
  {
    return OB_INVALID_TIMESTAMP;
  }
  virtual void set_c2r_timestamp(int64_t timestamp)
  {
    UNUSED(timestamp);
  }
  virtual int set_replica_type(const ObReplicaType& replica_type, const bool write_redo_log)
  {
    UNUSED(replica_type);
    UNUSED(write_redo_log);
    return common::OB_SUCCESS;
  }

private:
  common::ObPartitionKey partition_key_;
  ObFakePartitionStorage storage_;

  int64_t frozen_version_;
  int64_t major_freeze_status_;
  int64_t memtable_version_;
  common::ObRole role_;
};

class ObMajorFreezeTask : public share::ObAsyncTask {
public:
  ObMajorFreezeTask(ObIMajorFreezeCb* cb, const common::ObPartitionKey& partition_key, const int64_t cmd,
      const int64_t frozen_version, const int64_t frozen_timestamp, const int err);
  virtual ~ObMajorFreezeTask()
  {}

  virtual int process();
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(ObMajorFreezeTask);
  }
  virtual share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

private:
  ObIMajorFreezeCb* cb_;
  common::ObPartitionKey partition_key_;
  int64_t cmd_;
  int64_t frozen_version_;
  int64_t frozen_timestamp_;
  int err_;  // if OB_SUCCESS, means do major_freeze op succeed
};

class ObFakePartitionMgr : public ObIPartitionMgr {
public:
  class ObFakePartitionIter : public ObIPartitionIter {
  public:
    ObFakePartitionIter(ObFakePartitionMgr& pt_mgr) : index_(0), pt_mgr_(pt_mgr)
    {}
    virtual ~ObFakePartitionIter()
    {}

    virtual int get_next(ObIPartitionGroup*& partition);
    void reset()
    {
      index_ = 0;
    }
    // make iterator work even delete partition during iteration
    void fit_delete(const int64_t index);

  private:
    int64_t index_;
    ObFakePartitionMgr& pt_mgr_;
  };
  // use be test major freeze
  enum MajorFreezeTestMode {
    ALL_SUCCEED = 0,
    SOME_NOT_LEADER = 1,
    SOME_ERROR = 2,
    MIX = 3,
  };

  ObFakePartitionMgr() : partition_iter_(*this), partitions_()
  {}
  virtual ~ObFakePartitionMgr()
  {
    queue_.destroy();
  }

  virtual int init(ObIPartitionComponentFactory* cp_fty);
  virtual void destroy();

  virtual int get_all_partitions(common::ObIArray<ObIPartitionGroup*>& partition_list);

  virtual int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition);
  // Add new partition to partition manager
  virtual int add_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup** partition = NULL);
  virtual int add_partition(ObIPartitionGroup& partition);
  // Delete partition from partition manager
  virtual int del_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup** partitoin = NULL);

  virtual int revert_partition(ObIPartitionGroup* partition);
  virtual ObIPartitionIter* alloc_scan_iter();
  virtual int revert_scan_iter(ObIPartitionIter* iter);
  virtual common::ObArray<ObFakePartition>& get_partitions();
  virtual bool is_empty() const
  {
    return partitions_.size() == 0;
  }
  int64_t get_partition_count() const
  {
    return partitions_.size();
  }
  virtual int set_major_freeze_cb(ObIMajorFreezeCb* cb);
  virtual int prepare_major_freeze(
      const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp);
  virtual int commit_major_freeze(
      const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp);
  virtual int abort_major_freeze(
      const obrpc::ObPartitionList& partitions, const int64_t frozen_version, const int64_t frozen_timestamp);
  virtual int set_major_freeze_status(
      const common::ObPartitionKey& partition, const int64_t frozen_version, const int64_t major_freeze_status);
  virtual int get_major_freeze_status(
      const common::ObPartitionKey& partition, int64_t& frozen_version, int64_t& major_freeze_status);
  void set_mf_test_mode(MajorFreezeTestMode mode)
  {
    mf_test_mode_ = mode;
  }

private:
  int gen_err(const int64_t i);

  ObFakePartitionIter partition_iter_;
  common::ObArray<ObFakePartition> partitions_;
  common::ObArray<ObFakePartitionIter*> partition_iters_;
  ObIMajorFreezeCb* major_freeze_cb_;
  share::ObAsyncTaskQueue queue_;
  MajorFreezeTestMode mf_test_mode_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif
