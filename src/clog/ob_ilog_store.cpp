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

#include "ob_ilog_store.h"
#include "ob_file_id_cache.h"
#include "ob_ilog_file_builder.h"
#include "ob_ilog_memstore.h"
#include "ob_log_direct_reader.h"
#include "storage/ob_partition_service.h"
#include "share/redolog/ob_log_store_factory.h"
#include "share/ob_thread_mgr.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
using namespace storage;
using namespace common;
using namespace common::hash;
namespace clog {
PinnedMemory::PinnedMemory() : pinned_memory_(NULL), size_(0), is_inited_(false)
{}

PinnedMemory::~PinnedMemory()
{
  destroy();
}

int PinnedMemory::init(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "PinnedMemory init twice", K(ret));
  } else if (lib::is_mini_mode()) {
    size_ = 0;
    pinned_memory_ = NULL;
    is_inited_ = true;
    CLOG_LOG(INFO, "is_mini_mode, don't need to pin memory");
  } else if (OB_ISNULL(pinned_memory_ = static_cast<char*>(ob_malloc(size, ObModIds::OB_ILOG_MEMSTORE_CURSOR)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "allocate memory failed", K(ret));
  } else {
    size_ = size;
    memset(pinned_memory_, 0, size);
    is_inited_ = true;
    CLOG_LOG(INFO, "init PinnedMemory finished", K(size_), KP(pinned_memory_));
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void PinnedMemory::destroy()
{
  if (NULL != pinned_memory_) {
    ob_free(pinned_memory_);
    size_ = 0;
    pinned_memory_ = NULL;
  }
  is_inited_ = false;
  CLOG_LOG(INFO, "PinnedMemory destroy");
}

char* PinnedMemory::get_ptr()
{
  return pinned_memory_;
}

bool PinnedMemory::is_valid(const int64_t size) const
{
  return pinned_memory_ != NULL && size <= size_;
}

int ObIlogStore::IlogWriterTask::init(ObIlogStore* ilog_store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ilog_store)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "invalid arguments", K(ret));
  } else {
    ilog_store_ = ilog_store;
  }
  return ret;
}

void ObIlogStore::IlogWriterTask::runTimerTask()
{
  if (OB_ISNULL(ilog_store_)) {
    CSR_LOG(ERROR, "IlogWriterTask is not inited");
  } else {
    ilog_store_->runTimerTask();
  }
}

class ObIlogStore::UpdateMaxFlushedIlogFunctor {
public:
  UpdateMaxFlushedIlogFunctor(ObPartitionService* partition_service) : partition_service_(partition_service)
  {}
  ~UpdateMaxFlushedIlogFunctor()
  {}

public:
  bool operator()(const common::ObPartitionKey& partition_key, const IndexInfoBlockEntry& index_info_block_entry)
  {
    int ret = OB_SUCCESS;
    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
        NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      // do nothing
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CSR_LOG(WARN, "invalid partition", K(partition_key), K(index_info_block_entry));
      }
      ret = OB_SUCCESS;
    } else if (!(guard.get_partition_group()->is_valid())) {
      // do nothing
      CSR_LOG(WARN, "invalid partition", K(partition_key), K(index_info_block_entry));
      ret = OB_SUCCESS;
    } else if (OB_FAIL(log_service->update_max_flushed_ilog_id(index_info_block_entry.max_log_id_))) {
      CSR_LOG(ERROR, "update_max_flushed_ilog_id failed", K(ret), K(partition_key), K(index_info_block_entry));
    }

    return OB_SUCCESS == ret;
  }

private:
  ObPartitionService* partition_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(UpdateMaxFlushedIlogFunctor);
};

class ObIlogStore::MergeIlogMemstoreContainer {
public:
  MergeIlogMemstoreContainer() : is_inited_(false)
  {}
  ~MergeIlogMemstoreContainer()
  {}
  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      CLOG_LOG(ERROR, "MergeIlogMemstoreContainer has initted twice", K(ret));
    } else if (OB_FAIL(partition_info_block_map_.create(BUCKET_NUM, "InfoBlockMap"))) {
      CLOG_LOG(ERROR, "create info_block_map failed", K(ret));
    } else if (OB_FAIL(partition_memberlist_info_map_.create(BUCKET_NUM, "MemberListMap"))) {
      CLOG_LOG(ERROR, "create memberlist_info_map failed", K(ret));
    } else if (OB_FAIL(partition_log_cursor_map_.create(BUCKET_NUM, "LogCursorMap"))) {
      CLOG_LOG(ERROR, "create partition_log_cursor_set_ failed", K(ret));
    } else {
      is_inited_ = true;
    }
    if (IS_NOT_INIT) {
      destroy();
    }
    return ret;
  }
  int destroy()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(partition_info_block_map_.destroy())) {
      CLOG_LOG(ERROR, "destroy partition_info_block_map_ failed", K(ret));
    } else if (OB_FAIL(partition_memberlist_info_map_.destroy())) {
      CLOG_LOG(ERROR, "destroy partition_memberlist_info_map_ failed", K(ret));
    } else if (OB_FAIL(partition_log_cursor_map_.destroy())) {
      CLOG_LOG(ERROR, "destroy partition_log_cursor_map_ failed", K(ret));
    } else {
      is_inited_ = false;
    }
    time_gurad_.click(__FUNCTION__);
    CLOG_LOG(INFO, "destroy MergeIlogMemstoreContainer", K(ret), K(time_gurad_));
    return ret;
  }
  int merge_ilog_memstore_to_container(ObIlogMemstore* ilog_memstore)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(ERROR, "MergeIlogMemstoreContainer not init", K(ret));
    } else if (OB_ISNULL(ilog_memstore)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid arguments", K(ret), K(ret));
    } else {
      // 1. Merge partition_meta_info
      if (OB_SUCC(ret)) {
        MergePartitionMetaInfoFunctor functor(*this);
        if (OB_FAIL(ilog_memstore->operate_partition_meta_info(functor))) {
          ret = functor.get_ret_code();
          if (ret == OB_EAGAIN) {
            CLOG_LOG(WARN, "merge partition_meta_info log not continous", K(ret), K(ilog_memstore));
          } else {
            CLOG_LOG(ERROR, "merge partition_meta_info failed", K(ret));
          }
        }
        time_gurad_.click("operate_partition_meta_info");
      }
      // 2. Merge partition_memberlist_info
      if (OB_SUCC(ret)) {
        MergePartitionMemberListInfoFunctor functor(*this);
        if (OB_FAIL(ilog_memstore->operate_partition_memberlist_info(functor))) {
          ret = functor.get_ret_code();
          CLOG_LOG(ERROR, "merge partition_memberlist_info failed", K(ret));
        }
        time_gurad_.click("operate_partition_memberlist_info");
      }
      // 3. Merge log_cursor_ext_info
      if (OB_SUCC(ret)) {
        MergeLogCursorInfoFunctor functor(*this);
        if (OB_FAIL(ilog_memstore->operate_partition_log_cursor_ext_info(functor))) {
          ret = functor.get_ret_code();
          CLOG_LOG(ERROR, "merge partition_log_cursor_info failed", K(ret));
        }
        time_gurad_.click("operate_partition_log_cursor_ext_info");
      }
    }
    return ret;
  }

  int transfer_to_ilog_memstore(ObIlogMemstore* ilog_memstore)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ilog_memstore)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid argument", K(ret), KP(ilog_memstore));
      // 1. Write partition_info_block_map_ into ilog_memstore
    } else if (OB_FAIL(transfer_info_block_to_ilog_memstore_(ilog_memstore))) {
      // 2. Write partition_memberlist_info_map_ into ilog_memstore
      CLOG_LOG(ERROR, "transfer_info_block_to_ilog_memstore_ failed", K(ret));
    } else if (OB_FAIL(transfer_member_list_to_ilog_memstore_(ilog_memstore))) {
      CLOG_LOG(ERROR, "transfer_member_list_to_ilog_memstore_ failed", K(ret));
      // 3. Write partition_log_cursor_map_ into ilog_memstore
    } else if (OB_FAIL(transfer_log_cursor_to_ilog_memstore_(ilog_memstore))) {
      CLOG_LOG(ERROR, "transfer_log_cursor_to_ilog_memstore_ failed", K(ret));
    }
    return ret;
  }

private:
  typedef ObHashMap<common::ObPartitionKey, IndexInfoBlockEntry>::iterator InfoBlockIterator;
  typedef ObHashMap<common::ObPartitionKey, MemberListInfo>::iterator MemberListIterator;
  typedef ObHashMap<ObPartitionLogInfo, ObLogCursorExt>::iterator LogCursorIterator;
  static const int64_t BUCKET_NUM = 1000 * 50;

private:
  int insert_into_info_block_(const common::ObPartitionKey pkey, const IndexInfoBlockEntry& entry_to_merge)
  {
    int ret = OB_SUCCESS;
    IndexInfoBlockEntry entry_has_merged;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      CLOG_LOG(ERROR, "not init", K(ret));
    } else if (!pkey.is_valid() || !entry_to_merge.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid argument", K(ret));
    } else if (OB_FAIL(partition_info_block_map_.get_refactored(pkey, entry_has_merged)) && OB_HASH_NOT_EXIST != ret) {
      CLOG_LOG(ERROR, "get_refactored failed", K(ret), K(pkey), K(entry_has_merged), K(entry_to_merge));
    } else if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(partition_info_block_map_.set_refactored(pkey, entry_to_merge))) {
        CLOG_LOG(ERROR, "set_refactored failed", K(ret), K(entry_to_merge));
      }
    } else if (entry_has_merged.max_log_id_ != entry_to_merge.min_log_id_ - 1) {
      // during merge, log in different memstore may be not continous,
      // because of partition migrattion
      ret = OB_EAGAIN;
      CLOG_LOG(
          WARN, "insert_into_block failed because of not continous", K(ret), K(entry_to_merge), K(entry_has_merged));
      // noop log may cause the timestamp tp be equal
    } else if (entry_has_merged.max_log_id_ == entry_to_merge.min_log_id_ - 1 &&
               entry_has_merged.max_log_timestamp_ > entry_to_merge.min_log_timestamp_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "insert_into_block failed because of timestamp", K(ret));
    } else {
      entry_has_merged.max_log_id_ = entry_to_merge.max_log_id_;
      entry_has_merged.max_log_timestamp_ = entry_to_merge.max_log_timestamp_;
      if (OB_FAIL(partition_info_block_map_.set_refactored(pkey, entry_has_merged, 1))) {
        CLOG_LOG(ERROR, "set_refactored failed", K(ret), K(entry_has_merged), K(entry_to_merge));
      }
    }
    return ret;
  }
  int insert_into_memberlist_(const common::ObPartitionKey pkey, const MemberListInfo& member_list_info)
  {
    int ret = OB_NOT_SUPPORTED;
    UNUSED(pkey);
    UNUSED(member_list_info);
    return ret;
  }

  int insert_into_log_cursor_(const ObPartitionLogInfo& log_id_info, const ObLogCursorExt& log_cursor_to_merge)
  {
    int ret = OB_SUCCESS;
    ObIndexEntry ilog_entry;
    if (OB_UNLIKELY(!is_inited_)) {
      CLOG_LOG(ERROR, "not init", K(ret));
    } else if (OB_UNLIKELY(!log_id_info.is_valid()) || OB_UNLIKELY(!log_cursor_to_merge.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid arguments", K(ret), K(log_id_info), K(log_cursor_to_merge));
    } else if (OB_FAIL(partition_log_cursor_map_.set_refactored(log_id_info, log_cursor_to_merge))) {
      CLOG_LOG(ERROR, "log_id_info may be exist in the map", K(ret), K(log_id_info), K(log_cursor_to_merge));
    }
    return ret;
  }

  class MergePartitionMetaInfoFunctor {
  public:
    MergePartitionMetaInfoFunctor(MergeIlogMemstoreContainer& host) : host_(host), ret_code_(OB_SUCCESS)
    {}
    bool operator()(const ObPartitionKey& pkey, const IndexInfoBlockEntry& entry_to_merge)
    {
      bool bool_ret = false;
      int ret = OB_SUCCESS;
      if (OB_FAIL(host_.insert_into_info_block_(pkey, entry_to_merge)) && OB_EAGAIN != ret) {
        CLOG_LOG(ERROR, "insert_into_info_block_ failed", K(ret), K(pkey), K(entry_to_merge));
      }
      ret_code_ = ret;
      return OB_SUCCESS == ret;
    }
    int get_ret_code() const
    {
      return ret_code_;
    }

  private:
    MergeIlogMemstoreContainer& host_;
    int ret_code_;
  };

  // not support
  class MergePartitionMemberListInfoFunctor {
  public:
    MergePartitionMemberListInfoFunctor(MergeIlogMemstoreContainer& host) : host_(host), ret_code_(OB_SUCCESS)
    {}
    bool operator()(const ObPartitionKey& pkey, const MemberListInfo& memberlist_to_merge)
    {
      int ret = OB_SUCCESS;
      UNUSED(pkey);
      UNUSED(memberlist_to_merge);
      // not support
      ret_code_ = ret;
      return OB_SUCCESS == ret;
    }

    int get_ret_code() const
    {
      return ret_code_;
    }

  private:
    MergeIlogMemstoreContainer& host_;
    int ret_code_;
  };

  class MergeLogCursorInfoFunctor {
  public:
    MergeLogCursorInfoFunctor(MergeIlogMemstoreContainer& host) : host_(host), ret_code_(OB_SUCCESS)
    {}
    bool operator()(const ObPartitionLogInfo& log_info, const ObLogCursorExt& log_cursor_to_merge)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(host_.insert_into_log_cursor_(log_info, log_cursor_to_merge)) && OB_EAGAIN != ret) {
        CLOG_LOG(ERROR, "insert_into_log_cursor_ failed", K(ret), K(log_info), K(log_cursor_to_merge));
      }
      ret_code_ = ret;
      return OB_SUCCESS == ret;
    }
    int get_ret_code() const
    {
      return ret_code_;
    }

  private:
    MergeIlogMemstoreContainer& host_;
    int ret_code_;
  };

  int transfer_info_block_to_ilog_memstore_(ObIlogMemstore* ilog_memstore)
  {
    int ret = OB_SUCCESS;
    InfoBlockIterator info_iterator = partition_info_block_map_.begin();
    InfoBlockIterator info_iterator_end = partition_info_block_map_.end();
    if (OB_ISNULL(ilog_memstore)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid argument", K(ret), KP(ilog_memstore));
    }
    while (info_iterator != info_iterator_end && OB_SUCC(ret)) {
      ObPartitionKey pkey = info_iterator->first;
      IndexInfoBlockEntry entry = info_iterator->second;
      if (!pkey.is_valid() || !entry.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error", K(ret), K(pkey), K(entry));
      } else if (OB_FAIL(ilog_memstore->insert_partition_meta_info(pkey, entry))) {
        CLOG_LOG(ERROR, "insert_partition_meta_info failed", K(ret), K(pkey), K(entry));
      } else {
        ++info_iterator;
      }
    }
    time_gurad_.click(__FUNCTION__);
    return ret;
  }

  int transfer_member_list_to_ilog_memstore_(ObIlogMemstore* ilog_memstore)
  {
    int ret = OB_SUCCESS;
    MemberListIterator memberlist_iterator = partition_memberlist_info_map_.begin();
    MemberListIterator memberlist_iterator_end = partition_memberlist_info_map_.end();
    if (OB_ISNULL(ilog_memstore)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid argument", K(ret), KP(ilog_memstore));
    }
    while (memberlist_iterator != memberlist_iterator_end && OB_SUCC(ret)) {
      ObPartitionKey pkey = memberlist_iterator->first;
      MemberListInfo memberlist = memberlist_iterator->second;
      if (!pkey.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error", K(ret), K(pkey), K(memberlist));
      } else if (OB_FAIL(ilog_memstore->insert_partition_memberlist_info(pkey, memberlist))) {
        CLOG_LOG(ERROR, "insert_partition_memberlist_info failed", K(ret), K(pkey), K(memberlist));
      } else {
        ++memberlist_iterator;
      }
    }
    time_gurad_.click(__FUNCTION__);
    return ret;
  }

  int transfer_log_cursor_to_ilog_memstore_(ObIlogMemstore* ilog_memstore)
  {
    int ret = OB_SUCCESS;
    LogCursorIterator log_cursor_iterator = partition_log_cursor_map_.begin();
    LogCursorIterator log_cursor_iterator_end = partition_log_cursor_map_.end();
    if (OB_ISNULL(ilog_memstore)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid argument", K(ret), KP(ilog_memstore));
    }
    while (log_cursor_iterator != log_cursor_iterator_end && OB_SUCC(ret)) {
      ObPartitionLogInfo log_info = log_cursor_iterator->first;
      ObLogCursorExt log_cursor = log_cursor_iterator->second;
      if (!log_info.is_valid() || !log_cursor.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error", K(ret), K(log_info), K(log_cursor));
      } else if (OB_FAIL(ilog_memstore->insert_partition_log_cursor_ext_info(log_info, log_cursor))) {
        CLOG_LOG(ERROR, "insert_partition_log_cursor_ext_info failed", K(ret), K(log_info), K(log_cursor));
      } else {
        ++log_cursor_iterator;
      }
    }
    time_gurad_.click(__FUNCTION__);
    return ret;
  }

private:
  ObHashMap<ObPartitionKey, IndexInfoBlockEntry> partition_info_block_map_;
  ObHashMap<ObPartitionKey, MemberListInfo> partition_memberlist_info_map_;
  ObHashMap<ObPartitionLogInfo, ObLogCursorExt> partition_log_cursor_map_;
  bool is_inited_;
  ObTimeGuard time_gurad_;

private:
  DISALLOW_COPY_AND_ASSIGN(MergeIlogMemstoreContainer);
};

ObIlogStore::ObIlogStore()
    : lock_(),
      is_inited_(false),
      next_ilog_file_id_(OB_INVALID_FILE_ID),
      file_store_(NULL),
      file_id_cache_(NULL),
      direct_reader_(NULL),
      partition_service_(NULL),
      active_memstore_(NULL),
      frozen_memstore_array_(),
      memstore_cnt_(0),
      curr_memstore_seq_(-1),
      prev_consumed_memstore_seq_(-1),
      task_()
{}

ObIlogStore::~ObIlogStore()
{
  destroy();
}

int ObIlogStore::init(const file_id_t next_ilog_file_id, ObILogFileStore* file_store, ObFileIdCache* file_id_cache,
    ObLogDirectReader* direct_reader, storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CSR_LOG(ERROR, "ObIlogStore init twice", K(ret));
  } else if (!is_valid_file_id(next_ilog_file_id) || OB_ISNULL(file_store) || OB_ISNULL(file_id_cache) ||
             OB_ISNULL(direct_reader) || OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(next_ilog_file_id),
        KP(file_store),
        KP(file_id_cache),
        KP(direct_reader),
        KP(partition_service));
  } else if (OB_FAIL(raise_memstore_())) {
    CSR_LOG(ERROR, "raise_memstore_ failed", K(ret));
  } else if (OB_FAIL(task_.init(this))) {
    CSR_LOG(ERROR, "task_ init failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ILOGFlush))) {
    CSR_LOG(ERROR, "timer_ init failed", K(ret));
  } else if (OB_FAIL(pinned_memory_.init(PINNED_MEMORY_SIZE))) {
    CLOG_LOG(ERROR, "pinned_memory_ init failed", K(ret));
  } else {
    file_store_ = file_store;
    next_ilog_file_id_ = next_ilog_file_id;
    file_id_cache_ = file_id_cache;
    direct_reader_ = direct_reader;
    partition_service_ = partition_service;
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  CSR_LOG(INFO, "ObIlogStore init finished", K(ret));
  return ret;
}

void ObIlogStore::destroy()
{
  WLockGuard guard(lock_);
  stop();
  wait();
  is_inited_ = false;
  next_ilog_file_id_ = OB_INVALID_FILE_ID;
  file_store_ = nullptr;
  file_id_cache_ = NULL;
  direct_reader_ = NULL;
  partition_service_ = NULL;
  free_memstore_();
  TG_DESTROY(lib::TGDefIDs::ILOGFlush);
  CSR_LOG(INFO, "ObIlogStore destroy");
  return;
}

int ObIlogStore::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ILOGFlush, task_, TIMER_TASK_INTERVAL, repeat))) {
    CSR_LOG(WARN, "fail to schedule task", K(ret));
  }
  CSR_LOG(INFO, "ObIlogStore start finished", K(ret));
  return ret;
}

void ObIlogStore::stop()
{
  TG_STOP(lib::TGDefIDs::ILOGFlush);
  CSR_LOG(INFO, "ObIlogStore stop finished");
}

void ObIlogStore::wait()
{
  TG_WAIT(lib::TGDefIDs::ILOGFlush);
  CSR_LOG(INFO, "ObIlogStore wait finished");
}

int ObIlogStore::submit_cursor(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  ObIlogFreezeTriggerType trigger_type = OB_INVALID_TRIGGER_TYPE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id) || !log_cursor_ext.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  } else {
    lock_.rdlock();
    if (OB_FAIL(active_memstore_->check_need_freeze(partition_key, log_id, trigger_type))) {
      CSR_LOG(ERROR, "check_need_freeze failed", K(ret), K(partition_key), K(log_id));
    } else if (OB_INVALID_TRIGGER_TYPE == trigger_type) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "unexpected error occured", K(ret));
    } else if (OB_NO_NEED_FREEZE_TRIGGER_TYPE == trigger_type) {
      if (OB_FAIL(active_memstore_->submit_cursor(partition_key, log_id, log_cursor_ext))) {
        CSR_LOG(ERROR, "submit_cursor failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
      }
      lock_.unlock();
    } else {
      lock_.unlock();
      lock_.wrlock();
      // double check
      if (OB_FAIL(active_memstore_->check_need_freeze(partition_key, log_id, trigger_type))) {
        CSR_LOG(ERROR, "check_need_freeze failed", K(ret), K(partition_key), K(log_id));
      } else if (OB_INVALID_TRIGGER_TYPE == trigger_type) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error occured", K(ret));
      } else if (OB_NO_NEED_FREEZE_TRIGGER_TYPE != trigger_type) {
        if (OB_FAIL(froze_memstore_(trigger_type))) {
          CSR_LOG(ERROR, "froze_memstore_ failed", K(ret));
        } else if (OB_FAIL(raise_memstore_())) {
          CSR_LOG(ERROR, "raise_memstore_ failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(active_memstore_->submit_cursor(partition_key, log_id, log_cursor_ext))) {
          CSR_LOG(ERROR, "active_memstore_ failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
        }
      }
      lock_.unlock();
    }
  }

  if (OB_SUCCESS != ret) {
    CSR_LOG(ERROR, "submit_cursor return value unexpected", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  }
  CSR_LOG(TRACE, "submit_cursor", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  return ret;
}

int ObIlogStore::submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
    const int64_t memberlist_version)
{
  int ret = OB_SUCCESS;
  bool need_freeze = false;
  ObIlogFreezeTriggerType trigger_type = OB_INVALID_TRIGGER_TYPE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id) || !log_cursor_ext.is_valid() ||
             !memberlist.is_valid() || replica_num <= 0 || memberlist_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(partition_key),
        K(log_id),
        K(log_cursor_ext),
        K(memberlist),
        K(replica_num),
        K(memberlist_version));
  } else {
    lock_.rdlock();
    if (OB_FAIL(active_memstore_->check_need_freeze(partition_key, log_id, trigger_type))) {
      CSR_LOG(ERROR, "check_need_freeze failed", K(ret), K(partition_key), K(log_id));
    } else if (OB_INVALID_TRIGGER_TYPE == trigger_type) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "unexpected error occured", K(ret));
    } else if (OB_NO_NEED_FREEZE_TRIGGER_TYPE == trigger_type) {
      if (OB_FAIL(active_memstore_->submit_cursor(
              partition_key, log_id, log_cursor_ext, memberlist, replica_num, memberlist_version))) {
        CSR_LOG(ERROR,
            "submit_cursor failed",
            K(ret),
            K(partition_key),
            K(log_id),
            K(log_cursor_ext),
            K(memberlist),
            K(replica_num),
            K(memberlist_version));
      }
      lock_.unlock();
    } else {
      lock_.unlock();
      lock_.wrlock();
      // double check
      if (OB_FAIL(active_memstore_->check_need_freeze(partition_key, log_id, trigger_type))) {
        CSR_LOG(ERROR, "check_need_freeze failed", K(ret), K(partition_key), K(log_id));
      } else if (OB_INVALID_TRIGGER_TYPE == trigger_type) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error occured", K(ret));
      } else if (OB_NO_NEED_FREEZE_TRIGGER_TYPE != trigger_type) {
        if (OB_FAIL(froze_memstore_(trigger_type))) {
          CSR_LOG(ERROR, "froze_memstore_ failed", K(ret));
        } else if (OB_FAIL(raise_memstore_())) {
          CSR_LOG(ERROR, "raise_memstore_ failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(active_memstore_->submit_cursor(
                partition_key, log_id, log_cursor_ext, memberlist, replica_num, memberlist_version))) {
          CSR_LOG(ERROR,
              "active_memstore_ failed",
              K(ret),
              K(partition_key),
              K(log_id),
              K(log_cursor_ext),
              K(memberlist),
              K(replica_num),
              K(memberlist_version));
        }
      }
      lock_.unlock();
    }
  }

  if (OB_SUCCESS != ret) {
    CSR_LOG(ERROR,
        "submit_cursor(with memberlist info) return value unexpected",
        K(ret),
        K(partition_key),
        K(log_id),
        K(log_cursor_ext),
        K(memberlist),
        K(replica_num),
        K(memberlist_version));
  }
  CSR_LOG(TRACE,
      "submit_cursor",
      K(ret),
      K(partition_key),
      K(log_id),
      K(log_cursor_ext),
      K(memberlist),
      K(replica_num),
      K(memberlist_version));
  return ret;
}

int ObIlogStore::get_cursor_from_memstore(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObGetCursorResult& result) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else {
    // avoid holding read lock in long time
    const int64_t MAX_BATCH_SIZE = 64;
    const int64_t batch_size = min(result.arr_len_, MAX_BATCH_SIZE);
    int64_t count = 0;
    RLockGuard guard(lock_);
    for (; OB_SUCC(ret) && count < batch_size; count++) {
      if (OB_FAIL(get_cursor_from_memstore_(partition_key, log_id + count, result.csr_arr_[count]))) {
        if (OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_ENTRY_NOT_EXIST == ret) {
          CSR_LOG(DEBUG, "get_cursor_from_memstore, entry not exist", K(ret), K(partition_key), K(log_id));
        } else {
          CSR_LOG(WARN, "get_cursor_from_memstore_ failed", K(ret), K(partition_key), K(log_id));
        }
      }
    }
    if ((OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_ENTRY_NOT_EXIST == ret) && count > 1) {
      --count;
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      result.ret_len_ = count;
    }
  }

  if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    CSR_LOG(ERROR, "get_cursor_from_memstore return value unexpected", K(ret), K(partition_key), K(log_id), K(result));
  }
  CSR_LOG(TRACE, "get_cursor_from_memstore", K(ret), K(partition_key), K(log_id), K(result));
  return ret;
}

int ObIlogStore::get_cursor_from_memstore_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(active_memstore_->get_cursor(partition_key, log_id, log_cursor_ext)) && OB_ENTRY_NOT_EXIST != ret &&
      OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    CSR_LOG(ERROR, "get_cursor failed", K(ret), K(partition_key), K(log_id));
  } else if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
    // do nothing
  } else if (OB_FAIL(get_cursor_from_frozen_memstore_(partition_key, log_id, log_cursor_ext)) &&
             OB_ENTRY_NOT_EXIST != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    CSR_LOG(ERROR, "get_cursor_from_frozen_memstore_ failed", K(ret), K(partition_key), K(log_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogStore::get_cursor_from_frozen_memstore_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObLogCursorExt& log_cursor_ext) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = frozen_memstore_array_.count() - 1; i >= 0; i--) {
    ObIlogMemstore* memstore = frozen_memstore_array_[i].memstore_;
    if (OB_FAIL(memstore->get_cursor(partition_key, log_id, log_cursor_ext)) && OB_ENTRY_NOT_EXIST != ret &&
        OB_ERR_OUT_OF_UPPER_BOUND != ret) {
      CSR_LOG(ERROR, "get_cursor failed", K(ret), K(partition_key), K(log_id));
      break;
    } else if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      // do nothing
      break;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      continue;
    }
  }
  if (frozen_memstore_array_.count() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObIlogStore::get_cursor_from_ilog_file(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    const Log2File& log2file_item, ObGetCursorResult& result) const
{
  int ret = OB_SUCCESS;
  // only for local observer
  const common::ObAddr addr = GCTX.self_addr_;
  const int64_t seq = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", KR(ret), K(partition_key), K(log_id), K(log2file_item));
  } else if (OB_FAIL(direct_reader_->get_batch_log_cursor(partition_key, addr, seq, log_id, log2file_item, result))) {
    CSR_LOG(WARN, "failed to get_batch_log_cursor", KR(ret), K(partition_key), K(log_id), K(log2file_item));
  } else { /*do nothing*/
  }
  return ret;
}

int ObIlogStore::get_max_ilog_from_memstore(const common::ObPartitionKey& partition_key, uint64_t& ret_max_log_id) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_max_log_ts = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else {
    RLockGuard guard(lock_);
    if (OB_FAIL(active_memstore_->get_max_log_id_and_ts(partition_key, ret_max_log_id, tmp_max_log_ts)) &&
        OB_PARTITION_NOT_EXIST != ret) {
      CSR_LOG(ERROR, "active_memstore_ get_max_log_id_and_ts failed", K(ret), K(partition_key));
    } else if (OB_PARTITION_NOT_EXIST == ret) {
      if (OB_FAIL(get_max_log_from_frozen_memstore_(partition_key, ret_max_log_id)) && OB_PARTITION_NOT_EXIST != ret) {
        CSR_LOG(ERROR, "get_max_log_from_frozen_memstore_ failed", K(ret), K(partition_key));
      }
    } else {
      // do nothing
    }
  }

  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_max_ilog_from_memstore return value unexpected", K(ret), K(partition_key), K(ret_max_log_id));
  }
  CSR_LOG(TRACE, "get_max_ilog_from_memstore", K(ret), K(partition_key), K(ret_max_log_id));
  return ret;
}

int ObIlogStore::get_memstore_min_log_id_and_ts(
    const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const
{
  RLockGuard guard(lock_);
  return get_memstore_min_log_id_and_ts_unlock_(partition_key, ret_min_log_id, ret_min_log_ts);
}

int ObIlogStore::get_memstore_min_log_id_and_ts_unlock_(
    const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else {
    ret_min_log_id = OB_INVALID_ID;
    ret_min_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(active_memstore_->get_min_log_id_and_ts(partition_key, ret_min_log_id, ret_min_log_ts)) &&
        OB_PARTITION_NOT_EXIST != ret) {
      CSR_LOG(ERROR, "active_memstore_ get_min_log_id_and_ts failed", K(ret), K(partition_key));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS ==
          (tmp_ret = (get_min_log_from_frozen_memstore_(partition_key, ret_min_log_id, ret_min_log_ts)))) {
        ret = OB_SUCCESS;
      } else if (OB_PARTITION_NOT_EXIST == tmp_ret) {
        // keep ret
      } else {
        CSR_LOG(ERROR, "get_min_log_from_frozen_memstore_ failed", K(tmp_ret), K(ret), K(partition_key));
        ret = tmp_ret;
      }
    }
  }

  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR,
        "get_memstore_min_log_id_and_ts return value unexpected",
        K(ret),
        K(partition_key),
        K(ret_min_log_id),
        K(ret_min_log_ts));
  }
  CSR_LOG(TRACE, "get_memstore_min_log_id_and_ts", K(ret), K(partition_key), K(ret_min_log_id), K(ret_min_log_ts));
  return ret;
}

void ObIlogStore::runTimerTask()
{
  int ret = OB_SUCCESS;
  bool need_dump = false;
  int64_t curr_memstore_seq = ATOMIC_LOAD(&curr_memstore_seq_);
  (void)timer_check_need_freeze_();

  if (OB_FAIL(check_need_dump_(need_dump, curr_memstore_seq))) {
    CLOG_LOG(ERROR, "check_need_dump_ failed", K(ret), K(need_dump), K(frozen_memstore_array_));
  } else if (!need_dump) {
    CLOG_LOG(TRACE,
        "frozen_memstore_array_ has not changed, no need dump",
        K(ret),
        K(need_dump),
        K(prev_consumed_memstore_seq_),
        K(curr_memstore_seq_));
  } else {
    CLOG_LOG(INFO,
        "frozen_memstore_array_ need dump",
        K(ret),
        K(need_dump),
        K(prev_consumed_memstore_seq_),
        K(curr_memstore_seq));
    FrozenMemstore frozen_memstore;
    FrozenMemstore memstore_after_merge;
    int64_t end_idx = 0;
    int64_t start_ts = ObTimeUtility::current_time();
    do {
      RLockGuard guard(lock_);
      frozen_memstore = frozen_memstore_array_[0];
    } while (0);
    if (!frozen_memstore.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "frozen_memstore is unvalid",
          K(ret),
          K(frozen_memstore),
          K(curr_memstore_seq),
          K(prev_consumed_memstore_seq_));
    } else if (need_merge_frozen_memstore_array_by_trigger_type_(frozen_memstore.trigger_type_)) {
      if (OB_FAIL(merge_frozen_memstore_(end_idx, memstore_after_merge))) {
        CLOG_LOG(WARN, "merge_frozen_memstore_ failed", K(ret), K(end_idx), K(memstore_after_merge));
      }
    } else {
      memstore_after_merge = frozen_memstore;
    }

    if (OB_SUCC(ret)) {
      ObIlogMemstore* memstore = memstore_after_merge.memstore_;
      IlogMemstoreArray free_memstore_array;
      ObIlogFileBuilder builder;
      if (NULL == memstore) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "invalid argument", K(ret));
      } else if (OB_FAIL(free_memstore_array.reserve(end_idx + 1))) {
        CSR_LOG(ERROR, "reserve free_memstore_array failed", K(ret));
      } else if (OB_FAIL(builder.init(memstore, &pinned_memory_))) {
        CSR_LOG(ERROR, "builder init failed", K(ret));
        // whatever the trigger_type is, need to dump memstore to disk
      } else if (OB_FAIL(build_and_write_file_(builder))) {
        CLOG_LOG(ERROR, "build_and_write_file_ failed", K(ret));
        // whatever the trigger_type is, need to update flused_ilog_id
      } else if (OB_FAIL(update_max_flushed_ilog_id_(builder.get_index_info_block_map()))) {
        CSR_LOG(ERROR, "update_max_flushed_ilog_id_ failed", K(ret));
      } else if (OB_FAIL(handle_different_trigger_type_(memstore_after_merge, end_idx, free_memstore_array, builder))) {
        CLOG_LOG(ERROR, "handle_different_trigger_type_ failed", K(ret), K(memstore_after_merge), K(end_idx));
      } else {
        free_memstore_(free_memstore_array);
      }

      // prevent memory leaks, when any step executed failed, need to
      // release memstore_after_merge.
      do {
        RLockGuard guard(lock_);
        if (OB_FAIL(ret) && frozen_memstore_array_.count() >= 1) {
          if (frozen_memstore_array_[0].memstore_ != memstore) {
            free_memstore_(memstore_after_merge.memstore_);
          }
        }
      } while (0);
    }
    int64_t cost = ObTimeUtility::current_time() - start_ts;
    CLOG_LOG(INFO, "runTimerTask success", K(cost));
  }
}

int ObIlogStore::get_max_log_from_frozen_memstore_(
    const common::ObPartitionKey& partition_key, uint64_t& ret_max_log_id) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_max_log_ts = OB_INVALID_TIMESTAMP;
  for (int64_t i = frozen_memstore_array_.count() - 1; i >= 0; i--) {
    ObIlogMemstore* memstore = frozen_memstore_array_[i].memstore_;
    if (OB_FAIL(memstore->get_max_log_id_and_ts(partition_key, ret_max_log_id, tmp_max_log_ts)) &&
        OB_PARTITION_NOT_EXIST != ret) {
      CSR_LOG(ERROR, "get_max_log_id_and_ts failed", K(ret), K(partition_key));
      break;
    } else if (OB_SUCCESS == ret) {
      break;
    } else {
      continue;
    }
  }
  if (frozen_memstore_array_.count() == 0) {
    ret = OB_PARTITION_NOT_EXIST;
  }
  return ret;
}

int ObIlogStore::get_min_log_from_frozen_memstore_(
    const common::ObPartitionKey& partition_key, uint64_t& ret_min_log_id, int64_t& ret_min_log_ts) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = frozen_memstore_array_.count() - 1; i >= 0; i--) {
    IndexInfoBlockEntry index_entry;
    ObIlogMemstore* memstore = frozen_memstore_array_[i].memstore_;
    if (OB_FAIL(memstore->get_index_entry(partition_key, index_entry)) && OB_PARTITION_NOT_EXIST != ret) {
      CSR_LOG(ERROR, "get_min_log_id_and_ts failed", K(ret), K(partition_key));
      break;
    } else if (OB_PARTITION_NOT_EXIST == ret) {
      continue;
    } else {
      if (OB_INVALID_ID != ret_min_log_id && index_entry.max_log_id_ >= ret_min_log_id) {
        // memstore is not continous, just ignore previous memstore
        break;
      } else {
        ret_min_log_id = index_entry.min_log_id_;
        ret_min_log_ts = index_entry.min_log_timestamp_;
        continue;
      }
    }
  }
  if (0 == frozen_memstore_array_.count() && OB_INVALID_ID == ret_min_log_id) {
    ret = OB_PARTITION_NOT_EXIST;
  }
  return ret;
}

int ObIlogStore::update_max_flushed_ilog_id_(IndexInfoBlockMap& index_info_block_map)
{
  int ret = OB_SUCCESS;
  UpdateMaxFlushedIlogFunctor fn(partition_service_);
  if (OB_FAIL(index_info_block_map.for_each(fn))) {
    CSR_LOG(ERROR, "update_max_flushed_ilog_id_ failed", K(ret));
  }
  return ret;
}

int ObIlogStore::write_file_(const file_id_t next_ilog_file_id, char* buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(file_store_->write_file(next_ilog_file_id, buffer, buffer_size))) {
    CSR_LOG(ERROR, "write ilog file failed", K(ret), K(next_ilog_file_id));
  } else {
    file_store_->update_max_file_id(next_ilog_file_id);
  }
  int64_t cost = ObTimeUtility::current_time() - start_ts;
  CLOG_LOG(INFO, "write_file_ success", K(cost), K(next_ilog_file_id));
  return ret;
}

// caller should add lock
int ObIlogStore::froze_memstore_(const ObIlogFreezeTriggerType& trigger_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (NULL == active_memstore_) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "active_memstore_ is NULL", KP(active_memstore_));
  } else {
    FrozenMemstore frozen_memstore(trigger_type, active_memstore_, curr_memstore_seq_);
    while (true) {
      if (OB_SUCCESS != (tmp_ret = frozen_memstore_array_.push_back(frozen_memstore))) {
        CSR_LOG(ERROR, "frozen_memstore_array_ push_back failed, need retry", K(tmp_ret));
      } else {
        active_memstore_ = NULL;
        break;
      }
      usleep(1000);
    }
    CSR_LOG(INFO,
        "froze_memstore_ finished",
        K(ret),
        K(frozen_memstore),
        K(prev_consumed_memstore_seq_),
        K(curr_memstore_seq_));
  }
  return ret;
}

int ObIlogStore::raise_memstore_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (active_memstore_ != NULL) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "active_memstore_ is not NULL", KP(active_memstore_));
  } else {
    (void)alloc_memstore_(active_memstore_);
    ATOMIC_INC(&memstore_cnt_);
    ATOMIC_INC(&curr_memstore_seq_);
  }
  CSR_LOG(INFO, "raise_memstore_ finished", K(ret), K(memstore_cnt_));
  return ret;
}

void ObIlogStore::free_memstore_(IlogMemstoreArray& free_memstore_array)
{
  for (int64_t i = 0; i < free_memstore_array.count(); i++) {
    ObIlogMemstore* memstore = free_memstore_array[i];
    free_memstore_(memstore);
  }
}

void ObIlogStore::free_memstore_(ObIlogMemstore* memstore)
{
  int64_t start_ts = ObTimeUtility::current_time();
  op_reclaim_free(memstore);
  memstore = NULL;
  ATOMIC_DEC(&memstore_cnt_);
  int64_t cost = ObTimeUtility::current_time() - start_ts;
  CSR_LOG(INFO, "free_memstore_ finished", K(memstore_cnt_));
}

void ObIlogStore::free_memstore_()
{
  free_memstore_(active_memstore_);
  active_memstore_ = NULL;
  for (int64_t i = 0; i < frozen_memstore_array_.count(); i++) {
    ObIlogMemstore* memstore = frozen_memstore_array_[i].memstore_;
    free_memstore_(memstore);
    memstore = NULL;
  }
  frozen_memstore_array_.destroy();
  CSR_LOG(INFO, "free_memstore_ all finished");
}

void ObIlogStore::timer_check_need_freeze_()
{
  int ret = OB_SUCCESS;
  bool need_freeze = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStore is not inited", K(ret));
  } else {
    lock_.rdlock();
    if (OB_FAIL(active_memstore_->timer_check_need_freeze(need_freeze))) {
      CSR_LOG(ERROR, "timer_check_need_freeze failed", K(ret));
    }
    lock_.unlock();
    if (need_freeze) {
      lock_.wrlock();
      // double check
      if (OB_FAIL(active_memstore_->timer_check_need_freeze(need_freeze))) {
        CSR_LOG(ERROR, "timer_check_need_freeze failed", K(ret));
      } else if (need_freeze) {
        if (OB_FAIL(froze_memstore_(OB_TIMER_TRIGGER_TYPE))) {
          CSR_LOG(ERROR, "froze_memstore_ failed", K(ret));
        } else if (OB_FAIL(raise_memstore_())) {
          CSR_LOG(ERROR, "raise_memstore_ failed", K(ret));
        }
      }
      lock_.unlock();
    }
  }
}

int ObIlogStore::get_merge_range_(int64_t& end_idx, bool& is_ilog_not_continous_trigger)
{
  int ret = OB_SUCCESS;
  is_ilog_not_continous_trigger = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ilog_store not inited", K(ret));
  } else {
    RLockGuard guard(lock_);
    int64_t size = frozen_memstore_array_.count();
    int64_t cursor_size = 0;
    end_idx = 0;
    // limit each ilog file should less than ObIlogMemstore::CURSOR_SIZE_TRIGGER
    while (end_idx < size && cursor_size < ObIlogMemstore::CURSOR_SIZE_TRIGGER) {
      int64_t tmp_cursor_size = 0;
      ObIlogMemstore* memstore = frozen_memstore_array_[end_idx].memstore_;
      ObIlogFreezeTriggerType trigger_type = frozen_memstore_array_[end_idx].trigger_type_;
      // In normal case, when end_idx is 0, trigger_type mustn't be OB_ILOG_NOT_CONTINOUS_TRIGGER_TYPE
      if (frozen_memstore_array_[end_idx].trigger_type_ == OB_ILOG_NOT_CONTINOUS_TRIGGER_TYPE) {
        is_ilog_not_continous_trigger = true;
        break;
      } else if (OB_UNLIKELY(NULL == memstore)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpect error becauese memstore is nullptr", K(ret));
      } else if (OB_FAIL(memstore->get_cursor_size(tmp_cursor_size))) {
        CLOG_LOG(ERROR, "get_cusrsor_size failed", K(ret));
      }
      cursor_size += tmp_cursor_size;
      end_idx++;
    }
    end_idx -= 1;
  }
  return ret;
}

int ObIlogStore::merge_frozen_memstore_(int64_t& end_idx, FrozenMemstore& memstore_after_merge)
{
  int ret = OB_SUCCESS;
  bool is_ilog_not_continous_trigger = false;
  end_idx = 0;
  int64_t start_ts = ObTimeUtility::current_time();
  if (memstore_after_merge.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(memstore_after_merge), K(end_idx));
  } else {
    if (OB_FAIL(get_merge_range_(end_idx, is_ilog_not_continous_trigger))) {
      CLOG_LOG(ERROR, "get_merge_range_ failed", K(ret));
    } else {
      // used to reduce the critical section size
      FrozenMemstoreArray tmp_frozen_memstore_array;
      do {
        RLockGuard guard(lock_);
        for (int64_t i = 0; i <= end_idx && OB_SUCC(ret); i++) {
          const FrozenMemstore& memstore = frozen_memstore_array_[i];
          if (!memstore.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(ERROR, "unexpect error, invalid frozen_memstore", K(ret), K(memstore));
          } else if (OB_FAIL(tmp_frozen_memstore_array.push_back(memstore))) {
            CSR_LOG(ERROR, "tmp_ilog_memstore_array push_back failed", K(ret), K(memstore));
          }
        }
      } while (0);

      if (OB_SUCC(ret) && OB_FAIL(do_merge_frozen_memstore_(
                              tmp_frozen_memstore_array, is_ilog_not_continous_trigger, memstore_after_merge))) {
        if (ret == OB_EAGAIN) {
          CLOG_LOG(WARN, "log not continous in do_merge_frozen_memstore_", K(tmp_frozen_memstore_array));
        } else {
          CLOG_LOG(ERROR, "do_merge_frozen_memstore_ failed", K(ret), K(tmp_frozen_memstore_array));
        }
      }
    }
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  CLOG_LOG(INFO, "merge_frozen_memstore_ success", K(ret), K(cost_ts), K(end_idx));
  return ret;
}

int ObIlogStore::build_and_write_file_(ObIlogFileBuilder& builder)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ilog_store  not init", K(ret));
  } else {
    char* buffer = NULL;
    int64_t buffer_size = 0;
    if (OB_FAIL(builder.get_file_buffer(buffer, buffer_size))) {
      CSR_LOG(ERROR, "builder get_file_buffer failed", K(ret));
    } else if (OB_FAIL(write_file_(next_ilog_file_id_, buffer, buffer_size))) {
      CSR_LOG(ERROR, "writer_file_ failed", K(ret), K(next_ilog_file_id_));
    } else {
      int64_t cost = ObTimeUtility::current_time() - start_ts;
      CSR_LOG(INFO, "success to write ilog file", K(next_ilog_file_id_), K(cost));
    }
  }
  return ret;
}

int ObIlogStore::check_need_dump_(bool& need_dump, int64_t curr_memstore_seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ilog_store not init", K(ret));
  } else {
    if (curr_memstore_seq - prev_consumed_memstore_seq_ > 1) {
      need_dump = true;
    } else {
      need_dump = false;
    }
  }
  return ret;
}

// 1. if reanme failed, it will kill observer
// 2. if push item to free_memstore_array failed, it will do same thing above
// 3. if remove item from free_memstore_array failed, it will do same thing above
int ObIlogStore::handle_different_trigger_type_(const FrozenMemstore& memstore_after_merge, const int64_t& end_idx,
    IlogMemstoreArray& free_memstore_array, ObIlogFileBuilder& builder)
{
  int ret = OB_SUCCESS;
  int64_t cost = OB_INVALID_ID;
  ObIlogFreezeTriggerType trigger_type = memstore_after_merge.trigger_type_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ilog store not inited", K(ret));
  } else if (!memstore_after_merge.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguent", K(ret), K(memstore_after_merge));
  } else if (!need_merge_frozen_memstore_array_by_trigger_type_(trigger_type) &&
             OB_FAIL(file_id_cache_->append(next_ilog_file_id_, builder.get_index_info_block_map()))) {
    CSR_LOG(ERROR, "file_id_cache_ append failed", K(ret), K(next_ilog_file_id_));
  } else {
    const int64_t begin = ObTimeUtility::current_time();
    WLockGuard guard(lock_);
    for (int64_t i = 0; i < end_idx && OB_SUCC(ret); i++) {
      ObIlogMemstore* memstore = frozen_memstore_array_[0].memstore_;
      if (OB_FAIL(free_memstore_array.push_back(memstore))) {
        CSR_LOG(ERROR, "frozen_memstore_array push_back failed", K(ret));
      } else if (OB_FAIL(frozen_memstore_array_.remove(0))) {
        CSR_LOG(ERROR, "frozen_memstore_array_ remove failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // insert memstore_after_merge into frozen_memstore_array_
      ObIlogMemstore* memstore = frozen_memstore_array_[0].memstore_;
      if (!need_merge_frozen_memstore_array_by_trigger_type_(trigger_type)) {
        if (OB_FAIL(free_memstore_array.push_back(memstore))) {
          CSR_LOG(ERROR, "free_memstore_array push back failed", K(ret), K(free_memstore_array), K(memstore));
        } else if (OB_FAIL(frozen_memstore_array_.remove(0))) {
          CSR_LOG(ERROR, "frozen_memstore_array_ remove failed", K(ret));
        } else if (trigger_type == OB_MERGE_NEED_SWITCH_FILE_TRIGGER_TYPE &&
                   OB_FAIL(free_memstore_array.push_back(memstore_after_merge.memstore_))) {
          CSR_LOG(ERROR, "free_memstore_array push_back failed", K(free_memstore_array), K(memstore_after_merge));
        }
      } else {
        frozen_memstore_array_[0] = memstore_after_merge;
        if (OB_FAIL(free_memstore_array.push_back(memstore))) {
          CSR_LOG(ERROR, "free_memstore_array push_back failed", K(ret), K(free_memstore_array), K(memstore));
        }
      }
    }

    while (OB_FAIL(ret)) {
      CLOG_LOG(ERROR, "unexpected error!!!", K(ret));
      sleep(1000 * 1000);
    }
    cost = ObTimeUtility::current_time() - begin;
  }
  CLOG_LOG(INFO,
      "runTimerTask cost time under wrlock protect",
      K(cost),
      K(ret),
      K(next_ilog_file_id_),
      K(free_memstore_array));

  if (OB_SUCC(ret)) {
    prev_consumed_memstore_seq_ = memstore_after_merge.seq_;
    if (!need_merge_frozen_memstore_array_by_trigger_type_(trigger_type)) {
      next_ilog_file_id_++;
    }
  }
  return ret;
}

bool ObIlogStore::need_merge_frozen_memstore_array_by_trigger_type_(const ObIlogFreezeTriggerType& trigger_type) const
{
  return trigger_type == OB_TIMER_TRIGGER_TYPE;
}

int ObIlogStore::do_merge_frozen_memstore_(const FrozenMemstoreArray& tmp_frozen_memstore_array,
    bool is_ilog_not_continous_trigger, FrozenMemstore& memstore_after_merge)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (memstore_after_merge.is_valid() || tmp_frozen_memstore_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(tmp_frozen_memstore_array), K(memstore_after_merge));
  } else {
    ObIlogMemstore* memstore = NULL;
    const int64_t tmp_size = tmp_frozen_memstore_array.count();
    (void)alloc_memstore_(memstore);

    MergeIlogMemstoreContainer merge_container;
    if (OB_FAIL(merge_container.init())) {
      CLOG_LOG(ERROR, "init merge_container failed", K(ret));
    }
    for (int64_t i = 0; i < tmp_size && OB_SUCC(ret); i++) {
      FrozenMemstore frozen_memstore = tmp_frozen_memstore_array[i];
      if (!frozen_memstore.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpect error, invalid frozen_memstore", K(ret), K(frozen_memstore));
      } else if (OB_FAIL(merge_container.merge_ilog_memstore_to_container(frozen_memstore.memstore_))) {
        if (ret == OB_EAGAIN) {
          WLockGuard guard(lock_);
          frozen_memstore_array_[i].trigger_type_ = OB_ILOG_NOT_CONTINOUS_TRIGGER_TYPE;
          CLOG_LOG(WARN,
              "log not continous in merge_frozen_memstore, need modify its trigger_type",
              K(ret),
              K(frozen_memstore),
              K(tmp_frozen_memstore_array));
        } else {
          CLOG_LOG(ERROR, "ilog_memstore_merge failed", K(ret), K(frozen_memstore), K(tmp_frozen_memstore_array));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(merge_container.transfer_to_ilog_memstore(memstore))) {
      CLOG_LOG(ERROR, "transfer_to_ilog_memstore failed", K(ret));
    }

    if (OB_SUCCESS != (tmp_ret = merge_container.destroy())) {
      CLOG_LOG(WARN, "destroy merge_container failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      bool need_switch_file_by_not_continous = is_ilog_not_continous_trigger;
      bool need_switch_file_by_ilog_or_clog = false;
      if (OB_FAIL(memstore->check_need_switch_file(need_switch_file_by_ilog_or_clog))) {
        CLOG_LOG(ERROR, "check_need_switch_file failed", K(ret));
      } else {
        int64_t seq = tmp_frozen_memstore_array[tmp_size - 1].seq_;
        ObIlogFreezeTriggerType trigger_type = OB_INVALID_TRIGGER_TYPE;
        if (need_switch_file_by_not_continous || need_switch_file_by_ilog_or_clog) {
          trigger_type = OB_MERGE_NEED_SWITCH_FILE_TRIGGER_TYPE;
        } else {
          trigger_type = OB_TIMER_TRIGGER_TYPE;
        }

        if (OB_FAIL(memstore_after_merge.set_frozen_memstore(trigger_type, memstore, seq))) {
          CLOG_LOG(ERROR, "set_frozen_memstore failed", K(memstore_after_merge), K(trigger_type), K(memstore), K(seq));
        }
      }
    }

    if (OB_FAIL(ret) && NULL != memstore) {
      memstore->destroy();
      op_reclaim_free(memstore);
      memstore = NULL;
    }
  }
  return ret;
}

void ObIlogStore::alloc_memstore_(ObIlogMemstore*& memstore)
{
  int tmp_ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  while (true) {
    if (NULL == (memstore = op_reclaim_alloc(ObIlogMemstore))) {
      CSR_LOG(ERROR, "op_reclaim_alloc failed, need retry");
    } else if (OB_SUCCESS != (tmp_ret = memstore->init())) {
      CSR_LOG(ERROR, "active_memstore_ init failed", K(tmp_ret));
      memstore->destroy();
      op_reclaim_free(memstore);
      memstore = NULL;
    } else {
      break;
    }
    usleep(1000);
  }
  int64_t cost = ObTimeUtility::current_time() - start_ts;
  CLOG_LOG(INFO, "alloc_memstore_ success", K(memstore), K(cost));
}

}  // namespace clog
}  // namespace oceanbase
