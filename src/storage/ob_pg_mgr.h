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

#ifndef OCEANBASE_STORAGE_OB_PG_MGR_
#define OCEANBASE_STORAGE_OB_PG_MGR_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "common/ob_partition_key.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/ob_pg_memory_garbage_collector.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroup;
class ObPGPartition;
class ObPGPartitionGuard;
class ObIPartitionGroupGuard;
class ObPGMgr {
public:
  friend class ObPartitionGroupIterator;
  friend class ObPGPartitionIterator;
  ObPGMgr()
      : is_inited_(false),
        partition_cnt_(0),
        pg_cnt_(0),
        stand_alone_partition_cnt_(0),
        partition_buckets_(nullptr),
        buckets_lock_(nullptr),
        cp_fty_(nullptr)
  {
    reset();
  }
  ~ObPGMgr()
  {
    destroy();
  }
  void reset();
  void destroy();
  int init(ObPartitionComponentFactory* cp_fty);
  // allow_multi_true is used during replay
  int add_pg(ObIPartitionGroup& partition, const bool need_check_tenant, const bool allow_multi_value);
  int del_pg(const common::ObPartitionKey& pkey, const int64_t* file_id);
  int get_pg(const common::ObPartitionKey& pkey, const int64_t* file_id, ObIPartitionGroupGuard& guard) const;
  OB_INLINE void revert_pg(ObIPartitionGroup* pg) const;
  template <typename Function>
  int operate_pg(const common::ObPartitionKey& pkey, const int64_t* file_id, Function& fn);
  bool is_empty() const
  {
    return 0 == partition_cnt_;
  }
  int64_t get_pg_count() const
  {
    return ATOMIC_LOAD(&pg_cnt_);
  }
  int64_t get_total_partition_count() const
  {
    return ATOMIC_LOAD(&partition_cnt_);
  }
  int64_t get_stand_alone_partition_count() const
  {
    return ATOMIC_LOAD(&stand_alone_partition_cnt_);
  }
  static TCRef& get_tcref()
  {
    static TCRef tcref(16);
    return tcref;
  }
  int remove_duplicate_pgs();

private:
  OB_INLINE void free_pg(ObIPartitionGroup* pg) const;
  void del_pg_impl(ObIPartitionGroup* pg);
  int choose_preserve_pg(ObIPartitionGroup* left_pg, ObIPartitionGroup* right_pg, ObIPartitionGroup*& result_pg);
  int remove_duplicate_pg_in_linklist(ObIPartitionGroup*& head);

private:
  static const bool ENABLE_RECOVER_ALL_ZONE = false;
  bool is_inited_;
  // total pg + total standalone partition
  int64_t partition_cnt_;
  // total pg in current server
  int64_t pg_cnt_;
  // total standalone partition in current server
  int64_t stand_alone_partition_cnt_;
  ObIPartitionGroup** partition_buckets_;
  const static int64_t BUCKETS_CNT = 1 << 18;
  TCRWLock* buckets_lock_;
  ObPartitionComponentFactory* cp_fty_;
};

// iterate all pgs and all standalone partitions
class ObIPartitionGroupIterator {
public:
  ObIPartitionGroupIterator()
  {}
  virtual ~ObIPartitionGroupIterator()
  {}
  virtual int get_next(ObIPartitionGroup*& partition) = 0;
};

class ObPartitionGroupIterator : public ObIPartitionGroupIterator {
public:
  ObPartitionGroupIterator();
  virtual ~ObPartitionGroupIterator();
  virtual int get_next(ObIPartitionGroup*& partition);
  void reset();
  void set_pg_mgr(ObPGMgr& pg_mgr)
  {
    pg_mgr_ = &pg_mgr;
  }

private:
  common::ObArray<ObIPartitionGroup*> partitions_;
  int64_t bucket_pos_;
  int64_t array_idx_;
  ObPGMgr* pg_mgr_;
};

class ObIPartitionArrayGuard {
public:
  ObIPartitionArrayGuard() : pg_mgr_(nullptr), partitions_()
  {}
  virtual ~ObIPartitionArrayGuard()
  {
    reuse();
  }
  void set_pg_mgr(const ObPGMgr &pg_mgr)
  {
    pg_mgr_ = &pg_mgr;
  }
  int push_back(ObIPartitionGroup *partition)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(pg_mgr_)) {
      ret = common::OB_NOT_INIT;
    } else if (OB_SUCC(partitions_.push_back(partition))) {
      partition->inc_ref();
    }
    return ret;
  }
  ObIPartitionGroup *at(int64_t i)
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
  TO_STRING_KV(KP_(pg_mgr), K_(partitions));

private:
  const ObPGMgr *pg_mgr_;
  common::ObSEArray<ObIPartitionGroup *, OB_DEFAULT_PARTITION_KEY_COUNT> partitions_;
  DISALLOW_COPY_AND_ASSIGN(ObIPartitionArrayGuard);
};

typedef common::ObSEArray<ObPGPartitionGuard*, 16> ObPGPartitionGuardArray;

// iterate all pg partition in current server
class ObIPGPartitionIterator {
public:
  ObIPGPartitionIterator() : need_trans_table_(false), pg_guard_arr_()
  {}
  virtual ~ObIPGPartitionIterator()
  {}
  virtual int get_next(ObPGPartition*& pg_partition) = 0;
  virtual void set_pg_mgr(ObPGMgr& pg_mgr)
  {
    pg_guard_arr_.set_pg_mgr(pg_mgr);
  }
  int get_pg_partition_guard_array(ObIPartitionGroup* partition, ObPGPartitionGuardArray& pg_partition_guard_arr);

protected:
  bool need_trans_table_;
  ObIPartitionArrayGuard pg_guard_arr_;
};

class ObSinglePGPartitionIterator : public ObIPGPartitionIterator {
public:
  ObSinglePGPartitionIterator();
  virtual ~ObSinglePGPartitionIterator();
  int init(ObPGMgr* pg_mgr, ObIPartitionGroup* pg, const bool need_trans_table = false);
  void reset();
  virtual int get_next(ObPGPartition*& pg_partition);
  ObPGPartitionGuardArray& get_pg_guard_array()
  {
    return pg_partition_guard_arr_;
  }

private:
  ObPGPartitionGuardArray pg_partition_guard_arr_;
  int64_t array_idx_;
  bool is_inited_;
};

class ObPGPartitionIterator : public ObIPGPartitionIterator {
public:
  ObPGPartitionIterator();
  virtual ~ObPGPartitionIterator();
  virtual int get_next(ObPGPartition*& pg_partition);
  void reset();
  virtual void set_pg_mgr(ObPGMgr& pg_mgr) override
  {
    pg_mgr_ = &pg_mgr;
    pg_guard_arr_.set_pg_mgr(pg_mgr);
  }

private:
  int next_pg_();

private:
  int64_t bucket_pos_;
  ObPGPartitionGuardArray pg_partition_guard_arr_;
  int64_t array_idx_;
  ObPGMgr* pg_mgr_;
};

class ObIPartitionGroupGuard {
public:
  ObIPartitionGroupGuard() : pg_mgr_(nullptr), pg_(nullptr)
  {}
  ObIPartitionGroupGuard(const ObPGMgr &pg_mgr, ObIPartitionGroup &pg) : pg_mgr_(nullptr), pg_(nullptr)
  {
    set_partition_group(pg_mgr, pg);
  }
  virtual ~ObIPartitionGroupGuard()
  {
    reset();
  }
  void set_partition_group(const ObPGMgr& pg_mgr, ObIPartitionGroup& pg)
  {
    reset();
    pg_mgr_ = &pg_mgr;
    pg_ = &pg;
    pg_->inc_ref();
  }
  void reset()
  {
    if (OB_NOT_NULL(pg_mgr_) && OB_NOT_NULL(pg_)) {
      pg_mgr_->revert_pg(pg_);
      pg_mgr_ = nullptr;
      pg_ = nullptr;
    }
  }
  inline ObIPartitionGroup* get_partition_group()
  {
    return pg_;
  }

private:
  const ObPGMgr* pg_mgr_;
  ObIPartitionGroup* pg_;
  DISALLOW_COPY_AND_ASSIGN(ObIPartitionGroupGuard);
};

OB_INLINE void ObPGMgr::free_pg(ObIPartitionGroup* pg) const
{
  int ret = OB_SUCCESS;
  bool can_free = false;
  pg->clear();
  if (OB_FAIL(pg->check_can_free(can_free))) {
    STORAGE_LOG(WARN, "fail to check pg can free", K(ret));
  } else if (can_free) {
    cp_fty_->free(pg);
  }
  if (!can_free) {
    STORAGE_LOG(WARN, "pg can not free now", K(pg), KPC(pg));
    ObPGMemoryGarbageCollector::get_instance().add_pg(pg);
  }
}

OB_INLINE void ObPGMgr::revert_pg(ObIPartitionGroup* pg) const
{
  if (OB_NOT_NULL(pg) && OB_NOT_NULL(cp_fty_)) {
    if (0 == pg->dec_ref()) {
      free_pg(pg);
    }
  }
}

template <typename Function>
int ObPGMgr::operate_pg(const common::ObPartitionKey& pkey, const int64_t* file_id, Function& fn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPGMgr not init", K(ret), K(pkey));
  } else {
    const int64_t pos = pkey.hash() % BUCKETS_CNT;
    ObIPartitionGroup* pg = partition_buckets_[pos];
    TCRLockGuard bucket_guard(buckets_lock_[pos]);
    while (OB_NOT_NULL(pg)) {
      if (pg->get_partition_key() == pkey) {
        if (OB_NOT_NULL(file_id)) {
          if (*file_id == pg->get_file_id()) {
            break;
          } else {
            pg = static_cast<ObIPartitionGroup*>(pg->next_);
          }
        } else {
          break;
        }
      } else {
        pg = static_cast<ObIPartitionGroup*>(pg->next_);
      }
    }
    if (OB_ISNULL(pg)) {
      ret = OB_PARTITION_NOT_EXIST;
    } else {
      ret = fn(pkey, file_id, pg);
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PG_MGR_
