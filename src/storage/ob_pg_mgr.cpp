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

#define USING_LOG_PREFIX STORAGE

#include "storage/ob_pg_mgr.h"
#include "share/rc/ob_context.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_partition.h"
#include "observer/omt/ob_tenant_node_balancer.h"

namespace oceanbase {
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
namespace storage {

int ObIPGPartitionIterator::get_pg_partition_guard_array(
    ObIPartitionGroup* partition, ObPGPartitionGuardArray& pg_partition_guard_arr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(partition));
  } else {
    // iterate all partitions of current pg
    ObPartitionArray pkeys;
    if (OB_FAIL(partition->get_pg_storage().get_all_pg_partition_keys(pkeys, need_trans_table_))) {
      STORAGE_LOG(WARN, "get all pg partition keys error", K(ret));
    } else {
      ObPGPartitionGuard* pg_partition_guard = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
        if (NULL == (pg_partition_guard = op_alloc(ObPGPartitionGuard))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "alloc memory fail", K(ret), KP(pg_partition_guard));
        } else if (OB_FAIL(partition->get_pg_partition(pkeys.at(i), *pg_partition_guard))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "get pg partition error", K(ret), K(pkeys.at(i)));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_ISNULL(pg_partition_guard->get_pg_partition())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "pg partition is null", KP(pg_partition_guard->get_pg_partition()));
        } else if (OB_FAIL(pg_partition_guard_arr.push_back(pg_partition_guard))) {
          STORAGE_LOG(WARN, "push back pg partition error", K(ret), KP(pg_partition_guard));
        } else {
          pg_partition_guard = NULL;
        }
        if (NULL != pg_partition_guard) {
          op_free(pg_partition_guard);
          pg_partition_guard = NULL;
        }
      }
    }
  }

  return ret;
}

ObSinglePGPartitionIterator::ObSinglePGPartitionIterator() : pg_partition_guard_arr_(), array_idx_(0), is_inited_(false)
{}

ObSinglePGPartitionIterator::~ObSinglePGPartitionIterator()
{
  reset();
}

int ObSinglePGPartitionIterator::init(ObIPartitionGroup* partition, const bool need_trans_table)
{
  int ret = OB_SUCCESS;
  need_trans_table_ = need_trans_table;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSinglePGPartitionIterator has already been inited", K(ret));
  } else if (OB_FAIL(get_pg_partition_guard_array(partition, pg_partition_guard_arr_))) {
    LOG_WARN("fail to get pg partition guard array", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSinglePGPartitionIterator::get_next(ObPGPartition*& pg_partition)
{
  int ret = OB_SUCCESS;
  pg_partition = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSinglePGPartitionIterator has not been inited", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (array_idx_ < pg_partition_guard_arr_.count()) {
        pg_partition = pg_partition_guard_arr_.at(array_idx_)->get_pg_partition();
        array_idx_++;
        if (pg_partition->is_inited()) {
          break;
        } else {
          pg_partition = nullptr;
        }
      } else {
        break;
      }
    }
    if (OB_SUCC(ret) && nullptr == pg_partition) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

void ObSinglePGPartitionIterator::reset()
{
  for (int64_t i = 0; i < pg_partition_guard_arr_.count(); ++i) {
    if (NULL != pg_partition_guard_arr_.at(i)) {
      op_free(pg_partition_guard_arr_.at(i));
      pg_partition_guard_arr_.at(i) = NULL;
    }
  }
  pg_partition_guard_arr_.reset();
  array_idx_ = 0;
  is_inited_ = false;
}

ObPGPartitionIterator::ObPGPartitionIterator() : bucket_pos_(0), array_idx_(0), pg_mgr_(NULL)
{}

ObPGPartitionIterator::~ObPGPartitionIterator()
{
  reset();
}

void ObPGPartitionIterator::reset()
{
  if (OB_NOT_NULL(pg_mgr_)) {
    for (int64_t i = 0; i < pg_partition_guard_arr_.count(); ++i) {
      if (OB_NOT_NULL(pg_partition_guard_arr_.at(i))) {
        op_free(pg_partition_guard_arr_.at(i));
        pg_partition_guard_arr_.at(i) = NULL;
      }
    }
    bucket_pos_ = 0;
    array_idx_ = 0;
    pg_mgr_ = NULL;
  }
}

int ObPGPartitionIterator::next_pg_()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;

  if (OB_ISNULL(pg_mgr_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The partition service is NULL, ", K(ret));
  } else {
    if (pg_partition_guard_arr_.count() > 0) {
      for (int64_t i = 0; i < pg_partition_guard_arr_.count(); ++i) {
        if (OB_NOT_NULL(pg_partition_guard_arr_.at(i))) {
          op_free(pg_partition_guard_arr_.at(i));
          pg_partition_guard_arr_.at(i) = NULL;
        }
      }
      pg_partition_guard_arr_.reset();
    }
    // iterate pg partition guard arrry
    array_idx_ = 0;

    if (OB_ISNULL(pg_mgr_->partition_buckets_) || bucket_pos_ >= pg_mgr_->BUCKETS_CNT) {
      ret = OB_ITER_END;
    } else {
      if (OB_NOT_NULL(pg_mgr_->partition_buckets_[bucket_pos_])) {
        TCRLockGuard guard(pg_mgr_->buckets_lock_[bucket_pos_]);
        partition = pg_mgr_->partition_buckets_[bucket_pos_];
        // iterate all pgs in current bucket
        while (NULL != partition && OB_SUCC(ret)) {
          if (OB_FAIL(get_pg_partition_guard_array(partition, pg_partition_guard_arr_))) {
            LOG_WARN("fail to get pg partition guard array", K(ret));
          } else {
            // iterate next pg in current bucket
            partition = (ObIPartitionGroup*)partition->next_;
          }
        }
      }
      bucket_pos_++;
    }
  }

  return ret;
}

int ObPGPartitionIterator::get_next(ObPGPartition*& pg_partition)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pg_mgr_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The partition service is NULL, ", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (array_idx_ < pg_partition_guard_arr_.count()) {
        pg_partition = pg_partition_guard_arr_.at(array_idx_)->get_pg_partition();
        array_idx_++;
        if (pg_partition->is_inited()) {
          break;
        }
      } else if (OB_FAIL(next_pg_())) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "get next pg error", K(ret));
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

ObPartitionGroupIterator::ObPartitionGroupIterator() : partitions_(), bucket_pos_(0), array_idx_(0), pg_mgr_(NULL)
{}

ObPartitionGroupIterator::~ObPartitionGroupIterator()
{
  reset();
}

void ObPartitionGroupIterator::reset()
{
  if (OB_NOT_NULL(pg_mgr_)) {
    for (int64_t i = 0; i < partitions_.count(); ++i) {
      pg_mgr_->revert_pg(partitions_.at(i));
    }
    partitions_.reuse();
    bucket_pos_ = 0;
    array_idx_ = 0;
  }
}

int ObPartitionGroupIterator::get_next(ObIPartitionGroup*& partition)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pg_mgr_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The partition service is NULL, ", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (array_idx_ < partitions_.count()) {
        partition = partitions_.at(array_idx_);
        array_idx_++;
        break;
      } else {
        if (partitions_.count() > 0) {
          for (int64_t i = 0; i < partitions_.count(); ++i) {
            pg_mgr_->revert_pg(partitions_.at(i));
          }
          partitions_.reuse();
        }
        array_idx_ = 0;

        if (OB_ISNULL(pg_mgr_->partition_buckets_) || bucket_pos_ >= pg_mgr_->BUCKETS_CNT) {
          ret = OB_ITER_END;
        } else {
          if (OB_NOT_NULL(pg_mgr_->partition_buckets_[bucket_pos_])) {
            TCRLockGuard guard(pg_mgr_->buckets_lock_[bucket_pos_]);
            partition = pg_mgr_->partition_buckets_[bucket_pos_];

            while (OB_NOT_NULL(partition)) {
              if (OB_FAIL(partitions_.push_back(partition))) {
                STORAGE_LOG(WARN, "Fail to push partition to array, ", K(ret));
                break;
              } else {
                partition->inc_ref();
                partition = (ObIPartitionGroup*)partition->next_;
              }
            }
          }

          bucket_pos_++;
        }
      }
    }
  }
  return ret;
}

void ObPGMgr::reset()
{
  is_inited_ = false;
  partition_cnt_ = 0;
  pg_cnt_ = 0;
  stand_alone_partition_cnt_ = 0;
  partition_buckets_ = NULL;
  cp_fty_ = 0;
}

void ObPGMgr::destroy()
{
  if (OB_NOT_NULL(partition_buckets_)) {
    ObIPartitionGroup* partition = nullptr;
    ObIPartitionGroup* next_part = nullptr;
    for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
      partition = partition_buckets_[i];
      while (OB_NOT_NULL(partition)) {
        next_part = (ObIPartitionGroup*)partition->next_;
        revert_pg(partition);
        partition = next_part;
      }
    }
    ob_free(partition_buckets_);
    partition_buckets_ = NULL;
  }
  if (OB_NOT_NULL(buckets_lock_)) {
    ob_free(buckets_lock_);
    buckets_lock_ = nullptr;
  }
  is_inited_ = false;
}

int ObPGMgr::init(ObPartitionComponentFactory* cp_fty)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_PARTITION_SERVICE);
  void* buf = NULL;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPGMgr init twice", K(ret), KP(cp_fty));
  } else if (OB_ISNULL(cp_fty)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KP(cp_fty));
  } else if (OB_ISNULL(buckets_lock_ = (TCRWLock*)ob_malloc(sizeof(TCRWLock) * BUCKETS_CNT, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObIPartitionGroup*) * BUCKETS_CNT, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), LITERAL_K(BUCKETS_CNT));
  } else {
    cp_fty_ = cp_fty;
    MEMSET(buf, 0, sizeof(ObIPartitionGroup*) * BUCKETS_CNT);
    partition_buckets_ = new (buf) ObIPartitionGroup*[BUCKETS_CNT];
    for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
      new (buckets_lock_ + i) TCRWLock(ObLatchIds::DEFAULT_SPIN_RWLOCK, get_tcref());
    }
    is_inited_ = true;
  }

  return ret;
}

int ObPGMgr::add_pg(ObIPartitionGroup& partition, const bool need_check_tenant, const bool allow_multi_value)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* prev = NULL;
  ObIPartitionGroup* curr = NULL;
  const ObPartitionKey& pkey = partition.get_partition_key();
  const int64_t file_id = partition.get_file_id();
  const uint64_t tenant_id = pkey.get_tenant_id();

  STORAGE_LOG(INFO, "pg mgr add partition group", K(pkey), K(file_id), KP(&partition), "ref", partition.get_ref());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPGMgr not init", K(ret), K(pkey), K(need_check_tenant));
  } else {
    int64_t pos = pkey.hash() % BUCKETS_CNT;
    TCWLockGuard guard(buckets_lock_[pos]);
    curr = partition_buckets_[pos];
    while (OB_NOT_NULL(curr)) {
      if (curr->get_partition_key() == pkey) {
        if (allow_multi_value && ENABLE_RECOVER_ALL_ZONE) {
          if (curr->get_file_id() == file_id) {
            break;
          } else {
            prev = curr;
            curr = static_cast<ObIPartitionGroup*>(curr->next_);
          }
        } else {
          break;
        }
      } else {
        prev = curr;
        curr = static_cast<ObIPartitionGroup*>(curr->next_);
      }
    }

    if (OB_ISNULL(curr)) {
      if (need_check_tenant && !omt::ObTenantNodeBalancer::get_instance().is_tenant_exist(tenant_id)) {
        ret = OB_TENANT_NOT_IN_SERVER;
        STORAGE_LOG(WARN, "tenant not exists, cannot create partition", K(ret), K(tenant_id));
      } else {
        int64_t cnt = ATOMIC_AAF(&partition_cnt_, 1);
        int64_t pg_cnt = 0;
        int64_t stand_alone_partition_cnt = 0;
        if (pkey.is_pg()) {
          pg_cnt = ATOMIC_AAF(&pg_cnt_, 1);
        } else {
          stand_alone_partition_cnt = ATOMIC_AAF(&stand_alone_partition_cnt_, 1);
        }
        if (cnt > OB_MAX_PARTITION_NUM_PER_SERVER || pg_cnt > OB_MAX_PG_NUM_PER_SERVER ||
            stand_alone_partition_cnt > OB_MAX_SA_PARTITION_NUM_PER_SERVER) {
          ATOMIC_DEC(&partition_cnt_);
          if (pkey.is_pg()) {
            ATOMIC_DEC(&pg_cnt_);
          } else {
            ATOMIC_DEC(&stand_alone_partition_cnt_);
          }
          ret = OB_TOO_MANY_PARTITIONS_ERROR;
          STORAGE_LOG(WARN,
              "too many partitions",
              K(ret),
              K(cnt),
              K(pg_cnt),
              K(stand_alone_partition_cnt),
              K(OB_MAX_PARTITION_NUM_PER_SERVER));
        } else {
          int tmp_ret = ret;
          FETCH_ENTITY(TENANT_SPACE, tenant_id)
          {
            ObTenantStorageInfo* tenant_store_info = MTL_GET(ObTenantStorageInfo*);
            if (pkey.is_pg()) {
              ++tenant_store_info->pg_cnt_;
            } else {
              ++tenant_store_info->part_cnt_;
            }
          }
          ret = tmp_ret;

          partition.inc_ref();
          partition.next_ = partition_buckets_[pos];
          partition_buckets_[pos] = &partition;
        }
      }
    } else {
      ret = OB_ENTRY_EXIST;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(curr)) {
    revert_pg(curr);
  }
  return ret;
}

int ObPGMgr::del_pg(const ObPartitionKey& pkey, const int64_t* file_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* prev = NULL;
  ObIPartitionGroup* partition = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPGMgr not init", K(ret), K(pkey));
  } else {
    int64_t pos = pkey.hash() % BUCKETS_CNT;
    // remove partition from map
    TCWLockGuard guard(buckets_lock_[pos]);
    partition = partition_buckets_[pos];
    while (OB_NOT_NULL(partition)) {
      if (partition->get_partition_key() == pkey) {
        if (ENABLE_RECOVER_ALL_ZONE && nullptr != file_id) {
          if (*file_id == partition->get_file_id()) {
            break;
          } else {
            prev = partition;
            partition = (ObIPartitionGroup*)partition->next_;
          }
        } else {
          break;
        }
      } else {
        prev = partition;
        partition = (ObIPartitionGroup*)partition->next_;
      }
    }

    if (OB_ISNULL(partition)) {
      ret = OB_PARTITION_NOT_EXIST;
    } else {
      const int64_t file_id = partition->get_file_id();
      STORAGE_LOG(
          INFO, "partition service del partition", K(pkey), K(file_id), KP(partition), "ref", partition->get_ref());
      if (OB_ISNULL(prev)) {
        partition_buckets_[pos] = (ObIPartitionGroup*)partition->next_;
      } else {
        prev->next_ = partition->next_;
      }
      partition->next_ = NULL;
      del_pg_impl(partition);
    }
  }

  return ret;
}

void ObPGMgr::del_pg_impl(ObIPartitionGroup* pg)
{
  int ret = OB_SUCCESS;
  if (nullptr != pg) {
    const ObPartitionKey& pkey = pg->get_partition_key();
    ATOMIC_DEC(&partition_cnt_);
    if (pkey.is_pg()) {
      ATOMIC_DEC(&pg_cnt_);
    } else {
      ATOMIC_DEC(&stand_alone_partition_cnt_);
    }
    // update partition count for this tenant
    const uint64_t tenant_id = pkey.get_tenant_id();
    FETCH_ENTITY(TENANT_SPACE, tenant_id)
    {
      ObTenantStorageInfo* tenant_store_info = MTL_GET(ObTenantStorageInfo*);
      if (pkey.is_pg()) {
        --tenant_store_info->pg_cnt_;
      } else {
        --tenant_store_info->part_cnt_;
      }
    }
    revert_pg(pg);
  }
}

int ObPGMgr::get_pg(const common::ObPGKey& pg_key, const int64_t* file_id, ObIPartitionGroupGuard& guard) const
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* pg = NULL;
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPGMgr not init", K(ret), K(pg_key));
  } else {
    pos = pg_key.hash() % BUCKETS_CNT;
    TCRLockGuard bucket_guard(buckets_lock_[pos]);
    pg = partition_buckets_[pos];
    while (OB_NOT_NULL(pg)) {
      if (pg->get_partition_key() == pg_key) {
        if (ENABLE_RECOVER_ALL_ZONE && nullptr != file_id) {
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
      guard.set_partition_group(*this, *pg);
    }
  }
  return ret;
}

int ObPGMgr::remove_duplicate_pgs()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMgr has not been inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKETS_CNT; ++i) {
      TCWLockGuard bucket_guard(buckets_lock_[i]);
      if (nullptr != partition_buckets_[i]) {
        if (OB_FAIL(remove_duplicate_pg_in_linklist(partition_buckets_[i]))) {
          LOG_WARN("fail to remove same pg in linklist", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPGMgr::choose_preserve_pg(ObIPartitionGroup* left_pg, ObIPartitionGroup* right_pg, ObIPartitionGroup*& result_pg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == left_pg || nullptr == right_pg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(left_pg), KP(right_pg));
  } else {
    const ObReplicaType left_replica_type = left_pg->get_replica_type();
    const ObReplicaType right_replica_type = right_pg->get_replica_type();
    if (ObReplicaTypeCheck::is_writable_replica(left_replica_type)) {
      result_pg = left_pg;
    } else if (ObReplicaTypeCheck::is_writable_replica(right_replica_type)) {
      result_pg = right_pg;
    } else if (ObReplicaTypeCheck::is_readonly_replica(left_replica_type)) {
      result_pg = left_pg;
    } else if (ObReplicaTypeCheck::is_readonly_replica(right_replica_type)) {
      result_pg = right_pg;
    } else {
      result_pg = left_pg;
    }
  }
  return ret;
}

int ObPGMgr::remove_duplicate_pg_in_linklist(ObIPartitionGroup*& head)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<ObPartitionKey, ObIPartitionGroup*> effective_pg_map;
  const int64_t MAX_PG_CNT_IN_BUCKET = 10L;
  lib::ObLabel label("PGMGR_TMP_MAP");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMgr has not been inited", K(ret));
  } else if (OB_ISNULL(head)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(head));
  } else if (OB_FAIL(effective_pg_map.create(MAX_PG_CNT_IN_BUCKET, label))) {
    LOG_WARN("fail to create effetive pg hash map", K(ret));
  } else {
    ObIPartitionGroup* curr = head;
    ObIPartitionGroup* next = nullptr;
    bool has_same_pg = false;
    while (OB_SUCC(ret) && curr != nullptr) {
      ObIPartitionGroup* pg = nullptr;
      next = static_cast<ObIPartitionGroup*>(curr->next_);
      bool need_set = false;
      if (OB_FAIL(effective_pg_map.get_refactored(curr->get_partition_key(), pg))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(effective_pg_map.set_refactored(curr->get_partition_key(), curr))) {
            LOG_WARN("fail to set to effective pg map", K(ret));
          }
        }
      } else {
        ObIPartitionGroup* choose_pg = nullptr;
        has_same_pg = true;
        if (OB_FAIL(choose_preserve_pg(curr, pg, choose_pg))) {
          LOG_WARN("fail to choose preserve pg", K(ret));
        } else {
          if (choose_pg == curr) {
            if (OB_FAIL(effective_pg_map.set_refactored(curr->get_partition_key(), choose_pg, true /*overwrite*/))) {
              LOG_WARN("fail to set to effective pg map", K(ret));
            } else {
              del_pg_impl(pg);
            }
          } else {
            del_pg_impl(curr);
          }
        }
      }
      curr = next;
    }
    if (OB_SUCC(ret) && has_same_pg) {
      ObIPartitionGroup* prev = nullptr;
      for (ObHashMap<ObPartitionKey, ObIPartitionGroup*>::iterator iter = effective_pg_map.begin();
           iter != effective_pg_map.end();
           ++iter) {
        if (nullptr == prev) {
          head = iter->second;
          prev = head;
        } else {
          prev->next_ = iter->second;
          prev = iter->second;
        }
      }
      if (OB_SUCC(ret)) {
        prev->next_ = nullptr;
      }
    }
    effective_pg_map.destroy();
  }
  return ret;
}

};  // end namespace storage
};  // end namespace oceanbase
