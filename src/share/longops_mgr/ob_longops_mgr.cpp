// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX SHARE

#include "ob_longops_mgr.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{

namespace share
{
ObLongopsMgr::ObLongopsMgr()
  : is_inited_(false), allocator_(), bucket_lock_(), map_()
{
}

ObLongopsMgr &ObLongopsMgr::get_instance()
{
  static ObLongopsMgr longops_mgr;
  return longops_mgr;
}

int ObLongopsMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = 100 * 1024L * 1024L; // 100MB
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLongopsMgr has been inited", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(DEFAULT_BUCKET_NUM))) {
    LOG_WARN("failed to init bucket lock", K(ret));
  } else if (OB_FAIL(map_.create(DEFAULT_BUCKET_NUM, "ObLongopsMgr"))) {
    LOG_WARN("failed to init resource map", K(ret));
  } else if (OB_FAIL(allocator_.init(DEFAULT_ALLOCATOR_PAGE_SIZE,
                                     lib::ObLabel("LongopsMgr"),
                                     OB_SERVER_TENANT_ID,
                                     memory_limit))) {
    LOG_WARN("failed to init allocator", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObLongopsMgr::destroy()
{
  if (map_.created()) {
    map_.destroy();
  }
}

void ObLongopsMgr::free_longops(ObILongopsStat *stat)
{
  stat->~ObILongopsStat();
  allocator_.free(stat);
}

int ObLongopsMgr::register_longops(ObILongopsStat *stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongopsMgr has not been inited", K(ret));
  } else if (OB_ISNULL(stat) || OB_UNLIKELY(!stat->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(stat));
  } else {
    ObBucketHashWLockGuard guard(bucket_lock_, stat->get_longops_key().hash());
    if (OB_FAIL(map_.set_refactored(stat->get_longops_key(), stat))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_ENTRY_EXIST;
      }
    } else {
      LOG_INFO("register longops finish", K(ret), K(*stat));
    }
  }
  return ret;
}

int ObLongopsMgr::unregister_longops(ObILongopsStat *stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongopsMgr has not been inited", K(ret));
  } else if (OB_ISNULL(stat) || OB_UNLIKELY(!stat->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(stat));
  } else {
    ObBucketHashWLockGuard guard(bucket_lock_, stat->get_longops_key().hash());
    ObILongopsKey key = stat->get_longops_key();
    if (OB_FAIL(map_.erase_refactored(stat->get_longops_key()))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to erase map", K(ret), KPC(stat));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        free_longops(stat);
      }
    } else {
      free_longops(stat);
    }
    LOG_INFO("unregister longops finish", K(ret), K(key));
  }
  return ret;
}

int ObLongopsMgr::get_longops(const ObILongopsKey &key, ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  ObILongopsStat *stat = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongopsMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    ObBucketHashRLockGuard guard(bucket_lock_, key.hash());
    if (OB_FAIL(map_.get_refactored(key, stat))) {
      LOG_WARN("failed to get key", K(ret), K(key));
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_ISNULL(stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("longops stat is null", K(ret));
    } else if (OB_FAIL(stat->get_longops_value(value))) {
      LOG_WARN("failed to get longops value", K(ret), KPC(stat));
    }
  }
  return ret;
}

int ObLongopsMgr::begin_iter(ObLongopsIterator &iter)
{
  int ret = OB_SUCCESS;
  iter.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongopsMgr has not been inited", K(ret));
  } else if (OB_FAIL(iter.init(this))) {
    LOG_WARN("failed to init longops iter", K(ret));
  }
  return ret;
}

template <typename Callback>
int ObLongopsMgr::foreach(Callback &callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongopsMgr has not been inited", K(ret));
  } else if (OB_FAIL(map_.foreach_refactored(callback))) {
    LOG_WARN("fail to foreach map", K(ret));
  }
  return ret;
}

ObLongopsIterator::ObKeySnapshotCallback::ObKeySnapshotCallback(
    ObIArray<ObILongopsKey> &key_snapshot)
  : key_snapshot_(key_snapshot)
{
}

int ObLongopsIterator::ObKeySnapshotCallback::operator()(PAIR &pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_snapshot_.push_back(pair.first))) {
    LOG_WARN("fail to push back key", K(ret));
  }
  return ret;
}


ObLongopsIterator::ObLongopsIterator()
  : is_inited_(false), key_snapshot_(), key_cursor_(0), longops_mgr_(nullptr)
{
}

ObLongopsIterator::~ObLongopsIterator()
{
  reset();
}

void ObLongopsIterator::reset()
{
  key_snapshot_.reset();
  key_cursor_ = 0;
  longops_mgr_ = nullptr;
  is_inited_ = false;
}

int ObLongopsIterator::init(ObLongopsMgr *longops_mgr)
{
  int ret = OB_SUCCESS;
  ObKeySnapshotCallback callback(key_snapshot_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLongopsIterator has been inited twice", K(ret));
  } else if (OB_ISNULL(longops_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(longops_mgr));
  } else if (OB_FAIL(longops_mgr->foreach(callback))) {
    LOG_WARN("failed to do foreach map", K(ret));
  } else {
    key_cursor_ = 0;
    longops_mgr_ = longops_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObLongopsIterator::get_next(const uint64_t tenant_id, ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLongopsIterator has not been inited", K(ret));
  } else {
    bool need_retry = true;
    while (OB_SUCC(ret) && need_retry && key_cursor_ < key_snapshot_.count()) {
      const ObILongopsKey &key = key_snapshot_.at(key_cursor_);
      if (!is_sys_tenant(tenant_id) && key.tenant_id_ != tenant_id) {
        // Normal user tenants can only check their own longops tasks.
      } else if (OB_FAIL(longops_mgr_->get_longops(key, value))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to get parition stat", K(ret), K(key));
        } else {
          need_retry = true;
          ret = OB_SUCCESS;
        }
      } else {
        need_retry = false;
      }
      ++key_cursor_;
    }

    if (OB_SUCC(ret)) {
      // reach the end, but get no longops record.
      ret = need_retry && key_cursor_ >= key_snapshot_.count() ? OB_ITER_END : ret;
    }
  }
  return ret;
}

} //end namespace share
} //end namespace oceanbase
