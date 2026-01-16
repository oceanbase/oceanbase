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

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_copy_dependency_mgr.h"

namespace oceanbase
{
namespace storage
{

ObTabletCopyDependencyMgr::ObTabletCopyDependencyMgr()
  : is_inited_(false),
    lock_(),
    dependency_info_allocator_(),
    ready_info_allocator_(sizeof(ReadyTabletInfo), ObMemAttr(MTL_ID(), "ReadyTblInfo")),
    is_ready_(false),
    dependency_map_(),
    ready_tablets_(),
    ready_tablets_cnt_(0)
{
}

ObTabletCopyDependencyMgr::~ObTabletCopyDependencyMgr()
{
  destroy();
}

int ObTabletCopyDependencyMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUCKET_NUM = 8192;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet copy dependency mgr init twice", K(ret));
  } else if (OB_FAIL(dependency_info_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                     ObMemAttr(MTL_ID(), "TabletDepInfo")))) {
    LOG_WARN("failed to init dependency info allocator", K(ret));
  } else if (OB_FAIL(dependency_map_.create(MAX_BUCKET_NUM, "TbltDepMapBkt", "TbltDepMapNode", MTL_ID()))) {
    LOG_WARN("failed to create dependency map", K(ret));
  } else {
    is_ready_ = false;
    is_inited_ = true;

    LOG_INFO("tablet copy dependency mgr init success", K(ret));
  }
  return ret;
}

void ObTabletCopyDependencyMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_INFO("tablet copy dependency mgr do not init, no need to reset");
  } else {
    common::SpinWLockGuard guard(lock_);
    inner_reset_();
  }
}

int ObTabletCopyDependencyMgr::destroy()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    LOG_INFO("tablet copy dependency mgr has not been inited, no need to destroy", K_(is_inited));
  } else {
    common::SpinWLockGuard guard(lock_);
    inner_reset_();
    if (OB_FAIL(dependency_map_.destroy())) {
      LOG_WARN("failed to destroy dependency map", K(ret));
    }
    is_inited_ = false;
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("failed to destroy tablet copy dependency mgr", K(ret));
  }

  return ret;
}

int ObTabletCopyDependencyMgr::add_independent_tablet(
  const ObTabletID &tablet_id,
  const int64_t transfer_seq,
  const ObCopyTabletStatus::STATUS status,
  const int64_t data_size)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || transfer_seq < 0 || !ObCopyTabletStatus::is_valid(status) || data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add_independent_tablet get invalid argument", K(ret), K(tablet_id), K(transfer_seq), K(status), K(data_size));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObLogicTabletID logic_tablet_id;
    TabletDependencyInfo *tablet_dep_info = nullptr;

    if (is_ready_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet copy dependency mgr is ready, cannot add independent tablet", K(ret), K(is_ready_));
    } else if (OB_FAIL(get_dependency_info_(tablet_id, tablet_dep_info))) {
      LOG_WARN("failed to get dependency info", K(ret), K(tablet_id));
    } else if (!tablet_dep_info->is_fake_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet info is not fake (already exist), cannot add independent tablet", K(ret), KPC(tablet_dep_info));
    } else if (OB_FAIL(logic_tablet_id.init(tablet_id, transfer_seq))) {
      LOG_WARN("failed to init logic tablet id", K(ret), K(tablet_id));
    } else {
      tablet_dep_info->logic_tablet_id_ = logic_tablet_id;
      tablet_dep_info->status_ = status;
      tablet_dep_info->data_size_ = data_size;
      tablet_dep_info->is_fake_ = false;

      LOG_INFO("add independent tablet", K(tablet_id), K(transfer_seq), K(status), K(data_size));
    }

  }

  return ret;
}

int ObTabletCopyDependencyMgr::add_dependent_tablet_pair(
  const ObTabletID &tablet_id,
  const ObTabletID &parent_id,
  const int64_t transfer_seq,
  const ObCopyTabletStatus::STATUS status,
  const int64_t data_size)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()
          || !parent_id.is_valid()
          || transfer_seq < 0
          || !ObCopyTabletStatus::is_valid(status)
          || data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add_dependent_tablet_pair get invalid argument", K(ret), K(tablet_id), K(parent_id), K(transfer_seq), K(status), K(data_size));
  } else if (tablet_id == parent_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent id is equal to child id", K(ret), K(parent_id), "child_id", tablet_id);
  } else {
    common::SpinWLockGuard guard(lock_);
    TabletDependencyInfo *parent_info = nullptr;
    TabletDependencyInfo *child_info = nullptr;

    if (is_ready_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet copy dependency mgr is ready, cannot add dependent tablet pair", K(ret), K(is_ready_));
    } else if (OB_FAIL(get_dependency_info_(parent_id, parent_info))) {
      // get parent info, if not exist, create a fake parent
      LOG_WARN("failed to get parent info", K(ret), K(parent_id));
    } else if (OB_FAIL(get_dependency_info_(tablet_id, child_info))) {
      // get child info, if not exist, create a new child
      // if exist, it must be a fake node, update it
      LOG_WARN("failed to get child info", K(ret), K(tablet_id));
    } else if (!child_info->is_fake_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child info is not fake", K(ret), KPC(child_info));
    } else if (OB_FAIL(child_info->logic_tablet_id_.init(tablet_id, transfer_seq))) {
      LOG_WARN("failed to init logic tablet id", K(ret), K(tablet_id));
    } else if (FALSE_IT(child_info->status_ = status)) {
    } else if (FALSE_IT(child_info->data_size_ = data_size)) {
    } else if (FALSE_IT(child_info->is_fake_ = false)) {
    } else if (OB_FAIL(add_dependency_(parent_info, child_info))) {
      // add dependency between parent and child
      LOG_WARN("failed to add dependency", K(ret), KPC(parent_info), KPC(child_info));
    } else {
      LOG_INFO("add dependent tablet pair", K(parent_id), K(tablet_id), K(transfer_seq), K(status), K(data_size));
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::remove_tablet_dependency(const ObTabletID &parent_id)
{
  int ret = OB_SUCCESS;
  TabletDependencyInfo *tablet_dep_info = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else if (!parent_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove_tablet_dependency get invalid argument", K(ret), K(parent_id));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(dependency_map_.get_refactored(parent_id, tablet_dep_info))) {
      LOG_WARN("failed to get parent info", K(ret), K(parent_id));
    } else if (OB_ISNULL(tablet_dep_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent info is null", K(ret), K(parent_id));
    } else if (tablet_dep_info->in_degree_ > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent tablet has dependency, cannot remove", K(ret), KPC(tablet_dep_info));
    } else if (OB_FAIL(remove_parent_dependency_(parent_id, true /*need_push_ready_queue*/))){
      LOG_WARN("failed to remove parent dependency", K(ret), K(parent_id));
    } else {
      LOG_INFO("succeed to remove tablet dependency", K(parent_id));
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::fetch_ready_tablet_group(
  const int64_t tablet_count_threshold,
  const int64_t tablet_group_size_threshold,
  common::ObIArray<ObLogicTabletID> &tablet_group_ids)
{
  int ret = OB_SUCCESS;
  tablet_group_ids.reset();
  int64_t tablet_group_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else if (tablet_count_threshold < 0 || tablet_group_size_threshold < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_count_threshold), K(tablet_group_size_threshold));
  } else {
    common::SpinWLockGuard guard(lock_);

    if (!is_ready_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet copy dependency mgr is not ready", K(ret), K(is_ready_));
    } else if (OB_FAIL(tablet_group_ids.reserve(tablet_count_threshold))) {
      LOG_WARN("failed to reserve tablet group ids", K(ret), K(tablet_count_threshold));
    } else {
      ReadyTabletInfo *ready_tablet = nullptr;

      while (OB_SUCC(ret) && !ready_tablets_.is_empty()) {
        ready_tablet = nullptr;
        if (tablet_count_threshold != common::OB_INVALID_COUNT
         && tablet_group_ids.count() >= tablet_count_threshold) {
          LOG_INFO("reach tablet count threshold", K(tablet_group_ids.count()), K(tablet_count_threshold));
          break;
        } else if (OB_FAIL(get_first_ready_tablet_(ready_tablet))) {
          LOG_WARN("failed to get first ready tablet", K(ret));
        } else if (tablet_group_ids.count() > 0
                && tablet_group_size + ready_tablet->data_size_ > tablet_group_size_threshold) {
          LOG_INFO("reach tablet group size threshold",
            "tablet_count", tablet_group_ids.count(), K(tablet_group_size), K(tablet_group_size_threshold));
          break;
        } else if (FALSE_IT(tablet_group_size += ready_tablet->data_size_)) {
        } else if (OB_FAIL(pop_ready_tablet_(ready_tablet))) { // won't fail while there is no concurrent pop
          LOG_WARN("failed to pop ready tablet", K(ret), KP(ready_tablet));
        } else if (OB_FAIL(tablet_group_ids.push_back(ready_tablet->logic_tablet_id_))) { // won't fail because array memory is already reserved
          LOG_WARN("failed to push back ready tablet", K(ret), KPC(ready_tablet));
        } else {
          free_ready_tablet_(ready_tablet);
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("fetch ready tablet group", "tablet_count", tablet_group_ids.count(), K(tablet_group_ids));
      }
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::refresh_ready_tablets()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (is_ready_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet copy dependency mgr is ready, no need to refresh", K(ret), K(is_ready_));
    } else {
      ObArray<ObTabletID> fake_tablet_ids;

      // remove all fake tablet
      FOREACH_X(it, dependency_map_, OB_SUCC(ret)) {
        TabletDependencyInfo *tablet_dep_info = it->second;
        if (OB_ISNULL(tablet_dep_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet dependency info is null", K(ret), KPC(tablet_dep_info));
        } else if (tablet_dep_info->is_fake_) {
          if (OB_FAIL(fake_tablet_ids.push_back(it->first))) {
            LOG_WARN("failed to push back fake tablet id", K(ret), KPC(tablet_dep_info));
          }
        }
      }

      // remove fake tablet dependency
      LOG_DEBUG("remove fake tablet dependency", K(fake_tablet_ids));
      for (int64_t idx = 0; OB_SUCC(ret) && idx < fake_tablet_ids.count(); ++idx) {
        ObTabletID fake_tablet_id = fake_tablet_ids.at(idx);
        TabletDependencyInfo *fake_tablet_info = nullptr;
        if (OB_FAIL(dependency_map_.get_refactored(fake_tablet_id, fake_tablet_info))) {
          LOG_WARN("failed to get fake tablet info", K(ret), K(fake_tablet_id));
        } else if (OB_ISNULL(fake_tablet_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fake tablet info is null", K(ret), K(fake_tablet_id));
        } else if (OB_FAIL(remove_parent_dependency_(fake_tablet_id, false /*need_push_ready_queue*/))) {
          // remove fake tablet from map, won't push tablet to ready queue (check and push later)
          LOG_WARN("failed to remove parent dependency", K(ret), K(fake_tablet_id));
        }
      }

      // add all ready tablet to ready queue
      FOREACH_X(it, dependency_map_, OB_SUCC(ret)) {
        TabletDependencyInfo *tablet_dep_info = it->second;
        if (OB_ISNULL(tablet_dep_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet dependency info is null", K(ret), KPC(tablet_dep_info));
        } else if (tablet_dep_info->in_degree_ == 0) {
          ReadyTabletInfo *ready_tablet = nullptr;
          if (OB_FAIL(construct_ready_tablet_(
              tablet_dep_info->logic_tablet_id_, tablet_dep_info->status_, tablet_dep_info->data_size_, ready_tablet))) {
            LOG_WARN("failed to construct ready tablet", K(ret), KPC(tablet_dep_info));
          } else if (OB_FAIL(push_ready_tablet_(ready_tablet))) {
            LOG_WARN("failed to push ready tablet", K(ret), KPC(ready_tablet));
          } else {
            LOG_DEBUG("push ready tablet", KPC(tablet_dep_info), KPC(ready_tablet));
          }

          if (OB_FAIL(ret)) {
            // allocate success but push failed
            if (OB_NOT_NULL(ready_tablet)) {
              free_ready_tablet_(ready_tablet);
            }
          }
        } else {
          LOG_DEBUG("tablet is not ready, has parent", KPC(tablet_dep_info));
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("refresh ready tablets finish", K(ret));
        is_ready_ = true;
      }
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::get_tablet_array(common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  tablet_ids.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    ObTabletID tablet_id;
    FOREACH_X(it, dependency_map_, OB_SUCC(ret)) {
      tablet_id = it->first;
      if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::check_is_done(bool &is_done) const
{
  int ret = OB_SUCCESS;
  is_done = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy dependency mgr do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    is_done = dependency_map_.empty() && ready_tablets_.is_empty();
  }

  return ret;
}

int64_t ObTabletCopyDependencyMgr::get_tablet_count() const
{
  return dependency_map_.size();
}

void ObTabletCopyDependencyMgr::print_diagnosis_info() const
{
  int ret = OB_SUCCESS;

  ObArray<ObTabletID> unfinished_tablets;
  if (IS_NOT_INIT) {
    LOG_INFO("tablet copy dependency mgr do not init, cannot print diagnosis info");
  } else {
    common::SpinRLockGuard guard(lock_);
    int64_t iter_cnt = 0;
    FOREACH_X(it, dependency_map_, OB_SUCC(ret)) {
      TabletDependencyInfo *tablet_dep_info = it->second;
      if (OB_ISNULL(tablet_dep_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet dependency info is null", K(ret), KPC(tablet_dep_info));
      } else if (OB_FAIL(unfinished_tablets.push_back(it->first))) {
        LOG_WARN("failed to push back tablet id", K(ret), KPC(tablet_dep_info));
      }
      // only sample 1000 tablets
      if (++iter_cnt >= 1000) {
        LOG_INFO("[TABLET_COPY_DEP_MGR] reach sample limit, stop iteration", K(iter_cnt));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("[TABLET_COPY_DEP_MGR] diagnosis info", K(ret),
          "rest_tablets_count", dependency_map_.size(),
          "ready_tablets_count", ready_tablets_cnt_,
          "unfinished_tablets_count", dependency_map_.size(),
          "sample_unfinished_tablets_count", unfinished_tablets.count(),
          "sample_unfinished_tablets", unfinished_tablets);
    } else {
      LOG_WARN("failed to print diagnosis info", K(ret));
    }
  }
}

ObTabletCopyDependencyMgr::TabletDependencyInfo::TabletDependencyInfo(common::ObIAllocator &allocator)
  : is_fake_(false),
    logic_tablet_id_(),
    status_(ObCopyTabletStatus::STATUS::MAX_STATUS),
    data_size_(-1),
    in_degree_(-1),
    child_tablet_infos_(allocator)
{
}

void ObTabletCopyDependencyMgr::TabletDependencyInfo::reset()
{
  is_fake_ = false;
  logic_tablet_id_.reset();
  status_ = ObCopyTabletStatus::STATUS::MAX_STATUS;
  data_size_ = -1;
  in_degree_ = -1;
  child_tablet_infos_.reset();
}

bool ObTabletCopyDependencyMgr::TabletDependencyInfo::is_valid() const
{
  bool bool_ret = false;
  if (is_fake_) {
    bool_ret = true;
  } else {
    bool_ret = logic_tablet_id_.is_valid() && ObCopyTabletStatus::is_valid(status_) && data_size_ >= 0 && in_degree_ >= 0;
  }
  return bool_ret;
}

ObTabletCopyDependencyMgr::ReadyTabletInfo::ReadyTabletInfo()
  : logic_tablet_id_(),
    status_(ObCopyTabletStatus::STATUS::MAX_STATUS),
    data_size_(-1)
{
}

void ObTabletCopyDependencyMgr::ReadyTabletInfo::reset()
{
  logic_tablet_id_.reset();
  status_ = ObCopyTabletStatus::STATUS::MAX_STATUS;
  data_size_ = -1;
  ObLink::reset();
}

bool ObTabletCopyDependencyMgr::ReadyTabletInfo::is_valid() const
{
  return logic_tablet_id_.is_valid() && ObCopyTabletStatus::is_valid(status_) && data_size_ >= 0;
}

void ObTabletCopyDependencyMgr::inner_reset_()
{
  reset_dependency_map_();
  reset_ready_queue_();
  ready_info_allocator_.destroy();
  dependency_info_allocator_.reset();
  is_ready_ = false;
}

void ObTabletCopyDependencyMgr::reuse()
{
  reset_dependency_map_();
  reset_ready_queue_();
  is_ready_ = false;
}

void ObTabletCopyDependencyMgr::reset_dependency_map_()
{
  int ret = OB_SUCCESS;

  // ret != OB_SUCCESS won't break the loop
  TabletDependencyInfo *tablet_dep_info = nullptr;
  FOREACH(it, dependency_map_) {
    tablet_dep_info = it->second;
    if (OB_ISNULL(tablet_dep_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet dependency info is null", K(ret), KPC(tablet_dep_info));
    } else if (FALSE_IT(tablet_dep_info->reset())) {
    } else {
      free_dependency_info_(tablet_dep_info);
    }
  }
  dependency_map_.clear();
  if(OB_FAIL(ret)) {
    LOG_ERROR("error occurred while resetting dependency map", K(ret));
  }
}

void ObTabletCopyDependencyMgr::reset_ready_queue_()
{
  int ret = OB_SUCCESS;

  ReadyTabletInfo *ready_tablet = nullptr;
  while (!ready_tablets_.is_empty()) {
    ready_tablet = nullptr;
    if (OB_FAIL(pop_ready_tablet_(ready_tablet))) {
      LOG_WARN("failed to pop ready tablet", K(ret), KP(ready_tablet));
    }

    if (OB_NOT_NULL(ready_tablet)) {
      free_ready_tablet_(ready_tablet);
    }
  }

  if(OB_FAIL(ret)) {
    LOG_ERROR("error occurred while resetting ready queue", K(ret));
  }
}

int ObTabletCopyDependencyMgr::get_dependency_info_(const ObTabletID &tablet_id, TabletDependencyInfo *&tablet_dep_info)
{
  int ret = OB_SUCCESS;
  tablet_dep_info = nullptr;

  if (OB_FAIL(dependency_map_.get_refactored(tablet_id, tablet_dep_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // if not exist, create a fake node
      if (OB_FAIL(prepare_fake_dependency_info_(tablet_dep_info))) {
        LOG_WARN("failed to prepare fake dependency info", K(ret), K(tablet_id));
      } else if (OB_FAIL(dependency_map_.set_refactored(tablet_id, tablet_dep_info))) {
        LOG_WARN("failed to set fake dependency info", K(ret), K(tablet_id));
        if (OB_NOT_NULL(tablet_dep_info)) {
          // prepare success, set map failed, free memory
          free_dependency_info_(tablet_dep_info);
        }
      }
    } else {
      LOG_WARN("failed to get dependency info", K(ret), K(tablet_id));
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::prepare_fake_dependency_info_(TabletDependencyInfo *&tablet_dep_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(alloc_dependency_info_(tablet_dep_info))) {
    LOG_WARN("failed to alloc dependency info", K(ret));
  } else {
    tablet_dep_info->is_fake_ = true;
    tablet_dep_info->in_degree_ = 0;
  }

  return ret;
}

int ObTabletCopyDependencyMgr::alloc_dependency_info_(TabletDependencyInfo *&tablet_dep_info)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  TabletDependencyInfo *tmp_value = nullptr;
  tablet_dep_info = nullptr;

  if (OB_ISNULL(buf = dependency_info_allocator_.alloc(sizeof(TabletDependencyInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(tmp_value = new (buf) TabletDependencyInfo(dependency_info_allocator_))) {
  } else {
    tablet_dep_info = tmp_value;
    tmp_value = nullptr;
  }

  if (OB_NOT_NULL(tmp_value)) {
    free_dependency_info_(tmp_value);
  }

  return ret;
}

void ObTabletCopyDependencyMgr::free_dependency_info_(TabletDependencyInfo *&tablet_dep_info)
{
  if (OB_NOT_NULL(tablet_dep_info)) {
    tablet_dep_info->~TabletDependencyInfo();
    dependency_info_allocator_.free(tablet_dep_info);
    tablet_dep_info = nullptr;
  }
}

int ObTabletCopyDependencyMgr::add_dependency_(TabletDependencyInfo *parent_info, TabletDependencyInfo *child_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(parent_info) || OB_ISNULL(child_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add dependency get invalid argument", K(ret), KPC(parent_info), KPC(child_info));
  } else if (OB_FAIL(parent_info->child_tablet_infos_.push_back(child_info))) {
    LOG_WARN("failed to add child info", K(ret), KPC(parent_info), KPC(child_info));
  } else {
    child_info->in_degree_ += 1;
  }

  return ret;
}

int ObTabletCopyDependencyMgr::remove_parent_dependency_(const ObTabletID &parent_id, const bool need_push_ready_queue)
{
  int ret = OB_SUCCESS;
  TabletDependencyInfo *parent_info = nullptr;

  if (OB_FAIL(dependency_map_.get_refactored(parent_id, parent_info))) {
    LOG_WARN("failed to get parent info", K(ret), K(parent_id));
  } else {
    ObArray<ReadyTabletInfo *> ready_tablet_infos;
    TabletDependencyInfo *child_info = nullptr;
    ReadyTabletInfo *ready_tablet_info = nullptr;

    // push all ready tablet infos to ready tablet infos array
    FOREACH_X(it, parent_info->child_tablet_infos_, OB_SUCC(ret)) {
      child_info = *it;
      ready_tablet_info = nullptr;
      if (OB_ISNULL(child_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child info is null", K(ret), KPC(child_info), KPC(parent_info));
      } else if (need_push_ready_queue && child_info->in_degree_ == 1) {
        if (OB_FAIL(construct_ready_tablet_(
            child_info->logic_tablet_id_, child_info->status_, child_info->data_size_, ready_tablet_info))) {
          LOG_WARN("failed to construct ready tablet info", K(ret), KPC(child_info), KPC(parent_info));
        } else if (OB_FAIL(ready_tablet_infos.push_back(ready_tablet_info))) {
          LOG_WARN("failed to push ready tablet info", K(ret), KPC(child_info), KPC(parent_info));
        }

        if (OB_FAIL(ret)) {
          // allocate success but push failed
          if (OB_NOT_NULL(ready_tablet_info)) {
            free_ready_tablet_(ready_tablet_info);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_push_ready_queue) {
        ReadyTabletInfo *ready_tablet_info = nullptr;
        FOREACH_X(it, ready_tablet_infos, OB_SUCC(ret)) {
          ready_tablet_info = *it;
          // won't fail if ready_tablet_info is not null
          if (OB_FAIL(push_ready_tablet_(ready_tablet_info))) {
            LOG_WARN("failed to push ready tablet info", K(ret), KPC(ready_tablet_info));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // decrease in degree of all child tablet infos
        FOREACH_X(it, parent_info->child_tablet_infos_, OB_SUCC(ret)) {
          child_info = *it;
          // won't fail if child info is not null
          if (OB_FAIL(remove_dependency_(child_info))) {
            LOG_WARN("failed to remove dependency", K(ret), KPC(child_info), KPC(parent_info));
          }
        }
      }

      // after erase parent info from map, should not fail
      if (FAILEDx(dependency_map_.erase_refactored(parent_id))) { // won't fail if hash exist
        LOG_WARN("failed to erase parent info", K(ret), K(parent_id));
      } else {
        free_dependency_info_(parent_info);
      }
    }

    if (OB_FAIL(ret)) {
      // if failed, free all ready tablet infos which are already allocated
      ReadyTabletInfo *ready_tablet_info = nullptr;
      FOREACH(it, ready_tablet_infos) {
        ready_tablet_info = *it;
        if (OB_NOT_NULL(ready_tablet_info)) {
          free_ready_tablet_(ready_tablet_info);
        }
      }
    }
  }

  return ret;
}

int ObTabletCopyDependencyMgr::remove_dependency_(TabletDependencyInfo *child_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(child_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove dependency get invalid argument", K(ret), KPC(child_info));
  } else if (child_info->in_degree_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in degree is less than 1", K(ret), KPC(child_info));
  } else  {
    child_info->in_degree_ -= 1;
  }

  return ret;
}


int ObTabletCopyDependencyMgr::push_ready_tablet_(ReadyTabletInfo *ready_tablet)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ready_tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("push ready tablet get invalid argument", K(ret), KPC(ready_tablet));
  } else if (OB_FAIL(ready_tablets_.push(ready_tablet))) {
    LOG_WARN("failed to push ready tablet", K(ret), KPC(ready_tablet));
  } else {
    ready_tablets_cnt_ += 1;
  }

  return ret;
}

int ObTabletCopyDependencyMgr::pop_ready_tablet_(ReadyTabletInfo *&ready_tablet)
{
  int ret = OB_SUCCESS;
  ObLink *link = nullptr;
  ready_tablet = nullptr;

  if (OB_FAIL(ready_tablets_.pop(link))) {
    LOG_WARN("failed to pop ready tablet", K(ret));
  } else if (FALSE_IT(ready_tablets_cnt_ -= 1)) {
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ready tablet is null", K(ret), KP(link));
  } else if (OB_ISNULL(ready_tablet = static_cast<ReadyTabletInfo *>(link))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ready tablet", K(ret), KP(link), KP(ready_tablet));
  }

  return ret;
}

int ObTabletCopyDependencyMgr::get_first_ready_tablet_(ReadyTabletInfo *&ready_tablet)
{
  int ret = OB_SUCCESS;
  ObLink *link = nullptr;

  if (OB_FAIL(ready_tablets_.top(link))) {
    LOG_WARN("failed to get first ready tablet", K(ret));
  } else if (FALSE_IT(ready_tablet = static_cast<ReadyTabletInfo *>(link))) {
  } else if (OB_ISNULL(ready_tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ready tablet is null", K(ret), KP(ready_tablet));
  }

  return ret;
}

int ObTabletCopyDependencyMgr::construct_ready_tablet_(
  const ObLogicTabletID &logic_tablet_id,
  const ObCopyTabletStatus::STATUS status,
  const int64_t data_size,
  ReadyTabletInfo *&ready_tablet)
{
  int ret = OB_SUCCESS;
  ready_tablet = nullptr;

  if (OB_FAIL(alloc_ready_tablet_(ready_tablet))) {
    LOG_WARN("failed to alloc ready tablet", K(ret), K(logic_tablet_id));
  } else {
    ready_tablet->logic_tablet_id_ = logic_tablet_id;
    ready_tablet->status_ = status;
    ready_tablet->data_size_ = data_size;
  }

  return ret;
}

int ObTabletCopyDependencyMgr::alloc_ready_tablet_(ReadyTabletInfo *&ready_tablet)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ReadyTabletInfo *tmp_value = nullptr;
  ready_tablet = nullptr;

  if (OB_ISNULL(buf = ready_info_allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(tmp_value = new (buf) ReadyTabletInfo())) {
  } else {
    ready_tablet = tmp_value;
    tmp_value = nullptr;
  }

  if (OB_NOT_NULL(tmp_value)) {
    free_ready_tablet_(tmp_value);
  }

  return ret;
}

void ObTabletCopyDependencyMgr::free_ready_tablet_(ReadyTabletInfo *&ready_tablet)
{
  if (OB_NOT_NULL(ready_tablet)) {
    ready_tablet->~ReadyTabletInfo();
    ready_info_allocator_.free(ready_tablet);
    ready_tablet = nullptr;
  }
}

}
}
