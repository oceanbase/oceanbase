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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_backup_tablet_reorganize_helper.h"
#include "share/ob_tablet_reorganize_history_table_operator.h"

using namespace oceanbase::common::hash;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace share
{

/* ObTabletReorganizeInfo */

ObTabletReorganizeInfo::ObTabletReorganizeInfo()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    src_tablet_id_(),
    dest_tablet_id_()
{
}

ObTabletReorganizeInfo::~ObTabletReorganizeInfo()
{
}

void ObTabletReorganizeInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  src_tablet_id_.reset();
  dest_tablet_id_.reset();
}

bool ObTabletReorganizeInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && src_tablet_id_.is_valid()
      && dest_tablet_id_.is_valid();
}

/* ObReorganizeTablet */

ObReorganizeTablet::ObReorganizeTablet()
  : tablet_id_(),
    children_()
{
}

ObReorganizeTablet::~ObReorganizeTablet()
{
}

void ObReorganizeTablet::reset()
{
  tablet_id_.reset();
  children_.reset();
}

int ObReorganizeTablet::set(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
  }
  return ret;
}

bool ObReorganizeTablet::is_valid() const
{
  return tablet_id_.is_valid();
}

/* ObBackupTabletReorganizeHelper */

int ObBackupTabletReorganizeHelper::check_tablet_has_reorganized(common::ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id, const common::ObTabletID &tablet_id, share::ObLSID &ls_id, bool &reorganized)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletReorganizeHistoryTableOperator::check_tablet_has_reorganized(
      sql_proxy, tenant_id, tablet_id, ls_id, reorganized))) {
    LOG_WARN("failed to check tablet has reorganized", K(ret), K(tenant_id), K(tablet_id), K(ls_id));
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::get_leaf_children(ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id, const common::ObTabletID &tablet_id, const share::ObLSID &ls_id,
    common::ObIArray<ObTabletID> &descendent_list)
{
  int ret = OB_SUCCESS;
  descendent_list.reset();
  ObArray<ObTabletReorganizeInfo> history_infos;
  if (!tablet_id.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(get_all_tablet_reorganize_history_infos_(sql_proxy, tenant_id, ls_id, history_infos))) {
    LOG_WARN("failed to get all tablet reorganize history infos", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_leaf_children_from_history(tenant_id, history_infos, tablet_id, descendent_list))) {
    LOG_WARN("failed to get leaf children from history", K(ret), K(history_infos), K(tablet_id));
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::get_leaf_children(ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id, const common::ObIArray<common::ObTabletID> &tablet_list,
    const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &descendent_list)
{
  int ret = OB_SUCCESS;
  descendent_list.reset();
  ObHashSet<ObTabletID> tablet_set;
  ObArray<ObTabletReorganizeInfo> history_infos;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id));
  } else if (OB_FAIL(tablet_set.create(DEFAULT_BUCKET_SIZE))) {
    LOG_WARN("failed to create tablet set", K(ret));
  } else if (OB_FAIL(get_all_tablet_reorganize_history_infos_(sql_proxy, tenant_id, ls_id, history_infos))) {
    LOG_WARN("failed to get all tablet reorganize history infos", K(ret), K(tenant_id), K(ls_id));
  } else {
    ARRAY_FOREACH_X(tablet_list, idx, cnt, OB_SUCC(ret)) {
      const common::ObTabletID &tablet_id = tablet_list.at(idx);
      common::ObArray<common::ObTabletID> leaf_children;
      if (OB_FAIL(get_leaf_children_from_history(tenant_id, history_infos, tablet_id, leaf_children))) {
        LOG_WARN("failed to get all children", K(ret), K(tenant_id), K(tablet_id));
      } else if (OB_FAIL(put_array_to_set_(leaf_children, tablet_set))) {
        LOG_WARN("failed to put array to set", K(ret), K(leaf_children));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(convert_set_to_array_(tablet_set, descendent_list))) {
        LOG_WARN("failed to convert set to array", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::get_ls_to_tablet_reorganize_info_map(ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id, const common::hash::ObHashSet<share::ObLSID> &ls_id_set,
    common::hash::ObHashMap<share::ObLSID, common::ObArray<ObTabletReorganizeInfo>> &tablet_reorganize_ls_map)
{
  int ret = OB_SUCCESS;
  FOREACH_X(iter, ls_id_set, OB_SUCC(ret)) {
    const share::ObLSID &ls_id = iter->first;
    ObArray<ObTabletReorganizeInfo> tablet_reorganize_info_array;
    if (OB_FAIL(get_all_tablet_reorganize_history_infos_(sql_proxy, tenant_id, ls_id, tablet_reorganize_info_array))) {
      LOG_WARN("failed to get all tablet reorganize history infos", K(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(tablet_reorganize_ls_map.set_refactored(ls_id, tablet_reorganize_info_array, 1/*cover exist object*/))) {
      LOG_WARN("failed to set_refactored", K(ret), K(ls_id));
    }
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::get_leaf_children_from_history(const uint64_t tenant_id,
    const common::ObIArray<ObTabletReorganizeInfo> &history_infos, const common::ObTabletID &tablet_id,
    common::ObIArray<common::ObTabletID> &descendent_list)
{
  int ret = OB_SUCCESS;
  ObReorganizeTablet *root = NULL;
  ObHashSet<ObTabletID> tablet_set;
  if (OB_FAIL(tablet_set.create(DEFAULT_BUCKET_SIZE))) {
    LOG_WARN("failed to create tablet set", K(ret));
  } else if (OB_FAIL(alloc_reorganize_tablet_info_(tablet_id, root))) {
    LOG_WARN("failed to alloc reorganize tablet info", K(ret), K(tablet_id));
  } else if (OB_FAIL(build_tree_from_history_(history_infos, root))) {
    LOG_WARN("failed to build tree from history", K(ret), K(history_infos), KPC(root));
  } else if (OB_FAIL(get_leaf_tablets_(root, tablet_set))) {
    LOG_WARN("failed to get leaf tablets", K(ret), K(root));
  } else if (OB_FAIL(convert_set_to_array_(tablet_set, descendent_list))) {
    LOG_WARN("failed to convert set to array", K(ret));
  }
  free_and_delete_node_(root);
  return ret;
}

int ObBackupTabletReorganizeHelper::get_all_tablet_reorganize_history_infos_(
    ObMySQLProxy &sql_proxy, const uint64_t tenant_id, const share::ObLSID &ls_id,
    common::ObIArray<ObTabletReorganizeInfo> &history_infos)
{
  int ret = OB_SUCCESS;
  ObArray<ReorganizeTabletPair> tablet_pairs;
  if (OB_FAIL(ObTabletReorganizeHistoryTableOperator::get_all_split_tablet_pairs(
      sql_proxy, tenant_id, ls_id, tablet_pairs))) {
    LOG_WARN("failed to get all split tablet pairs", K(ret));
  } else {
    ObTabletReorganizeInfo info;
    ARRAY_FOREACH_X(tablet_pairs, idx, cnt, OB_SUCC(ret)) {
      const ReorganizeTabletPair &pair = tablet_pairs.at(idx);
      info.tenant_id_ = tenant_id;
      info.ls_id_ = ls_id;
      info.src_tablet_id_ = pair.src_tablet_id_;
      info.dest_tablet_id_ = pair.dest_tablet_id_;
      if (OB_FAIL(history_infos.push_back(info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::alloc_reorganize_tablet_info_(
    const common::ObTabletID &tablet_id, ObReorganizeTablet *&tablet)
{
  int ret = OB_SUCCESS;
  tablet = NULL;
  void *buf = NULL;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(tablet_id));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObReorganizeTablet), ObModIds::BACKUP))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (OB_ISNULL(tablet = new (buf) ObReorganizeTablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to new tablet", K(ret));
  } else if (OB_FAIL(tablet->set(tablet_id))) {
    LOG_WARN("failed to set tablet", K(ret), K(tablet_id));
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::build_tree_from_history_(
    const common::ObIArray<ObTabletReorganizeInfo> &history_infos,
    ObReorganizeTablet *parent)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent should not be null", K(ret));
  } else {
    ARRAY_FOREACH_X(history_infos, idx, cnt, OB_SUCC(ret)) {
      const ObTabletReorganizeInfo &reorganize_info = history_infos.at(idx);
      if (reorganize_info.src_tablet_id_ == parent->tablet_id_) {
        ObReorganizeTablet *child = NULL;
        if (OB_FAIL(alloc_reorganize_tablet_info_(reorganize_info.dest_tablet_id_, child))) {
          LOG_WARN("failed to alloc reorganize tablet info", K(ret), K(reorganize_info));
        } else if (OB_FAIL(parent->children_.push_back(child))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(build_tree_from_history_(history_infos, child))) {
          LOG_WARN("failed to build tree from history", K(ret), K(history_infos));
        }
      }
    }
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::get_leaf_tablets_(
    const ObReorganizeTablet *tablet, hash::ObHashSet<common::ObTabletID> &tablet_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be null", K(ret));
  } else if (tablet->children_.empty()) {
    const common::ObTabletID &tablet_id = tablet->tablet_id_;
    if (OB_FAIL(tablet_set.set_refactored(tablet_id))) {
      LOG_WARN("failed to set tablet id", K(ret), K(tablet_id));
    }
  } else {
    const ObArray<ObReorganizeTablet *> &children = tablet->children_;
    ARRAY_FOREACH_X(children, idx, cnt, OB_SUCC(ret)) {
      const ObReorganizeTablet *reorganize_tablet = children.at(idx);
      if (OB_FAIL(get_leaf_tablets_(reorganize_tablet, tablet_set))) {
        LOG_WARN("failed to get leaf tablets", K(ret), KPC(reorganize_tablet));
      }
    }
  }
  return ret;
}

void ObBackupTabletReorganizeHelper::free_and_delete_node_(ObReorganizeTablet *tablet)
{
  if (OB_ISNULL(tablet)) {
    return;
  }

  for (int64_t i = 0; i < tablet->children_.count(); ++i) {
    free_and_delete_node_(tablet->children_.at(i));
  }

  tablet->~ObReorganizeTablet();
  mtl_free(tablet);
  tablet = NULL;
}

int ObBackupTabletReorganizeHelper::convert_set_to_array_(
    const common::hash::ObHashSet<ObTabletID> &tablet_set,
    common::ObIArray<ObTabletID> &tablet_list)
{
  int ret = OB_SUCCESS;
  FOREACH_X(iter, tablet_set, OB_SUCC(ret)) {
    const ObTabletID &tablet_id = iter->first;
    if (OB_FAIL(tablet_list.push_back(tablet_id))) {
      LOG_WARN("failed to push back", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObBackupTabletReorganizeHelper::put_array_to_set_(
    const common::ObIArray<common::ObTabletID> &tablet_list,
    common::hash::ObHashSet<ObTabletID> &tablet_set)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(tablet_list, idx, cnt, OB_SUCC(ret)) {
    const common::ObTabletID &tablet_id = tablet_list.at(idx);
    if (OB_FAIL(tablet_set.set_refactored(tablet_id))) {
      LOG_WARN("failed to set tablet id", K(ret), K(tablet_id));
    }
  }
  return ret;
}

}
}
