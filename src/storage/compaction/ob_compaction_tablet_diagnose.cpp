/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_compaction_tablet_diagnose.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace compaction
{
/*
 * ObDiagnoseTabletMgr implement
 * */
int ObDiagnoseTabletMgr::mtl_init(ObDiagnoseTabletMgr *&diagnose_tablet_mgr)
{
  return diagnose_tablet_mgr->init();
}

ObDiagnoseTabletMgr::ObDiagnoseTabletMgr()
  : is_inited_(false),
    diagnose_tablet_map_(),
    diagnose_lock_(common::ObLatchIds::COMPACTION_DIAGNOSE_LOCK)
{}

int ObDiagnoseTabletMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDiagnoseTabletMgr has already been initiated", K(ret));
  } else if (OB_FAIL(diagnose_tablet_map_.create(MAX_DIAGNOSE_TABLET_BUCKET_NUM, "DiaTabletMap", "DiaTabletNode", MTL_ID()))) {
    LOG_WARN("Fail to create diagnose tablet map", K(ret));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

void ObDiagnoseTabletMgr::destroy()
{
  if (diagnose_tablet_map_.created()) {
    (void)diagnose_tablet_map_.destroy();
  }
}

// for diagnose
int ObDiagnoseTabletMgr::add_diagnose_tablet(
  const share::ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const share::ObDiagnoseTabletType type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDiagnoseTabletMgr is not init", K(ret));
  } else {
    if (!ls_id.is_valid() || !tablet_id.is_valid()
        || share::ObDiagnoseTabletType::TYPE_SPECIAL > type
        || share::ObDiagnoseTabletType::TYPE_DIAGNOSE_TABLET_MAX <= type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(type));
    } else {
      lib::ObMutexGuard guard(diagnose_lock_);
      ObDiagnoseTablet diagnose_tablet(ls_id, tablet_id);
      int64_t flag = 0;
      if (OB_FAIL(diagnose_tablet_map_.get_refactored(diagnose_tablet, flag))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get diagnose tablet from map", K(ret), K(diagnose_tablet));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (ObDiagnoseTablet::is_flagged(flag, type)) {
        ret = OB_HASH_EXIST;
      }
      if (OB_SUCC(ret)) {
        ObDiagnoseTablet::set_flag(flag, type);
        if (OB_FAIL(diagnose_tablet_map_.set_refactored(diagnose_tablet, flag, 1))) {
          LOG_WARN("fail to add diagnose tablet into map", K(ret), K(diagnose_tablet), K(flag));
        }
      } else if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDiagnoseTabletMgr::get_diagnose_tablets(ObIArray<ObDiagnoseTablet> &diagnose_tablets)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDiagnoseTabletMgr is not init", K(ret));
  } else {
    lib::ObMutexGuard guard(diagnose_lock_);
    for (DiagnoseTabletMap::iterator iter = diagnose_tablet_map_.begin();
        OB_SUCC(ret) && iter != diagnose_tablet_map_.end(); ++iter) {
      if (OB_FAIL(diagnose_tablets.push_back(iter->first))) {
        LOG_WARN("fail to get diagnose tablet", K(ret));
      }
    }
  }
  return ret;
}

int ObDiagnoseTabletMgr::delete_diagnose_tablet(
  const share::ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const share::ObDiagnoseTabletType type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
   LOG_WARN("ObDiagnoseTabletMgr is not init", K(ret));
  } else {
    if (!ls_id.is_valid() || !tablet_id.is_valid()
        || share::ObDiagnoseTabletType::TYPE_SPECIAL > type
        || share::ObDiagnoseTabletType::TYPE_DIAGNOSE_TABLET_MAX <= type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(type));
    } else {
      lib::ObMutexGuard guard(diagnose_lock_);
      ObDiagnoseTablet diagnose_tablet(ls_id, tablet_id);
      int64_t flag = 0;
      if (OB_FAIL(diagnose_tablet_map_.get_refactored(diagnose_tablet, flag))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get diagnose tablet from map", K(ret), K(diagnose_tablet));
        }
      } else {
        ObDiagnoseTablet::del_flag(flag, type);
        if (0 == flag) {
          if (OB_FAIL(diagnose_tablet_map_.erase_refactored(diagnose_tablet))) {
            LOG_WARN("fail to delete diagnose tablet", K(ret), K(diagnose_tablet));
          }
        } else if (OB_FAIL(diagnose_tablet_map_.set_refactored(diagnose_tablet, flag, 1))) {
          LOG_WARN("fail to add diagnose tablet into map", K(ret), K(diagnose_tablet), K(flag));
        }
      }
    }
  }
  return ret;
}

void ObDiagnoseTabletMgr::remove_diagnose_tablets(ObIArray<ObDiagnoseTablet> &tablets)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    LOG_WARN_RET(tmp_ret, "ObDiagnoseTabletMgr is not init", K(tmp_ret));
  } else {
    lib::ObMutexGuard guard(diagnose_lock_);
    for (int64_t i = 0; i < tablets.count(); ++i) {
      const ObDiagnoseTablet &tablet = tablets.at(i);
      if (OB_TMP_FAIL(diagnose_tablet_map_.erase_refactored(tablet))) {
        LOG_WARN_RET(tmp_ret, "fail to delete diagnose tablet", K(tmp_ret), K(tablet));
      }
    }
  }
}

}
}
