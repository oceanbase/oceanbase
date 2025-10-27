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
#define USING_LOG_PREFIX STORAGE

#include "storage/meta_store/ob_storage_meta_io_util.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#endif

namespace oceanbase
{
namespace storage
{
#ifdef OB_BUILD_SHARED_STORAGE
int ObStorageMetaIOUtil::check_meta_existence(
    const blocksstable::ObStorageObjectOpt &opt, const int64_t ls_epoch, bool &is_exist)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId object_id;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get object id", K(ret), K(opt));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_is_exist_object(object_id, ls_epoch, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(object_id));
  }
  return ret;
}
#endif

/*static*/int ObStorageMetaIOUtil::collect_private_tablet_versions(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const common::ObTabletID &tablet_id,
    const int64_t transfer_seq,
    const int64_t start_version,
    const int64_t end_version,
    common::ObIArray<int64_t> &tablet_versions)
{
  int ret = OB_SUCCESS;
  static const int64_t LIST_THRESHOLD = 10;
  bool is_exist = false;
  bool use_list = false;
  tablet_versions.reset();
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for SN mode", K(ret), K(common::lbt()));
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else if (OB_UNLIKELY(!ls_id.is_valid()
                       || (ls_epoch < 0)
                       || !tablet_id.is_valid()
                       || (transfer_seq < 0)
                       || (start_version < 0)
                       || (end_version < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id), K(transfer_seq),
             K(start_version), K(end_version));
  } else if (OB_UNLIKELY(start_version > end_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected version", KR(ret), K(start_version), K(end_version));
  } else if ((end_version - start_version + 1) < LIST_THRESHOLD) {
    for (int64_t ver = start_version; OB_SUCC(ret) && (ver <= end_version); ++ver) {
      blocksstable::ObStorageObjectOpt opt;
      opt.set_ss_private_tablet_meta_object_opt(ls_id.id(), tablet_id.id(), ver, transfer_seq);
      if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, ls_epoch, is_exist))) {
        LOG_WARN("failed to check meta existence", KR(ret), K(opt), K(ls_epoch));
      } else if (is_exist) {
        if (OB_FAIL(tablet_versions.push_back(ver))) {
          LOG_WARN("failed to push_back", KR(ret), K(ver));
        }
      }
    }
  } else {
    use_list = true;
    ObTenantFileManager *tfm = nullptr;
    if (OB_ISNULL(tfm = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager should not be null", KR(ret));
    } else if (OB_FAIL(tfm->list_private_tablet_meta(ls_id.id(), ls_epoch, tablet_id.id(),
                                                     transfer_seq, tablet_versions))) {
      LOG_WARN("failed to list private tablet meta", KR(ret), K(ls_id), K(ls_epoch), K(tablet_id),
               K(transfer_seq));
    } else {
      const int64_t total = tablet_versions.count();
      int64_t cur = 0;
      for (int64_t i = 0; OB_SUCC(ret) && (i < total); ++i) {
        const int64_t ver = tablet_versions.at(i);
        if ((ver >= start_version) && (ver <= end_version)) {
          tablet_versions.at(cur) = ver;
          ++cur;
        } else if (ver < start_version) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected existing tablet version(tablet might leak)", KR(ret),
                    K(tablet_versions), K(start_version), K(end_version));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < (total - cur)); ++i) {
        tablet_versions.pop_back();
      }
    }
  }
  int64_t cost_time_us = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("collect tablet meta version finished", KR(ret), K(cost_time_us), K(use_list),
           K(start_version), K(end_version), "gc_versions", tablet_versions);
#endif
  return ret;
}
} // namespace storage
} // namespace oceanbase