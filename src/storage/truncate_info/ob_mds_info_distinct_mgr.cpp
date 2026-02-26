//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/compaction/ob_mds_filter_info.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/truncate_info/ob_truncate_info_kv_cache.h"
namespace oceanbase
{
using namespace compaction;
namespace storage
{
ERRSIM_POINT_DEF(EN_COMPACTION_SKIP_MDS_WHEN_CHECK);
/*
 * ObMdsInfoDistinctMgr
 * */
ObMdsInfoDistinctMgr::ObMdsInfoDistinctMgr()
    : array_(),
      distinct_array_(),
      is_inited_(false)
{
  distinct_array_.set_attr(ObMemAttr(MTL_ID(), "TrunDisArr"));
}

int ObMdsInfoDistinctMgr::init(
    ObArenaAllocator &allocator,
    storage::ObTablet &tablet,
    const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObVersionRange &read_version_range,
    const bool for_access)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_UNLIKELY(!read_version_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_version_range));
  } else if (OB_FAIL(tablet.read_truncate_info_array(allocator, read_version_range, for_access, array_))) {
    LOG_WARN("failed to read truncate info array", KR(ret), K(read_version_range));
  } else if (OB_FAIL(read_split_truncate_info_array(split_extra_tablet_handles, read_version_range, for_access))) {
    LOG_WARN("failed to read split extra truncate infos", K(ret), K(split_extra_tablet_handles));
  } else if (OB_FAIL(build_distinct_array(read_version_range, for_access))) {
    LOG_WARN("failed to build distinct array", KR(ret));
  } else {
    if (!distinct_array_.empty()
        && read_version_range.base_version_ <= tablet.get_last_major_snapshot_version()
        && TRUN_SRC_KV_CACHE != array_.get_src()) { // try to put into kv cache
      const ObTabletID &tablet_id = tablet.get_tablet_id();
      int tmp_ret = OB_SUCCESS;
      const ObTruncateInfoCacheKey cache_key(
        MTL_ID(),
        tablet_id,
        distinct_array_.at(distinct_array_.count() - 1)->schema_version_,
        tablet.get_last_major_snapshot_version());
      if (OB_TMP_FAIL(ObTruncateInfoKVCacheUtil::put_truncate_info_array(cache_key, distinct_array_))) {
        if (OB_ENTRY_EXIST != tmp_ret) {
          LOG_WARN("failed to put truncate info array into kv cache", KR(tmp_ret), K(tablet_id), K(distinct_array_));
        }
      }
      LOG_DEBUG("put truncate info array into kv cache", K(tmp_ret), K(tablet.get_tablet_id()), K(cache_key), K(distinct_array_), K(read_version_range));
    }
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && !is_inited_) {
    reset();
  }
  return ret;
}

int ObMdsInfoDistinctMgr::read_split_truncate_info_array(
    const common::ObIArray<ObTabletHandle> *split_extra_tablet_handles,
    const ObVersionRange &read_version_range,
    const bool for_access)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != split_extra_tablet_handles)) {
    ObArenaAllocator tmp_allocator("SplitTrunI");
    ObTruncateInfoArray tmp_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < split_extra_tablet_handles->count(); i++) {
      ObTablet *tablet = split_extra_tablet_handles->at(i).get_obj();
      tmp_array.reset();
      tmp_allocator.reuse();
      if (OB_ISNULL(tablet)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", KR(ret));
      } else if (OB_FAIL(tablet->read_truncate_info_array(tmp_allocator, read_version_range, for_access, tmp_array))) {
        LOG_WARN("failed to read truncate info array", KR(ret), K(tablet->get_tablet_id()), K(read_version_range));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_array.count(); i++) {
          const ObTruncateInfo &info = *tmp_array.at(i);
          if (OB_FAIL(array_.append_with_deep_copy(info))) {
            LOG_WARN("failed to append", K(ret), K(info));
          }
        }
      }
    }
  }
  return ret;
}

int ObMdsInfoDistinctMgr::build_distinct_array(
  const ObVersionRange &read_version_range,
  const bool for_access)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTruncateInfo *> &input_array = array_.get_array();
  const ObTruncateInfo *input_info = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < array_.count(); ++idx) {
    bool exist = false;
    if (OB_ISNULL(input_info = input_array.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr in array", KR(ret), K(idx), K_(distinct_array));
    } else if (for_access && input_info->commit_version_ > read_version_range.snapshot_version_) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_INFO("refused to old snapshot access before truncate partition DDL", KR(ret), K(read_version_range), KPC(input_info));
    } else if (input_info->commit_version_ <= read_version_range.base_version_
              || input_info->commit_version_ > read_version_range.snapshot_version_) {
      continue;
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !exist && j < distinct_array_.count(); ++j) {
        if (OB_ISNULL(distinct_array_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr in distinct_array", KR(ret), K(j), K_(distinct_array));
        } else {
          bool equal = false;
          const ObTruncateInfo &exist_info = *distinct_array_.at(j);
          if (OB_FAIL(exist_info.compare_truncate_part_info(*input_info, equal))) {
            LOG_WARN("failed to compare two truncate partition", KR(ret), K(j), K(exist_info),
              K(idx), KPC(input_info));
          } else if (equal) {
            if (exist_info.commit_version_ < input_info->commit_version_) {
              if (OB_UNLIKELY(exist_info.schema_version_ >= input_info->schema_version_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("truncate info is invalid when compare", KR(ret), K(exist_info), KPC(input_info));
              } else {
                // use new input info to replace exist info
                distinct_array_.at(j) = input_array.at(idx);
                LOG_INFO("use new input info to replace exist info", KR(ret), K(exist_info), KPC(input_info));
              }
            }
            exist = true;
            break;
          }
        }
      } // for
      if (OB_SUCC(ret) && !exist) {
        if (OB_FAIL(distinct_array_.push_back(input_array.at(idx)))) {
          LOG_WARN("failed to push into distinct_array", KR(ret), K(idx), K(input_array.at(idx)));
        }
      }
    }
  } // for
  if (OB_SUCC(ret)) {
    lib::ob_sort(distinct_array_.begin(), distinct_array_.end(), ObTruncateInfoArray::compare);
    LOG_INFO("[TRUNCATE INFO] success to build distinct array", KR(ret), "distinct_array_cnt", distinct_array_.count(),
      K_(distinct_array), K(input_array));
  }
  return ret;
}

int ObMdsInfoDistinctMgr::fill_mds_filter_info(
  ObIAllocator &allocator,
  ObMdsFilterInfo &mds_filter_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("truncate info mgr is not inited", KR(ret));
  } else {
    ObSEArray<ObTruncateInfoKey, 8> truncate_info_keys;
    for (int64_t j = 0; OB_SUCC(ret) && j < distinct_array_.count(); ++j) {
      if (OB_ISNULL(distinct_array_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr in distinct_array", KR(ret), K(j), K_(distinct_array));
      } else if (OB_FAIL(truncate_info_keys.push_back(distinct_array_.at(j)->key_))) {
        LOG_WARN("failed to push truncate info keys", KR(ret), K(j), K_(distinct_array));
      }
    } // for
    if (FAILEDx(mds_filter_info.init_truncate_keys(allocator, truncate_info_keys))) {
      LOG_WARN("failed to init truncate info keys", KR(ret), K(truncate_info_keys));
    } else {
      LOG_INFO("[TRUNCATE INFO] success to init mds filter info", KR(ret), K(mds_filter_info));
    }
  }
  return ret;
}

int ObMdsInfoDistinctMgr::check_mds_filter_info(
  const compaction::ObMdsFilterInfo &mds_filter_info)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (OB_SUCC(ret) && EN_COMPACTION_SKIP_MDS_WHEN_CHECK) {
    FLOG_INFO("ERRSIM EN_COMPACTION_SKIP_MDS_WHEN_CHECK", KR(ret), K_(distinct_array));
    distinct_array_.reuse();
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("truncate info mgr is not inited", KR(ret));
  } else if (distinct_array_.empty() && mds_filter_info.is_empty()) {
    // do nothing
  } else if (OB_UNLIKELY(distinct_array_.empty() || mds_filter_info.is_empty())) {
    ret = OB_ERR_SYS;
    LOG_WARN("mds cnt is unexpected unequal", KR(ret), K(mds_filter_info), K_(distinct_array));
  } else {
    const ObMdsFilterInfo::ObTruncateInfoKeyArray &truncate_keys = mds_filter_info.get_truncate_keys();
    if (OB_UNLIKELY(truncate_keys.count() != distinct_array_.count())) {
      // truncate clog may replayed after medium clog
      ret = OB_EAGAIN;
      LOG_WARN("truncate keys cnt is unequal", KR(ret), K(truncate_keys), K_(distinct_array));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < distinct_array_.count(); ++j) {
      if (distinct_array_.at(j)->key_ != truncate_keys.at(j)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unequal key", KR(ret), K(j), K(mds_filter_info), K_(distinct_array));
      }
    } // for
  }
  return ret;
}

int ObMdsInfoDistinctMgr::get_distinct_truncate_info_array(
    ObTruncateInfoArray &input_distinct_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("truncate info mgr is not inited", KR(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < distinct_array_.count(); ++idx) {
      if (OB_UNLIKELY(nullptr == distinct_array_.at(idx) || !distinct_array_.at(idx)->is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid ptr in distinct array", KR(ret), K(idx), KPC(distinct_array_.at(idx)));
      } else if (OB_FAIL(input_distinct_array.append_with_deep_copy(*distinct_array_.at(idx)))) {
        LOG_WARN("failed to append truncate info", KR(ret), K(idx), KPC(distinct_array_.at(idx)));
      }
    } // for
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
