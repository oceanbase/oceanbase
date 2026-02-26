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

#ifndef OCEANBASE_MITTEST_SHARED_STORAGE_SS_MACRO_CACHE_MGR_UTIL_H_
#define OCEANBASE_MITTEST_SHARED_STORAGE_SS_MACRO_CACHE_MGR_UTIL_H_

#define protected public
#define private public
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "share/rc/ob_tenant_base.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{

class TestSSMacroCacheMgrUtil
{
public:
  static int clean_macro_cache_mgr()
  {
    int ret = OB_SUCCESS;
    ObSSMacroCacheMgr *macro_cache_mgr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "ObSSMacroCacheMgr is NULL", KR(ret));
    } else {
      const int64_t meta_num = macro_cache_mgr->meta_map_.size();
      blocksstable::MacroBlockId macro_id_arr[meta_num];
      SSMacroCacheMetaMap::const_iterator it = macro_cache_mgr->meta_map_.begin();
      int64_t i = 0;
      while (OB_SUCC(ret) && it != macro_cache_mgr->meta_map_.end()) {
        macro_id_arr[i] = it->first;
        i++;
        it++;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < meta_num; i++) {
        if (OB_FAIL(macro_cache_mgr->erase(macro_id_arr[i]))) {
          OB_LOG(WARN, "fail to erase macro cache meta",
              KR(ret), K(i), K(macro_id_arr[i]), K(meta_num));
        }
      }

      if (OB_SUCC(ret)) {
        macro_cache_mgr->tablet_stat_map_.clear();
      }

      if (OB_SUCC(ret) && OB_UNLIKELY(!macro_cache_mgr->meta_map_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "fail to clean macro cache mgr", KR(ret),
            K(meta_num), K(macro_cache_mgr->meta_map_.size()));
      }
    }
    return ret;
  }

  static int wait_macro_cache_ckpt_replay()
  {
    int ret = OB_SUCCESS;
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "ObSSMacroCacheMgr is NULL", KR(ret));
    } else {
      const int64_t start_us = ObTimeUtility::current_time_us();
      while (!macro_cache_mgr->is_ckpt_replayed() && OB_SUCC(ret)) {
        usleep(10 * 1000L); // sleep 10ms
        if (OB_UNLIKELY((ObTimeUtility::current_time_us() - start_us) > 20 * 1000L * 1000L)) { // 20s
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "wait too long time", KR(ret));
        }
      }
    }
    return ret;
  }
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_MITTEST_SHARED_STORAGE_SS_MACRO_CACHE_MGR_UTIL_H_ */