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

#include "ob_vector_index_ivf_cache_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace share
{
int ObIvfCacheUtil::ObIvfWriteCacheFunc::operator()(int64_t dim, float *data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cent_cache_.write_centroid_with_real_idx(cent_idx_, data, dim * sizeof(float)))) {
    LOG_WARN("fail to write centroid", K(ret), K(cent_idx_), K(dim));
  } else {
    ++cent_idx_;
  }
  return ret;
}

int ObIvfCacheUtil::scan_and_write_ivf_cent_cache(ObPluginVectorIndexService &service,
                                                  const ObTableID &table_id,
                                                  const ObTabletID &tablet_id,
                                                  ObIvfCentCache &cent_cache)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  if (cent_cache.set_writing_if_idle()) {
    RWLock::WLockGuard guard(cent_cache.get_lock());
    ObIvfWriteCacheFunc write_func(cent_cache);
    if (OB_FAIL(service.process_ivf_aux_info(table_id, tablet_id, tmp_allocator, write_func))) {
      LOG_WARN("failed to get centers", K(ret));
      cent_cache.reuse();
    } else {
      if (cent_cache.is_full_cache()) {
        cent_cache.set_completed();
      } else {
        cent_cache.reuse();
      }
    }
  }

  return ret;
}

int ObIvfCacheUtil::is_cache_writable(const ObLSID &ls_id, int64_t table_id,
                                      const ObTabletID &tablet_id,
                                      const ObVectorIndexParam &vec_param, int64_t dim,
                                      IvfCacheType cache_type, bool &is_writable)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfCentCache *cent_cache = nullptr;

  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->acquire_ivf_cache_mgr_guard(
                 ls_id, tablet_id, vec_param, dim, table_id, cache_guard))) {
    LOG_WARN("fail to acquire ivf cache mgr with vec param",
             K(ret),
             K(ls_id),
             K(tablet_id),
             K(vec_param),
             K(dim));
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node(cache_type, cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret), K(cache_type));
  } else {
    is_writable = cent_cache->is_idle();
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
