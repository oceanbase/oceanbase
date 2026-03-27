/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_IVF_CACHE_UTIL_H_
#define OCEANBASE_SHARE_IVF_CACHE_UTIL_H_
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace share
{
class ObIvfCacheUtil
{
public:
  static int is_cache_writable(const ObLSID &ls_id, int64_t table_id, const ObTabletID &tablet_id,
                               const ObVectorIndexParam &vec_param, int64_t dim,
                               IvfCacheType cache_type, bool &is_writable);
  static int scan_and_write_ivf_cent_cache(ObPluginVectorIndexService &service,
                                           const ObTableID &table_id,
                                           const ObTabletID &tablet_id,
                                           const ObVectorIndexParam &vec_param,
                                           ObIvfCentCache &cent_cache);

private:
  class ObIvfWriteCacheFunc
  {
  public:
    ObIvfWriteCacheFunc(ObIvfCentCache &cent_cache) : cent_cache_(cent_cache), cent_idx_(0) {}
    int operator()(int64_t dim, float *data);

  private:
    ObIvfCentCache &cent_cache_;
    int64_t cent_idx_;
  };
  DISALLOW_COPY_AND_ASSIGN(ObIvfCacheUtil);
};

}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_SHARE_IVF_CACHE_UTIL_H_
