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
                               const ObVectorIndexParam &vec_param, int64_t dim, bool &is_writable);
  static int scan_and_write_ivf_cent_cache(ObPluginVectorIndexService &service,
                                           const ObTableID &table_id, const ObTabletID &tablet_id,
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
