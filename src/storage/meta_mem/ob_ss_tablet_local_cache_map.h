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

#ifndef OCEANBASE_STORAGE_OB_ss_tablet_local_cache_map_H_
#define OCEANBASE_STORAGE_OB_ss_tablet_local_cache_map_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

/*
  * As SS_Tablet_Local_Cache_Map Finally
  TODO: FEIDU:
    1. Add SS_Tablet_Count_Threshold
    2. Add Wash Policy
    3. Add Function of Fetch SS_Tablet_Meta From Shared_Storage
*/
class ObSSTabletLocalCacheMap
{
public:
  ObSSTabletLocalCacheMap();
  int init(const int64_t bucket_num, const uint64_t tenant_id);
  int check_exist(const ObSSTabletMapKey &key, bool &exist);
  int insert_or_update_ss_tablet(const ObSSTabletMapKey &key, const ObTabletHandle &tablet_hdl);
  int fetch_tablet(const ObSSTabletMapKey &key, ObTabletHandle &tablet_hdl);
  int64_t count() const { return ss_tablet_map_.size(); }
  void destroy();
private:
  int unreg_tablet_(const ObSSTabletMapKey &key);
private:
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
  common::hash::ObHashMap<ObSSTabletMapKey, ObTabletHandle> ss_tablet_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletLocalCacheMap);
private:
  const int64_t SS_TABLET_CACHE_THRESHOLD = 1000;
};


}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_ss_tablet_local_cache_map_H_ */
