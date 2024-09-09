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

#ifndef OCEANBASE_STORAGE_OB_EXTERNAL_TABLET_CNT_MAP_H_
#define OCEANBASE_STORAGE_OB_EXTERNAL_TABLET_CNT_MAP_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "storage/meta_mem/ob_tablet_map_key.h"

namespace oceanbase
{
namespace storage
{

class ObExternalTabletCntMap
{
public:
  ObExternalTabletCntMap();
  int init(const int64_t bucket_num, const uint64_t tenant_id);
  int check_exist(const ObDieingTabletMapKey &key, bool &exist);
  int reg_tablet(const ObDieingTabletMapKey &key);
  int unreg_tablet(const ObDieingTabletMapKey &key);
  void destroy();
private:
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
  common::hash::ObHashMap<ObDieingTabletMapKey, int64_t> ex_tablet_map_;
  DISALLOW_COPY_AND_ASSIGN(ObExternalTabletCntMap);
};


}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_EXTERNAL_TABLET_CNT_MAP_H_ */
