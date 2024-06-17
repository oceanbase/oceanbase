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

#include "storage/ob_locality_manager.h"
#include "lib/hash/ob_hashmap.h"                // ObHashMap
#include "logservice/palf/palf_callback.h"

namespace oceanbase
{
using namespace storage;
using namespace palf;
namespace unittest
{

typedef common::hash::ObHashMap<common::ObAddr, common::ObRegion> LogMemberRegionMap;

class CallBack
{
  public:
    void operator () (hash::HashMapPair<common::ObAddr, common::ObRegion> &v)
    {
      v.second = v_;
    };
    void set_v(common::ObRegion v)
    {
      v_ = v;
    };
  private:
    common::ObRegion v_;
};

class MockObLocalityManager : public palf::PalfLocalityInfoCb, public ObLocalityManager
{
public:
  MockObLocalityManager(): is_inited_(false) { }
  ~MockObLocalityManager() { destroy(); }
  int init(LogMemberRegionMap *region_map)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
    } else if (OB_ISNULL(region_map)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid argument", KP(region_map));
    } else {
      region_map_ = region_map;
      is_inited_ = true;
    }
    return ret;
  }
  void destroy()
  {
    is_inited_ = false;
    region_map_ = NULL;
  }
  int set_server_region(const common::ObAddr &server,
                        const common::ObRegion &region)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(region_map_)) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(region_map_->set_refactored(server, region)) && ret != OB_HASH_EXIST) {
      SERVER_LOG(WARN, "set_refactored failed", K(server), K(region));
    } else {
      CallBack callback;
      callback.set_v(region);
      if (OB_FAIL(region_map_->atomic_refactored(server, callback))) {
        SERVER_LOG(WARN, "atomic_refactored failed", K(server), K(region));
      }
    }
    SERVER_LOG(INFO, "set_server_region finish", K(ret), K(server), K(region));
    return ret;
  }
  int get_server_region(const common::ObAddr &server,
                        common::ObRegion &region) const override final
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(region_map_)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_FAIL(region_map_->get_refactored(server, region))) {
      ret = OB_ENTRY_NOT_EXIST;
      SERVER_LOG(WARN, "get_server_region failed", K(server), K(region));
    }
    return ret;
  }
private:
  LogMemberRegionMap *region_map_;
  bool is_inited_;
};
}// storage
}// oceanbase
