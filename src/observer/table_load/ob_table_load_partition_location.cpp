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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_partition_location.h"
#include "observer/ob_server.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace share;
using namespace storage;
using namespace table;

int ObTableLoadPartitionLocation::check_tablet_has_same_leader(const ObTableLoadPartitionLocation &other, bool &result)
{
  int ret = OB_SUCCESS;
  result = true;
  if (tablet_ids_.count() != other.tablet_ids_.count()) {
    result = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && result &&  i < tablet_ids_.count(); i ++) {
    PartitionLocationInfo info1;
    PartitionLocationInfo info2;
    if (OB_FAIL(partition_map_.get_refactored(tablet_ids_.at(i), info1))) {
      LOG_WARN("fail to get location info", KR(ret));
    } else if (OB_FAIL(other.partition_map_.get_refactored(other.tablet_ids_.at(i), info2))) {
      LOG_WARN("fail to get location info", KR(ret));
    } else if (info1.leader_addr_ != info2.leader_addr_) {
      result = false;
    }
  }
  return ret;
}

int ObTableLoadPartitionLocation::fetch_ls_id(uint64_t tenant_id, const ObTabletID &tablet_id,
                                              ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLocationService &location_service = OBSERVER.get_location_service();
  const int64_t cluster_id = GCONF.cluster_id.get_value();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  bool is_cache_hit = false;
  if (OB_FAIL(tenant_guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to switch tenant", KR(ret), K(OB_SYS_TENANT_ID));
  } else if (OB_FAIL(location_service.get(tenant_id, tablet_id, INT64_MAX, is_cache_hit, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableLoadPartitionLocation::fetch_ls_location(uint64_t tenant_id, const ObTabletID &tablet_id,
                                                    ObLSLocation &ls_location, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLocationService &location_service = OBSERVER.get_location_service();
  const int64_t cluster_id = GCONF.cluster_id.get_value();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  bool is_cache_hit = false;
  if (OB_FAIL(tenant_guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to switch tenant", KR(ret), K(OB_SYS_TENANT_ID));
  } else if (OB_FAIL(location_service.get(tenant_id, tablet_id, INT64_MAX, is_cache_hit, ls_id))) {
    LOG_WARN("fail to get ls id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(location_service.get(cluster_id, tenant_id, ls_id, INT64_MAX, is_cache_hit,
                                          ls_location))) {
    LOG_WARN("fail to get location", KR(ret));
  }
  return ret;
}

int ObTableLoadPartitionLocation::fetch_ls_locations(uint64_t tenant_id,
    const ObTableLoadArray<ObTableLoadPartitionId> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_ids;

  for (int64_t i = 0; OB_SUCC(ret) && (i < partition_ids.count()); ++i) {
    const ObTabletID &tablet_id = partition_ids[i].tablet_id_;
    if (OB_FAIL(tablet_ids_.push_back(tablet_id))) {
      LOG_WARN("failed to push back tablet_id", K(tablet_id), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(*(GCTX.sql_proxy_),
      tenant_id, tablet_ids_, ls_ids))) {
    if (OB_LIKELY(OB_ITEM_NOT_MATCH == ret)) {
      ret = OB_SCHEMA_NOT_UPTODATE;
    }
    LOG_WARN("table_load_partition failed to batch get ls", KR(ret), K(tenant_id));
  } else {
    ObLSLocation location;
    ObHashMap<ObLSID, ObAddr> ls_location_map;
    ObLocationService &location_service = OBSERVER.get_location_service();
    const int64_t cluster_id = GCONF.cluster_id.get_value();
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    bool is_cache_hit = false;

    if (OB_FAIL(tenant_guard.switch_to(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to switch tenant", KR(ret), K(OB_SYS_TENANT_ID));
    } else if (OB_FAIL(ls_location_map.create(1024, "TLD_PartLoc", "TLD_PartLoc", tenant_id))) {
      LOG_WARN("fail to create location info map", KR(ret));
    } else {
      // avoid redundant location info lookups
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
        const ObLSID &ls_id = ls_ids.at(i);
        PartitionLocationInfo info;
        info.partition_id_.part_tablet_id_ = partition_ids.at(i);
        info.partition_id_.ls_id_ = ls_id;

        if (OB_FAIL(ls_location_map.get_refactored(ls_id, info.leader_addr_))) {
          if (ret != OB_HASH_NOT_EXIST) {
            LOG_WARN("failed to get refactored", K(ret), K(i), K(ls_id));
          } else if (OB_FAIL(location_service.get(cluster_id,
              tenant_id, ls_id, INT64_MAX, is_cache_hit, location))) {
            LOG_WARN("fail to get location", KR(ret));
          } else if (OB_FAIL(location.get_leader(info.leader_addr_))) {
            LOG_WARN("fail to get leader", KR(ret));
          } else if (OB_FAIL(ls_location_map.set_refactored(ls_id, info.leader_addr_))) {
            LOG_WARN("failed to set refactored", K(ret), K(ls_id), K(info));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(partition_map_.set_refactored(tablet_ids_.at(i), info))) {
          LOG_WARN("fail to set refactored", KR(ret), K(i), K(info));
        }
      }
    }
  }

  return ret;
}

int ObTableLoadPartitionLocation::fetch_location_leader(uint64_t tenant_id,
                                                        const ObTabletID &tablet_id, PartitionLocationInfo &info)
{
  int ret = OB_SUCCESS;
  ObLSLocation location;
  if (OB_FAIL(fetch_ls_location(tenant_id, tablet_id, location, info.partition_id_.ls_id_))) {
    LOG_WARN("fail to fetch ls location", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(location.get_leader(info.leader_addr_))) {
    LOG_WARN("fail to get leader", KR(ret));
  }
  return ret;
}

int ObTableLoadPartitionLocation::fetch_tablet_handle(uint64_t tenant_id, const ObLSID &ls_id,
                                                      const ObTabletID &tablet_id,
                                                      ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL ObLSService failed", KR(ret), "tenant_id", OB_SYS_TENANT_ID, K(MTL_ID()));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_UNLIKELY(OB_LS_NOT_EXIST == ret)) {
      LOG_WARN("get ls handle failed", KR(ret), "log_stream_id", ls_id.id());
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_handle.get_ls() is nullptr", KR(ret));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service should not be NULL", KR(ret), KP(tablet_service));
  } else if (OB_FAIL(tablet_service->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("fail to get tablet", KR(ret));
  }
  return ret;
}

int ObTableLoadPartitionLocation::fetch_tablet_handle(uint64_t tenant_id,
                                                      const ObTabletID &tablet_id,
                                                      ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  if (OB_FAIL(fetch_ls_id(tenant_id, tablet_id, ls_id))) {
    LOG_WARN("fail to fetch ls id", KR(ret));
  } else if (OB_FAIL(fetch_tablet_handle(tenant_id, ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("fail to fetch tablet handle", KR(ret));
  }
  return ret;
}

int ObTableLoadPartitionLocation::init(
  uint64_t tenant_id, const ObTableLoadArray<ObTableLoadPartitionId> &partition_ids,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadPartitionLocation init twice", KR(ret));
  } else if (OB_UNLIKELY(partition_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(partition_ids));
  } else {
    if (OB_FAIL(partition_map_.create(1024, "TLD_PartLoc", "TLD_PartLoc", tenant_id))) {
      LOG_WARN("fail to create map", KR(ret));
    } else if (OB_FAIL(init_all_partition_location(tenant_id, partition_ids, allocator))) {
      LOG_WARN("fail to init all partition location", KR(ret));
    } else if (OB_FAIL(init_all_leader_info(allocator))) {
      LOG_WARN("fail to init all leader info", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadPartitionLocation::init_all_partition_location(
  uint64_t tenant_id, const ObTableLoadArray<ObTableLoadPartitionId> &partition_ids,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fetch_ls_locations(tenant_id, partition_ids))) {
    LOG_WARN("fail to fetch locations", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableLoadPartitionLocation::init_all_leader_info(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("TLD_PL_Tmp");
  ObHashMap<ObAddr, ObIArray<ObTableLoadLSIdAndPartitionId> *> addr_map;
  ObHashMap<ObAddr, ObIArray<ObTableLoadLSIdAndPartitionId> *>::const_iterator addr_iter;
  int64_t pos = 0;
  tmp_allocator.set_tenant_id(MTL_ID());
  // 将所有addr存到set中
  if (OB_FAIL(addr_map.create(64, "TLD_PL_Tmp", "TLD_PL_Tmp"))) {
    LOG_WARN("fail to create hashmap", KR(ret));
  } else {
    ObHashMap<ObTabletID, PartitionLocationInfo>::const_iterator iter;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids_.count(); i ++) {
      PartitionLocationInfo info;
      if (OB_FAIL(partition_map_.get_refactored(tablet_ids_.at(i), info))) {
        LOG_WARN("fail to get tablet info", K(tablet_ids_.at(i)), KR(ret));
      }
      const ObTableLoadLSIdAndPartitionId &partition_id = info.partition_id_;
      const ObAddr &addr = info.leader_addr_;
      ObIArray<ObTableLoadLSIdAndPartitionId> *partition_id_array = nullptr;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(addr_map.get_refactored(addr, partition_id_array))) {
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
            LOG_WARN("fail to get refactored", KR(ret), K(addr));
          } else {
            if (OB_ISNULL(partition_id_array =
                            OB_NEWx(ObArray<ObTableLoadLSIdAndPartitionId>, (&tmp_allocator)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to new array", KR(ret));
            } else if (OB_FAIL(addr_map.set_refactored(addr, partition_id_array))) {
              LOG_WARN("fail to set refactored", KR(ret), K(addr));
            }
            if (OB_FAIL(ret)) {
              if (nullptr != partition_id_array) {
                partition_id_array->~ObIArray<ObTableLoadLSIdAndPartitionId>();
                tmp_allocator.free(partition_id_array);
                partition_id_array = nullptr;
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(partition_id_array->push_back(partition_id))) {
            LOG_WARN("fail to push back partition id", KR(ret));
          }
        }
      }
    }
  }
	// 将set中的addr存到array中
	if (OB_SUCC(ret)) {
		if (OB_FAIL(all_leader_addr_array_.create(addr_map.size(), allocator))) {
			LOG_WARN("fail to create leader addr array", KR(ret));
		} else if (OB_FAIL(all_leader_info_array_.create(addr_map.size(), allocator))) {
			LOG_WARN("fail to create leader info array", KR(ret));
		}
	}
  ObArray<LeaderInfoForSort> sort_array;
  for (addr_iter = addr_map.begin(); OB_SUCC(ret) && addr_iter != addr_map.end(); ++pos, ++addr_iter) {
    LeaderInfoForSort item;
    item.addr_ = addr_iter->first;
    item.partition_id_array_ptr_ = addr_iter->second;
    if (OB_FAIL(sort_array.push_back(item))) {
      LOG_WARN("fail to push_back item", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(sort_array.begin(), sort_array.end(), [](const ObTableLoadPartitionLocation::LeaderInfoForSort &a,
                 ObTableLoadPartitionLocation::LeaderInfoForSort &b) {
                return a.addr_ < b.addr_;
              });
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_array.count(); i ++) {
    const ObAddr &addr = sort_array.at(i).addr_;
    ObIArray<ObTableLoadLSIdAndPartitionId> *partition_id_array = sort_array.at(i).partition_id_array_ptr_;
    all_leader_addr_array_[i] = addr;
    LeaderInfo &leader_info = all_leader_info_array_[i];
    leader_info.addr_ = addr;
    if (OB_FAIL(ObTableLoadUtils::deep_copy(*partition_id_array, leader_info.partition_id_array_,
                                            allocator))) {
      LOG_WARN("fail to deep copy partition id array", KR(ret));
    }
    partition_id_array->~ObIArray<ObTableLoadLSIdAndPartitionId>();
    tmp_allocator.free(partition_id_array);
  }

  return ret;
}

int ObTableLoadPartitionLocation::get_leader(ObTabletID tablet_id, PartitionLocationInfo &info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPartitionLocation not init", KR(ret));
  } else {
    if (OB_FAIL(partition_map_.get_refactored(tablet_id, info))) {
      LOG_WARN("fail to get refactored", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObTableLoadPartitionLocation::get_all_leader(ObTableLoadArray<ObAddr> &addr_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPartitionLocation not init", KR(ret));
  } else {
    addr_array = all_leader_addr_array_;
  }
  return ret;
}

int ObTableLoadPartitionLocation::get_all_leader_info(ObTableLoadArray<LeaderInfo> &info_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadPartitionLocation not init", KR(ret));
  } else {
    info_array = all_leader_info_array_;
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
