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

#define USING_LOG_PREFIX RS

#include "ob_single_partition_balance.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/string/ob_strings.h"
#include "rootserver/ob_unit_manager.h"
#include "ob_zone_manager.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_table_iterator.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace schema;
namespace rootserver {

ObSinglePartBalance::ObSinglePartBalance()
    : inited_(false),
      unit_mgr_(NULL),
      zone_mgr_(NULL),
      pt_operator_(NULL),
      tenant_unit_rep_cnt_vec_(0, NULL, "TenUnitRepCnt"),
      allocator_("SingParBalance")
{}

int ObSinglePartBalance::init(
    ObUnitManager& unit_mgr, ObZoneManager& zone_mgr, share::ObPartitionTableOperator& pt_operator)
{
  int ret = OB_SUCCESS;
  unit_mgr_ = &unit_mgr;
  zone_mgr_ = &zone_mgr;
  pt_operator_ = &pt_operator;
  inited_ = true;
  return ret;
}

int ObSinglePartBalance::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr is null", KR(ret));
  } else if (OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator_ is null", KR(ret));
  }
  return ret;
}

int ObSinglePartBalance::destory_tenant_unit_array()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tenant_unit_rep_cnt_vec_.count() && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(tenant_unit_rep_cnt_vec_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", KR(ret));
    } else {
      tenant_unit_rep_cnt_vec_.at(i)->~TenantUnitRepCnt();
      allocator_.free(tenant_unit_rep_cnt_vec_.at(i));
      tenant_unit_rep_cnt_vec_.at(i) = NULL;
    }
  }
  tenant_unit_rep_cnt_vec_.reset();
  return ret;
}

bool ObSinglePartBalance::compare_with_tenant_id(
    const share::TenantUnitRepCnt* lhs, const share::TenantUnitRepCnt& ten_unit_rep)
{
  return NULL != lhs ? (lhs->tenant_id_ < ten_unit_rep.tenant_id_) : false;
}

bool ObSinglePartBalance::compare_with_tenant_id_up(
    const share::TenantUnitRepCnt& ten_unit_rep, const share::TenantUnitRepCnt* lhs)
{
  return NULL != lhs ? (ten_unit_rep.tenant_id_ < lhs->tenant_id_) : false;
}

bool ObSinglePartBalance::compare_with_unit_id(const uint64_t lhs, const uint64_t unit_id)
{
  return lhs < unit_id;
}

int ObSinglePartBalance::sort_and_push_back_unit_ids(
    const common::ObIArray<share::ObUnitInfo>& unit_infos, common::ObArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(unit_ids.push_back(unit_infos.at(i).unit_.unit_id_))) {
      LOG_WARN("fail to push back unit id", KR(ret), "unit_id", unit_infos.at(i).unit_.unit_id_);
    }
  }
  std::sort(unit_ids.begin(), unit_ids.end(), compare_with_unit_id);
  return ret;
}

int ObSinglePartBalance::create_unit_replica_counter(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnitInfo> unit_infos;
  common::ObArray<uint64_t> unit_ids;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to init", KR(ret));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(tenant_id, unit_infos))) {
    LOG_WARN("fail to get active unit infos by tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sort_and_push_back_unit_ids(unit_infos, unit_ids))) {
    LOG_WARN("fail to sort and push back unit ids", KR(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    ObArray<share::TenantUnitRepCnt> update_ten_unit_reps;
    share::TenantUnitRepCnt ten_unit_rep;
    ten_unit_rep.tenant_id_ = tenant_id;
    share::TenantUnitRepCntVec::iterator iter_lower =
        tenant_unit_rep_cnt_vec_.lower_bound(ten_unit_rep, compare_with_tenant_id);
    share::TenantUnitRepCntVec::iterator iter_upper =
        tenant_unit_rep_cnt_vec_.upper_bound(ten_unit_rep, compare_with_tenant_id_up);
    if (iter_upper != tenant_unit_rep_cnt_vec_.end()) {
      // There are tenants behind
      if (OB_FAIL(copy_ten_unit_rep_data(update_ten_unit_reps, iter_upper))) {
        LOG_WARN("fail to copy ten unit rep data", KR(ret), K(tenant_id));
      } else if (OB_FAIL(remove_tenant_unit_rep(iter_lower))) {
        LOG_WARN("fail to remove tenant unit rep", KR(ret), K(tenant_id));
      } else if (OB_FAIL(create_tenant_unit_rep(unit_ids, tenant_id))) {
        LOG_WARN("fail to update ten unit rep", KR(ret), K(tenant_id));
      } else if (OB_FAIL(push_back_other_tenant_unit_rep(update_ten_unit_reps))) {
        LOG_WARN("fail to push back other tenant unit rep", KR(ret), K(tenant_id));
      }
    } else {
      // There is no tenant later, just update this tenant
      if (OB_FAIL(create_tenant_unit_rep(unit_ids, tenant_id))) {
        LOG_WARN("fail to update tenant unit rep", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::create_tenant_unit_rep(const ObIArray<uint64_t>& unit_ids, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < unit_ids.count() && OB_SUCC(ret); ++i) {
    void* buff = allocator_.alloc(sizeof(TenantUnitRepCnt));
    share::TenantUnitRepCnt* tenant_unit_rep_cnt = NULL;
    share::UnitReplicaCounter unit_rep_cnt;
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", KR(ret));
    } else if (FALSE_IT(tenant_unit_rep_cnt = new (buff) TenantUnitRepCnt(unit_ids.at(i), tenant_id, unit_rep_cnt))) {
      // never be here
    } else if (OB_FAIL(tenant_unit_rep_cnt_vec_.push_back(tenant_unit_rep_cnt))) {
      LOG_WARN("fail to push back tenant unit rep cnt", KR(ret), K(tenant_unit_rep_cnt));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != tenant_unit_rep_cnt) {
        tenant_unit_rep_cnt->~TenantUnitRepCnt();
        allocator_.free(tenant_unit_rep_cnt);
        tenant_unit_rep_cnt = nullptr;
      } else if (nullptr != buff) {
        allocator_.free(buff);
        buff = nullptr;
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::refresh_unit_replica_counter(const uint64_t tenant_id, const int64_t start,
    const ObIArray<share::ObUnitInfo>& unit_infos, share::schema::ObSchemaGetterGuard& schema_guard,
    ObIArray<share::TenantUnitRepCnt>& tmp_ten_unit_reps)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> unit_ids;
  common::ObArray<uint64_t> non_partition_table;
  int64_t index_num = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(prepare_replica_capacity(tenant_id, schema_guard, index_num, non_partition_table))) {
    LOG_WARN("fail to prepare replica capacity", KR(ret));
  } else {
    int64_t part_cnt = 0;
    if (OB_FAIL(sort_and_push_back_unit_ids(unit_infos, unit_ids))) {
      LOG_WARN("fail to sort and push back unit ids", KR(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; i < unit_ids.count() && OB_SUCC(ret); ++i) {
        share::TenantUnitRepCnt tmp_ten_unit_rep;
        tmp_ten_unit_rep.unit_id_ = unit_ids.at(i);
        tmp_ten_unit_rep.tenant_id_ = tenant_id;
        if (OB_FAIL(tmp_ten_unit_reps.push_back(tmp_ten_unit_rep))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < non_partition_table.count(); ++i) {
      ObPartitionInfo partition_info;
      ObArenaAllocator partition_allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      partition_info.set_allocator(&partition_allocator);
      ObTablePartitionIterator iter;
      if (OB_FAIL(iter.init(non_partition_table.at(i), schema_guard, *pt_operator_))) {
        LOG_WARN("fail to init table partition iterator", KR(ret));
      } else if (OB_FAIL(iter.next(partition_info))) {
        LOG_WARN("fail to get partition_info", KR(ret));
      } else if (OB_FAIL(partition_info.get_partition_cnt(part_cnt))) {
        LOG_WARN("fail to get partition cnt", KR(ret));
      } else if (part_cnt > 1) {  // single partition or non-partition
        LOG_WARN("part_cnt need <= 1", KR(ret));
      } else {
        FOREACH_CNT_X(r, partition_info.get_replicas_v2(), OB_SUCC(ret))
        {
          // Traverse all replicas in the partition
          for (int64_t i = 0; i < tmp_ten_unit_reps.count() && OB_SUCC(ret); ++i) {
            if (tmp_ten_unit_reps.at(i).unit_id_ == r->unit_id_) {
              if (OB_FAIL(set_unit_replica_capacity(
                      *r, tmp_ten_unit_reps.at(i), index_num, start, non_partition_table.count()))) {
                LOG_WARN("fail to set unit replica capacity", KR(ret));
              }
            }
          }
        }  // end for each
      }
    }  // end for
  }
  return ret;
}

int ObSinglePartBalance::prepare_replica_capacity(const uint64_t tenant_id,
    share::schema::ObSchemaGetterGuard& schema_guard, int64_t& index_num,
    common::ObIArray<uint64_t>& non_partition_table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("not init", KR(ret));
  } else {
    const share::schema::ObTenantSchema* tenant_schema = NULL;
    common::ObArray<const share::schema::ObTableSchema*> table_schemas;
    common::ObArray<const share::schema::ObTablegroupSchema*> tablegroup_schemas;
    common::ObArray<uint64_t> non_partition_tg;
    ObSEArray<ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score_array;
    ObSEArray<ObRawPrimaryZoneUtil::RegionScore, MAX_ZONE_NUM> region_score_array;
    if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("tenant not exists", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroup_schemas))) {
      LOG_WARN("fail to get table group schemas in tenant", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablegroup_schemas.count(); ++i) {
        const share::schema::ObTablegroupSchema* tablegroup_schema = tablegroup_schemas.at(i);
        if (OB_ISNULL(tablegroup_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema ptr is null", KR(ret));
        } else if (tablegroup_schema->get_all_part_num() > 1 || !tablegroup_schema->has_self_partition()) {  // skip
        } else {
          if (OB_FAIL(non_partition_tg.push_back(tablegroup_schema->get_tablegroup_id()))) {
            LOG_WARN("fail to push back", KR(ret), K(tablegroup_schema->get_tablegroup_id()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(balancer::StatFinderUtil::get_need_balance_table_schemas_in_tenant(
                schema_guard, tenant_id, table_schemas))) {
          LOG_WARN("fail to get table schemas in tenant", KR(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
            const share::schema::ObTableSchema* table_schema = table_schemas.at(i);
            if (OB_ISNULL(table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema ptr is null", KR(ret));
            } else if (table_schema->has_self_partition() && table_schema->get_all_part_num() <= 1 &&
                       table_schema->get_duplicate_scope() == ObDuplicateScope::DUPLICATE_SCOPE_NONE &&
                       share::OB_ALL_DUMMY_TID != extract_pure_id(table_schema->get_table_id()) &&
                       OB_INVALID_ID == table_schema->get_tablegroup_id() &&
                       rootserver::ObTenantUtils::is_balance_target_schema(*table_schema)) {
              if (OB_FAIL(non_partition_table.push_back(table_schema->get_table_id()))) {
                LOG_WARN("fail to push back", KR(ret), K(table_schema->get_table_id()));
              } else {
                index_num = non_partition_table.count();
              }
            }
          }  // end for
          if (OB_SUCC(ret)) {
            for (int64_t i = 0; OB_SUCC(ret) && i < non_partition_tg.count(); ++i) {
              if (OB_FAIL(non_partition_table.push_back(non_partition_tg.at(i)))) {
                LOG_WARN("fail to push back", KR(ret), K(non_partition_tg.at(i)));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::get_tenant_unit_array(
    ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::TenantUnitRepCnt ten_unit_rep;
  ten_unit_rep.tenant_id_ = tenant_id;
  share::TenantUnitRepCntVec::iterator iter_lower =
      tenant_unit_rep_cnt_vec_.lower_bound(ten_unit_rep, compare_with_tenant_id);
  share::TenantUnitRepCntVec::iterator iter_upper =
      tenant_unit_rep_cnt_vec_.upper_bound(ten_unit_rep, compare_with_tenant_id_up);
  if (iter_lower == tenant_unit_rep_cnt_vec_.end()) {
    // The tenant is not in the cache, create a cache for the tenant
    if (OB_FAIL(create_unit_replica_counter(tenant_id))) {
      LOG_WARN("fail to create unit replica counter", KR(ret), K(tenant_id));
    } else {
      iter_lower = tenant_unit_rep_cnt_vec_.lower_bound(ten_unit_rep, compare_with_tenant_id);
      iter_upper = tenant_unit_rep_cnt_vec_.upper_bound(ten_unit_rep, compare_with_tenant_id_up);
    }
  }
  for (; iter_lower != iter_upper && OB_SUCC(ret); ++iter_lower) {
    if (OB_ISNULL(*iter_lower)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant unit rep cnt is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ten_unit_arr.push_back(*iter_lower))) {
      LOG_WARN("fail to push back ten_unit_arr", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObSinglePartBalance::set_tenant_unit_replica_capacity(const uint64_t tenant_id, const int64_t start,
    const ObIArray<share::ObUnitInfo>& unit_infos, const ObIArray<share::TenantUnitRepCnt>& tmp_ten_unit_reps)
{
  int ret = OB_SUCCESS;
  int64_t unit_count = 0;
  bool need_clear = false;
  ObArray<uint64_t> unit_ids;
  ObArray<share::TenantUnitRepCnt> update_ten_unit_reps;
  bool need_update = true;
  for (int64_t i = 0; i < tenant_unit_rep_cnt_vec_.count() && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(tenant_unit_rep_cnt_vec_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", KR(ret), K(tenant_id), K(tenant_unit_rep_cnt_vec_.count()), "unit sequence", i);
    } else if (tenant_unit_rep_cnt_vec_.at(i)->tenant_id_ == tenant_id) {
      unit_count++;
      if (OB_FAIL(unit_ids.push_back(tenant_unit_rep_cnt_vec_.at(i)->unit_id_))) {
        LOG_WARN("fail to push back", KR(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < unit_infos.count() && OB_SUCC(ret); ++i) {
      if (has_exist_in_array(unit_ids, unit_infos.at(i).unit_.unit_id_)) {
        // nothing todo
      } else {
        need_clear = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObArray<uint64_t> tmp_unit_ids;
    if (OB_FAIL(sort_and_push_back_unit_ids(unit_infos, tmp_unit_ids))) {
      LOG_WARN("fail to sort and push back unit ids", KR(ret), K(tenant_id));
    } else if ((tmp_ten_unit_reps.count() != unit_count || unit_count == 0 || need_clear)) {
      // unit_count==0 may be that all units have been replaced,
      // or it may be that tenant_id is not in the vec
      // Clear the original data first
      share::TenantUnitRepCnt ten_unit_rep;
      ten_unit_rep.tenant_id_ = tenant_id;
      share::TenantUnitRepCntVec::iterator iter_lower =
          tenant_unit_rep_cnt_vec_.lower_bound(ten_unit_rep, compare_with_tenant_id);
      share::TenantUnitRepCntVec::iterator iter_upper =
          tenant_unit_rep_cnt_vec_.upper_bound(ten_unit_rep, compare_with_tenant_id_up);
      if (iter_upper != tenant_unit_rep_cnt_vec_.end()) {
        // There will be tenants in the future
        if (OB_FAIL(copy_ten_unit_rep_data(update_ten_unit_reps, iter_upper))) {
          LOG_WARN("fail to copy ten unit rep data", KR(ret), K(tenant_id));
        } else if (OB_FAIL(remove_tenant_unit_rep(iter_lower))) {
          LOG_WARN("fail to remove tenant unit rep", KR(ret), K(tenant_id));
        } else if (OB_FAIL(update_ten_unit_rep(unit_infos, tenant_id))) {
          LOG_WARN("fail to update ten unit rep", KR(ret), K(tenant_id));
        } else if (OB_FAIL(push_back_other_tenant_unit_rep(update_ten_unit_reps))) {
          LOG_WARN("fail to push back other tenant unit rep", KR(ret), K(tenant_id));
        }
      } else if (iter_lower != tenant_unit_rep_cnt_vec_.end()) {
        // There is no tenant in the future, just update the tenant
        if (OB_FAIL(remove_tenant_unit_rep(iter_lower))) {
          LOG_WARN("fail to remove tenant unit rep cnt vec", KR(ret), K(tenant_id));
        } else if (OB_FAIL(update_ten_unit_rep(unit_infos, tenant_id))) {
          LOG_WARN("fail to update tenant unit rep", KR(ret), K(tenant_id));
        }
      } else {
        if (OB_FAIL(update_ten_unit_rep(unit_infos, tenant_id))) {
          LOG_WARN("fail to update tenant unit rep", KR(ret), K(tenant_id));
        }
      }
    } else {
      share::TenantUnitRepCnt ten_unit_rep;
      ten_unit_rep.tenant_id_ = tenant_id;
      share::TenantUnitRepCntVec::iterator iter_lower =
          tenant_unit_rep_cnt_vec_.lower_bound(ten_unit_rep, compare_with_tenant_id);
      if (OB_ISNULL(*(iter_lower))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("is null", KR(ret), K(tenant_id));
      } else {
        if (iter_lower != tenant_unit_rep_cnt_vec_.end() && start < (*(iter_lower))->now_time_) {
          // nothing todo
        } else {
          for (int64_t j = 0;
               j < tmp_ten_unit_reps.count() && (iter_lower + j != tenant_unit_rep_cnt_vec_.end()) && OB_SUCC(ret);
               ++j) {
            (*(iter_lower + j))->unit_id_ = tmp_ten_unit_reps.at(j).unit_id_;
            (*(iter_lower + j))->tenant_id_ = tmp_ten_unit_reps.at(j).tenant_id_;
            (*(iter_lower + j))->unit_rep_cnt_ = tmp_ten_unit_reps.at(j).unit_rep_cnt_;
          }
        }
      }
    }
  }
  LOG_INFO("finish set_tenant_unit_replica_capacity",
      KR(ret),
      K(tenant_id),
      "unit_count",
      unit_infos.count(),
      "tenant_vec_count",
      tenant_unit_rep_cnt_vec_.count());
  return ret;
}

int ObSinglePartBalance::copy_ten_unit_rep_data(
    ObIArray<share::TenantUnitRepCnt>& update_ten_unit_reps, share::TenantUnitRepCntVec::iterator& iter_upper)
{
  int ret = OB_SUCCESS;
  for (share::TenantUnitRepCntVec::iterator iter = iter_upper; iter != tenant_unit_rep_cnt_vec_.end() && OB_SUCC(ret);
       iter++) {
    if (OB_ISNULL(*(iter))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant unit rep cnt is null", KR(ret));
    } else {
      share::TenantUnitRepCnt* tenant_unit_rep = *iter;
      if (OB_FAIL(update_ten_unit_reps.push_back(*tenant_unit_rep))) {
        LOG_WARN("fail to update ten unit reps", KR(ret), "tenant_id", (*iter)->tenant_id_);
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::remove_tenant_unit_rep(share::TenantUnitRepCntVec::iterator& iter_lower)
{
  int ret = OB_SUCCESS;
  for (share::TenantUnitRepCntVec::iterator iter = tenant_unit_rep_cnt_vec_.end() - 1;
       iter >= iter_lower && OB_SUCC(ret);
       iter--) {
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null", KR(ret));
    } else if (OB_FAIL(tenant_unit_rep_cnt_vec_.remove(iter))) {
      LOG_WARN("fail to remove tenant unit rep cnt", KR(ret));
    }
    if (nullptr != *iter) {
      (*iter)->~TenantUnitRepCnt();
      allocator_.free(*iter);
      *iter = nullptr;
    }
  }
  return ret;
}

int ObSinglePartBalance::update_ten_unit_rep(const ObIArray<share::ObUnitInfo>& unit_infos, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> unit_ids;
  share::UnitReplicaCounter unit_rep_cnt;
  if (OB_FAIL(sort_and_push_back_unit_ids(unit_infos, unit_ids))) {
    LOG_WARN("fail to sort and push back unit ids", KR(ret), K(tenant_id));
  } else {
    // First insert the unit corresponding to the new tenant
    for (int64_t i = 0; i < unit_ids.count() && OB_SUCC(ret); ++i) {
      void* buff = allocator_.alloc(sizeof(TenantUnitRepCnt));
      share::TenantUnitRepCnt* tenant_unit_rep_cnt = NULL;
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", KR(ret));
      } else if (FALSE_IT(tenant_unit_rep_cnt = new (buff) TenantUnitRepCnt(unit_ids.at(i), tenant_id, unit_rep_cnt))) {
        // never be here
      } else if (OB_FAIL(tenant_unit_rep_cnt_vec_.push_back(tenant_unit_rep_cnt))) {
        LOG_WARN("fail to push back tenant unit rep cnt", KR(ret), K(tenant_unit_rep_cnt));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tenant_unit_rep_cnt) {
          tenant_unit_rep_cnt->~TenantUnitRepCnt();
          allocator_.free(tenant_unit_rep_cnt);
          tenant_unit_rep_cnt = nullptr;
        } else if (nullptr != buff) {
          allocator_.free(buff);
          buff = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::push_back_other_tenant_unit_rep(const ObIArray<share::TenantUnitRepCnt>& update_ten_unit_reps)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    // Then insert other tenants
    for (int64_t i = 0; i < update_ten_unit_reps.count() && OB_SUCC(ret); ++i) {
      void* buff = allocator_.alloc(sizeof(TenantUnitRepCnt));
      share::TenantUnitRepCnt* tenant_unit_rep_cnt = NULL;
      const share::TenantUnitRepCnt& tmp_ten_unit_rep = update_ten_unit_reps.at(i);
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", KR(ret));
      } else if (FALSE_IT(
                     tenant_unit_rep_cnt = new (buff) TenantUnitRepCnt(
                         tmp_ten_unit_rep.unit_id_, tmp_ten_unit_rep.tenant_id_, tmp_ten_unit_rep.unit_rep_cnt_))) {
        // never be here
      } else if (OB_FAIL(tenant_unit_rep_cnt_vec_.push_back(tenant_unit_rep_cnt))) {
        LOG_WARN("fail to push back tenant unit rep cnt", KR(ret), K(tenant_unit_rep_cnt));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != tenant_unit_rep_cnt) {
          tenant_unit_rep_cnt->~TenantUnitRepCnt();
          allocator_.free(tenant_unit_rep_cnt);
          tenant_unit_rep_cnt = nullptr;
        } else if (nullptr != buff) {
          allocator_.free(buff);
          buff = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::update_tenant_unit_replica_capacity(
    const uint64_t tenant_id, const ObIArray<share::TenantUnitRepCnt*>& tmp_ten_unit_reps)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  share::TenantUnitRepCnt ten_unit_rep;
  ten_unit_rep.tenant_id_ = tenant_id;
  share::TenantUnitRepCntVec::iterator iter_lower =
      tenant_unit_rep_cnt_vec_.lower_bound(ten_unit_rep, compare_with_tenant_id);
  share::TenantUnitRepCntVec::iterator iter_upper =
      tenant_unit_rep_cnt_vec_.upper_bound(ten_unit_rep, compare_with_tenant_id_up);
  if (OB_ISNULL(*(iter_lower))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", KR(ret), K(tenant_id));
  } else if (iter_upper - iter_lower != tmp_ten_unit_reps.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit count different",
        KR(ret),
        K(tenant_id),
        "vec_unit_count",
        iter_upper - iter_lower,
        "actual unit count",
        tmp_ten_unit_reps.count());
  } else if (iter_lower != tenant_unit_rep_cnt_vec_.end() && start < (*(iter_lower))->now_time_) {
    // nothing todo
  } else {
    for (int64_t j = 0;
         j < tmp_ten_unit_reps.count() && (iter_lower + j != tenant_unit_rep_cnt_vec_.end()) && OB_SUCC(ret);
         ++j) {
      if (OB_ISNULL(tmp_ten_unit_reps.at(j))) {
        // nothing todo
        LOG_WARN("ten unit rep is null", K(tenant_id), KR(ret));
      } else {
        (*(iter_lower + j))->unit_id_ = tmp_ten_unit_reps.at(j)->unit_id_;
        (*(iter_lower + j))->tenant_id_ = tmp_ten_unit_reps.at(j)->tenant_id_;
        (*(iter_lower + j))->unit_rep_cnt_ = tmp_ten_unit_reps.at(j)->unit_rep_cnt_;
      }
    }
  }
  return ret;
}

int ObSinglePartBalance::set_unit_replica_capacity(const share::ObPartitionReplica& r,
    share::TenantUnitRepCnt& tenant_unit_rep_cnt, const int64_t index_num, const int64_t now_time,
    const int64_t non_table_cnt)
{
  int ret = OB_SUCCESS;
  if (r.replica_type_ == REPLICA_TYPE_FULL && r.get_memstore_percent() != 0) {
    tenant_unit_rep_cnt.unit_rep_cnt_.f_replica_cnt_++;
  } else if (r.replica_type_ == REPLICA_TYPE_FULL && r.get_memstore_percent() == 0) {
    tenant_unit_rep_cnt.unit_rep_cnt_.d_replica_cnt_++;
  } else if (r.replica_type_ == REPLICA_TYPE_LOGONLY) {
    tenant_unit_rep_cnt.unit_rep_cnt_.l_replica_cnt_++;
  } else if (r.replica_type_ == REPLICA_TYPE_READONLY) {
    tenant_unit_rep_cnt.unit_rep_cnt_.r_replica_cnt_++;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not allow replica type", KR(ret), K(r.replica_type_));
  }
  if (OB_SUCC(ret)) {
    tenant_unit_rep_cnt.unit_rep_cnt_.index_num_ = index_num;
    tenant_unit_rep_cnt.now_time_ = now_time;
    tenant_unit_rep_cnt.non_table_cnt_ = non_table_cnt;
    if (r.is_leader_like()) {
      // Record leader information
      tenant_unit_rep_cnt.unit_rep_cnt_.leader_cnt_++;
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
