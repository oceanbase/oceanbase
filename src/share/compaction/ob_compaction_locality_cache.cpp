//Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX COMMON
#include "share/compaction/ob_compaction_locality_cache.h"
#include "src/storage/compaction/ob_medium_compaction_func.h"
#include "src/storage/compaction/ob_compaction_util.h"
#include "src/share/ob_zone_merge_info.h"
#include "observer/ob_server_struct.h"
#include "src/share/ob_zone_merge_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace share
{

ObCompactionLocalityCache::ObCompactionLocalityCache()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    merge_info_mgr_(nullptr),
    ls_infos_map_()
{}

ObCompactionLocalityCache::~ObCompactionLocalityCache()
{
  destroy();
}

int ObCompactionLocalityCache::init(const uint64_t tenant_id, rootserver::ObMajorMergeInfoManager *merge_info_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ls_infos_map_.create(OB_MAX_LS_NUM_PER_TENANT_PER_SERVER, "CaLsInfoMap", "CaLsInfoNode", tenant_id))) {
    LOG_WARN("fail to create ls info map", K(ret));
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    tenant_id_ = tenant_id;
    merge_info_mgr_ = merge_info_mgr;
    is_inited_ = true;
  }
  return ret;
}

void ObCompactionLocalityCache::destroy()
{
  is_inited_ = false;
  merge_info_mgr_ = nullptr;
  if (ls_infos_map_.created()) {
    ls_infos_map_.destroy();
  }
}

int ObCompactionLocalityCache::refresh_ls_locality(const bool force_refresh)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = inner_refresh_ls_locality();
#else
  if (force_refresh || empty() || REACH_TENANT_TIME_INTERVAL(CHECK_LS_LOCALITY_INTERVAL)) {
    ret = inner_refresh_ls_locality();
  }
#endif
  return ret;
}

int ObCompactionLocalityCache::inner_refresh_ls_locality()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObSEArray<ObZone, 10> zone_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageLocalityCache is not inited", KR(ret), K_(tenant_id));
  } else if (nullptr != merge_info_mgr_) {
    if (OB_FAIL(merge_info_mgr_->get_zone_merge_mgr().get_zone(zone_list))) {
      LOG_WARN("failed to get zone list", KR(ret));
    }
  } else if (OB_FAIL(get_zone_list_from_inner_table(zone_list))) {
    LOG_WARN("failed to get zone list", K(ret), K_(tenant_id));
  } else if (zone_list.empty()) {
    LOG_INFO("zone list is empty, skip get ls locality", K(ret), K_(tenant_id));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(MTL(compaction::ObDiagnoseTabletMgr *)->add_diagnose_tablet(UNKNOW_LS_ID, UNKNOW_TABLET_ID,
        share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE))) {
      LOG_WARN("failed to add diagnose tablet for locality cache", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // 1. clear ls_infos cached in memory
    ls_infos_map_.reuse();
    // 2. load ls_infos from __all_ls_meta_table
    ObArray<ObLSInfo> ls_infos;
    ls_infos.set_attr(ObMemAttr(tenant_id_, "RefLSInfos"));
    const bool inner_table_only = false;
    if (OB_ISNULL(GCTX.lst_operator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lst_operator is null", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(GCTX.lst_operator_->get_by_tenant(tenant_id_, inner_table_only, ls_infos))) {
      LOG_WARN("fail to get ls infos", KR(ret), K_(tenant_id));
    } else {
      // 3. update ls_infos cached in memory
      const int64_t ls_infos_cnt = ls_infos.count();
      for (int64_t i = 0; (i < ls_infos_cnt) && OB_SUCC(ret); ++i) {
        const ObLSInfo &ls_info = ls_infos.at(i);
        if (OB_FAIL(refresh_by_zone(ls_info, zone_list))) {
          LOG_WARN("fail to refresh by zone", K(ret), K(ls_info), K_(tenant_id), K(zone_list));
        }
      }
    }
    cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
    LOG_INFO("finish to refresh ls locality cache", KR(ret), K_(tenant_id), K(cost_ts), K(zone_list));
  }
  return ret;
}

int ObCompactionLocalityCache::get_ls_info(const share::ObLSID &ls_id, share::ObLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageLocalityCache is not inited", KR(ret));
  } else if (OB_FAIL(ls_infos_map_.get_refactored(ls_id, ls_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get ls_info from ls_info_map", KR(ret), K(ls_id));
    }
  }
  return ret;
}

struct ObMemberListInfo
{
  ObMemberListInfo(const uint64_t tenant_id)
    : member_list_array_()
  {
    member_list_array_.set_attr(ObMemAttr(tenant_id, "MemListInfo"));
  }
  ~ObMemberListInfo() {}
  int build(const ObLSReplica &tmp_replica);
  bool check_exist(const common::ObAddr &addr);
  bool empty() { return member_list_array_.empty(); }
  TO_STRING_KV("member_list_cnt", member_list_array_.count(), K(member_list_array_));
  ObSEArray<common::ObAddr, 6> member_list_array_; // including member_list & learner_list
};

int ObMemberListInfo::build(
  const ObLSReplica &tmp_replica)
{
  int ret = OB_SUCCESS;
  const ObLSReplica::MemberList *member_list = &tmp_replica.get_member_list();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < member_list->count(); ++idx) {
    if (OB_FAIL(member_list_array_.push_back(member_list->at(idx)))) {
      LOG_WARN("failed to push addr", KR(ret));
    }
  } // for
  const common::GlobalLearnerList &learner_list = tmp_replica.get_learner_list();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < learner_list.get_member_number(); ++idx) {
    common::ObAddr addr;
    if (OB_FAIL(learner_list.get_server_by_index(idx, addr))) {
      LOG_WARN("failed to push addr", KR(ret), K(idx));
    } else if (OB_FAIL(member_list_array_.push_back(addr))) {
      LOG_WARN("failed to push addr", KR(ret), K(addr));
    }
  } // for
  return ret;
}

bool ObMemberListInfo::check_exist(const common::ObAddr &addr)
{
  bool found = false;
  for (int64_t idx = 0; !found && idx < member_list_array_.count(); ++idx) {
    if (member_list_array_.at(idx) == addr) {
      found = true;
    }
  }
  return found;
}

int ObCompactionLocalityCache::refresh_by_zone(
    const share::ObLSInfo &ls_info,
    const ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  if (!ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_info", K(ret), K(ls_info), K(zone_list));
  } else {
    const ObLSID &ls_id = ls_info.get_ls_id();
    // create with tenant_id to set_attr for ObSArray<ObLSReplica> in ObLSInfo
    ObLSInfo tmp_ls_info(tenant_id_, ls_id);
    const ObLSInfo::ReplicaArray &all_replicas = ls_info.get_replicas();
    ObMemberListInfo member_list_array(tenant_id_); // including member_list & learner_list
    for (int64_t i = 0; OB_SUCC(ret) && i < all_replicas.count(); ++i) {
      const ObLSReplica &tmp_replica = all_replicas.at(i);
      if (ObRole::LEADER == tmp_replica.get_role()) {
        if (OB_FAIL(member_list_array.build(tmp_replica))) {
          LOG_WARN("failed to build member list", KR(ret), K(tmp_replica));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && member_list_array.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no leader in ls replica", KR(ret), K(all_replicas));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_replicas.count(); ++i) {
      const ObLSReplica &tmp_replica = all_replicas.at(i);
      if (replica_in_zone_list(tmp_replica, zone_list) // replica in zone list
          && member_list_array.check_exist(tmp_replica.get_server())) { // replica in member list
        if (tmp_ls_info.is_valid()) {
          if (OB_FAIL(tmp_ls_info.add_replica(tmp_replica))) {
            LOG_WARN("fail to add replica", KR(ret), K(tmp_replica));
          }
        } else if (OB_FAIL(tmp_ls_info.init_by_replica(tmp_replica))) {
          LOG_WARN("fail to init ls_info by replica", KR(ret), K(tmp_replica));
        }
      }
    }
    if (FAILEDx(ls_infos_map_.set_refactored(ls_id, tmp_ls_info, 1/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(ls_id), K(tmp_ls_info));
    } else {
      FLOG_INFO("success to refresh cached ls_info", K(ret), K(tmp_ls_info), K(zone_list), K(member_list_array));
    }
  }
  return ret;
}

bool ObCompactionLocalityCache::replica_in_zone_list(
    const ObLSReplica &ls_replica,
    const ObIArray<common::ObZone> &zone_list)
{
  bool ret = false;
  if (ls_replica.is_valid()) {
    for (int64_t i = 0; i < zone_list.count(); ++i) {
      if (ls_replica.get_zone() == zone_list.at(i)) {
        ret = true;
        break;
      }
    }
  }
  return ret;
}

int ObCompactionLocalityCache::get_zone_list_from_inner_table(ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reuse();
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' AND previous_locality = ''",
        OB_ALL_TENANT_TNAME, tenant_id_))) {
      LOG_WARN("fail to append sql", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K_(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), K_(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next", KR(ret), K_(tenant_id), K(sql));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      int64_t tmp_real_str_len = 0; // used to fill output argument
      SMART_VAR(char[MAX_ZONE_LIST_LENGTH], zone_list_str) {
        zone_list_str[0] = '\0';
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "zone_list", zone_list_str,
                                  MAX_ZONE_LIST_LENGTH, tmp_real_str_len);
        if (FAILEDx(str2zone_list(zone_list_str, zone_list))) {
          LOG_WARN("fail to str2zone_list", KR(ret), K(zone_list_str));
        }
      }
    }
  }
  return ret;
}

int ObCompactionLocalityCache::str2zone_list(
    const char *str,
    ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  zone_list.reuse();
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is null", KP(str), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ";", &save_ptr);
      if (NULL != item_str) {
        if (OB_FAIL(zone_list.push_back(ObZone(item_str)))) {
          LOG_WARN("fail to push_back", KR(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
