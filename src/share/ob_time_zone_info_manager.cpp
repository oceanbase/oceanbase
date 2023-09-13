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
#include "rootserver/ob_root_service.h"
#include "share/ob_time_zone_info_manager.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_fast_convert.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/lock/ob_spin_rwlock.h"

using namespace oceanbase::share;
using namespace oceanbase::observer;
namespace oceanbase
{
namespace common
{

ObTZInfoMap ObTimeZoneInfoManager::shared_tz_info_map_;
int64_t ObTimeZoneInfoManager::loaded_tz_info_count_ = 0;
SpinRWLock ObTimeZoneInfoManager::sys_rwlock_;
const char *ObTimeZoneInfoManager::FETCH_TZ_INFO_SQL =
    "SELECT * "
    "FROM ("
    "SELECT t1.time_zone_id, t1.inner_tz_id, t1.name, t3.transition_time, t2.offset, t2.is_dst, "
    "t2.transition_type_id, t2.abbreviation, "
    "row_number() over (partition by t1.inner_tz_id, t3.transition_time order by t2.transition_type_id) as tran_row_number "
    "FROM (SELECT time_zone_id, row_number() over (order by time_zone_id) as inner_tz_id, name "
    "      FROM oceanbase.__all_time_zone_name "
    "      ORDER BY time_zone_id "
    ") t1 "
    "JOIN oceanbase.__all_time_zone_transition_type t2 "
    "ON t1.time_zone_id = t2.time_zone_id "
    "LEFT JOIN oceanbase.__all_time_zone_transition t3 "
    "ON t2.time_zone_id = t3.time_zone_id "
    "  AND t2.transition_type_id=t3.transition_type_id "
    ") tz_info WHERE tz_info.tran_row_number = 1 "
    "ORDER BY tz_info.time_zone_id, tz_info.transition_time ";

const char *ObTimeZoneInfoManager::FETCH_TENANT_TZ_INFO_SQL =
    "SELECT * "
    "FROM ("
    "SELECT t1.time_zone_id, t1.inner_tz_id, t1.name, t3.transition_time, t2.offset, t2.is_dst, "
    "t2.transition_type_id, t2.abbreviation, "
    "row_number() over (partition by t1.inner_tz_id, t3.transition_time order by t2.transition_type_id) as tran_row_number "
    "FROM (SELECT time_zone_id, row_number() over (order by time_zone_id) as inner_tz_id, name "
    "      FROM oceanbase.__all_tenant_time_zone_name "
    "      ORDER BY time_zone_id "
    ") t1 "
    "JOIN oceanbase.__all_tenant_time_zone_transition_type t2 "
    "ON t1.time_zone_id = t2.time_zone_id "
    "LEFT JOIN oceanbase.__all_tenant_time_zone_transition t3 "
    "ON t2.time_zone_id = t3.time_zone_id "
    "  AND t2.transition_type_id=t3.transition_type_id "
    ") tz_info WHERE tz_info.tran_row_number = 1 "
    "ORDER BY tz_info.time_zone_id, tz_info.transition_time ";

const char *ObTimeZoneInfoManager::FETCH_LATEST_TZ_VERSION_SQL =
  "SELECT value from oceanbase.__all_sys_stat where name = 'current_timezone_version'";

int ObTimeZoneInfoManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, "TZInfoMgrMap");
    if (OB_FAIL(tz_info_map_buf_.init(attr))) {
      LOG_WARN("init tz info map failed", K(ret), K(tenant_id_));
    } else if (OB_FAIL(tz_info_map_.init(attr))) {
      LOG_WARN("init tz info map failed", K(ret), K(tenant_id_));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObTimeZoneInfoManager::update_time_zone_info(int64_t tz_info_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(last_version_ < tz_info_version)) {
    LOG_INFO("begin update time zone info", K(last_version_), K(tz_info_version));
    if (OB_FAIL(fetch_time_zone_info())) {
      LOG_WARN("fail to request tz_info", K(ret));
    }
  } else if (OB_UNLIKELY(-1 == last_version_)) {
    is_usable_ = true;
    LOG_INFO("set is_usable", K(last_version_), K(tz_info_version));
  }
  return ret;
}

bool ObTimeZoneInfoManager::FillRequestTZInfoResult::operator() (
    ObTZIDKey key, ObTimeZoneInfoPos *tz_info)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  if (OB_ISNULL(tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz info is NULL", K(tz_info));
  } else if (OB_FAIL(tz_result_.tz_array_.push_back(*tz_info))) {
    LOG_WARN("fail to serialize tz_info", KPC(tz_info), K(ret));
  }
  return OB_SUCCESS == ret;
}

int ObTimeZoneInfoManager::response_time_zone_info(ObRequestTZInfoResult &tz_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    tz_result.tz_array_.reset();
    tz_result.last_version_ = last_version_;
    FillRequestTZInfoResult fr_func(tz_result);
    if (OB_FAIL(tz_info_map_.id_map_->for_each(fr_func))) {
      LOG_WARN("fail to call for_each", K(ret));
    }
  }
  return ret;
}

void ObTimeZoneInfoManager::TaskProcessThread::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task is NULL", K(ret));
  } else {
    TZInfoTask *tz_info_task = static_cast<TZInfoTask *>(task);
    if (OB_FAIL(tz_info_task->run_task())) {
      LOG_WARN("fail to run task", K(ret));
    }
  }
}

int ObTimeZoneInfoManager::fetch_time_zone_info()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t current_tz_version = -1;
    ObSQLClientRetryWeak sql_client_retry_weak(&sql_proxy_, tenant_id_, OB_ALL_SYS_STAT_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id_, FETCH_LATEST_TZ_VERSION_SQL))) {
        LOG_WARN("fail to execute sql", K(ret), K(tenant_id_));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", K(result), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // all_sys_stat中没有timezone_version，说明处于升级过程中
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("ObMySQLResult next failed", K(ret));
        }
      } else {
        ObString version_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", version_str);
        bool is_valid = false;
        current_tz_version = ObFastAtoi<int64_t>::atoi(version_str.ptr(),
            version_str.ptr() + version_str.length(), is_valid);
        if (!is_valid) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid key version", K(ret), K(version_str));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fetch_time_zone_info_from_tenant_table(current_tz_version))) {
      LOG_WARN("fetch timezone info from tenant tz table failed", K(ret));
    }
  }

  return ret;
}

// 使用sql
int ObTimeZoneInfoManager::fetch_time_zone_info_from_tenant_table(const int64_t current_tz_version)
{
  int ret = OB_SUCCESS;
  if (current_tz_version == last_version_) {
    // already latest
  } else if (current_tz_version < last_version_) {
    LOG_ERROR("current timezone version lower than local tz map version, wierd",
      K(tenant_id_), K(current_tz_version), K(last_version_));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(&sql_proxy_, tenant_id_, OB_ALL_TENANT_TIME_ZONE_NAME_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id_, FETCH_TENANT_TZ_INFO_SQL))){
        LOG_WARN("fetch time zone data failed", K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", K(result), K(ret));
      } else {
        bool same_with_sys = false;
        bool first_map = 1 == ATOMIC_AAF(&loaded_tz_info_count_, 1);
        if (first_map) {
          SpinWLockGuard wlock_guard(sys_rwlock_);
          // the first tenant load timezone map into shared_tz_info_map_.
          // Other tenants load timezone map into tz_info_map_buf_ and then cmp with shared_tz_info_map_.
          ObMemAttr attr(OB_SERVER_TENANT_ID, "TZInfoMgrMap");
          if (OB_FAIL(shared_tz_info_map_.init(attr))) {
            LOG_WARN("init share tz info map failed", K(ret));
          } else if (OB_FAIL(fill_tz_info_map(*result, shared_tz_info_map_, tenant_id_))) {
            LOG_ERROR("fail to fill tz_info_map for shared map", K(ret), K(tenant_id_));
          } else {
            tz_info_map_.id_map_ = shared_tz_info_map_.id_map_;
            tz_info_map_.name_map_ = shared_tz_info_map_.name_map_;
          }
          LOG_INFO("share tz info inited", K(ret), K(shared_tz_info_map_.id_map_),
                  K(shared_tz_info_map_.name_map_));
        } else {
          if (OB_FAIL(fill_tz_info_map(*result, tz_info_map_buf_, tenant_id_))) {
            LOG_ERROR("fail to fill tz_info_map", K(ret));
          } else {
            same_with_sys = cmp_tz_info_map(shared_tz_info_map_, tz_info_map_buf_);
            if (same_with_sys) {
              // same with sys tenant, reset tz_info_map_buf_.
              // if reset link hash map, memory of hash node will not be released, so use destroy.
              tz_info_map_buf_.~ObTZInfoMap();
              // Use destroyed ObTZInfoMap will lead to a crash.
              // According to current design, upgrade timezone is not supported,
              // so ObTZInfoMap will not be used again. This is a defense code.
              new (&tz_info_map_buf_) ObTZInfoMap();
              LOG_INFO("reset tz info map buf", K(tenant_id_));
              tz_info_map_.id_map_ = shared_tz_info_map_.id_map_;
              tz_info_map_.name_map_ = shared_tz_info_map_.name_map_;
            } else {
              tz_info_map_.id_map_ = tz_info_map_buf_.id_map_;
              tz_info_map_.name_map_ = tz_info_map_buf_.name_map_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          last_version_ = current_tz_version;
          LOG_INFO("success to fetch tz_info map", K(ret), K(last_version_), K(tenant_id_),
                    "new_last_version", current_tz_version, K(same_with_sys),
                    K(tz_info_map_.id_map_->size()), K(first_map));
        }
      }
    }
  }
  if (OB_SUCC(ret) &&
      OB_UNLIKELY(OB_SYS_TENANT_ID == tenant_id_ && false == OTTZ_MGR.is_usable())) {
    OTTZ_MGR.set_usable();
  }

  return ret;
}

int ObTimeZoneInfoManager::set_tz_info_map(ObTimeZoneInfoPos *&stored_tz_info,
    ObTimeZoneInfoPos &new_tz_info,
    ObTZInfoMap &tz_info_map)
{
  int ret = OB_SUCCESS;
  ObTimeZoneInfoPos *tz_pos_value = NULL;
  ObTZNameIDInfo *name_id_value = NULL;
  bool is_equal = false;
  lib::ObMemAttr attr1(OB_SERVER_TENANT_ID, "TZInfoArray");
  if (NULL == stored_tz_info) {
    if (OB_ISNULL(tz_pos_value = op_alloc(ObTimeZoneInfoPos))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for ObTimeZoneInfoPos ", K(ret));
    } else if (OB_ISNULL(name_id_value = op_alloc(ObTZNameIDInfo))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for ObTZNameIDInfo ", K(ret));
    } else if (FALSE_IT(tz_pos_value->set_tz_type_attr(attr1))) {
    } else if (OB_FAIL(tz_pos_value->assign(new_tz_info))) {
      LOG_WARN("fail to assign ObTimeZoneInfoPos", K(new_tz_info), K(ret));
    } else if (FALSE_IT(name_id_value->set(new_tz_info.get_tz_id(), new_tz_info.get_tz_name()))) {
    } else if (OB_FAIL(tz_info_map.id_map_->insert_and_get(new_tz_info.get_tz_id(), tz_pos_value))) {
      LOG_WARN("fail to insert new_tz_info to tz_info_id_map", KPC(tz_pos_value), K(ret));
    } else if (OB_FAIL(tz_info_map.name_map_->insert_and_get(new_tz_info.get_tz_name(), name_id_value))) {
      tz_info_map.id_map_->revert(tz_pos_value);
      tz_info_map.id_map_->del(new_tz_info.get_tz_id());
      tz_pos_value = NULL;
      LOG_WARN("fail to insert new_tz_info to tz_info_name_map_", K(name_id_value), K(ret));
    } else {
      tz_info_map.id_map_->revert(tz_pos_value);
      tz_info_map.name_map_->revert(name_id_value);
      LOG_INFO("succ to add new time zone info", K(&tz_info_map), K(&(tz_info_map.name_map_)),
               KPC(name_id_value), KPC(tz_pos_value));
      tz_pos_value = NULL;
      name_id_value = NULL;
    }
  } else if (OB_FAIL(stored_tz_info->compare_upgrade(new_tz_info, is_equal))) {
    LOG_WARN("fail to compare_upgrade", KPC(stored_tz_info), K(new_tz_info), K(ret));
  } else if (is_equal) {
    //do nothing
  } else {
    LOG_INFO("need to upgrade transition time", KPC(stored_tz_info), K(new_tz_info));
    common::ObSArray<ObTZTransitionTypeInfo> &next_tz_tran_types = stored_tz_info->get_next_tz_tran_types();
    common::ObSArray<ObTZRevertTypeInfo> &next_tz_revt_types = stored_tz_info->get_next_tz_revt_types();
    if (OB_FAIL(next_tz_tran_types.assign(new_tz_info.get_tz_tran_types()))) {
      LOG_WARN("fail to assign next_tz_tran_types", K(new_tz_info.get_tz_tran_types()), K(ret));
    } else if (OB_FAIL(next_tz_revt_types.assign(new_tz_info.get_tz_revt_types()))) {
      LOG_WARN("fail to assign next_tz_revt_types", K(new_tz_info.get_tz_revt_types()), K(ret));
    } else {
      stored_tz_info->inc_curr_idx();
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != tz_pos_value) {
      op_free(tz_pos_value);
      tz_pos_value = NULL;
    }
    if (NULL != name_id_value) {
      op_free(name_id_value);
      name_id_value = NULL;
    }
  }
  return ret;
}

int ObTimeZoneInfoManager::prepare_tz_info(const ObIArray<ObTZTransitionTypeInfo> &types_with_null,
                                           ObTimeZoneInfoPos &type_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_default_tran_type(types_with_null, type_info))) {
    LOG_WARN("fail to calc default transition type", K(ret));
  } else if (OB_FAIL(type_info.calc_revt_types())) {
    LOG_WARN("fail to calc revert types", K(ret));
  }
  return ret;
}

int ObTimeZoneInfoManager::calc_default_tran_type(const ObIArray<ObTZTransitionTypeInfo> &types_with_null,
                                                  ObTimeZoneInfoPos &type_info)
{
  int ret = OB_SUCCESS;
  int64_t type_count = types_with_null.count();
  int64_t i = 0;
  if (OB_UNLIKELY(type_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type count", K(ret));
  } else {
    for (; i <type_count && types_with_null.at(i).is_dst(); ++i) { /*do nothing*/ }
    i = (type_count == i ? 0 : i);
    if (OB_FAIL(type_info.set_default_tran_type(types_with_null.at(i)))) {
      LOG_WARN("fail to set default tran type", K(i), "default_tran_type", types_with_null.at(i), K(types_with_null),K(ret));
    }
  }
  return ret;
}

int ObTimeZoneInfoManager::fill_tz_info_map(sqlclient::ObMySQLResult &result,
    ObTZInfoMap &tz_info_map,
    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObString tz_name_str;
  ObString tz_abbr_str;
  int64_t tz_id = 0;
  UNUSED(tz_id);
  int64_t inner_tz_id = 0;
  ObTZTransitionTypeInfo tz_tran_type;
  ObTimeZoneInfoPos tz_info;
  ObTimeZoneInfoPos *stored_tz_info = NULL;;
  ObArray<ObTZTransitionTypeInfo> types_with_null;
  bool is_tran_time_null = false;
  while(OB_SUCC(ret) && OB_SUCC(result.next())) {
    tz_tran_type.reset();
    is_tran_time_null = false;
    inner_tz_id = 0;
    tz_name_str.reset();
    tz_abbr_str.reset();
    EXTRACT_INT_FIELD_MYSQL(result, "time_zone_id", tz_id, int64_t);
    EXTRACT_UINT_FIELD_MYSQL(result, "inner_tz_id", inner_tz_id, int64_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "name", tz_name_str)
    EXTRACT_INT_FIELD_MYSQL(result, "transition_type_id", tz_tran_type.info_.tran_type_id_, int32_t);
    EXTRACT_VARCHAR_FIELD_MYSQL(result, "abbreviation", tz_abbr_str)
    EXTRACT_INT_FIELD_MYSQL(result, "offset", tz_tran_type.info_.offset_sec_, int32_t);
    EXTRACT_BOOL_FIELD_MYSQL(result, "is_dst", tz_tran_type.info_.is_dst_);

    if (OB_SUCC(ret)) {
      int64_t int_value = 0;
      if (OB_FAIL(result.get_int("transition_time", int_value))) {
        //is NULL transition time
        if (OB_ERR_NULL_VALUE == ret) {
          ret = OB_SUCCESS;
          is_tran_time_null = true;
        } else {
          LOG_WARN("fail to get column transition_time in row", K(ret));
        }
      } else {
        tz_tran_type.lower_time_ = int_value;
      }
    }

    if (OB_SUCC(ret)) {
      if (tz_name_str.empty() || tz_name_str.length() >= OB_MAX_TZ_NAME_LEN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("tz name size is overflow", K(tz_name_str), K(ret));
      } else if (tz_abbr_str.empty() || tz_abbr_str.length() >= OB_MAX_TZ_ABBR_LEN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("tz abbr size is overflow", K(tz_abbr_str), K(ret));
      } else {
        tz_tran_type.set_tz_abbr(tz_abbr_str);
      }
    }

    LOG_DEBUG("succ to read one row", K(tz_tran_type), K(tz_id), K(inner_tz_id), K(tz_name_str), K(tz_abbr_str));

    //set tz_info_map and create new tz_info
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!ObOTimestampData::is_valid_tz_id(static_cast<int32_t>(inner_tz_id)))
        || OB_UNLIKELY(!ObOTimestampData::is_valid_tran_type_id(tz_tran_type.info_.tran_type_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tz_id or tran_type_id", K(inner_tz_id), "tran_type_id", tz_tran_type.info_.tran_type_id_, K(ret));
    } else if (inner_tz_id != tz_info.get_tz_id()) {
      if (tz_info.is_valid()) {//not the first record
        if (OB_FAIL(prepare_tz_info(types_with_null, tz_info))) {
          LOG_WARN("fail to prepare prepare time zone info", K(tz_info), K(ret));
        } else if (OB_FAIL(set_tz_info_map(stored_tz_info, tz_info, tz_info_map))) {
          LOG_WARN("fail to set tz_info map", K(ret));
        } else {
          tz_info.reset();
          types_with_null.reset();
          tz_info_map.id_map_->revert(stored_tz_info);
          stored_tz_info = NULL;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tz_info.set_tz_name(tz_name_str.ptr(), tz_name_str.length()))) {
        LOG_WARN("fail to set tz_name", K(ret));
      } else {
        tz_info.set_tz_id(inner_tz_id);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(stored_tz_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stored_tz_info should be null here", K(ret));
      } else if (OB_FAIL(tz_info_map.get_tz_info_by_name(tz_name_str, stored_tz_info))) {
        if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
          LOG_DEBUG("fail to get stored_tz_info, it is a new tz info", K(tz_name_str), K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get stored_tz_info", K(tz_name_str), K(ret));
        }
      } else if (OB_UNLIKELY(stored_tz_info->get_tz_id() != inner_tz_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tz id should not changed", K(tz_name_str), KPC(stored_tz_info), K(inner_tz_id), K(ret));
      }
    }

    //add transition_type to tz_info
    if (OB_SUCC(ret)) {
      if (OB_FAIL(types_with_null.push_back(tz_tran_type))) {
        LOG_WARN("fail to push bak tz_tran_type", K(ret));
      } else if (!is_tran_time_null && OB_FAIL(tz_info.add_tran_type_info(tz_tran_type))) {
        LOG_WARN("fail to push bak tz_tran_type", K(ret));
      } else {
        LOG_DEBUG("succ to add tz_tran_type", K(tz_name_str), K(tz_tran_type));
      }
    }
  }//while
  //set last tz_info
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get_next row", K(ret));
  } else if (tz_info.is_valid()) {
    if (OB_FAIL(prepare_tz_info(types_with_null, tz_info))) {
      LOG_WARN("fail to prepare prepare time zone info", K(tz_info), K(ret));
    } else if (OB_FAIL(set_tz_info_map(stored_tz_info, tz_info, tz_info_map))) {
      LOG_WARN("fail to set tz_info map", K(ret));
    }
  }

  tz_info_map.id_map_->revert(stored_tz_info);
  stored_tz_info = NULL;
  return ret;
}

int ObTimeZoneInfoManager::find_time_zone_info(const common::ObString &tz_name, ObTimeZoneInfoPos &tz_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tz_info_map_.get_tz_info_by_name(tz_name, tz_info))) {
    LOG_WARN("fail to get time zone info", K(tz_name), K(ret));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_UNKNOWN_TIME_ZONE;
    }
  }
  return ret;
}

bool ObTimeZoneInfoManager::cmp_tz_info_map(ObTZInfoMap &map1, ObTZInfoMap &map2)
{
  bool same = true;
  bool locked = false;
  if (!(locked = sys_rwlock_.try_rdlock())) {
    same = false;
  } else if (map1.id_map_->size() != map2.id_map_->size() || map1.name_map_->size() != map2.name_map_->size()) {
    same = false;
    LOG_INFO("tz info map not same", K(map1.id_map_->size()), K(map2.id_map_->size()),
             K(map1.name_map_->size()), K(map2.name_map_->size()));
  }
  if (same) {
    // compare id map.
    ObTZInfoIDPosMap::Iterator iter1(*map1.id_map_);
    ObTZInfoIDPosMap::Iterator iter2(*map2.id_map_);
    ObTimeZoneInfoPos *tz_pos1 = NULL;
    ObTimeZoneInfoPos *tz_pos2 = NULL;
    tz_pos1 = iter1.next(tz_pos1);
    tz_pos2 = iter2.next(tz_pos2);
    while (tz_pos1 != NULL && tz_pos2 != NULL && same) {
      same = (*tz_pos1) == (*tz_pos2);
      iter1.revert(tz_pos1);
      iter2.revert(tz_pos2);
      tz_pos1 = iter1.next(tz_pos1);
      tz_pos2 = iter2.next(tz_pos2);
    }
      iter1.revert(tz_pos1);
      iter2.revert(tz_pos2);
    same = same && (NULL == tz_pos1) && (NULL == tz_pos2);
  }
  if (same) {
    // compare name map.
    ObTZInfoNameIDMap::Iterator iter1(*map1.name_map_);
    ObTZInfoNameIDMap::Iterator iter2(*map2.name_map_);
    ObTZNameIDInfo *tz_id_info1 = NULL;
    ObTZNameIDInfo *tz_id_info2 = NULL;
    tz_id_info1 = iter1.next(tz_id_info1);
    tz_id_info2 = iter2.next(tz_id_info2);
    while (tz_id_info1 != NULL && tz_id_info2 != NULL && same) {
      same = (*tz_id_info1) == (*tz_id_info2);
      iter1.revert(tz_id_info1);
      iter2.revert(tz_id_info2);
      tz_id_info1 = iter1.next(tz_id_info1);
      tz_id_info2 = iter2.next(tz_id_info2);
    }
    iter1.revert(tz_id_info1);
    iter2.revert(tz_id_info2);
    same = same && (NULL == tz_id_info1) && (NULL == tz_id_info2);
  }
  if (locked) {
    sys_rwlock_.unlock();
  }
  return same;
}

OB_SERIALIZE_MEMBER(ObRequestTZInfoArg, obs_addr_, tenant_id_);


OB_SERIALIZE_MEMBER(ObRequestTZInfoResult, last_version_, tz_array_);

}//common
}//oceanbase
