//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_storage_locality_cache.h"
#include "src/share/ob_zone_merge_info.h"
#include "observer/ob_server_struct.h"
#include "src/share/ob_zone_merge_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace compaction
{
using namespace share;

int ObLSLocalityInCache::assgin(ObLSLocalityInCache &ls_locality)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_locality.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "input ls locality is invalid", K(ret), K(ls_locality));
  } else {
    reset();
    if (OB_FAIL(svr_addr_list_.assign(ls_locality.svr_addr_list_))) {
      STORAGE_LOG(WARN, "failed to copy svr addr list", K(ret), K(ls_locality));
    } else {
      ls_id_ = ls_locality.ls_id_;
    }
  }
  return ret;
}

// locality will return exist when ls locality is invalid
bool ObLSLocality::check_exist(const common::ObAddr &addr) const
{
  bool exist = true;
  if (is_valid()) {
    exist = false;
    for (int64_t j = 0; j < svr_addr_list_.count(); ++j) {
      if (addr == svr_addr_list_.at(j)) {
        exist = true;
        break;
      }
    } // end of for
  }
  return exist;
}

ObStorageLocalityCache::ObStorageLocalityCache()
  : is_inited_(false),
    lock_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr),
    alloc_buf_(nullptr),
    ls_locality_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{}

ObStorageLocalityCache::~ObStorageLocalityCache()
{
  reset();
}

int ObStorageLocalityCache::init(
  uint64_t tenant_id,
  ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(sql_proxy));
  } else {
    ObMemAttr attr(tenant_id, "StoLocCache");
    allocator_.set_attr(attr);
    ls_locality_array_.set_attr(attr);
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObStorageLocalityCache::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = nullptr;
  ls_locality_array_.reset();
  if (OB_NOT_NULL(alloc_buf_)) {
    allocator_.free(alloc_buf_);
    alloc_buf_ = nullptr;
  }
  allocator_.reset();
}

int ObStorageLocalityCache::refresh_ls_locality()
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  ObSEArray<ObZone, 10> zone_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageLocalityCache is not inited", KR(ret));
  } else if (OB_FAIL(get_zone_list(zone_list))) {
    LOG_WARN("failed to get zone list", K(ret));
  } else if (OB_FAIL(get_ls_locality_by_zone(zone_list))) {
    LOG_WARN("failed to get ls locality by zone", K(ret));
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  LOG_INFO("refresh ls locality cache", K(ret), K(cost_ts), "ls_count", ls_locality_array_.count(),
    K_(ls_locality_array));
  return ret;
}

int ObStorageLocalityCache::get_zone_list(ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reuse();
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' AND previous_locality = ''",
        OB_ALL_TENANT_TNAME, tenant_id_))) {
      LOG_WARN("fail to append sql", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
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

// TODO(@lixia.yq) replace ls_locality cache in RS::ObZoneMergeManagerBase
int ObStorageLocalityCache::str2zone_list(
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

int ObStorageLocalityCache::get_ls_locality_cnt(
  ObMySQLTransaction &trans,
  const ObIArray<ObZone> &zone_list,
  int64_t &ls_locality_cnt)
{
  int ret = OB_SUCCESS;
  ls_locality_cnt = 0;
  const uint64_t meta_tenant_id = get_private_table_exec_tenant_id(tenant_id_);
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("select count(*) ls_count from %s WHERE tenant_id = '%lu' AND zone IN ( ",
        OB_ALL_LS_META_TABLE_TNAME, tenant_id_))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(append_zone_info(zone_list, sql))) {
        LOG_WARN("failed to append zone info", K(ret), K(zone_list));
    } else if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("failed to append sql", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(trans.read(res, meta_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is nullptr", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("no result", K(ret), K(sql));
    } else {
      int64_t count = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_LS_COUNT, count, int64_t);
      if (OB_SUCC(ret)) {
        ls_locality_cnt = count;
      }
    }
  }
  return ret;
}

int ObStorageLocalityCache::get_ls_locality_by_zone(const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = get_private_table_exec_tenant_id(tenant_id_);
  int64_t ls_locality_cnt = 0;
  ObMySQLTransaction trans; // read trans
  if (OB_UNLIKELY(zone_list.empty())) {
    LOG_TRACE("zone list is empty, skip get ls locality", K(ret), K_(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(get_ls_locality_cnt(trans, zone_list, ls_locality_cnt))) {
    LOG_WARN("failed to get ls locality cnt", K(ret));
  } else if (OB_UNLIKELY(0 == ls_locality_cnt)) {
    LOG_WARN("empty ls locality", K(ls_locality_cnt));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT ls_id,svr_ip,svr_port FROM %s WHERE tenant_id = '%lu' AND zone IN ( ",
          OB_ALL_LS_META_TABLE_TNAME, tenant_id_))) {
        LOG_WARN("failed to append sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(append_zone_info(zone_list, sql))) {
        LOG_WARN("failed to append zone info", K(ret), K(zone_list));
      } else if (OB_FAIL(sql.append_fmt(") ORDER BY ls_id"))) {
        LOG_WARN("failed to append sql", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(trans.read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K_(tenant_id), K(sql));
      } else if (OB_FAIL(generate_ls_locality(ls_locality_cnt, *result))) {
        LOG_WARN("failed to generate ls locality", K(ret), K(ls_locality_cnt));
      }
    }
  }
  return ret;
}

int ObStorageLocalityCache::generate_ls_locality(
  const int64_t ls_locality_cnt,
  sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObAddr *svr_addr_list = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAddr) * ls_locality_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ls svr addr list", K(ret));
  } else {
    svr_addr_list = new (buf) ObAddr[ls_locality_cnt];
  }
  common::ObArray<ObLSLocalityInCache> tmp_ls_locality_array;
  ObLSLocalityInCache ls_locality;
  ls_locality.set_attr(tenant_id_);
  int64_t ls_id = 0;
  ObString svr_ip;
  int64_t svr_port = 0;

  int64_t idx = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next result", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_UNLIKELY(idx >= ls_locality_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx is unexpected invalid", K(ret), K(idx), K(ls_locality_cnt));
    } else {
      ObAddr &svr_addr = svr_addr_list[idx++];
      EXTRACT_INT_FIELD_MYSQL(result, "ls_id", ls_id, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(result, "svr_ip", svr_ip);
      EXTRACT_INT_FIELD_MYSQL(result, "svr_port", svr_port, int64_t);
      if (OB_FAIL(ret)) {
      } else if (!ls_locality.ls_id_.is_valid()) {
        ls_locality.ls_id_ = ObLSID(ls_id);
      } else if (ls_locality.ls_id_.id() != ls_id) { // not same ls
        if (OB_FAIL(tmp_ls_locality_array.push_back(ls_locality))) {
          LOG_WARN("failed to push ls_locality into array", K(ret), K(ls_locality));
        } else {
          ls_locality.reset();
          ls_locality.ls_id_ = ObLSID(ls_id);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (false == svr_addr.set_ip_addr(svr_ip, svr_port)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set svr addr", K(ret));
      } else if (OB_FAIL(ls_locality.svr_addr_list_.push_back(&svr_addr))) {
        LOG_WARN("failed to add svr_addr to list", K(ret), K(svr_addr));
      }
    }
  } // end of while
  if (OB_SUCC(ret)) {
    if (ls_locality.is_valid() && OB_FAIL(tmp_ls_locality_array.push_back(ls_locality))) {
      LOG_WARN("failed to push ls_locality into array", K(ret), K(ls_locality));
    } else {
      lib::ObMutexGuard guard(lock_);
      if (OB_FAIL(ls_locality_array_.assign(tmp_ls_locality_array))) {
        LOG_WARN("failed to assign ls_locality array", K(ret), K(tmp_ls_locality_array));
      } else {
        if (OB_NOT_NULL(alloc_buf_)) {
          allocator_.free(alloc_buf_);
        }
        alloc_buf_ = buf;
        LOG_DEBUG("success to refresh ls locality", K(ret), K(ls_locality_array_));
      }
    } // end of lock
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    ls_locality.reset();
    tmp_ls_locality_array.reset();
    allocator_.free(buf);
  }
  return ret;
}

int ObStorageLocalityCache::append_zone_info(
    const ObIArray<ObZone> &zone_list,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && (idx < zone_list.count()); ++idx) {
    const ObZone &zone = zone_list.at(idx);
    if (OB_UNLIKELY(zone.is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone info is unexpected empty", K(ret), K(zone));
    } else if (OB_FAIL(sql.append_fmt(
        "%s '%s'",
        0 == idx ? "" : ",",
        zone.str().ptr()))) {
      LOG_WARN("fail to assign sql", KR(ret), K(idx), K(zone));
    }
  }
  return ret;
}

// if locality not exist in cache, will return invalid ls_locality(not filter any addr)
int ObStorageLocalityCache::get_ls_locality(
  const share::ObLSID &ls_id,
  ObLSLocality &ls_locality)
{
  int ret = OB_SUCCESS;
  ls_locality.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageLocalityCache is not inited", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    lib::ObMutexGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_locality_array_.count(); ++i) {
      ObLSLocalityInCache &locality = ls_locality_array_[i];
      if (locality.ls_id_ == ls_id) {
        ls_locality.ls_id_ = ls_id;
        for (int64_t j = 0; OB_SUCC(ret) && j < locality.svr_addr_list_.count(); ++j) {
          if (OB_ISNULL(locality.svr_addr_list_[j])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("svr addr in locality cache is unexpected null", K(ret), K(j), K(locality));
          } else if (OB_FAIL(ls_locality.svr_addr_list_.push_back(*locality.svr_addr_list_[j]))) {
            LOG_WARN("failed to push back addr", K(ret), K(locality));
          }
        }
        break;
      }
    } // end of for
  } // end of lock
  return ret;
}

} // namespace compaction
} // namespace oceanbase
