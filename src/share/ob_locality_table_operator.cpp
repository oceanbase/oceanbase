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

#include "lib/string/ob_sql_string.h"
#include "share/ob_define.h"
#include "share/ob_locality_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_locality_priority.h"
#include "share/ob_primary_zone_util.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{

class LocalityQueryResult
{
public:
  LocalityQueryResult()
      :svr_ip_(""),
       svr_port_(0),
       zone_(""),
       info_(""),
       value_(0),
       record_name_(""),
       start_service_time_(0),
       server_stop_time_(0),
       svr_status_("")
       {}
  bool is_same_server(LocalityQueryResult &other) const
  {
    int bool_ret = false;
    if (strlen(svr_ip_) == strlen(other.svr_ip_)
        && 0 == strncmp(svr_ip_, other.svr_ip_, strlen(svr_ip_))
        && strlen(zone_) == strlen(other.zone_)
        && 0 == strncmp(zone_, other.zone_, strlen(zone_))
        && svr_port_ == other.svr_port_) {
      bool_ret = true;
    }
    return bool_ret;
  }

  int retrieve_result(ObMySQLResult &result)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(result.next())) {
      if (OB_UNLIKELY(ret != OB_ITER_END)) {
        LOG_WARN("invalid result", K(ret));
      }
    } else {
      int64_t tmp_real_str_len = 0; // used to fill the output argument
      UNUSED(tmp_real_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "svr_ip", svr_ip_,
                                 static_cast<int64_t>(sizeof(svr_ip_)), tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(result, "svr_port", svr_port_, int32_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "zone", zone_,
                                 static_cast<int64_t>(sizeof(zone_)), tmp_real_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "info", info_,
                                 static_cast<int64_t>(sizeof(info_)), tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(result, "value", value_, int32_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "name", record_name_,
                                 static_cast<int64_t>(sizeof(record_name_)), tmp_real_str_len);
      EXTRACT_INT_FIELD_MYSQL(result, "start_service_time", start_service_time_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "stop_time", server_stop_time_, int64_t);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "status", svr_status_,
                                 OB_SERVER_STATUS_LENGTH, tmp_real_str_len);
    }
    return ret;
  }

  bool is_idc_record() const
  {
    const char *IDC_NAME = "idc";
    return strlen(IDC_NAME) == strlen(record_name_) && 0 == strncmp(IDC_NAME, record_name_, strlen(IDC_NAME));
  }

  bool is_region_record() const
  {
    const char *REGION_NAME = "region";
    return strlen(REGION_NAME) == strlen(record_name_) && 0 == strncmp(REGION_NAME, record_name_, strlen(REGION_NAME));
  }

  bool is_status_record(bool &is_active) const
  {
    bool b_ret = false;
    is_active = false;
    const char *STATUS_NAME = "status";
    const char *ACTIVE_VALUE = "ACTIVE";
    if (strlen(STATUS_NAME) == strlen(record_name_)
        && 0 == strncmp(STATUS_NAME, record_name_, strlen(STATUS_NAME))) {
      b_ret = true;
      if (strlen(ACTIVE_VALUE) == strlen(info_)
          && 0 == strncmp(ACTIVE_VALUE, info_, strlen(ACTIVE_VALUE))) {
        is_active = true;
      }
    }
    return b_ret;
  }

  bool is_zone_type_record()
  {
    const char *ZONE_TYPE_NAME = "zone_type";
    return strlen(ZONE_TYPE_NAME) == strlen(record_name_) && 0 == strncmp(ZONE_TYPE_NAME, record_name_, strlen(ZONE_TYPE_NAME));
  }

  TO_STRING_KV(KCSTRING(svr_ip_), K(svr_port_), KCSTRING(zone_), KCSTRING(info_), K(value_),
               KCSTRING(record_name_), K(start_service_time_), K(server_stop_time_), KCSTRING(svr_status_));

  char svr_ip_[MAX_IP_ADDR_LENGTH];
  int32_t svr_port_;
  char zone_[MAX_ZONE_LENGTH];
  char info_[MAX_REGION_LENGTH];
  int32_t value_;
  char record_name_[32];
  int64_t start_service_time_;
  int64_t server_stop_time_;
  char svr_status_[OB_SERVER_STATUS_LENGTH];
};

int ObLocalityTableOperator::load_region(const ObAddr &addr,
                                         const bool &is_self_cluster,
                                         ObISQLClient &sql_client,
                                         ObLocalityInfo &locality_info,
                                         ObServerLocalityCache &server_locality_cache)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSQLClientRetryWeak sql_client_retry_weak(&sql_client);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char ip_buffer[common::OB_IP_STR_BUFF];
    bool has_readonly_zone = false;
    (void)addr.ip_to_string(ip_buffer, common::OB_IP_STR_BUFF);
    if (OB_FAIL(sql.assign_fmt("select svr_ip, svr_port, a.zone, info, value, b.name, a.status, a.start_service_time, a.stop_time"
                               " from __all_server a LEFT JOIN __all_zone b ON a.zone = b.zone"
                               " WHERE (b.name = 'region' or b.name = 'idc' or b.name = 'status'"
                               " or b.name = 'zone_type') and a.zone != '' order by svr_ip, svr_port, b.name")))  {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else {
      ObArray<ObServerLocality> server_locality_array;
      ObServerLocality server_locality;
      locality_info.reset();
      while (OB_SUCC(ret)) {
        //get idc
        LocalityQueryResult idc_result;
        if (OB_FAIL(idc_result.retrieve_result(*result))) {
          if (OB_UNLIKELY(ret != OB_ITER_END)) {
            LOG_WARN("fail to retrieve result", K(ret));
          }
        } else if (OB_UNLIKELY(false == idc_result.is_idc_record())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(idc_result), K(ret));
        }

        //get region
        LocalityQueryResult region_result;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(region_result.retrieve_result(*result))) {
          LOG_WARN("fail to retrieve result", K(ret));
        } else if (OB_UNLIKELY(false == region_result.is_region_record())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(region_result), K(ret));
        } else if (OB_UNLIKELY(false == region_result.is_same_server(idc_result))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(region_result), K(ret));
        }

        //get zone_status
        LocalityQueryResult status_result;
        bool is_active = false;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(status_result.retrieve_result(*result))) {
          LOG_WARN("fail to retrieve result", K(ret));
        } else if (OB_UNLIKELY(false == status_result.is_status_record(is_active))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(status_result), K(ret));
        } else if (OB_UNLIKELY(false == status_result.is_same_server(idc_result))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(status_result), K(ret));
        }

        //get zone_type
        LocalityQueryResult zone_type_result;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(zone_type_result.retrieve_result(*result))) {
          LOG_WARN("fail to retrieve result", K(ret));
        } else if (OB_UNLIKELY(false == zone_type_result.is_zone_type_record())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(zone_type_result), K(ret));
        } else if (OB_UNLIKELY(false == zone_type_result.is_same_server(idc_result))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result", K(zone_type_result), K(ret));
        } else if (ZONE_TYPE_READONLY == static_cast<ObZoneType>(zone_type_result.value_)) {
          has_readonly_zone = true;
        }
        if (OB_SUCC(ret)) {
          server_locality.reset();
          server_locality.set_start_service_time(idc_result.start_service_time_);
          server_locality.set_server_stop_time(idc_result.server_stop_time_);
          if (OB_FAIL(server_locality.set_server_status(idc_result.svr_status_))) {
            LOG_WARN("fail to set server status", K(idc_result), K(ret));
          } else if (OB_FAIL(server_locality.init(idc_result.svr_ip_,
                                                  idc_result.svr_port_,
                                                  idc_result.zone_,
                                                  static_cast<ObZoneType>(zone_type_result.value_),
                                                  idc_result.info_,
                                                  region_result.info_,
                                                  is_active))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to init server locality", K(ret), K(idc_result), K(region_result), K(zone_type_result), K(status_result));
          } else if (OB_FAIL(server_locality_array.push_back(server_locality))) {
            LOG_WARN("fail to push back server locality", K(ret), K(server_locality));
          }
        }

        if (OB_SUCC(ret)) {
          if (addr.get_port() == idc_result.svr_port_ && strlen(ip_buffer) == strlen(idc_result.svr_ip_)
              && (0 == strncmp(ip_buffer, idc_result.svr_ip_, strlen(idc_result.svr_ip_)))) {
            if (locality_info.local_region_.is_empty() && locality_info.local_zone_.is_empty()) {
              locality_info.local_region_ = region_result.info_;
              locality_info.local_zone_ = idc_result.zone_;
              locality_info.local_idc_ = idc_result.info_;
              locality_info.local_zone_type_ = static_cast<decltype(ObZoneType::ZONE_TYPE_INVALID)>(zone_type_result.value_);
              locality_info.local_zone_status_ = static_cast<decltype(ObZoneStatus::UNKNOWN)>(status_result.value_);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("same ip and port in result", K(ret), K(addr), K(idc_result.svr_ip_), K(idc_result.svr_port_));
            }
          }
        }
        if (OB_SUCC(ret)) {
          int64_t i = 0;
          const int64_t region_count = locality_info.locality_region_array_.count();
          for (i = 0; OB_SUCC(ret) && i < region_count; i++) {
            if (strlen(region_result.info_) == strlen(locality_info.locality_region_array_.at(i).region_.ptr())
                && (0 == strncmp(region_result.info_, locality_info.locality_region_array_.at(i).region_.ptr(), strlen(region_result.info_)))) {
              break;
            }
          }

          if (OB_SUCC(ret)) {
            if (i == region_count) {
              ObLocalityRegion locality_region;
              locality_region.region_ = region_result.info_;
              locality_region.zone_array_.reset();
              locality_region.region_priority_ = UINT64_MAX;
              if (OB_FAIL(locality_info.locality_region_array_.push_back(locality_region))) {
                LOG_WARN("region push back error", K(ret), K(locality_info), K(region_result), K(idc_result));
              }
            }

            if (OB_SUCC(ret)) {
              int64_t j = 0;
              const int64_t zone_count = locality_info.locality_region_array_.at(i).zone_array_.count();
              // check if a duplicated zone exists
              for (j = 0; OB_SUCC(ret) && j < zone_count; j++) {
                if (strlen(idc_result.zone_) == strlen(locality_info.locality_region_array_.at(i).zone_array_.at(j).ptr())
                    && (0 == strncmp(idc_result.zone_, locality_info.locality_region_array_.at(i).zone_array_.at(j).ptr(), strlen(idc_result.zone_)))) {
                  break;
                }
              }

              if (OB_SUCC(ret)) {
                if (j == zone_count) {
                  if (OB_FAIL(locality_info.locality_region_array_.at(i).zone_array_.push_back(idc_result.zone_))) {
                    LOG_WARN("zone push back error", K(ret), K(locality_info), K(idc_result));
                  }
                }
              }
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get result failed", K(ret), K(sql));
      }

      if (OB_SUCC(ret)) {
        // to make the granularity small, use server_locality_array to record the info then push back to server_locality_cache
        if (OB_FAIL(server_locality_cache.set_server_locality_array(server_locality_array, has_readonly_zone))) {
          LOG_WARN("fail to set server locality array", K(ret), K(server_locality_array), K(has_readonly_zone));
        } else {
          // calculate region_priority
          schema::ObSchemaGetterGuard schema_guard;
          locality_info.locality_zone_array_.reset();
          common::ObArray<uint64_t> tenant_ids;
          uint64_t region_priority = UINT64_MAX;
          share::schema::ObMultiVersionSchemaService *&schema_service = GCTX.schema_service_;
          if (OB_ISNULL(schema_service)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema service is null", K(ret));
          } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
            LOG_WARN("failed to get schema guard", K(ret));
          } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
            LOG_WARN("get_tenant_ids failed", K(ret));
          } else {
            ObSEArray<ObLocalityRegion, 5> tenant_primary_region_array;
            FOREACH_CNT_X(t, tenant_ids, OB_SUCC(ret)) {
              const uint64_t tenant_id = *t;
              const schema::ObTenantSchema *tenant_schema = NULL;
              if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema)) || OB_ISNULL(tenant_schema)) {
                LOG_WARN("Fail to get tenant schema", K(ret), K(tenant_schema));
              } else {
                tenant_primary_region_array.reset();
                region_priority = UINT64_MAX;
                if (ObPrimaryZoneUtil::no_need_to_check_primary_zone(tenant_schema->get_primary_zone())
                    || !is_self_cluster) {
                  //FIXME: do not process the semantics of leader balance of primary_zone
                  LOG_INFO( "tenant_schema primary_zone is NULL, or no need calc region priority", K(tenant_id),
                           "primary_zone", tenant_schema->get_primary_zone());
                } else {
                  if (OB_FAIL(ObLocalityPriority::get_primary_region_prioriry(tenant_schema->get_primary_zone().ptr(),
                                                                              locality_info.locality_region_array_, tenant_primary_region_array))) {
                    LOG_WARN("get_primary_zone_prioriry error", K(ret),
                             "primary_zone", tenant_schema->get_primary_zone(),
                             "locality_region_array", locality_info.locality_region_array_);
                  } else {
                    LOG_INFO("get_primary_zone_prioriry",
                             "primary_zone", tenant_schema->get_primary_zone(),
                             K(tenant_primary_region_array), K(locality_info));
                    region_priority = UINT64_MAX;
                    if (OB_FAIL(ObLocalityPriority::get_region_priority(locality_info,
                                                                        tenant_primary_region_array, region_priority))) {
                      LOG_WARN("ObLocalityPriority get_region_priority error", K(ret), K(locality_info),
                               K(tenant_primary_region_array));
                    }
                  }
                }

                if (OB_SUCC(ret)) {
                  ObLocalityZone locality_zone;
                  if (OB_FAIL(locality_zone.init(tenant_id, region_priority))) {
                    LOG_WARN("ObLocalityZone init error", K(ret), K(tenant_id), K(region_priority));
                  } else if (OB_FAIL(locality_info.add_locality_zone(locality_zone))) {
                    LOG_WARN("add_locality_zone error", K(ret), K(locality_zone));
                  } else {
                    LOG_INFO("add_locality_zone success", K(locality_zone));
                  }
                }
              }
            } // FOREACH
          }
        }
      }
    }

    LOG_INFO("load region", K(ret), K(locality_info));
  }
  return ret;
}
}//end namespace share
}//end namespace oceanbase
