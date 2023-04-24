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
 *
 */

#include "ob_testbench_systable_helper.h"

namespace oceanbase
{
  namespace testbench
  {
    ObTestbenchSystableHelper::ObTestbenchSystableHelper() : is_inited_(false), mysql_conn_()
    {
    }

    ObTestbenchSystableHelper::~ObTestbenchSystableHelper()
    {
    }

    void ObTestbenchSystableHelper::destroy()
    {
    }

    int ObTestbenchSystableHelper::init_conn(const libobcdc::MySQLConnConfig &cfg)
    {
      int ret = OB_SUCCESS;
      if (IS_INIT)
      {
        ret = OB_INIT_TWICE;
        TESTBENCH_LOG(WARN, "ObTestbenchSystableHelper init twice", KR(ret));
      }
      else if (OB_FAIL(mysql_conn_.init(cfg, false)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper mysql_conn_ init fail", KR(ret), K(cfg));
      }
      else
      {
        is_inited_ = true;
        TESTBENCH_LOG(INFO, "ObTestbenchSystableHelper init_conn success");
      }
      return ret;
    }

    int ObTestbenchSystableHelper::do_query_(libobcdc::MySQLQueryBase &query)
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper not init", KR(ret));
      }
      else if (OB_UNLIKELY(!mysql_conn_.is_inited()))
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper mysql_conn_ not init", KR(ret));
      }
      else if (OB_FAIL(mysql_conn_.query(query)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper mysql_conn_ query fail", KR(ret), K(query.get_server()), K(query.get_mysql_err_code()), K(query.get_mysql_err_msg()));
      }
      return ret;
    }

    int ObTestbenchSystableHelper::query_cluster_info(ClusterInfo &record)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryClusterIdStrategy query_cluster_id_strategy;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(WARN, "ObTestbenchSystableHelper not init", KR(ret));
      }
      else if (OB_FAIL(query.init(&query_cluster_id_strategy)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper query_cluster_info init fail", KR(ret));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "ObTestbenchSystableHelper do_query_ query_cluster_info fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper do_query_ query_cluster_info fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(record)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "ObTestbenchSystableHelper query get_records query_cluster_info fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper query get_records query_cluster_info fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else
      {
        TESTBENCH_LOG(INFO, "query_cluster_info", K(record), "query_svr", query.get_server());
      }
      return ret;
    }

    int ObTestbenchSystableHelper::query_cluster_min_observer_version(uint64_t &min_observer_version)
    {
      int ret = OB_SUCCESS;
      libobcdc::ObLogSysTableHelper::BatchSQLQuery query;
      libobcdc::QueryObserverVersionStrategy query_observer_version_strategy;
      ObServerVersionInfoArray records;
      min_observer_version = OB_INVALID_ID;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper not init", KR(ret));
      }
      else if (OB_FAIL(query.init(&query_observer_version_strategy)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchSystableHelper query_cluster_min_observer_version init fail", KR(ret));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_cluster_min_observer_version fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_cluster_min_observer_version fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(records)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_cluster_min_observer_version, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_cluster_min_observer_version", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }

      if (OB_SUCC(ret))
      {
        // Get minimum version of servers in cluster
        for (int64_t idx = 0; OB_SUCC(ret) && idx < records.count(); ++idx)
        {
          uint64_t server_version = records.at(idx).server_version_;
          if (OB_INVALID_ID == min_observer_version)
          {
            min_observer_version = server_version;
          }
          else
          {
            min_observer_version = std::min(min_observer_version, server_version);
          }
        }

        if (OB_INVALID_ID == min_observer_version)
        {
          ret = OB_NEED_RETRY;
        }
      }

      TESTBENCH_LOG(INFO, "query_cluster_min_observer_version", KR(ret), K(min_observer_version), K(records), "query_svr", query.get_server());

      return ret;
    }

    int ObTestbenchSystableHelper::query_timezone_info_version(const uint64_t tenant_id,
                                                               int64_t &timezone_info_version)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryTimeZoneInfoVersionStrategy query_timezone_info_version_strategy(tenant_id);
      ObServerTZInfoVersionInfo record;
      timezone_info_version = OB_INVALID_TIMESTAMP;

      if (IS_NOT_INIT)
      {
        TESTBENCH_LOG(ERROR, "not init");
        ret = OB_NOT_INIT;
      }
      else if (OB_FAIL(query.init(&query_timezone_info_version_strategy)))
      {
        TESTBENCH_LOG(ERROR, "init QueryTimeZoneInfoVersionquery fail", KR(ret));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_timezone_info_version fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_timezone_info_version fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(record)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_timezone_info_version, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_timezone_info_version", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else
      {
        if (false == record.is_timezone_info_version_exist_)
        {
          // 1. 1451 version ob cluster __all_zone table does not have timezone_info_version, query is empty
          // 2. 226 versions of the ob cluster have the timezone table split into tenants, if the tenants are not imported, then the query is not available
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          timezone_info_version = record.timezone_info_version_;
        }
      }

      TESTBENCH_LOG(INFO, "query_timezone_info_version", KR(ret), K(timezone_info_version), K(record), "query_svr", query.get_server());

      return ret;
    }

    int ObTestbenchSystableHelper::query_tenant_ls_info(const uint64_t tenant_id,
                                                        TenantLSIDs &tenant_ls_ids)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryTenantLSInfoStrategy query_tenant_ls_info_strategy(tenant_id);
      tenant_ls_ids.reset();

      if (IS_NOT_INIT)
      {
        TESTBENCH_LOG(ERROR, "not init");
        ret = OB_NOT_INIT;
      }
      else if (OB_FAIL(query.init(&query_tenant_ls_info_strategy)))
      {
        TESTBENCH_LOG(ERROR, "init cluster info query fail", KR(ret));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_tenant_ls_info fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_tenant_ls_info fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(tenant_ls_ids)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_tenant_ls_info, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_tenant_ls_info", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }

      TESTBENCH_LOG(INFO, "query_tenant_ls_info", KR(ret), K(tenant_id), K(tenant_ls_ids), "query_svr", query.get_server());

      return ret;
    }

    int ObTestbenchSystableHelper::query_sql_server_list(common::ObIArray<common::ObAddr> &sql_server_list)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryAllServerInfoStrategy query_all_server_info_strategy;
      sql_server_list.reset();

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "systable_helper not init", KR(ret));
      }
      else if (OB_FAIL(query.init(&query_all_server_info_strategy)))
      {
        TESTBENCH_LOG(ERROR, "init all_server_info query fail", KR(ret));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_all_server_info fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_all_server_info fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(sql_server_list)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_all_server_info, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_all_server_info", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }

      TESTBENCH_LOG(INFO, "query_all_server_info", KR(ret), K(sql_server_list));

      return ret;
    }

    int ObTestbenchSystableHelper::query_tenant_info_list(common::ObIArray<TenantInfo> &tenant_info_list)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryAllTenantStrategy query_all_tenant_strategy;
      tenant_info_list.reset();

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "systable_helper not init", KR(ret));
      }
      else if (OB_FAIL(query.init(&query_all_tenant_strategy)))
      {
        TESTBENCH_LOG(ERROR, "init all_tenant_info query failed", KR(ret));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_all_tenant_info fail, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_all_tenant_info fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(tenant_info_list)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_all_tenant_info, need retry", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_all_tenant_info", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }

      TESTBENCH_LOG(INFO, "query_all_tenant_info", KR(ret), K(tenant_info_list));

      return ret;
    }

    int ObTestbenchSystableHelper::query_tenant_list(common::ObIArray<uint64_t> &tenant_id_list, common::ObIArray<ObTenantName> &tenant_name_list)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryAllTenantStrategy query_all_tenant_strategy;
      common::ObSEArray<TenantInfo, 16> tenant_info_list;

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "systable_helper not init", KR(ret));
      }
      else if (OB_FAIL(query_tenant_info_list(tenant_info_list)))
      {
        TESTBENCH_LOG(ERROR, "query tenant_id, tenant_name list failed", KR(ret), K(tenant_info_list));
      }
      else
      {
        const int64_t tenant_list_size = tenant_info_list.count();
        ARRAY_FOREACH_N(tenant_info_list, i, tenant_list_size)
        {
          const TenantInfo &tenant_id_name = tenant_info_list.at(i);
          if (OB_FAIL(tenant_id_list.push_back(tenant_id_name.tenant_id)))
          {
            TESTBENCH_LOG(ERROR, "push tenant_id into tenant_id_list failed", K(tenant_id_name), K(tenant_id_list),
                          K(tenant_info_list));
          }
          else if (OB_FAIL(tenant_name_list.push_back(tenant_id_name.tenant_name)))
          {
            TESTBENCH_LOG(ERROR, "push tenant_name into tenant_name_list failed", K(tenant_id_name), K(tenant_name_list), K(tenant_info_list));
          }
        }
      }

      TESTBENCH_LOG(INFO, "query_all_tenant_info", KR(ret), K(tenant_id_list), K(tenant_name_list));

      return ret;
    }

    int ObTestbenchSystableHelper::query_tenant_sql_server_list(
        const uint64_t tenant_id,
        common::ObIArray<common::ObAddr> &tenant_server_list)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryTenantServerListStrategy query_tenant_serverlist_strategy(tenant_id);
      tenant_server_list.reset();

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "systable_helper not init", KR(ret));
      }
      else if (OB_FAIL(query.init(&query_tenant_serverlist_strategy)))
      {
        TESTBENCH_LOG(ERROR, "init tenant_server_list query failed", KR(ret), K(tenant_id));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_tenant_serverlist fail, need retry", KR(ret), K(tenant_id),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_tenant_serverlist fail", KR(ret), K(tenant_id),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(tenant_server_list)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_tenant_serverlist, need retry", KR(ret), K(tenant_id),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_tenant_serverlist", KR(ret), K(tenant_id),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }

      TESTBENCH_LOG(INFO, "query_tenant_serverlist", KR(ret), K(tenant_id), K(tenant_server_list));

      return ret;
    }

    int ObTestbenchSystableHelper::query_tenant_status(
        const uint64_t tenant_id,
        share::schema::TenantStatus &tenant_status)
    {
      int ret = OB_SUCCESS;
      BatchSQLQuery query;
      libobcdc::QueryTenantStatusStrategy query_tenant_status_strategy(tenant_id);
      tenant_status = share::schema::TenantStatus::TENANT_STATUS_INVALID;

      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "systable_helper not init", KR(ret));
      }
      else if (OB_FAIL(query.init(&query_tenant_status_strategy)))
      {
        TESTBENCH_LOG(ERROR, "init tenant_status query failed", KR(ret), K(tenant_id));
      }
      else if (OB_FAIL(do_query_(query)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "do query_tenant_status fail, need retry", KR(ret), K(tenant_id),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "do query_tenant_status fail", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }
      else if (OB_FAIL(query.get_records(tenant_status)))
      {
        if (OB_NEED_RETRY == ret)
        {
          TESTBENCH_LOG(WARN, "get_records fail while query_tenant_status, need retry", KR(ret), K(tenant_id),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
        else
        {
          TESTBENCH_LOG(ERROR, "get_records fail while query_tenant_status", KR(ret),
                        "mysql_error_code", query.get_mysql_err_code(),
                        "mysql_error_msg", query.get_mysql_err_msg());
        }
      }

      TESTBENCH_LOG(INFO, "query_tenant_status", KR(ret), K(tenant_id), K(tenant_status));

      return ret;
    }
  }
}