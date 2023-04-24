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

#ifndef _OCEANBASE_TESTBENCH_SYSTABLE_HELPER_H_
#define _OCEANBASE_TESTBENCH_SYSTABLE_HELPER_H_

#include "logservice/libobcdc/src/ob_log_mysql_connector.h"
#include "logservice/libobcdc/src/ob_log_systable_helper.h"

namespace oceanbase
{
  namespace testbench
  {
    typedef ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1> ObTenantName;
    class ObTestbenchSystableHelper
    {
    public:
      static const int64_t DEFAULT_RECORDS_NUM = 16;
      static const int64_t ALL_SERVER_DEFAULT_RECORDS_NUM = 32;

      typedef common::ObSEArray<share::ObLSID, DEFAULT_RECORDS_NUM> TenantLSIDs;
      typedef libobcdc::ObLogSysTableHelper::ObServerVersionInfo ObServerVersionInfo;
      typedef common::ObSEArray<ObServerVersionInfo, DEFAULT_RECORDS_NUM> ObServerVersionInfoArray;
      typedef libobcdc::ObLogSysTableHelper::ObServerTZInfoVersionInfo ObServerTZInfoVersionInfo;
      typedef libobcdc::ObLogSysTableHelper::ClusterInfo ClusterInfo;
      typedef libobcdc::ObLogSysTableHelper::TenantInfo TenantInfo;
      typedef libobcdc::ObLogSysTableHelper::BatchSQLQuery BatchSQLQuery;

    public:
      ObTestbenchSystableHelper();
      ~ObTestbenchSystableHelper();
      int init_conn(const libobcdc::MySQLConnConfig &cfg);
      void destroy();
      int do_query_(libobcdc::MySQLQueryBase &query);
      int query_cluster_info(ClusterInfo &cluster_info);
      int query_cluster_min_observer_version(uint64_t &min_observer_version);
      int query_timezone_info_version(const uint64_t tenant_id,
                                      int64_t &timezone_info_version);
      int query_tenant_ls_info(const uint64_t tenant_id,
                               TenantLSIDs &tenant_ls_ids);
      int query_sql_server_list(common::ObIArray<ObAddr> &sql_server_list);
      int query_tenant_list(common::ObIArray<uint64_t> &tenant_id_list, common::ObIArray<ObTenantName> &tenant_name_list);
      int query_tenant_info_list(common::ObIArray<TenantInfo> &tenant_info_list);
      int query_tenant_sql_server_list(
          const uint64_t tenant_id,
          common::ObIArray<common::ObAddr> &tenant_server_list);
      int query_tenant_status(
          const uint64_t tenant_id,
          share::schema::TenantStatus &tenant_status);
      int reset_connection();

    private:
      bool is_inited_;
      libobcdc::ObLogMySQLConnector mysql_conn_;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObTestbenchSystableHelper);
    };
  }
}
#endif