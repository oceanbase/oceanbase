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
 * ObCDCSysTableHelper impl
 * Class For Query System Table
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_SYSTABLE_HELPER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_SYSTABLE_HELPER_H_

#include "lib/container/ob_se_array.h"                    // ObSEArray
#include "share/ob_define.h"                                // MAX_IP_ADDR_LENGTH, OB_INVALID_ID, ObReplicaType
#include "lib/mysqlclient/ob_mysql_server_provider.h"     // ObMySQLServerProvider
#include "common/ob_region.h"                             // ObRegin
#include "common/ob_zone.h"                               // ObZone
#include "share/ob_zone_info.h"                           // ObZoneStorageTyp
#include "common/ob_zone_type.h"                          // ObZoneType
#include "share/ob_server_status.h"                       // ObServerStatus


#include "ob_log_mysql_connector.h"                       // MySQLQueryBase

namespace oceanbase
{
namespace libobcdc
{
class ISQLStrategy
{
  // Class global variables
public:
  // indicates whether the replica_type information is valid, default is true, i.e. replica information is used
  // When sql fails and ret=OB_ERR_COLUMN_NOT_FOUND, the atom is set to false, won’t use replica_type information
  static bool  g_is_replica_type_info_valid;

public:
  /// SQL strategy
  ///
  /// @param [in]  sql_buf          aggregate sql buffer
  /// @param [in]  mul_statement_buf_len      length of aggregate sql buffer
  /// @param [out] pos              Returns the length of a single sql
  ///
  /// @retval OB_SUCCESS          Success
  /// @retval Other return values Failure
  virtual int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos) = 0;
};

///////////////////////// QueryClusterIdStrategy /////////////////////////
// 查询cluster id
class QueryClusterIdStrategy: public ISQLStrategy
{
public:
  QueryClusterIdStrategy() {}
  ~QueryClusterIdStrategy() {}

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  DISALLOW_COPY_AND_ASSIGN(QueryClusterIdStrategy);
};

///////////////////////// QueryObserverVersionStrategy /////////////////////////
// query cluster version
class QueryObserverVersionStrategy: public ISQLStrategy
{
public:
  QueryObserverVersionStrategy() {}
  ~QueryObserverVersionStrategy() {}

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  DISALLOW_COPY_AND_ASSIGN(QueryObserverVersionStrategy);
};

///////////////////////// QueryTimeZoneInfoVersionStrategy /////////////////////////
// query time_zone_info_version
class QueryTimeZoneInfoVersionStrategy: public ISQLStrategy
{
public:
  QueryTimeZoneInfoVersionStrategy(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~QueryTimeZoneInfoVersionStrategy() { tenant_id_ = common::OB_INVALID_TENANT_ID; }

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  uint64_t tenant_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryTimeZoneInfoVersionStrategy);
};

///////////////////////// QueryTenantLSInfoStrategy /////////////////////////
// query tenant ls info
class QueryTenantLSInfoStrategy: public ISQLStrategy
{
public:
  QueryTenantLSInfoStrategy(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~QueryTenantLSInfoStrategy() { tenant_id_ = common::OB_INVALID_TENANT_ID; }

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  uint64_t tenant_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryTenantLSInfoStrategy);
};

///////////////////////// QueryAllServerInfoStrategy /////////////////////////
// query all server info(ip/rpc_port/svr_port)
class QueryAllServerInfoStrategy : public ISQLStrategy {
public:
  QueryAllServerInfoStrategy() {}
  ~QueryAllServerInfoStrategy() {}
public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);
private:
  DISALLOW_COPY_AND_ASSIGN(QueryAllServerInfoStrategy);
};

///////////////////////// QueryAllTenantStrategy /////////////////////////
// query all tenant_id
class QueryAllTenantStrategy : public ISQLStrategy {
public:
  QueryAllTenantStrategy() {}
  ~QueryAllTenantStrategy() {}
public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);
private:
  DISALLOW_COPY_AND_ASSIGN(QueryAllTenantStrategy);
};

///////////////////////// QueryTenantServerListStrategy /////////////////////////
// query all server that served specified tenant
class QueryTenantServerListStrategy : public ISQLStrategy {
public:
  QueryTenantServerListStrategy(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~QueryTenantServerListStrategy() {}
public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);
private:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(QueryTenantServerListStrategy);
};

///////////////////////// QueryTenantEndpointStrategy /////////////////////////
// query tenant endpiont(or access point) in tenant_sync_mode
class QueryTenantEndpointStrategy : public ISQLStrategy {
public:
  QueryTenantEndpointStrategy() {}
  ~QueryTenantEndpointStrategy() {}
public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);
private:
  DISALLOW_COPY_AND_ASSIGN(QueryTenantEndpointStrategy);
};

///////////////////////// QueryTenantStatusStrategy /////////////////////////
// query tenant_status
class QueryTenantStatusStrategy : public ISQLStrategy {
public:
  QueryTenantStatusStrategy(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~QueryTenantStatusStrategy() {}
public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);
private:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(QueryTenantStatusStrategy);
};

/////////////////////////////////// IObLogSysTableHelper ///////////////////////////////////////
// query system table
class ObLogSvrBlacklist;
class ObTenantStatus;
class IObLogSysTableHelper
{
public:
  static const int64_t DEFAULT_RECORDS_NUM = 16;
  static const int64_t ALL_SERVER_DEFAULT_RECORDS_NUM = 32;

  struct ClusterInfo;
  struct TenantInfo;

  struct ObServerVersionInfo;
  typedef common::ObSEArray<ObServerVersionInfo, DEFAULT_RECORDS_NUM> ObServerVersionInfoArray;

  struct ObServerTZInfoVersionInfo;

  typedef common::ObSEArray<share::ObLSID, DEFAULT_RECORDS_NUM> TenantLSIDs;

public:
  virtual ~IObLogSysTableHelper() { }

public:
  class BatchSQLQuery;
  /// Using multi-statement to aggregate SQL queries
  ////
  /// Currently serving SvrFinder for querying Meta Table and Clog history to determine server list and leader information
  virtual int query_with_multiple_statement(BatchSQLQuery &batch_query) = 0;

  /// Query cluster-related information
  virtual int query_cluster_info(ClusterInfo &cluster_info) = 0;

  /// query min version of obsever in cluster
  virtual int query_cluster_min_observer_version(uint64_t &min_observer_version) = 0;

  /// query timezone info version
  virtual int query_timezone_info_version(const uint64_t tenant_id,
      int64_t &timezone_info_version) = 0;

  virtual int query_tenant_ls_info(const uint64_t tenant_id,
      TenantLSIDs &tenant_ls_ids) = 0;

  // query sql server list(ip:sql_port), filtered by blacklist(ip:rpc_port)
  virtual int query_sql_server_list(
      const ObLogSvrBlacklist &server_blacklist,
      common::ObIArray<common::ObAddr> &sql_server_list) = 0;

  virtual int query_tenant_info_list(common::ObIArray<TenantInfo> &tenant_info_list) = 0;

  // query tenant_id list
  virtual int query_tenant_id_list(common::ObIArray<uint64_t> &tenant_id_list) = 0;

  // query tenant server list based tenant_id
  virtual int query_tenant_sql_server_list(
      const uint64_t tenant_id,
      common::ObIArray<common::ObAddr> &tenant_server_list) = 0;

  virtual int query_tenant_status(
      const uint64_t tenant_id,
      share::schema::TenantStatus &tenant_status) = 0;

  virtual int refresh_tenant_endpoint() = 0;

  /// Reset the current thread connection to allow the next query to use a different Server
  virtual int reset_connection() = 0;

public:
  // Support batch query
  // 1. single-statement query
  // 2. multiple-statement query
  //
  // Usage.
  // 1. single-statement query
  // (1) Implementing SQL query policies(ISQLStrategy), and get_records()
  // (2) BatchSQLQuery::init(ISQLStrategy *), for initialization
  // (3) query
  //
  // 2. multiple-statement query
  // (1) Implementing a multi-statement SQL query strategy, and the corresponding get_records()
  // (2) BatchSQLQuery::init(const int64_t), complete with initialization-aggregate buffer declaration
  // (3) do_sql_aggregate() to aggregate request
  // (4) when aggregation is complete, query_with_multiple_statement(BatchSQLQuery &)
  // (5) Process all query results in turn. When processing is complete, the query is launched again and reset() is called
  class BatchSQLQuery : public MySQLQueryBase
  {
  public:
    BatchSQLQuery();
    virtual ~BatchSQLQuery();

  public:
    // single-statement query init
    int init(ISQLStrategy *strategy);
    // multiple-statement query init
    int init(const int64_t mul_statement_buf_len);
    void destroy();
    // multiple-statement reset
    void reset();
    // Doing aggregation based on SQL strategy
    int do_sql_aggregate(ISQLStrategy *strategy);
    // init sql
    int init_sql();

  public:
    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(bool &has_leader, common::ObAddr &leader);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(ClusterInfo &record);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(ObServerVersionInfoArray& records);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(ObServerTZInfoVersionInfo& record);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(TenantLSIDs &record);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(
        const ObLogSvrBlacklist &server_blacklist,
        common::ObIArray<common::ObAddr >&record);
    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(common::ObIArray<TenantInfo> &record);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(common::ObIArray<common::ObAddr> &record);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(share::schema::TenantStatus &record);

    int64_t get_batch_sql_count() const { return batch_sql_count_; }

  private:
    template <typename RecordsType>
    int get_records_tpl_(RecordsType &records, const char *event, int64_t &record_count);
    int parse_record_from_row_(ClusterInfo &record);
    int parse_record_from_row_(ObServerVersionInfoArray &records);
    int parse_record_from_row_(ObServerTZInfoVersionInfo &record);
    int parse_record_from_row_(TenantLSIDs &records);
    int parse_record_from_row_(
        const ObLogSvrBlacklist &server_blacklist,
        common::ObIArray<common::ObAddr> &records);
    int parse_record_from_row_(common::ObIArray<TenantInfo> &records);
    int parse_record_from_row_(common::ObIArray<common::ObAddr> &records);
    int parse_record_from_row_(share::schema::TenantStatus &records);

  private:
    bool        inited_;

    // multiple-statement SQL
    bool        enable_multiple_statement_;    // Identifies whether the multiple-statement is in effect
    char        *mul_statement_buf_;           // multiple-statement buffer, store aggregated SQL
    int64_t     mul_statement_buf_len_;        // multiple-statement buffer length

    // single SQL
    char        single_statement_buf_[DEFAULT_SQL_LENGTH];

    int64_t     pos_;                          // Record current fill position
    int64_t     batch_sql_count_;              // Number of records aggregated SQL

  private:
    DISALLOW_COPY_AND_ASSIGN(BatchSQLQuery);
  };

public:
  // Cluster related information
  struct ClusterInfo
  {
    ClusterInfo() { reset(); }

    // clusterID
    int64_t cluster_id_;

    void reset()
    {
      cluster_id_ = common::OB_INVALID_CLUSTER_ID;
    }

    TO_STRING_KV(K_(cluster_id));
  };

  struct TenantInfo {
  public:
    TenantInfo(): tenant_id(OB_INVALID_TENANT_ID), tenant_name() {}
    TenantInfo(const uint64_t id, const ObString &name): tenant_id(id), tenant_name(name) {}
    TO_STRING_KV(K(tenant_id), K(tenant_name));

    uint64_t tenant_id;
    ObFixedLengthString<OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name;
  };


  struct ObServerVersionInfo
  {
    ObServerVersionInfo() { reset(); }

    void reset()
    {
      server_version_ = common::OB_INVALID_ID;
    }

    uint64_t server_version_;

    TO_STRING_KV(K_(server_version));
  };

  struct ObServerTZInfoVersionInfo
  {
    ObServerTZInfoVersionInfo() { reset(); }

    void reset()
    {
      timezone_info_version_ = common::OB_INVALID_TIMESTAMP;
      is_timezone_info_version_exist_ = true;
    }

    int64_t timezone_info_version_;
    bool is_timezone_info_version_exist_;

    TO_STRING_KV(K_(timezone_info_version), K_(is_timezone_info_version_exist));
  };

};

/////////////////////////////////// ObLogSysTableHelper ///////////////////////////////////////

class ObLogConfig;
class ObLogMySQLConnector;
class ObLogSysTableHelper : public IObLogSysTableHelper
{
  typedef common::sqlclient::ObMySQLServerProvider SvrProvider;

public:
  ObLogSysTableHelper();
  virtual ~ObLogSysTableHelper();

public:
  /// FIXME: Note that there is an upper limit to the total number of threads that can be used by external modules:
  // access_systable_helper_thread_num, beyond which the module query interface will fail.
  int init(SvrProvider &svr_provider,
      const int64_t access_systable_helper_thread_num,
      const char *mysql_user,
      const char *mysql_password,
      const char *mysql_db);
  void destroy();

  /// The SvrFinder thread pool uses this interface
  virtual int query_with_multiple_statement(BatchSQLQuery &batch_query);

  /// Start the main thread ObLogInstance using this interface
  virtual int query_cluster_info(ClusterInfo &cluster_info);

  /// 1. Start the main thread ObLogInstance to use this interface to query and initialize the ObClusterVersion singleton
  /// 2. ObLogAllSvrCache thread uses this interface
  /// Query the cluster min observer version
  virtual int query_cluster_min_observer_version(uint64_t &min_observer_version);

  /// Query timezone info version, for oracle new timezone type synchronization
  /// ObCDCTimeZoneInfoGetter
  virtual int query_timezone_info_version(const uint64_t tenant_id,
      int64_t &timezone_info_version);

  virtual int query_tenant_ls_info(const uint64_t tenant_id,
      TenantLSIDs &tenant_ls_ids);
  virtual int query_sql_server_list(
      const ObLogSvrBlacklist &server_blacklist,
      common::ObIArray<ObAddr> &sql_server_list);
  virtual int query_tenant_id_list(common::ObIArray<uint64_t> &tenant_id_list);
  virtual int query_tenant_info_list(common::ObIArray<TenantInfo> &tenant_info_list);
  virtual int query_tenant_sql_server_list(
      const uint64_t tenant_id,
      common::ObIArray<common::ObAddr> &tenant_server_list);
  virtual int query_tenant_status(
      const uint64_t tenant_id,
      share::schema::TenantStatus &tenant_status);
  virtual int refresh_tenant_endpoint();

  /// Restart the connection used by the current thread
  virtual int reset_connection();

private:
  int do_query_(MySQLQueryBase &query);
  int do_query_and_handle_when_query_error_occurred_(BatchSQLQuery &query);
  void handle_column_not_found_when_query_meta_info_();
  int64_t thread_index_();
  int change_to_next_server_(const int64_t svr_idx, ObLogMySQLConnector &conn);
  int64_t get_next_svr_idx_(int64_t &next_svr_idx, const int64_t total_svr_count);
  bool need_change_server_(
      const int64_t last_change_server_tstamp,
      const ObLogMySQLConnector &conn,
      const int64_t tid,
      const int64_t next_svr_idx);

// Internal member variables
private:
  bool                  inited_;
  SvrProvider           *svr_provider_;
  int64_t               max_thread_num_;
  char                  mysql_user_[common::OB_MAX_USER_NAME_BUF_LENGTH];
  char                  mysql_password_[common::OB_MAX_PASSWORD_LENGTH + 1];
  char                  mysql_db_[common::OB_MAX_DATABASE_NAME_BUF_LENGTH];

  // MySQL connector array
  // One connector object for each thread for efficiency reasons
  ObLogMySQLConnector   *mysql_conns_;

  // The index of the server to which each ObLogMySQLConnector corresponds, corresponding to the server index in SvrProvider
  // Indicates the next server to be connected to
  int64_t               *next_svr_idx_array_;

  int64_t               thread_counter_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSysTableHelper);
};

}
}
#endif
