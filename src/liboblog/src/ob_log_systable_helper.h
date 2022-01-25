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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_SYSTABLE_HELPER_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_SYSTABLE_HELPER_H_

#include "lib/container/ob_se_array.h"                    // ObSEArray
#include "share/ob_define.h"                                // MAX_IP_ADDR_LENGTH, OB_INVALID_ID, ObReplicaType
#include "lib/mysqlclient/ob_mysql_server_provider.h"     // ObMySQLServerProvider
#include "common/ob_partition_key.h"                      // ObPartitionKey
#include "common/ob_region.h"                             // ObRegin
#include "common/ob_zone.h"                               // ObZone
#include "common/ob_zone_type.h"                          // ObZoneType
#include "share/ob_server_status.h"                       // ObServerStatus


#include "ob_log_mysql_connector.h"                       // MySQLQueryBase

namespace oceanbase
{
namespace liboblog
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

bool is_cluster_version_be_equal_or_greater_than_200_();
bool is_cluster_version_be_equal_or_greater_than_220_();

///////////////////////// QueryClogHistorySQLStrategy /////////////////////////
/// Query __all_clog_history_info_v2 policy
/// Query __all_clog_history_info_v2 based on log_id to get all servers with service log IDs greater than or equal to log_id logs
/// Query __all_clog_history_info_v2 based on timestamp to get all servers with service timestamp greater than or equal to timestamp log
class QueryClogHistorySQLStrategy : public ISQLStrategy
{
public:
  QueryClogHistorySQLStrategy();
  virtual ~QueryClogHistorySQLStrategy();

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

  int init_by_log_id_query(const common::ObPartitionKey &pkey, const uint64_t log_id);
  int init_by_tstamp_query(const common::ObPartitionKey &pkey, const int64_t tstamp);
  void destroy();

public:
  TO_STRING_KV(K_(pkey),
      K_(log_id),
      K_(tstamp),
      K_(query_by_log_id));

private:
  bool                    inited_;
  common::ObPartitionKey  pkey_;
  uint64_t                log_id_;
  int64_t                 tstamp_;
  bool                    query_by_log_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryClogHistorySQLStrategy);
};

///////////////////////// QueryMetaInfoSQLStrategy /////////////////////////
/// Query meta info policy
//// Query __all_meta_table / __all_root_table for information on the server that is serving the partition
/// Query __all_meta_table / __all_root_table for leader information
class QueryMetaInfoSQLStrategy : public ISQLStrategy
{
public:
  QueryMetaInfoSQLStrategy();
  virtual ~QueryMetaInfoSQLStrategy();

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

  int init(const common::ObPartitionKey &pkey, bool only_query_leader);
  void destroy();

public:
  TO_STRING_KV(K_(pkey),
      K_(only_query_leader));

private:
  bool                    inited_;
  common::ObPartitionKey  pkey_;
  bool                    only_query_leader_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryMetaInfoSQLStrategy);
};

///////////////////////// QueryAllServerInfo /////////////////////////
// Query __all_server table
class QueryAllServerInfoStrategy: public ISQLStrategy
{
public:
  QueryAllServerInfoStrategy() {}
  virtual ~QueryAllServerInfoStrategy() {}

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  DISALLOW_COPY_AND_ASSIGN(QueryAllServerInfoStrategy);
};


///////////////////////// QueryAllZoneInfo /////////////////////////
// Query the __all_zone table
class QueryAllZoneInfoStrategy: public ISQLStrategy
{
public:
  QueryAllZoneInfoStrategy() {}
  virtual ~QueryAllZoneInfoStrategy() {}

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  DISALLOW_COPY_AND_ASSIGN(QueryAllZoneInfoStrategy);
};

///////////////////////// QueryAllZoneType /////////////////////////
// Query __all_zone table
class QueryAllZoneTypeStrategy: public ISQLStrategy
{
public:
  QueryAllZoneTypeStrategy() {}
  virtual ~QueryAllZoneTypeStrategy() {}

public:
  int build_sql_statement(char *sql_buf, const int64_t mul_statement_buf_len, int64_t &pos);

private:
  DISALLOW_COPY_AND_ASSIGN(QueryAllZoneTypeStrategy);
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

/////////////////////////////////// IObLogSysTableHelper ///////////////////////////////////////
// query system table
class IObLogSysTableHelper
{
public:
  static const int64_t DEFAULT_RECORDS_NUM = 16;
  static const int64_t ALL_SERVER_DEFAULT_RECORDS_NUM = 32;

  struct ClogHistoryRecord;
  typedef common::ObSEArray<ClogHistoryRecord, DEFAULT_RECORDS_NUM> ClogHistoryRecordArray;

  struct MetaRecord;
  typedef common::ObSEArray<MetaRecord, DEFAULT_RECORDS_NUM> MetaRecordArray;

  struct AllServerRecord;
  typedef common::ObSEArray<AllServerRecord, ALL_SERVER_DEFAULT_RECORDS_NUM> AllServerRecordArray;

  struct AllZoneRecord;
  typedef common::ObSEArray<AllZoneRecord, DEFAULT_RECORDS_NUM> AllZoneRecordArray;

  struct AllZoneTypeRecord;
  typedef common::ObSEArray<AllZoneTypeRecord, DEFAULT_RECORDS_NUM> AllZoneTypeRecordArray;

  struct ClusterInfo;

  struct ObServerVersionInfo;
  typedef common::ObSEArray<ObServerVersionInfo, DEFAULT_RECORDS_NUM> ObServerVersionInfoArray;

  struct ObServerTZInfoVersionInfo;

public:
  virtual ~IObLogSysTableHelper() { }

public:
  class BatchSQLQuery;
  /// Using multi-statement to aggregate SQL queries
  ////
  /// Currently serving SvrFinder for querying Meta Table and Clog history to determine server list and leader information
  virtual int query_with_multiple_statement(BatchSQLQuery &batch_query) = 0;

  /// Query __all_server table for all active server information
  virtual int query_all_server_info(AllServerRecordArray &records) = 0;

  /// Query __all_zone table for all zone-region information
  virtual int query_all_zone_info(AllZoneRecordArray &records) = 0;

  /// Query __all_zone table for all zone-type information
  virtual int query_all_zone_type(AllZoneTypeRecordArray &records) = 0;

  /// Query cluster-related information
  virtual int query_cluster_info(ClusterInfo &cluster_info) = 0;

  /// query min version of obsever in cluster
  virtual int query_cluster_min_observer_version(uint64_t &min_observer_version) = 0;

  /// query timezone info version
  virtual int query_timezone_info_version(const uint64_t tenant_id,
      int64_t &timezone_info_version) = 0;

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
    int get_records(ClogHistoryRecordArray& records);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(MetaRecordArray& records);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(bool &has_leader, common::ObAddr &leader);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(AllServerRecordArray& records);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(AllZoneRecordArray& records);

    /*
     * Error codes
     * - OB_NEED_RETRY: Connection error encountered
     */
    int get_records(AllZoneTypeRecordArray& records);
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
    int get_records(ObServerTZInfoVersionInfo & record);

    int64_t get_batch_sql_count() const { return batch_sql_count_; }

  private:
    template <typename RecordsType>
    int get_records_tpl_(RecordsType &records, const char *event, int64_t &record_count);
    int parse_record_from_row_(ClogHistoryRecordArray &records);
    int parse_record_from_row_(MetaRecordArray& records);
    int parse_record_from_row_(AllServerRecordArray& records);
    int parse_record_from_row_(AllZoneRecordArray& records);
    int parse_record_from_row_(AllZoneTypeRecordArray& records);
    int parse_record_from_row_(ClusterInfo &record);
    int parse_record_from_row_(ObServerVersionInfoArray &records);
    int parse_record_from_row_(ObServerTZInfoVersionInfo &record);

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

  // records in table __all_clog_history_info_v2
  struct ClogHistoryRecord
  {
    ClogHistoryRecord() { reset(); }

    // Range.
    uint64_t start_log_id_;
    uint64_t end_log_id_;

    // Addr.
    char svr_ip_[common::MAX_IP_ADDR_LENGTH + 1];
    int32_t svr_port_;

    void reset()
    {
      start_log_id_ = common::OB_INVALID_ID;
      end_log_id_ = common::OB_INVALID_ID;
      svr_ip_[0] = '\0';
      svr_port_ = 0;
    }

    TO_STRING_KV(
        K_(start_log_id),
        K_(end_log_id),
        K_(svr_ip),
        K_(svr_port));
  };

  // records in table __all_meta_table/__all_root_table
  struct MetaRecord
  {
    char svr_ip_[common::MAX_IP_ADDR_LENGTH + 1];
    int32_t svr_port_;
    int64_t role_;
    // compatibility: connecting to a lower version of the observer, which does not have a replica_type field.
    // replica_type defaults to a REPLICA_TYPE_FULL
    common::ObReplicaType replica_type_;

    MetaRecord() { reset(); }

    void reset()
    {
      svr_ip_[0] = '\0';
      svr_port_ = 0;
      role_ = 0;
      replica_type_ = common::REPLICA_TYPE_MAX;
    }

    TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(role), K_(replica_type));
  };

  // records in table __all_server
  struct AllServerRecord
  {
    typedef share::ObServerStatus::DisplayStatus StatusType;

    char             svr_ip_[common::MAX_IP_ADDR_LENGTH + 1];
    int32_t          svr_port_;
    StatusType       status_;
    common::ObZone   zone_;

    AllServerRecord() { reset(); }

    void reset()
    {
      svr_ip_[0] = '\0';
      svr_port_ = 0;
      status_ = share::ObServerStatus::OB_SERVER_ACTIVE;
      zone_.reset();
    }

    TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(status), K_(zone));
  };

  struct AllZoneRecord
  {
    common::ObZone zone_;
    common::ObRegion region_;

    AllZoneRecord() { reset(); }

    void reset()
    {
      zone_.reset();
      region_.reset();
    }

    TO_STRING_KV(K_(zone), K_(region));
  };

  struct AllZoneTypeRecord
  {
    common::ObZone zone_;
    common::ObZoneType zone_type_;

    AllZoneTypeRecord() { reset(); }

    void reset()
    {
      zone_.reset();
      zone_type_ = common::ZONE_TYPE_INVALID;
    }

    TO_STRING_KV(K_(zone), K_(zone_type));
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

  /// The ObLogAllSvrCache thread pool uses this interface
  virtual int query_all_server_info(AllServerRecordArray &records);

  /// THe ObLogAllSvrCache thread pool uses this interface
  virtual int query_all_zone_info(AllZoneRecordArray &records);

  /// The ObLogAllSvrCache thread pool uses this interface
  virtual int query_all_zone_type(AllZoneTypeRecordArray &records);

  /// Start the main thread ObLogInstance using this interface
  virtual int query_cluster_info(ClusterInfo &cluster_info);

  /// 1. Start the main thread ObLogInstance to use this interface to query and initialize the ObClusterVersion singleton
  /// 2. ObLogAllSvrCache thread uses this interface
  /// Query the cluster min observer version
  virtual int query_cluster_min_observer_version(uint64_t &min_observer_version);

  /// Query timezone info version, for oracle new timezone type synchronization
  /// ObLogTimeZoneInfoGetter
  virtual int query_timezone_info_version(const uint64_t tenant_id,
      int64_t &timezone_info_version);

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
