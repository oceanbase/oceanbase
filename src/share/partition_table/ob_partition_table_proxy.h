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

#ifndef OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_PROXY_H_
#define OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_PROXY_H_

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_helper.h"
#include "ob_partition_info.h"
#include "share/ob_core_table_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_rpc_struct.h"
#include "common/ob_role.h"
namespace oceanbase {
namespace common {
class ObSqlString;
class ObServerConfig;
}  // namespace common
namespace share {

class ObPartitionInfo;
class ObPartitionReplica;
class ObDMLSqlSplicer;
class ObIMergeErrorCb;
// partition table (__all_core_table, __all_root_table, __all_tenant_meta_table) query && dml proxy
class ObPartitionTableProxy {
public:
  ObPartitionTableProxy(common::ObISQLClient& sql_proxy) : sql_proxy_(sql_proxy), merge_error_cb_(NULL), config_(NULL)
  {}
  virtual ~ObPartitionTableProxy()
  {}
  void set_merge_error_cb(ObIMergeErrorCb* cb);
  void set_server_config(common::ObServerConfig* config)
  {
    config_ = config;
  }

  int start_trans();
  int end_trans(const int return_code);
  virtual int reset()
  {
    return common::OB_SUCCESS;
  }
  bool is_trans_started() const
  {
    return trans_.is_started();
  }
  virtual int fetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false) = 0;

  // partition table sql operation
  // use select for update for reading if lock_replica is true.
  virtual int fetch_partition_info(const bool lock_replica, const uint64_t table_id, const int64_t partition_id,
      const bool filter_flag_replica, ObPartitionInfo& partition_info, const bool need_fetch_faillist = false,
      const int64_t cluster_id = common::OB_INVALID_ID) = 0;

  virtual int fetch_partition_infos(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum, bool specify_table_id,
      const bool need_fetch_faillist = false) = 0;

  virtual int fetch_partition_infos_pt(const uint64_t pt_table_id, const int64_t pt_partition_id,
      const uint64_t start_table_id, const int64_t start_partition_id, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false) = 0;

  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) = 0;

  virtual int update_replica(const ObPartitionReplica& replica, const bool replace) = 0;
  virtual int update_replica(const ObPartitionReplica& replica) = 0;
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role) = 0;
  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& replica_array, const common::ObRole new_role) = 0;

  // update replica role
  virtual int set_to_follower_role(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) = 0;

  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) = 0;

  virtual int set_unit_id(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const uint64_t unit_id) = 0;

  virtual int set_original_leader(
      const uint64_t table_id, const int64_t partition_id, const bool is_original_leader) = 0;

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild) = 0;

  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list) = 0;

  virtual int update_replica_status(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObReplicaStatus status) = 0;

  virtual int handover_partition(
      const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr);

  virtual int replace_partition(
      const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr);

  // add all replica filed to dml_splicer
  static int fill_dml_splicer(const ObPartitionReplica& replica, ObDMLSqlSplicer& dml_splicer);
  TO_STRING_EMPTY();

protected:
  common::ObISQLClient& get_sql_client();

  template <typename RESULT, typename COLS>
  int construct_partition_replica(RESULT& res, const COLS& cols, common::ObIAllocator* allocator,
      const bool ignore_row_checksum, ObPartitionReplica& replica, const bool need_fetch_faillist = false);

  template <typename RESULT, typename COLS>
  int construct_partition_info(RESULT& res, const COLS& cols, const bool filter_flag_replica,
      ObPartitionInfo& partition_info, const bool need_fetch_faillist = false);

  template <typename RESULT, typename COLS>
  int construct_partition_infos(RESULT& res, const COLS& cols, const bool filter_flag_replica, int64_t& fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
      const bool need_fetch_faillist = false);

  template <typename RESULT, typename COLS>
  int construct_partition_infos(RESULT& res, const COLS& cols, common::ObIAllocator& allocator,
      common::ObIArray<ObPartitionInfo*>& partition_infos);

protected:
  common::ObISQLClient& sql_proxy_;
  common::ObMySQLTransaction trans_;
  ObIMergeErrorCb* merge_error_cb_;
  common::ObServerConfig* config_;
};

// for __all_core_table
class ObKVPartitionTableProxy : public ObPartitionTableProxy {
public:
  ObKVPartitionTableProxy(common::ObISQLClient& sql_proxy)
      : ObPartitionTableProxy(sql_proxy),
        core_table_(OB_ALL_ROOT_TABLE_TNAME, sql_proxy),
        is_loaded_(false),
        implicit_trans_started_(false)
  {}
  virtual ~ObKVPartitionTableProxy()
  {}
  virtual int reset()
  {
    is_loaded_ = false;
    implicit_trans_started_ = false;
    return common::OB_SUCCESS;
  }
  int implicit_start_trans();
  int implicit_end_trans(const int return_code);
  int fetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t start_partition_id,
      const bool filter_flag_replica, int64_t& max_fetch_count, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int fetch_partition_info(const bool lock_replica, const uint64_t table_id, const int64_t partition_id,
      const bool filter_flag_replica, ObPartitionInfo& partition_info, const bool need_fetch_faillist = false,
      const int64_t cluster_id = common::OB_INVALID_ID);

  virtual int fetch_partition_infos(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum, const bool specify_table_id = false,
      const bool need_fetch_faillist = false) override;

  virtual int fetch_partition_infos_pt(const uint64_t pt_table_id, const int64_t pt_partition_id,
      const uint64_t start_table_id, const int64_t start_partition_id, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false);

  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int update_replica(const ObPartitionReplica& replica, const bool replace);
  virtual int update_replica(const ObPartitionReplica& replica);
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role);
  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& tasks, const common::ObRole new_role);

  virtual int set_to_follower_role(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server);

  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server);

  virtual int set_unit_id(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const uint64_t unit_id);

  virtual int set_original_leader(const uint64_t table_id, const int64_t partition_id, const bool is_original_leader);

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild);

  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list);

  virtual int update_replica_status(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const ObReplicaStatus status);

  static int fill_dml_splicer_for_update(const ObPartitionReplica& replica, ObDMLSqlSplicer& dml_splicer);

private:
  // start transaction and load __all_root_table for update
  int load_for_update();

private:
  ObCoreTableProxy core_table_;
  bool is_loaded_;
  bool implicit_trans_started_;
};

// for __all_root_table, __all_tenant_meta_table
class ObNormalPartitionTableProxy : public ObPartitionTableProxy {
public:
  const char* const NORMAL_STATUS = "REPLICA_STATUS_NORMAL";
  const char* const UNMERGED_STATUS = "REPLICA_STATUS_UNMERGED";

public:
  ObNormalPartitionTableProxy(common::ObISQLClient& sql_proxy) : ObPartitionTableProxy(sql_proxy)
  {}
  virtual ~ObNormalPartitionTableProxy()
  {}

  virtual int fetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false) override;

  virtual int fetch_partition_info(const bool lock_replica, const uint64_t table_id, const int64_t partition_id,
      const bool filter_flag_replica, ObPartitionInfo& partition_info, const bool need_fetch_faillist = false,
      const int64_t cluster_id = common::OB_INVALID_ID);

  virtual int fetch_partition_infos(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum, bool specify_table_id = false,
      const bool need_fetch_faillist = false) override;

  virtual int fetch_partition_infos_pt(const uint64_t pt_table_id, const int64_t pt_partition_id,
      const uint64_t start_table_id, const int64_t start_partition_id, int64_t& max_fetch_count,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false);

  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int update_replica(const ObPartitionReplica& replica, const bool replace);
  virtual int update_replica(const ObPartitionReplica& replica);
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role);
  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& tasks, const common::ObRole new_role);

  virtual int set_to_follower_role(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server);

  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server);

  virtual int set_unit_id(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const uint64_t unit_id);

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild);

  virtual int set_original_leader(const uint64_t table_id, const int64_t partition_id, const bool is_original_leader);
  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list);

  virtual int update_replica_status(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const ObReplicaStatus status);

  // batch update data_version of replicas which been creating right now.
  // Note: only support __all_tenant_meta_table partitions
  int update_fresh_replica_version(const common::ObAddr& addr, const int64_t sql_port,
      const common::ObIArray<obrpc::ObCreatePartitionArg>& create_partition_args, const int64_t data_version);
  // get partition infos for multiple pkeys
  int multi_fetch_partition_infos(const uint64_t tenant_id, const common::ObIArray<common::ObPartitionKey>& pkeys,
      const bool filter_flag_replicas, common::ObIArray<share::ObPartitionInfo>& partition_infos);

  static int fill_dml_splicer_for_update(const ObPartitionReplica& replica, ObDMLSqlSplicer& dml_splicer);

private:
  virtual int init_empty_string(common::ObSqlString& sql_string);
  virtual int fetch_partition_infos_impl(const char* pt_name, const uint64_t start_table_id,
      const int64_t start_partition_id, const bool filter_flag_replica, const int64_t max_fetch_count,
      const common::ObSqlString& specified_tenant_str, const common::ObSqlString& specified_partition_str,
      int64_t& fetch_count, common::ObIArray<ObPartitionInfo>& partition_infos, uint64_t sql_tenant_id,
      bool ignore_row_checksum, bool specify_table_id = false, const bool need_fetch_faillist = false);
  int partition_table_name(const uint64_t table_id, const char*& table_name, uint64_t& sql_tenant_id);
  int generate_batch_report_partition_role_sql(const char* table_name,
      const common::ObIArray<share::ObPartitionReplica>& replica_array, const common::ObRole new_role,
      common::ObSqlString& sql);
  int append_sub_batch_sql_fmt(common::ObSqlString& sql_string, int64_t& start,
      const common::ObIArray<common::ObPartitionKey>& pkeys, const uint64_t tenant_id);

  struct ObPartitionKeyCompare {
    bool operator()(const ObPartitionInfo* left, const common::ObPartitionKey& right);
  };
};

class ObNormalPartitionUpdateHelper : public ObDMLExecHelper {
public:
  ObNormalPartitionUpdateHelper(common::ObISQLClient& sql_client, const uint64_t tenant_id)
      : ObDMLExecHelper(sql_client, tenant_id)
  {}

  ~ObNormalPartitionUpdateHelper()
  {}
  int exec_update_leader_replica(
      const ObPartitionReplica& replica, const char* table_name, const ObPTSqlSplicer& splicer, int64_t& affected_rows);
  int exec_insert_update_replica(const char* table_name, const ObPTSqlSplicer& splicer, int64_t& affected_rows);
  int exec_batch_insert_update_replica(
      const char* table_name, const bool with_role, const ObPTSqlSplicer& splicer, int64_t& affected_rows);
};

class ObPartitionTableProxyFactory {
public:
  ObPartitionTableProxyFactory(common::ObISQLClient& sql_proxy, ObIMergeErrorCb* cb, common::ObServerConfig* config)
      : kv_proxy_(sql_proxy), normal_proxy_(sql_proxy), merge_error_cb_(cb), config_(config)
  {}
  virtual ~ObPartitionTableProxyFactory()
  {}

  // return NULL for invalid %table_id
  virtual int get_proxy(const uint64_t table_id, ObPartitionTableProxy*& proxy);
  virtual int get_tenant_proxies(const uint64_t tenant_id, common::ObIArray<ObPartitionTableProxy*>& proxies);
  // return NULL for invalid %pt_table_id
  virtual int get_proxy_of_partition_table(const uint64_t pt_table_id, ObPartitionTableProxy*& proxy);

private:
  // this factory can only be allocated on stack
  void* operator new(size_t);

private:
  ObKVPartitionTableProxy kv_proxy_;
  ObNormalPartitionTableProxy normal_proxy_;
  ObIMergeErrorCb* merge_error_cb_;
  common::ObServerConfig* config_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_PROXY_H_
