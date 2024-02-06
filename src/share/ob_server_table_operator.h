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

#ifndef OCEANBASE_SHARE_OB_SERVER_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_SERVER_TABLE_OPERATOR_H_

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_zone.h"
#include "share/ob_server_status.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObServerConfig;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
class ObDMLSqlSplicer;
struct ObServerInfoInTable
{
public:
  typedef ObFixedLengthString<common::OB_SERVER_VERSION_LENGTH> ObBuildVersion;
  ObServerInfoInTable();
  virtual ~ObServerInfoInTable();
  int init(
    const common::ObAddr &server,
    const uint64_t server_id,
    const common::ObZone &zone,
    const int64_t sql_port,
    const bool with_rootserver,
    const ObServerStatus::DisplayStatus status,
    const ObBuildVersion &build_version,
    const int64_t stop_time,
    const int64_t start_service_time,
    const int64_t last_offline_time);
  int assign(const ObServerInfoInTable &other);
  bool is_valid() const;
  void reset();
  int build_server_info_in_table(const share::ObServerStatus &server_status);
  int build_server_status(share::ObServerStatus &server_status) const;
  const common::ObAddr &get_server() const { return server_; }
  uint64_t get_server_id() const { return server_id_; }
  const common::ObZone& get_zone() const { return zone_; }
  int64_t get_sql_port() const { return sql_port_; }
  bool get_with_rootserver() const { return with_rootserver_; }
  ObServerStatus::DisplayStatus get_status() const { return status_; }
  const ObBuildVersion& get_build_version() const { return build_version_; }
  int64_t get_stop_time() const { return stop_time_; }
  int64_t get_start_service_time() const { return start_service_time_; }
  int64_t get_block_migrate_in_time() const { return block_migrate_in_time_; }
  int64_t get_last_offline_time() const { return last_offline_time_; }
  bool is_stopped() const { return 0 != stop_time_; }
  bool is_alive() const { return 0 == last_offline_time_; }
  bool in_service() const { return 0 != start_service_time_; }
  bool is_deleting() const { return ObServerStatus::OB_SERVER_DELETING == status_; }
  bool is_active() const { return ObServerStatus::OB_SERVER_ACTIVE == status_; }
  bool is_migrate_in_blocked() const { return 0 != block_migrate_in_time_; }
  bool can_migrate_in() const { return is_active() && !is_migrate_in_blocked(); }
  bool is_permanent_offline() const;
  bool is_temporary_offline() const;
  TO_STRING_KV(
      K_(server),
      K_(server_id),
      K_(zone),
      K_(sql_port),
      K_(with_rootserver),
      K_(status),
      K_(build_version),
      K_(stop_time),
      K_(start_service_time),
      K_(block_migrate_in_time),
      K_(last_offline_time))
private:
  common::ObAddr server_;
  uint64_t server_id_;
  common::ObZone zone_;
  int64_t sql_port_;    // sql listen port
  bool with_rootserver_;
  ObServerStatus::DisplayStatus status_;
  ObBuildVersion build_version_;
  int64_t stop_time_;
  int64_t start_service_time_;
  // in the old log (version < 4.2, last_hb_time is weakly equivalent to gmt_modified)
  // gmt_modified_ is compatible with last_hb_time
  // in the new logic (version >= 4.2), we do not set gmt_modified in __all_server table explicitly
  int64_t block_migrate_in_time_;
  int64_t last_offline_time_;
};
class ObServerTableOperator
{
public:
  ObServerTableOperator();
  virtual ~ObServerTableOperator();

  int init(common::ObISQLClient *proxy);
  common::ObISQLClient &get_proxy() const { return *proxy_; }
  virtual int get(common::ObIArray<share::ObServerStatus> &server_statuses);
  static int get(
    common::ObISQLClient &sql_proxy,
    common::ObIArray<share::ObServerStatus> &server_statuses);
  static int remove(const common::ObAddr &server, common::ObMySQLTransaction &trans);
  virtual int update(const share::ObServerStatus &server_status);
  virtual int reset_rootserver(const common::ObAddr &except);
  virtual int update_status(const common::ObAddr &server,
                            const share::ObServerStatus::DisplayStatus status,
                            const int64_t last_hb_time,
                            common::ObMySQLTransaction &trans);
  virtual int update_stop_time(const common::ObAddr &server,
      const int64_t stop_time);
  virtual int update_with_partition(const common::ObAddr &server, bool with_partition);
  int get_start_service_time(const common::ObAddr &server, int64_t &start_service_time) const;
  // read __all_server table and return all servers' info in the table
  //
  // @param[out]  all_servers_info_in_table			an array, which represents all rows in __all_server table
  //
  // @ret OB_SUCCESS 			        get servers' info from __all_server table successfully
  // @ret OB_TABLE_NOT_EXIST	    it occurs in the bootstrap period, we need to wait for some time.

  // @ret other error code			  failure
  static int get(
    common::ObISQLClient &sql_proxy,
    common::ObIArray<ObServerInfoInTable> &all_servers_info_in_table);
  // read the given server's corresponding row in __all_server table
  // this func can be called in version >= 4.2
  //
  // @param[out]  all_servers_info_in_table			an array, which represents all rows in __all_server table
  //
  // @ret OB_SUCCESS 			        get servers' info from __all_server table successfully
  // @ret OB_TABLE_NOT_EXIST	    it occurs in the bootstrap period, we need to wait for some time.

  // @ret other error code			  failure
  static int get(
      common::ObISQLClient &sql_proxy,
      const common::ObAddr &server,
      ObServerInfoInTable &server_info_in_table);
  // insert the new server's info into __all_server table,
  // it is only called when we want to add a new server into clusters
  //
  // @param[in]  trans			              transaction
  // @param[in]  server_info_in_table			the new server's info which is expected to be inserted
  //
  // @ret OB_SUCCESS 			      the insertion is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the server's info
  //                            has been inserted in __all_server table already
  // @ret other error code			failure
  static int insert(
      common::ObISQLClient &sql_proxy,
      const ObServerInfoInTable &server_info_in_table);
  // set the given server's status be new_status in __all_server table
  // the prerequisites of a successful setting is that the previous status new_status
  // @ret OB_SUCCESS 			      the setting is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the previous status is correct
  // @ret other error code			failure
  static int update_status(
      ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const ObServerStatus::DisplayStatus old_status,
      const ObServerStatus::DisplayStatus new_status);
  // the given server's with_rootserver will be set in __all_server table.
  // other servers' with_rootserver will be reset.
  //
  // @param[in]  trans			              transaction
  // @param[in]  server			              the server which we want to update its info
  //
  // @ret OB_SUCCESS 			      the updation is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the old value is correct
  // @ret other error code			failure
  static int update_with_rootserver(
      ObMySQLTransaction &trans,
      const common::ObAddr &server);

  // update the build_version of a given server in __all_server table
  // the prerequisites of a successful updation is that we give the right current value (old_build_version)
  //
  // @param[in]  trans			              transaction
  // @param[in]  server			              the server which we want to update its info
  // @param[in]  old_build_version			  the current value of the given server's build_version
  // @param[in]  new_build_version			  the expected new value of the given server's build_version
  //
  // @ret OB_SUCCESS 			      the updation is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the old value is correct
  // @ret other error code			failure
  static int update_build_version(
      ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const ObServerInfoInTable::ObBuildVersion &old_build_version,
      const ObServerInfoInTable::ObBuildVersion &new_build_version);

  // update the start_service_time of a given server in __all_server table
  // the prerequisites of a successful updation is that we give the right current value (old_start_service_time)
  //
  // @param[in]  trans			              transaction
  // @param[in]  server			              the server which we want to update its info
  // @param[in]  old_start_service_time		the current value of the given server's start_service_time
  // @param[in]  new_start_service_time		the expected new value of the given server's start_service_time
  //
  // @ret OB_SUCCESS 			      the updation is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the old value is correct
  // @ret other error code			failure
  static int update_start_service_time(
      ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const int64_t old_start_service_time,
      const int64_t new_start_service_time);
  // The server becomes active/online, its last_offline_time should be zero.
  // In addition, if the server's status is inactive, the status should be set active.
  // If the server's status is deleting, we do not need to change its status.
  //
  // @param[in]  trans			              transaction
  // @param[in]  server			              the server which we find it becomes inactive
  //
  // @ret OB_SUCCESS 			      the updation is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the old value is correct
  // @ret other error code			failure
  static int update_table_for_offline_to_online_server(
      ObMySQLTransaction &trans,
      const bool is_deleting,
      const common::ObAddr &server);
  // The server becomes inactive/offline, its last_offline_time should be set,
  // and its start_service_time should be zero.
  // In addition, if the server's status is active, the status should be set inactive.
  // If the server's status is deleting, we do not need to change its status.
  //
  // @param[in]  trans			              transaction
  // @param[in]  server			              the server which we find it becomes inactive
  //
  // @ret OB_SUCCESS 			      the updation is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether the old value is correct
  // @ret other error code			failure
  static int update_table_for_online_to_offline_server(
      ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const bool is_deleting,
      int64_t last_offline_time);
  // update the given server's stop_time,
  // if is_start is true, set stop_time = 0 (where stop_time != 0), otherwise stop_time will be now
  //
  // @param[in]  trans			              transaction
  // @param[in]  server			              the server which we want to update its stop_time
  // @param[in]  is_start                 if true, start server. Otherwise, stop server.
  //
  // @ret OB_SUCCESS 			      the updation is successful
  // @ret OB_NEED_RETRY	        no affected rows, probably we need to retry the operation
  //                            or check the table to see whether stop_time has been 0 already (if is_start)
  //                            or not 0 (if !is_start)
  // @ret other error code			failure
  static int update_stop_time(
      ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const int64_t old_stop_time,
      const int64_t new_stop_time);
private:
  static int build_server_status(
    const common::sqlclient::ObMySQLResult &res,
    share::ObServerStatus &server_status);
  static int exec_write(
      ObMySQLTransaction &trans,
      ObSqlString &sql,
      const bool is_multi_rows_affected);
  static int insert_dml_builder(
    const ObServerStatus &server_status,
    ObDMLSqlSplicer &dml);
private:
  bool inited_;
  common::ObISQLClient *proxy_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SERVER_TABLE_OPERATOR_H_