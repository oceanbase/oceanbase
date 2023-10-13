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

#ifndef OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_LS_STAT_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_LS_STAT_OPERATOR_H_
#include "lib/ob_define.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/schema/ob_schema_mgr.h"

#include "balance/ob_balance_group_define.h"    // ObBalanceGroupID, ObBalanceGroupName

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
}
namespace share
{
class ObLSAttr;
namespace schema
{
class ObSchemaGetterGuard;
class ObPartitionSchema;
class ObPartition;
class ObTableSchema;
}
}
namespace observer
{
class ObInnerSQLConnection;
}

namespace rootserver
{
class ObBalanceGroupLSStat
{
public:
  ObBalanceGroupLSStat() : tenant_id_(common::OB_INVALID_ID),
                           balance_group_id_(),
                           ls_id_(),
                           tablet_group_count_(-1),
                           balance_group_name_() {}
public:
  int build(
      const uint64_t tenant_id,
      const ObBalanceGroupID &balance_group_id,
      const share::ObLSID &ls_id,
      const int64_t tablet_group_count,
      const ObBalanceGroupName &balance_group_name);
  // get member
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObBalanceGroupID &get_balance_group_id() const { return balance_group_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  int64_t get_tablet_group_count() const { return tablet_group_count_; }
  const ObBalanceGroupName &get_balance_group_name() const { return balance_group_name_; }
  void add_tablet_group_count(const int64_t add_cnt) { tablet_group_count_ += add_cnt; }
public:
  bool is_valid() const {
    return OB_INVALID_ID != tenant_id_
           && balance_group_id_.is_valid()
           && ls_id_.is_valid()
           && tablet_group_count_ >= 0
           && !balance_group_name_.is_empty();
  }
  TO_STRING_KV(K(tenant_id_),
               K(balance_group_id_),
               K(ls_id_),
               K(tablet_group_count_),
               K(balance_group_name_));
  bool operator <(const ObBalanceGroupLSStat &that) {
    return this->tablet_group_count_ > that.tablet_group_count_;
  }
private:
  // primary key
  uint64_t tenant_id_;
  ObBalanceGroupID balance_group_id_;
  share::ObLSID ls_id_;
  // normal column
  int64_t tablet_group_count_;
  ObBalanceGroupName balance_group_name_;
};

class ObBalanceGroupLSStatOperator
{
public:
  ObBalanceGroupLSStatOperator();
  virtual ~ObBalanceGroupLSStatOperator();
  int init(
      common::ObMySQLProxy *sql_proxy);
public:
  int get_balance_group_ls_stat(
      const int64_t timeout,
      const uint64_t tenant_id,
      const ObBalanceGroupID &balance_group_id,
      common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array);
  int get_balance_group_ls_stat(
      const int64_t timeout,
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObBalanceGroupID &balance_group_id,
      const bool for_update,
      common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array);
  int insert_update_balance_group_ls_stat(
      const int64_t timeout_abs,
      const uint64_t tenant_id,
      const ObBalanceGroupID &balance_group_id,
      const common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array);
  int insert_update_balance_group_ls_stat(
      const int64_t timeout_abs,
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObBalanceGroupID &balance_group_id,
      const common::ObIArray<ObBalanceGroupLSStat> &balance_group_ls_stat_array);
  int inc_balance_group_ls_stat(
      const int64_t timeout_abs,
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObBalanceGroupLSStat &ls_stat);
  int delete_balance_group_ls_stat(
      const int64_t timeout,
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id);
private:
  int generate_insert_update_sql(
      const ObBalanceGroupLSStat &bg_ls_stat,
      common::ObSqlString &sql_string);
  int generate_inc_sql_(
      const ObBalanceGroupLSStat &bg_ls_stat,
      common::ObSqlString &sql_string);
private:
  bool inited_;
  common::ObMySQLProxy *sql_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBalanceGroupLSStatOperator);
};

class ObNewTableTabletAllocator
{
public:
  ObNewTableTabletAllocator(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLProxy *sql_proxy,
      const bool use_parallel_ddl = false);
  virtual ~ObNewTableTabletAllocator();
public:
  int init();
  int prepare(
      ObMySQLTransaction &trans,
      const share::schema::ObTableSchema &table_schema,
      bool is_add_partition = false);
  int prepare_like(
      const share::schema::ObTableSchema &table_schema);
  int get_ls_id_array(
      common::ObIArray<share::ObLSID> &ls_id_array);
  int finish(const bool commit);
private:
  int alloc_ls_for_meta_or_sys_tenant_tablet(
      const share::schema::ObTableSchema &table_schema);
  int alloc_ls_for_local_index_tablet(
      const share::schema::ObTableSchema &table_schema);
  int alloc_ls_for_global_index_tablet(
      const share::schema::ObTableSchema &table_schema);
  int alloc_ls_for_in_tablegroup_tablet(
      const share::schema::ObTableSchema &table_schema);
  int alloc_ls_for_normal_table_tablet(
      const share::schema::ObTableSchema &table_schema);
  int alloc_ls_for_duplicate_table_(
      const share::schema::ObTableSchema &table_schema);
private:
  int alloc_tablet_for_tablegroup(
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema);
  int alloc_tablet_for_tablegroup(
      const share::schema::ObTableSchema &primary_schema,
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema);
  int wait_ls_elect_leader_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);
  int get_tablet_id_array(
      const share::schema::ObTableSchema &table_schema,
      common::ObIArray<common::ObTabletID> &ls_id_array);
  int alloc_tablet_by_primary_schema(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_by_count_balance(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_non_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_non_partitioned_balance_group_by_cache_(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_one_level_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_two_level_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema,
      const int64_t part_idx);
  int get_available_ls(common::ObIArray<share::ObLSID> &ls_id_array);
  int alloc_tablet_for_create_balance_group(
      const ObBalanceGroupName &bg_name,
      const ObBalanceGroupID &bg_id,
      const common::ObIArray<share::ObLSID> &ls_id_array,
      const int64_t partition_num);
  int alloc_tablet_for_add_balance_group(
      const common::ObIArray<ObBalanceGroupLSStat> &bg_ls_stat_array,
      const ObBalanceGroupName &bg_name,
      const ObBalanceGroupID &bg_id,
      const common::ObIArray<share::ObLSID> &ls_id_array,
      const int64_t partition_num);
  int check_and_replace_ls_(common::ObMySQLTransaction &trans, const uint64_t tenant_id);
  void find_last_user_ls_(
      const common::ObIArray<share::ObLSID> &ls_id_array,
      int64_t &index);
  int lock_and_check_ls_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const common::ObIArray<share::ObLSID> &locked_ls_id_array,
      const share::ObLSID &ls_id,
      share::ObLSAttr &ls_attr);
  int choose_new_ls_(
      const uint64_t tenant_id,
      const share::ObLSAttr &old_ls_attr,
      const share::ObLSID &prev_ls_id,
      share::ObLSID &new_ls_id);
  int generate_ls_array_by_primary_schema(
      const share::schema::ObTableSchema &primary_schema,
      common::ObArray<share::ObLSID> &pre_ls_id_array);
  int extract_one_level_ls_array_by_primary_schema(
    const share::schema::ObTableSchema &primary_schema,
    common::ObArray<share::ObLSID> &all_ls_id_array,
    common::ObArray<share::ObLSID> &pre_ls_id_array);
  int alloc_tablet_for_add_part_in_tablegroup_sharding_partition(
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObTableSchema &origin_table_schema);
  int64_t fetch_ls_offset() {
    return ATOMIC_FAA(&alloc_tablet_ls_offset_, 1);
  }
private:
  static const int64_t MAX_TENANT_LS_CNT = 1024;
  static const int64_t WAIT_INTERVAL_US = 1000 * 1000; // 1s
  enum class MyStatus : int64_t {
    WAIT_TO_PREPARE = 0,
    WAIT_TO_OUTPUT,
    INVALID,
  };
private:
  uint64_t tenant_id_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  common::ObMySQLProxy *sql_proxy_;
  ObBalanceGroupLSStatOperator bg_ls_stat_operator_;
  MyStatus status_;
  common::ObArray<share::ObLSID> ls_id_array_;
  bool inited_;
  bool is_add_partition_;
  static int64_t alloc_tablet_ls_offset_;
  bool use_parallel_ddl_;
};

}//end namespace rootserver
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_LS_STAT_OPERATOR_H_
