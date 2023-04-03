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

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObPartitionSchema;
class ObPartition;
class ObTableSchema;
}
}

namespace rootserver
{
typedef common::ObFixedLengthString<
        common::OB_MAX_BALANCE_GROUP_NAME_LENGTH>
        ObBalanceGroupName;
struct ObBalanceGroupID
{
public:
  ObBalanceGroupID() : id_high_(common::OB_INVALID_ID),
                     id_low_(common::OB_INVALID_ID) {}
  ObBalanceGroupID(const uint64_t id_high,
                 const uint64_t id_low)
    : id_high_(id_high),
      id_low_(id_low) {}
public:
  uint64_t id_high_;
  uint64_t id_low_;

  TO_STRING_KV(K(id_high_),
               K(id_low_));
  bool is_valid() const {
    return OB_INVALID_ID != id_high_
           && OB_INVALID_ID != id_low_;
  }
};

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
private:
  int generate_insert_update_sql(
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
  static int get_non_partitioned_bg_info(
      const uint64_t tenant_id,
      ObBalanceGroupName &bg_name,
      ObBalanceGroupID &bg_id);
  static int get_one_level_partitioned_bg_info(
      const share::schema::ObPartitionSchema &entity_schema,
      ObBalanceGroupName &bg_name,
      ObBalanceGroupID &bg_id);
  static int get_two_level_partitioned_bg_info(
      const share::schema::ObPartitionSchema &entity_schema,
      const share::schema::ObPartition &partition_schema,
      ObBalanceGroupName &bg_name,
      ObBalanceGroupID &bg_id);
public:
  ObNewTableTabletAllocator(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLProxy *sql_proxy);
  virtual ~ObNewTableTabletAllocator();
public:
  int init();
  int prepare(
      const share::schema::ObTableSchema &table_schema);
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
private:
  int get_tablet_id_array(
      const share::schema::ObTableSchema &table_schema,
      common::ObIArray<common::ObTabletID> &ls_id_array);
  int alloc_tablet_by_primary_schema(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_by_count_balance(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_non_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_one_level_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema);
  int alloc_tablet_for_two_level_partitioned_balance_group(
      const share::schema::ObTableSchema &table_schema,
      const int64_t part_idx);
  int get_available_ls(
      common::ObIArray<share::ObLSStatusInfo> &ls_status_info_array);
  int get_balance_group_primary_schema(
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObPartitionSchema *&primary_schema);
  int alloc_tablet_for_create_balance_group(
      const ObBalanceGroupName &bg_name,
      const ObBalanceGroupID &bg_id,
      const common::ObIArray<share::ObLSStatusInfo> &ls_status_info_array,
      const int64_t partition_num);
  int alloc_tablet_for_add_balance_group(
      const common::ObIArray<ObBalanceGroupLSStat> &bg_ls_stat_array,
      const ObBalanceGroupName &bg_name,
      const ObBalanceGroupID &bg_id,
      const common::ObIArray<share::ObLSStatusInfo> &ls_status_info_array,
      const int64_t partition_num);
private:
  static const int64_t MAX_TENANT_LS_CNT = 1024;
  enum class MyStatus : int64_t {
    WAIT_TO_PREPARE = 0,
    WAIT_TO_OUTPUT,
    INVALID,
  };
private:
  uint64_t tenant_id_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObMySQLTransaction trans_;
  ObBalanceGroupLSStatOperator bg_ls_stat_operator_;
  MyStatus status_;
  common::ObArray<share::ObLSID> ls_id_array_;
  bool inited_;
};

}//end namespace rootserver
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_LS_STAT_OPERATOR_H_
