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

#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"

#ifndef OB_TABLET_REORGANIZE_HISTORY_TABLE_OPERATOR_H
#define OB_TABLET_REORGANIZE_HISTORY_TABLE_OPERATOR_H

namespace oceanbase
{
namespace share
{

enum ObTabletReorganizeType
{
  INVALID_REORGANIZE_TYPE = 0,
  SPLIT,
  MERGE,
  MAX_REORGANIZE_TYPE
};

class ObTabletReorganizeRecord final
{
public:
  ObTabletReorganizeRecord() = default;
  ObTabletReorganizeRecord(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObTabletID &src_tablet_id,
      const ObTabletID &dest_tablet_id,
      const ObTabletReorganizeType type,
      const uint64_t create_time,
      const uint64_t finish_time)
    : tenant_id_(tenant_id),
    ls_id_(ls_id),
    src_tablet_id_(src_tablet_id),
    dest_tablet_id_(dest_tablet_id),
    type_(type),
    create_time_(create_time),
    finish_time_(finish_time)
  {}
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_TENANT_ID &&
      ls_id_.is_valid() &&
      src_tablet_id_.is_valid() &&
      dest_tablet_id_.is_valid() &&
      type_ > INVALID_REORGANIZE_TYPE &&
      type_ < MAX_REORGANIZE_TYPE;
  }
  TO_STRING_KV(K(tenant_id_), K(ls_id_), K(src_tablet_id_), K(dest_tablet_id_),
      K(type_), K(create_time_), K(finish_time_));
  void reset() {
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_id_.reset();
    src_tablet_id_.reset();
    dest_tablet_id_.reset();
    type_ = INVALID_REORGANIZE_TYPE;
    create_time_ = 0;
    finish_time_ = 0;
  }
public:
  uint64_t tenant_id_;
  ObLSID ls_id_;
  ObTabletID src_tablet_id_;
  ObTabletID dest_tablet_id_;
  ObTabletReorganizeType type_;
  uint64_t create_time_;
  uint64_t finish_time_;
};

class ReorganizeTabletPair final
{
public:
  ReorganizeTabletPair() = default;
  ReorganizeTabletPair(
      const int64_t src_tablet_id,
      const int64_t dest_tablet_id)
    : src_tablet_id_(ObTabletID(src_tablet_id)),
    dest_tablet_id_(ObTabletID(dest_tablet_id))
  {}
  TO_STRING_KV(K_(src_tablet_id), K_(dest_tablet_id));
public:
  ObTabletID src_tablet_id_;
  ObTabletID dest_tablet_id_;
};

class ObTabletReorganizeHistoryTableOperator final
{
public:
  ObTabletReorganizeHistoryTableOperator();
  ~ObTabletReorganizeHistoryTableOperator() {}

  static int check_tablet_has_reorganized(
      common::ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      share::ObLSID &ls_id,
      bool &reorganized);
  static int get_all_split_tablet_pairs(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObIArray<ReorganizeTabletPair> &tablet_pairs);
  static int get_split_tablet_pairs_by_src(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      ObIArray<ReorganizeTabletPair> &tablet_pairs);
  static int get_split_tablet_pairs_by_dest(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      ReorganizeTabletPair &tablet_pair);
  static int insert_(
      ObISQLClient &sql_proxy,
      const ObTabletReorganizeRecord &incomplete_record,
      const ObIArray<ObTabletID> &dest_tablet_ids);
  static int insert(
      ObISQLClient &sql_proxy,
      const ObTabletReorganizeRecord &record);
  static int batch_insert(
      ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const obrpc::ObPartitionSplitArg &split_arg,
      const int64 start_time,
      const int64 finish_time);

private:
  static int inner_batch_insert_(
      ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const ObSArray<ObTabletID> &dest_tablet_ids,
      const int64 start_time,
      const int64 finish_time);
}; // class ObTabletReorganizeHistoryTableOperator

} // namespace share
} // namespace oceanbase

#endif  // OB_TABLET_REORGANIZE_HISTORY_TABLE_OPERATOR_H
