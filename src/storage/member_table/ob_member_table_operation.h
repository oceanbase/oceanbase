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

#ifndef OCEANBASE_MEMBER_TABLE_OB_MEMBER_TABLE_OPERATOR
#define OCEANBASE_MEMBER_TABLE_OB_MEMBER_TABLE_OPERATOR

#include "storage/member_table/ob_member_table.h"
#include "storage/member_table/ob_member_table_iterator.h"
#include "storage/member_table/ob_member_table_schema_helper.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace storage
{

class ObMemberTableDataGenerator final
{
public:
  ObMemberTableDataGenerator() {}
  ~ObMemberTableDataGenerator() {}
  static int gen_transfer_member_data(
      const common::ObTabletID &tablet_id,
      const share::SCN &reorganization_scn,
      const ObTabletStatus &tablet_status,
      const share::ObLSID &relative_ls_id,
      const int64_t transfer_seq,
      const share::SCN &transfer_scn,
      const share::SCN &src_reorganization_scn,
      common::ObIArray<ObMemberTableData> &member_table_data);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemberTableDataGenerator);
};

// operator for member table
class ObMemberTableWriteOperator final
{
public:
  ObMemberTableWriteOperator();
  ~ObMemberTableWriteOperator();
  int init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);
  int insert_row(
      const ObMemberTableData &member_table_data);
  int insert_rows(
      const common::ObIArray<ObMemberTableData> &member_table_data);
private:
  int inner_insert_row_(
      const ObMemberTableData &data,
      int64_t &affected_rows);

  int fill_data_(
      blocksstable::ObDatumRow *datum_row);
  int flush_and_wait_();
private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  common::ObISQLClient *sql_proxy_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObDataBuffer data_buffer_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemberTableWriteOperator);
};

class ObMemberTableReadOperator final
{
public:
  ObMemberTableReadOperator();
  ~ObMemberTableReadOperator();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObMemberTableDataKey &key,
      const int64_t timeout_us = 10 * 1000 * 1000); //tablet single get
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t timeout_us = 10 * 1000 * 1000); //tablet multi get
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t timeout_us = 10 * 1000 * 1000); //whole scan
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObMemberTableDataType::TYPE &start_type,
      const ObMemberTableDataType::TYPE &end_type,
      const int64_t timeout_us = 10 * 1000 * 1000);

  int get_next(ObMemberTableData &data);
  int get_next(
      ObMemberTableData &data,
      share::SCN &commit_scn);

private:
  int inner_init_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const bool is_get,
      const int64_t timeout_ts);
private:
  bool is_inited_;
  ObObj start_key_obj_[ObMemberTableSchemaDef::ROWKEY_COLUMN_NUM];
  ObObj end_key_obj_[ObMemberTableSchemaDef::ROWKEY_COLUMN_NUM];
  common::ObNewRange new_range_;
  ObMemberTableIterator iter_;
  bool allow_read_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemberTableReadOperator);
};

class ObTransferInfoIterator final
{
public:
  ObTransferInfoIterator();
  ~ObTransferInfoIterator();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObMemberTableDataType::TYPE &start_type,
      const ObMemberTableDataType::TYPE &end_type);
  int get_next(
      ObMemberTableDataKey &key,
      ObTransferDataValue &transfer_value);
private:
  bool is_inited_;
  ObMemberTableReadOperator read_op_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransferInfoIterator);
};


} // end namespace share
} // end namespace oceanbase
#endif
