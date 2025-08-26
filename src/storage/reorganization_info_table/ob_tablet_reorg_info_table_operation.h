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

#ifndef OCEANBASE_TABLE_TABLET_REORGANIZATION_INFO_TABLE_OPERATOR
#define OCEANBASE_TABLE_TABLET_REORGANIZATION_INFO_TABLE_OPERATOR

#include "ob_tablet_reorg_info_table.h"
#include "ob_tablet_reorg_info_table_iterator.h"
#include "ob_tablet_reorg_info_table_schema_helper.h"

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

class ObTabletReorgInfoTableDataGenerator final
{
public:
  ObTabletReorgInfoTableDataGenerator() {}
  ~ObTabletReorgInfoTableDataGenerator() {}
  static int gen_transfer_reorg_info_data(
      const common::ObTabletID &tablet_id,
      const share::SCN &reorganization_scn,
      const ObTabletStatus &tablet_status,
      const share::ObLSID &relative_ls_id,
      const int64_t transfer_seq,
      const share::SCN &transfer_scn,
      const share::SCN &src_reorganization_scn,
      common::ObIArray<ObTabletReorgInfoData> &reorg_info_data);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletReorgInfoTableDataGenerator);
};

// operator for tablet reorg info table
class ObTabletReorgInfoTableWriteOperator final
{
public:
  ObTabletReorgInfoTableWriteOperator();
  ~ObTabletReorgInfoTableWriteOperator();
  int init(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);
  int insert_row(
      const ObTabletReorgInfoData &member_table_data);
  int insert_rows(
      const common::ObIArray<ObTabletReorgInfoData> &member_table_data);
private:
  int inner_insert_row_(
      const ObTabletReorgInfoData &data,
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
  DISALLOW_COPY_AND_ASSIGN(ObTabletReorgInfoTableWriteOperator);
};

class ObTabletReorgInfoTableReadOperator final
{
public:
  ObTabletReorgInfoTableReadOperator();
  ~ObTabletReorgInfoTableReadOperator();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObTabletReorgInfoDataKey &key,
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
      const ObTabletReorgInfoDataType::TYPE &start_type,
      const ObTabletReorgInfoDataType::TYPE &end_type,
      const int64_t timeout_us = 10 * 1000 * 1000);

  int get_next(ObTabletReorgInfoData &data);
  int get_next(
      ObTabletReorgInfoData &data,
      share::SCN &commit_scn);

private:
  int inner_init_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const bool is_get,
      const int64_t timeout_ts);
private:
  bool is_inited_;
  ObObj start_key_obj_[ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM];
  ObObj end_key_obj_[ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM];
  common::ObNewRange new_range_;
  ObTabletReorgInfoTableIterator iter_;
  bool allow_read_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletReorgInfoTableReadOperator);
};

class ObTransferInfoIterator final
{
public:
  ObTransferInfoIterator();
  ~ObTransferInfoIterator();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObTabletReorgInfoDataType::TYPE &start_type,
      const ObTabletReorgInfoDataType::TYPE &end_type);
  int get_next(
      ObTabletReorgInfoDataKey &key,
      ObTransferDataValue &transfer_value);
private:
  bool is_inited_;
  ObTabletReorgInfoTableReadOperator read_op_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransferInfoIterator);
};


} // end namespace share
} // end namespace oceanbase
#endif
