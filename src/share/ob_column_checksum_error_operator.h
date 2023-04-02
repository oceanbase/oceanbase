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

#ifndef OCEANBASE_SHARE_OB_COLUMN_CHECKSUM_ERROR_OPERATOR_H_
#define OCEANBASE_SHARE_OB_COLUMN_CHECKSUM_ERROR_OPERATOR_H_

#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
struct ObColumnChecksumErrorInfo
{
public:
  ObColumnChecksumErrorInfo() 
    : tenant_id_(OB_INVALID_TENANT_ID), frozen_scn_(), is_global_index_(false),
      data_table_id_(OB_INVALID_ID), index_table_id_(OB_INVALID_ID), data_tablet_id_(),
      index_tablet_id_(), column_id_(OB_INVALID_ID), data_column_checksum_(-1),
      index_column_checksum_(-1) {}
  virtual ~ObColumnChecksumErrorInfo() = default;

  ObColumnChecksumErrorInfo(const uint64_t tenant_id, const SCN &frozen_scn, const bool is_global_index,
    const int64_t data_table_id, const int64_t index_table_id, const common::ObTabletID &data_tablet_id,
    const common::ObTabletID &index_tablet_id)
    : tenant_id_(tenant_id), frozen_scn_(frozen_scn), is_global_index_(is_global_index), 
      data_table_id_(data_table_id), index_table_id_(index_table_id), data_tablet_id_(data_tablet_id),
      index_tablet_id_(index_tablet_id), column_id_(OB_INVALID_ID), data_column_checksum_(-1),
      index_column_checksum_(-1) {}

  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), K_(frozen_scn), K_(is_global_index), K_(data_table_id), K_(index_table_id), 
    K_(data_tablet_id), K_(index_tablet_id), K_(column_id), K_(data_column_checksum), K_(index_column_checksum));
  
  uint64_t tenant_id_;
  SCN frozen_scn_;
  bool is_global_index_;
  int64_t data_table_id_;
  int64_t index_table_id_;
  common::ObTabletID data_tablet_id_;
  common::ObTabletID index_tablet_id_;
  int64_t column_id_;
  int64_t data_column_checksum_;
  int64_t index_column_checksum_;
};

// CRUD operation to __all_column_checksum_error_info table
class ObColumnChecksumErrorOperator
{
public:
  static int insert_column_checksum_err_info(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObColumnChecksumErrorInfo &info);
  // delete records whose 'frozen_scn' < min_frozen_scn
  static int delete_column_checksum_err_info(
      common::ObISQLClient &sql_client, 
      const uint64_t tenant_id,
      const SCN &min_frozen_scn);

private:
  static int insert_column_checksum_err_info_(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObColumnChecksumErrorInfo &info);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_COLUMN_CHECKSUM_ERROR_OPERATOR_H_
