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

#ifndef OB_ALL_VIRTUAL_SERVER_STORAGE_H_
#define OB_ALL_VIRTUAL_SERVER_STORAGE_H_

#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_scanner.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/object_storage/ob_object_storage_struct.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualServerStorage : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualServerStorage();
  virtual ~ObAllVirtualServerStorage();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  virtual int inner_open();

private:
  bool is_valid_timestamp_(const int64_t timestamp) const;

private:
  enum TABLE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    PATH,
    ENDPOINT,
    USED_FOR,
    ZONE,
    STORAGE_ID,
    MAX_IOPS,
    MAX_BANDWIDTH,
    CREATE_TIME,
    OP_ID,
    SUB_OP_ID,
    AUTHORIZATION,
    ENCRYPT_INFO,
    STATE,
    STATE_INFO,
    LAST_CHECK_TIMESTAMP,
    EXTENSION
  };
  struct ObServerStorageInfo
  {
  public:
    ObServerStorageInfo()
      : addr_(),
        path_(),
        endpoint_(),
        used_for_(),
        zone_(),
        storage_id_(OB_INVALID_ID),
        max_iops_(share::OB_INVALID_MAX_IOPS),
        max_bandwidth_(share::OB_INVALID_MAX_BANDWIDTH),
        create_time_(OB_INVALID_TIMESTAMP),
        op_id_(OB_INVALID_ID),
        sub_op_id_(OB_INVALID_ID),
        authorization_(),
        encrypt_info_(),
        state_(),
        state_info_(),
        last_check_timestamp_(OB_INVALID_TIMESTAMP),
        extension_() {}
    ~ObServerStorageInfo() {}
    TO_STRING_KV(K(addr_), K(path_), K(endpoint_), K(used_for_), K(zone_), K(storage_id_), K(max_iops_),
                 K(max_bandwidth_), K(create_time_), K(op_id_), K(sub_op_id_), K(authorization_),
                 K(encrypt_info_), K(state_), K(state_info_), K(last_check_timestamp_), K(extension_));

  public:
    common::ObAddr addr_;
    char path_[share::OB_MAX_BACKUP_DEST_LENGTH];
    char endpoint_[common::OB_MAX_BACKUP_ENDPOINT_LENGTH];
    char used_for_[share::OB_MAX_STORAGE_USED_FOR_LENGTH];
    char zone_[common::MAX_ZONE_LENGTH];
    uint64_t storage_id_;
    int64_t max_iops_;
    int64_t max_bandwidth_;
    int64_t create_time_;
    uint64_t op_id_;
    uint64_t sub_op_id_;
    char authorization_[share::OB_MAX_BACKUP_AUTHORIZATION_LENGTH];
    char encrypt_info_[share::OB_MAX_STORAGE_ENCRYPT_INFO_LENGTH];
    char state_[share::OB_MAX_STORAGE_STATE_LENGTH];
    char state_info_[share::OB_MAX_STORAGE_STATE_INFO_LENGTH];
    int64_t last_check_timestamp_;
    char extension_[common::OB_MAX_BACKUP_EXTENSION_LENGTH];
  };
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  common::ObArray<ObServerStorageInfo> server_storage_info_array_;
  int64_t storage_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerStorage);
};

} // namespace observer
} // namespace oceanbase

#endif /* OB_ALL_VIRTUAL_SERVER_STORAGE_H_*/