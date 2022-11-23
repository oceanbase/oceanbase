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

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/ls/ob_ls_meta_package.h"

#ifndef STORAGE_LOG_STREAM_BACKUP_EXTERN_INFO_MGR_H_
#define STORAGE_LOG_STREAM_BACKUP_EXTERN_INFO_MGR_H_

namespace oceanbase {
namespace backup {

struct ObBackupLSMetaInfo final {
  OB_UNIS_VERSION(1);

public:
  ObBackupLSMetaInfo();
  ~ObBackupLSMetaInfo();
  bool is_valid() const;
  int64_t get_total_serialize_buf_size() const;
  int serialize_to(char *buf, int64_t buf_size, int64_t &pos) const;
  int deserialize_from(char *buf, int64_t buf_size);
  TO_STRING_KV(K_(ls_meta_package));
  ObLSMetaPackage ls_meta_package_;
};

class ObExternLSMetaMgr {
public:
  ObExternLSMetaMgr();
  virtual ~ObExternLSMetaMgr();
  int init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
      const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id);
  int write_ls_meta_info(const ObBackupLSMetaInfo &ls_meta);
  int read_ls_meta_info(ObBackupLSMetaInfo &ls_meta);

private:
  int get_ls_meta_backup_path_(share::ObBackupPath &path);

private:
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  DISALLOW_COPY_AND_ASSIGN(ObExternLSMetaMgr);
};

}  // namespace backup
}  // namespace oceanbase

#endif
