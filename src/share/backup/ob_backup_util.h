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

#ifndef OCEANBASE_SHARE_OB_BACKUP_UTIL_H_
#define OCEANBASE_SHARE_OB_BACKUP_UTIL_H_

#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "common/storage/ob_device_common.h"

namespace oceanbase
{
namespace share
{
class ObBackupUtil
{
public:
  static int parse_str_to_array(const char *str, ObIArray<uint64_t> &array);
  static int get_ls_ids_from_traverse(
      const ObBackupPath &path,
      const common::ObObjectStorageInfo *storage_info,
      ObIArray<ObLSID> &ls_ids);
  static int parse_ls_id(const char *dir_name, int64_t &id_val);
};
}// share
}// oceanbase

#endif /* OCEANBASE_SHARE_OB_BACKUP_UTIL_H_ */