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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_

#include "lib/container/ob_array.h"           // Array
#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"              // LSN
#include "share/ob_define.h"
#include "share/backup/ob_backup_struct.h"     // ObBackupPathString
namespace oceanbase
{
namespace logservice
{
const int64_t MAX_FETCH_LOG_BUF_LEN = 4 * 1024 * 1024L;
const int64_t MAX_LS_FETCH_LOG_TASK_CONCURRENCY = 4;

typedef std::pair<share::ObBackupPathString, share::ObBackupPathString> DirInfo;
typedef common::ObSEArray<std::pair<share::ObBackupPathString, share::ObBackupPathString>, 1> DirArray;

struct ObLogRestoreErrorContext
{
  enum class ErrorType
  {
    FETCH_LOG,
    SUBMIT_LOG,
  };

  ErrorType error_type_;
  int ret_code_;
  share::ObTaskId trace_id_;
  palf::LSN err_lsn_;
  ObLogRestoreErrorContext() { reset(); }
  virtual ~ObLogRestoreErrorContext() { reset(); }
  void reset();
  ObLogRestoreErrorContext &operator=(const ObLogRestoreErrorContext &other);
  TO_STRING_KV(K_(ret_code), K_(trace_id), K_(error_type), K_(err_lsn));
};

struct ObRestoreLogContext
{
  bool seek_done_;
  palf::LSN lsn_;

  ObRestoreLogContext() { reset(); }
  ~ObRestoreLogContext() { reset(); }
  void reset();
  TO_STRING_KV(K_(seek_done), K_(lsn));
};

struct ObLogRestoreSourceTenant final
{
  ObLogRestoreSourceTenant() { reset(); }
  ~ObLogRestoreSourceTenant() { reset(); }
  void reset();
  int set(const ObLogRestoreSourceTenant &other);
  bool is_valid() const;
  int64_t source_cluster_id_;
  uint64_t source_tenant_id_;
  common::ObFixedLengthString<OB_MAX_USER_NAME_LENGTH> user_name_;
  common::ObFixedLengthString<OB_MAX_PASSWORD_LENGTH> user_passwd_;
  bool is_oracle_;
  common::ObArray<common::ObAddr> ip_list_;

  TO_STRING_KV(K_(source_cluster_id),
      K_(source_tenant_id),
      K_(user_name),
      K_(user_passwd),  // TODO remove it
      K_(is_oracle),
      K_(ip_list));
};
} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DEFINE_H_ */
