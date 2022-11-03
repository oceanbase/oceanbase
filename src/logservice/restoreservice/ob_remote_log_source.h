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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_SOURCE_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_SOURCE_H_

#include "lib/container/ob_se_array.h"     // SEArray
#include "lib/net/ob_addr.h"               // ObAddr
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"
#include "share/backup/ob_backup_struct.h"     // ObBackupPathString
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/restore/ob_log_archive_source.h"  // ObLogArchiveSourceType
#include "ob_log_archive_piece_mgr.h"           // ObLogArchivePieceContext
#include "ob_log_restore_define.h"              // ObLogRestoreErrorContext
namespace oceanbase
{
namespace logservice
{
using oceanbase::share::ObLogArchiveSourceType;
//using oceanbase::share::DirArray;
typedef common::ObSEArray<std::pair<share::ObBackupPathString, share::ObBackupPathString>, 1> DirArray;
// The management of remote log source, three types are supported, LOCATION/SERVICE/RAWPATH
class ObRemoteLogParent
{
public:
  explicit ObRemoteLogParent(const ObLogArchiveSourceType &type, const share::ObLSID &ls_id);
  virtual ~ObRemoteLogParent();

public:
  virtual int deep_copy_to(ObRemoteLogParent &other) = 0;
  virtual bool is_valid() const = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  virtual int update_locate_info(ObRemoteLogParent &source) = 0;
  bool to_end() const { return to_end_; }
  void set_to_end(const bool is_to_end, const int64_t timestamp);
  void get_end_ts(int64_t &timestamp) const { timestamp = end_fetch_log_ts_;}
  void get_upper_limit_ts(int64_t &timestamp) const { timestamp = upper_limit_ts_; }
  ObLogArchiveSourceType get_source_type() const { return type_; }
  const char *get_source_type_str(const ObLogArchiveSourceType &type) const;
  void mark_error(share::ObTaskId &trace_id, const int ret_code);
  void get_error_info(share::ObTaskId &trace_id, int &ret_code, bool &error_exist);

protected:
  void base_copy_to_(ObRemoteLogParent &other);
  bool is_valid_() const;

protected:
  share::ObLSID ls_id_;
  ObLogArchiveSourceType type_;
  int64_t upper_limit_ts_;
  bool to_end_;
  int64_t end_fetch_log_ts_;
  palf::LSN end_lsn_;

  ObLogRestoreErrorContext error_context_;    // 记录该source的错误信息, 仅leader有效
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogParent);
};

class ObRemoteSerivceParent : public ObRemoteLogParent
{
public:
  ObRemoteSerivceParent(const share::ObLSID &ls_id);
  virtual ~ObRemoteSerivceParent();

public:
  int set(const ObAddr &addr, const int64_t end_log_ts);
  void get(ObAddr &addr, int64_t &end_log_ts);
  int deep_copy_to(ObRemoteLogParent &other) override;
  bool is_valid() const override;
  int update_locate_info(ObRemoteLogParent &source) override { UNUSED(source); return OB_SUCCESS; }
  TO_STRING_KV("ObRemoteLogParent", get_source_type_str(type_), K_(ls_id), K_(server),
      K_(upper_limit_ts), K_(to_end), K_(end_fetch_log_ts), K_(end_lsn));

private:
  ObAddr server_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteSerivceParent);
};

class ObRemoteLocationParent : public ObRemoteLogParent
{
public:
  ObRemoteLocationParent(const share::ObLSID &ls_id);
  virtual ~ObRemoteLocationParent();

public:
  void get(share::ObBackupDest *&dest, ObLogArchivePieceContext *&piece_context, int64_t &end_log_ts);
  int set(const share::ObBackupDest &dest, const int64_t end_log_ts);
  int deep_copy_to(ObRemoteLogParent &other) override;
  bool is_valid() const override;
  int update_locate_info(ObRemoteLogParent &source) override;

  TO_STRING_KV("ObRemoteLogParent", get_source_type_str(type_), K_(ls_id), K_(root_path), K_(piece_context),
      K_(upper_limit_ts), K_(to_end), K_(end_fetch_log_ts), K_(end_lsn));

private:
  share::ObBackupDest root_path_;   // uri & storage_info
  ObLogArchivePieceContext piece_context_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLocationParent);
};

class ObRemoteRawPathParent : public ObRemoteLogParent
{
public:
  ObRemoteRawPathParent(const share::ObLSID &ls_id);
  virtual ~ObRemoteRawPathParent();

public:
  void get(DirArray &array, int64_t &end_log_ts);
  int set(const int64_t cluster_id, const ObAddr &addr);
  int deep_copy_to(ObRemoteLogParent &other) override;
  bool is_valid() const override;
  int set(DirArray &array, const int64_t end_log_ts);
  int update_locate_info(ObRemoteLogParent &source) override { UNUSED(source); return OB_NOT_SUPPORTED; }
  void get_locate_info(int64_t &piece_index, int64_t &min_file_id, int64_t &max_file_id) const;

  TO_STRING_KV("ObRemoteLogParent", get_source_type_str(type_), K_(ls_id),
      K_(upper_limit_ts), K_(to_end), K_(end_fetch_log_ts), K_(end_lsn),
      K_(paths), K_(piece_index), K_(min_file_id), K_(max_file_id));

private:
  DirArray paths_;

  int64_t piece_index_;
  int64_t min_file_id_;
  int64_t max_file_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteRawPathParent);
};

class ObRemoteSourceGuard
{
public:
  ObRemoteSourceGuard();
  ~ObRemoteSourceGuard();

  ObRemoteLogParent *get_source() const { return source_; }
  int set_source(ObRemoteLogParent *source);
private:
  ObRemoteLogParent *source_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteSourceGuard);
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_SOURCE_H_*/
