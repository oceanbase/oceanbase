/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_SERVER_EMPTY_CHECKER_H_
#define OCEANBASE_OBSERVER_OB_SERVER_EMPTY_CHECKER_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace observer
{

class ObServerEmptyCheckInfo
{
public:
  static const int64_t MAX_DIAG_LEN = 256;
  ObServerEmptyCheckInfo() { reset(); }
  void reset()
  {
    has_server_id_ = false;
    has_log_dir_ = false;
    has_wallet_ = false;
    has_data_version_file_ = false;
    server_id_ = OB_INVALID_ID;
  }
  void init(const uint64_t server_id,
            const uint64_t server_id_in_gconf,
            const bool has_log_dir,
            const bool has_wallet,
            const bool has_data_version_file)
  {
    has_server_id_ = is_valid_server_id(server_id) || is_valid_server_id(server_id_in_gconf);
    server_id_ = is_valid_server_id(server_id) ? server_id : server_id_in_gconf;
    has_log_dir_ = has_log_dir;
    has_wallet_ = has_wallet;
    has_data_version_file_ = has_data_version_file;
  }
  bool is_empty() const
  {
    return !has_server_id_ && !has_log_dir_ && !has_wallet_ && !has_data_version_file_;
  }
  bool has_server_id() const { return has_server_id_; }
  bool has_log_dir() const { return has_log_dir_; }
  bool has_wallet() const { return has_wallet_; }
  bool has_data_version_file() const { return has_data_version_file_; }
  uint64_t get_server_id() const { return server_id_; }
  const char *to_cstr(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (has_server_id_) {
      common::databuff_printf(buf, buf_len, pos, "server_id=%lu", server_id_);
    } else {
      common::databuff_printf(buf, buf_len, pos, "server_id=INVALID");
    }
    common::databuff_printf(buf, buf_len, pos,
        ", has_log_dir=%s, has_wallet=%s, has_data_version_file=%s",
        has_log_dir_ ? "TRUE" : "FALSE",
        has_wallet_ ? "TRUE" : "FALSE",
        has_data_version_file_ ? "TRUE" : "FALSE");
    return buf;
  }
  TO_STRING_KV(K_(has_server_id), K_(has_log_dir), K_(has_wallet),
               K_(has_data_version_file), K_(server_id));
private:
  bool has_server_id_;
  bool has_log_dir_;
  bool has_wallet_;
  bool has_data_version_file_;
  uint64_t server_id_;
};

class ObServerEmptyChecker
{
public:
  static int check_server_empty(bool &is_empty);
  static int check_server_empty(bool &is_empty, ObServerEmptyCheckInfo &result);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_SERVER_EMPTY_CHECKER_H_
