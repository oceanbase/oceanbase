/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIBOBLOG_STORE_KEY_H_
#define OCEANBASE_LIBOBLOG_STORE_KEY_H_

#include <stdint.h>
#include <string>
#include "lib/utility/ob_macro_utils.h"       // DISALLOW_COPY_AND_ASSIGN

namespace oceanbase
{
namespace liboblog
{
class ObLogStoreKey
{
public:
  ObLogStoreKey();
  ~ObLogStoreKey();
  void reset();
  int init(const int64_t tenant_id,
    const char *participant_key,
    const uint64_t log_id,
    const int32_t log_offset);
  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_id_; }

public:
  int get_key(std::string &key);
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  uint64_t tenant_id_;
  // StorageKey: participant_key_log_id_log_offset
  // Log ID, for redo data
  // 1. non-LOB record corresponding to LogEntry log_id
  // 2. First LogEntry log_id for LOB records
  const char    *participant_key_;
  uint64_t      log_id_;
  int32_t       log_offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStoreKey);
};

}; // end namespace liboblog
}; // end namespace oceanbase
#endif
