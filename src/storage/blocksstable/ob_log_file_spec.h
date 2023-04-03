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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOG_FILE_SPEC_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOG_FILE_SPEC_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObLogFileSpec
{
  const char *retry_write_policy_;
  const char *log_create_policy_;
  const char *log_write_policy_;

  ObLogFileSpec()
    : retry_write_policy_(nullptr),
      log_create_policy_(nullptr),
      log_write_policy_(nullptr)
  {
  }

  void reset()
  {
    retry_write_policy_ = nullptr;
    log_create_policy_ = nullptr;
    log_write_policy_ = nullptr;
  }

  bool is_valid() const
  {
    return nullptr != retry_write_policy_
        && nullptr != log_create_policy_
        && nullptr != log_write_policy_;
  }

  TO_STRING_KV(K_(retry_write_policy), K_(log_create_policy), K_(log_write_policy));
};
} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOG_FILE_SPEC_H_
