/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
