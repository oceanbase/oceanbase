/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_MEMORY_STAT_
#define OCEANBASE_TRANSACTION_OB_TRANS_MEMORY_STAT_

#include "ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{
class ObTransMemoryStat
{
public:
  ObTransMemoryStat() { reset(); }
  virtual ~ObTransMemoryStat() { }
  void reset();
  int init(const common::ObAddr &addr, const char *mod_type, const int64_t alloc_count,
      const int64_t release_count);

  const common::ObAddr &get_addr() const { return addr_; }
  const char *get_mod_type() const { return type_; }
  int64_t get_alloc_count() const { return alloc_count_; }
  int64_t get_release_count() const { return release_count_; }

  TO_STRING_KV(K_(addr), "type", type_, K_(alloc_count), K_(release_count));

public:
  static const int64_t OB_TRANS_MEMORY_MOD_TYPE_SIZE = 64;
private:
  common::ObAddr addr_;
  char type_[OB_TRANS_MEMORY_MOD_TYPE_SIZE];
  int64_t alloc_count_;
  int64_t release_count_;
};

} // transaction
} // oceanbase
#endif // OCEANABAE_TRANSACTION_OB_TRANS_MEMORY_STAT_
