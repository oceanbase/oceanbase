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

#ifndef OCEANBASE_SHARE_OB_FLASHBACK_STANDBY_LOG_STRUCT_H_
#define OCEANBASE_SHARE_OB_FLASHBACK_STANDBY_LOG_STRUCT_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include <share/scn.h>

namespace oceanbase
{
namespace share
{
class ObFlashbackStandbyLogArg
{
public:
  ObFlashbackStandbyLogArg() : tenant_id_(OB_INVALID_TENANT_ID), flashback_log_scn_() {};
  ~ObFlashbackStandbyLogArg() {};
  int init(const uint64_t tenant_id, const SCN &flashback_log_scn);
  bool is_valid() const;
  int assign(const ObFlashbackStandbyLogArg &other);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  const SCN &get_flashback_log_scn() const { return flashback_log_scn_; }
  TO_STRING_KV(K_(tenant_id), K_(flashback_log_scn));
private:
  uint64_t tenant_id_;
  SCN flashback_log_scn_;
};

class ObClearFetchedLogCacheArg
{
OB_UNIS_VERSION(1);
public:
  ObClearFetchedLogCacheArg() : tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObClearFetchedLogCacheArg() {}
  int init(const uint64_t tenant_id);
  bool is_valid() const;
  int assign(const ObClearFetchedLogCacheArg &other);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  TO_STRING_KV(K_(tenant_id));
private:
  uint64_t tenant_id_;
};
class ObClearFetchedLogCacheRes
{
OB_UNIS_VERSION(1);
public:
  ObClearFetchedLogCacheRes() : tenant_id_(OB_INVALID_TENANT_ID), addr_() {}
  ~ObClearFetchedLogCacheRes() {}
  int init(const uint64_t tenant_id, const ObAddr addr);
  bool is_valid() const;
  int assign(const ObClearFetchedLogCacheRes &other);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObAddr& get_addr() const { return addr_; }
  TO_STRING_KV(K_(tenant_id), K_(addr));
private:
  uint64_t tenant_id_;
  ObAddr addr_;
};
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_OB_FLASHBACK_STANDBY_LOG_STRUCT_H_