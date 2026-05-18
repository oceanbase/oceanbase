/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_TABLET_OB_DROP_GTT_V2_SESSION_TABLET_ARG_H
#define OCEANBASE_SHARE_TABLET_OB_DROP_GTT_V2_SESSION_TABLET_ARG_H

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{

// RPC argument used by truncate / session-cleanup of Oracle GTT v2 session tablets.
// The originator broadcasts this arg to every alive observer of the tenant; each
// observer with an is_creator_=true entry for any of the (table_id, sequence,
// session_id) tuples performs the actual storage delete in a single inner
// transaction; every observer removes the matching entries from the per-session
// gtt_tablet_info_map_ so that stale caches on non-creator observers are
// invalidated. table_ids_ carries the main table plus its index and lob aux
// tables so that one broadcast atomically replaces the previous per-table
// dispatch.
class ObDropGTTV2SessionTabletArg final
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t DEFAULT_TABLE_ID_COUNT = 4;
  ObDropGTTV2SessionTabletArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      table_ids_(),
      sequence_(INT64_MAX),
      session_id_(common::OB_INVALID_ID)
  {}
  ~ObDropGTTV2SessionTabletArg() = default;
  int init(const uint64_t tenant_id,
           const common::ObIArray<uint64_t> &table_ids,
           const int64_t sequence,
           const uint64_t session_id);
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_
           && !table_ids_.empty()
           && INT64_MAX != sequence_
           && common::OB_INVALID_ID != session_id_;
  }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObIArray<uint64_t> &get_table_ids() const { return table_ids_; }
  int64_t get_sequence() const { return sequence_; }
  uint64_t get_session_id() const { return session_id_; }
  TO_STRING_KV(K_(tenant_id), K_(table_ids), K_(sequence), K_(session_id));
public:
  uint64_t tenant_id_;
  common::ObSEArray<uint64_t, DEFAULT_TABLE_ID_COUNT> table_ids_;
  int64_t sequence_;
  uint64_t session_id_;
};

// RPC result. local_map_hit_ is true if this observer found at least one
// matching entry in some session's gtt_tablet_info_map_ (regardless of creator
// status); the originator uses this together with executed_on_creator_ to
// distinguish a clean no-op from the stale-cache-without-creator scenario.
class ObDropGTTV2SessionTabletRes final
{
  OB_UNIS_VERSION(1);
public:
  ObDropGTTV2SessionTabletRes()
    : executed_on_creator_(false),
      local_map_hit_(false),
      ret_(common::OB_SUCCESS)
  {}
  ~ObDropGTTV2SessionTabletRes() = default;
  bool is_executed_on_creator() const { return executed_on_creator_; }
  bool is_local_map_hit() const { return local_map_hit_; }
  int get_ret() const { return ret_; }
  void set_executed_on_creator(const bool v) { executed_on_creator_ = v; }
  void set_local_map_hit(const bool v) { local_map_hit_ = v; }
  void set_ret(const int ret) { ret_ = ret; }
  TO_STRING_KV(K_(executed_on_creator), K_(local_map_hit), K_(ret));
public:
  bool executed_on_creator_;
  bool local_map_hit_;
  int ret_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_TABLET_OB_DROP_GTT_V2_SESSION_TABLET_ARG_H
