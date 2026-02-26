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

#pragma once

#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
} // namespace schema
} // namespace share
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
} // namespace sql
namespace transaction
{
class ObTxDesc;
} // namespace transaction
namespace observer
{

class ObTableLoadExecCtx
{
public:
  ObTableLoadExecCtx() : exec_ctx_(nullptr), tx_desc_(nullptr) {}
  virtual ~ObTableLoadExecCtx() = default;
  common::ObIAllocator *get_allocator();
  sql::ObSQLSessionInfo *get_session_info();
  share::schema::ObSchemaGetterGuard *get_schema_guard();
  virtual int check_status();
  bool is_valid() const { return nullptr != exec_ctx_; }
  TO_STRING_KV(KP_(exec_ctx), KP_(tx_desc));
public:
  sql::ObExecContext *exec_ctx_;
  transaction::ObTxDesc *tx_desc_;
};

class ObTableLoadClientExecCtx : public ObTableLoadExecCtx
{
public:
  ObTableLoadClientExecCtx()
    : heartbeat_timeout_us_(0),
      last_heartbeat_time_(0),
      is_detached_(false)
  {
  }
  virtual ~ObTableLoadClientExecCtx() = default;
  virtual int check_status();
  void init_heart_beat(const int64_t heartbeat_timeout_us);
  void heart_beat();
  void detach() { is_detached_ = true; }
  TO_STRING_KV(KP_(exec_ctx), KP_(tx_desc), K_(heartbeat_timeout_us), K_(last_heartbeat_time), K_(is_detached));
private:
  int64_t heartbeat_timeout_us_;
  int64_t last_heartbeat_time_;
  bool is_detached_;
};

} // namespace observer
} // namespace oceanbase
