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
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
} // namespace sql
namespace observer
{

class ObTableLoadExecCtx
{
public:
  ObTableLoadExecCtx() = default;
  virtual ~ObTableLoadExecCtx() = default;
  virtual common::ObIAllocator *get_allocator() = 0;
  virtual sql::ObSQLSessionInfo *get_session_info() = 0;
  virtual sql::ObExecContext *get_exec_ctx() = 0; // for sql statistics
  virtual int check_status() = 0;
  virtual bool is_valid() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObTableLoadSqlExecCtx : public ObTableLoadExecCtx
{
public:
  ObTableLoadSqlExecCtx() : exec_ctx_(nullptr) {}
  virtual ~ObTableLoadSqlExecCtx() = default;
  common::ObIAllocator *get_allocator() override;
  sql::ObSQLSessionInfo *get_session_info() override;
  sql::ObExecContext *get_exec_ctx() override { return exec_ctx_; }
  int check_status() override;
  bool is_valid() const override { return nullptr != exec_ctx_; }
  TO_STRING_KV(KP_(exec_ctx));
public:
  sql::ObExecContext *exec_ctx_;
};

class ObTableLoadClientExecCtx : public ObTableLoadExecCtx
{
public:
  ObTableLoadClientExecCtx()
    : allocator_(nullptr),
      session_info_(nullptr),
      timeout_ts_(0),
      heartbeat_timeout_us_(0),
      last_heartbeat_time_(0)
  {
  }
  virtual ~ObTableLoadClientExecCtx() = default;
  common::ObIAllocator *get_allocator() override { return allocator_; }
  sql::ObSQLSessionInfo *get_session_info() override { return session_info_; }
  sql::ObExecContext *get_exec_ctx() override { return nullptr; } // not support sql statistics
  int check_status() override;
  bool is_valid() const override { return nullptr != allocator_ && nullptr != session_info_; }
  TO_STRING_KV(KP_(allocator), KP_(session_info), K_(timeout_ts));
public:
  common::ObIAllocator *allocator_;
  sql::ObSQLSessionInfo *session_info_;
  int64_t timeout_ts_;
  int64_t heartbeat_timeout_us_;
  int64_t last_heartbeat_time_;
};

} // namespace observer
} // namespace oceanbase
