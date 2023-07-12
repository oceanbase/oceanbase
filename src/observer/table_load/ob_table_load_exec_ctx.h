// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

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
  ObTableLoadClientExecCtx() : allocator_(nullptr), session_info_(nullptr), timeout_ts_(0) {}
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
};

} // namespace observer
} // namespace oceanbase
