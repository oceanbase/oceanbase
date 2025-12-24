/**
 * Copyright (c) 2023 OceanBase
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
#include "storage/direct_load/ob_direct_load_struct.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace sql 
{
class ObInsertStmt;
class ObOptimizerContext;
struct ObDefaultLoadMode
{
#define OB_DEFAULT_LOAD_MODE_DEF(DEF)      \
  DEF(DISABLED, = 0)                      \
  DEF(FULL_DIRECT_WRITE, = 1)             \
  DEF(INC_DIRECT_WRITE, = 2)              \
  DEF(INC_REPLACE_DIRECT_WRITE, = 3)      \
  DEF(MAX_MODE, )

  DECLARE_ENUM(Type, type, OB_DEFAULT_LOAD_MODE_DEF, static);
};

class ObDirectLoadOptimizerCtx
{
public:
  ObDirectLoadOptimizerCtx();
  ~ObDirectLoadOptimizerCtx() = default;
  void reset();
  int init_direct_load_ctx(ObExecContext *exec_ctx, ObLoadDataStmt &stmt);
  int init_direct_load_ctx(const ObInsertStmt &stmt, ObOptimizerContext &optimizer_ctx);
  static int check_direct_load_allow_fallback(const ObDirectLoadOptimizerCtx &optimize_ctx, ObExecContext *exec_ctx, bool &allow_fallback);
  bool can_use_direct_load() const { return can_use_direct_load_; }
  bool use_direct_load() const { return use_direct_load_; }
  void set_use_direct_load() { use_direct_load_ = true; }
  bool is_optimized_by_default_load_mode() { return is_optimized_by_default_load_mode_; }
  bool is_full_direct_load() const { return load_method_ == ObDirectLoadMethod::FULL; }
  bool is_inc_direct_load() const { return load_method_ == ObDirectLoadMethod::INCREMENTAL && insert_mode_ == ObDirectLoadInsertMode::NORMAL; }
  bool is_inc_replace_direct_load() const { return load_method_ == ObDirectLoadMethod::INCREMENTAL && insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE; }
  bool is_insert_overwrite() const { return ObDirectLoadMode::is_insert_overwrite(load_mode_); }
  bool is_insert_into() const { return load_mode_ == ObDirectLoadMode::INSERT_INTO; }
  void set_is_online_gather_statistics(bool is_online_gather_statistics) { is_online_gather_statistics_ = is_online_gather_statistics; }
  void set_online_sample_percent(double online_sample_percent) { online_sample_percent_ = online_sample_percent; }
  TO_STRING_KV(K_(table_id), K_(load_method), K_(insert_mode), K_(load_mode), K_(load_level),
               K_(dup_action), K_(max_error_row_count), K_(need_sort), K_(can_use_direct_load),
               K_(use_direct_load), K_(is_optimized_by_default_load_mode),
               K_(is_online_gather_statistics), K_(online_sample_percent));

private:
  void enable_by_direct_load_hint(const ObDirectLoadHint &hint);
  void enable_by_append_hint();
  void enable_by_config();
  void enable_by_overwrite();
  int check_semantics();
  int check_support_insert_overwrite(const ObGlobalHint &global_hint);
  int check_support_direct_load(ObExecContext *exec_ctx);
public:
  uint64_t table_id_;
  storage::ObDirectLoadMethod::Type load_method_;
  storage::ObDirectLoadInsertMode::Type insert_mode_;
  storage::ObDirectLoadMode::Type load_mode_;
  storage::ObDirectLoadLevel::Type load_level_;
  sql::ObLoadDupActionType dup_action_;
  int64_t max_error_row_count_;
  bool need_sort_;
  bool can_use_direct_load_;
  bool use_direct_load_;
  bool is_optimized_by_default_load_mode_;  // optimized by default load mode
  bool is_online_gather_statistics_;
  double online_sample_percent_;
};

} // namespace sql
} // namespace oceanbase
