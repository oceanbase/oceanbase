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

#ifndef OCEANBASE_SRC_PL_OB_PL_ANALYZE_FLAG_H_
#define OCEANBASE_SRC_PL_OB_PL_ANALYZE_FLAG_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace pl
{

/**
 * Shared bit-field layout for PL compile unit analysis flags.
 * Used by ObPLCompileUnitAST, ObPLCompileUnit, and ObTriggerArg.
 *
 * ObTriggerArg reuses this type directly, so new flags are automatically available.
 * WARNING: Any modification to this union must keep all consumers in sync.
 */
union ObPLAnalyzeFlag {
  uint64_t flag_;
  struct {
    uint64_t is_no_sql_ : 1;
    uint64_t is_reads_sql_data_ : 1;
    uint64_t is_modifies_sql_data_ : 1;
    uint64_t is_contains_sql_ : 1;
    uint64_t is_wps_ : 1;
    uint64_t is_rps_ : 1;
    uint64_t is_has_sequence_ : 1;
    uint64_t is_has_out_param_ : 1;
    uint64_t is_external_state_ : 1;
    uint64_t is_has_auto_trans_ : 1;  // only for system trigger, has PRAGMA_AUTONOMOUS_TRANSACTION
    uint64_t has_continue_handler_ : 1;
    uint64_t reserved_ : 53;
  };

  ObPLAnalyzeFlag() : flag_(0) {}
  ObPLAnalyzeFlag(uint64_t v) : flag_(v) {}

  uint64_t get_flag() const { return flag_; }
  void set_flag(uint64_t v) { flag_ = v; }

  void set_no_sql() { is_no_sql_ = 1; is_reads_sql_data_ = 0; is_modifies_sql_data_ = 0; is_contains_sql_ = 0; }
  bool is_no_sql() const { return is_no_sql_; }
  void set_reads_sql_data() { is_no_sql_ = 0; is_reads_sql_data_ = 1; is_modifies_sql_data_ = 0; is_contains_sql_ = 0; }
  bool is_reads_sql_data() const { return is_reads_sql_data_; }
  void set_modifies_sql_data() { is_no_sql_ = 0; is_reads_sql_data_ = 0; is_modifies_sql_data_ = 1; is_contains_sql_ = 0; }
  bool is_modifies_sql_data() const { return is_modifies_sql_data_; }
  void set_contains_sql() { is_no_sql_ = 0; is_reads_sql_data_ = 0; is_modifies_sql_data_ = 0; is_contains_sql_ = 1; }
  bool is_contains_sql() const { return is_contains_sql_; }

  void set_wps() { is_wps_ = 1; }
  bool is_wps() const { return is_wps_; }
  void set_rps() { is_rps_ = 1; }
  bool is_rps() const { return is_rps_; }
  void set_has_sequence() { is_has_sequence_ = 1; }
  bool is_has_sequence() const { return is_has_sequence_; }
  void set_has_out_param() { is_has_out_param_ = 1; }
  bool is_has_out_param() const { return is_has_out_param_; }
  void set_external_state() { is_external_state_ = 1; }
  bool is_external_state() const { return is_external_state_; }
  void set_has_auto_trans() { is_has_auto_trans_ = 1; }
  bool is_has_auto_trans() const { return is_has_auto_trans_; }
  void set_has_continue_handler() { has_continue_handler_ = 1; }
  bool has_continue_handler() const { return has_continue_handler_; }

  // bitmask constants for bitwise aggregation (e.g., ObTrigDMLCtDef::is_execute_single_row)
  static constexpr uint64_t FLAG_NO_SQL = (1ULL << 0);
  static constexpr uint64_t FLAG_READS_SQL_DATA = (1ULL << 1);
  static constexpr uint64_t FLAG_MODIFIES_SQL_DATA = (1ULL << 2);
  static constexpr uint64_t FLAG_CONTAINS_SQL = (1ULL << 3);
  static constexpr uint64_t FLAG_WPS = (1ULL << 4);
  static constexpr uint64_t FLAG_RPS = (1ULL << 5);
  static constexpr uint64_t FLAG_HAS_SEQUENCE = (1ULL << 6);
  static constexpr uint64_t FLAG_HAS_OUT_PARAM = (1ULL << 7);
  static constexpr uint64_t FLAG_EXTERNAL_STATE = (1ULL << 8);
  static constexpr uint64_t FLAG_HAS_AUTO_TRANS = (1ULL << 9);
  static constexpr uint64_t FLAG_HAS_CONTINUE_HANDLER = (1ULL << 10);

  TO_STRING_KV(K_(flag));
};

static_assert(sizeof(ObPLAnalyzeFlag) == sizeof(uint64_t),
              "ObPLAnalyzeFlag must be exactly 64 bits");

} // namespace pl

namespace common
{
namespace serialization
{

inline int encode(char *buf, const int64_t buf_len, int64_t &pos,
                  const ::oceanbase::pl::ObPLAnalyzeFlag &val)
{
  // Keep the wire format identical to the historical uint64_t analyze_flag_.flag_ encoding.
  return encode_vi64(buf, buf_len, pos, static_cast<int64_t>(val.get_flag()));
}

inline int decode(const char *buf, const int64_t data_len, int64_t &pos,
                  ::oceanbase::pl::ObPLAnalyzeFlag &val)
{
  int ret = OB_SUCCESS;
  int64_t flag = 0;
  ret = decode_vi64(buf, data_len, pos, &flag);
  if (OB_SUCC(ret)) {
    val.set_flag(static_cast<uint64_t>(flag));
  }
  return ret;
}

inline int64_t encoded_length(const ::oceanbase::pl::ObPLAnalyzeFlag &val)
{
  return encoded_length_vi64(static_cast<int64_t>(val.get_flag()));
}

} // namespace serialization
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_OB_PL_ANALYZE_FLAG_H_
