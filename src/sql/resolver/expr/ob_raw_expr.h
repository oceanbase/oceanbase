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

#ifndef OCEANBASE_SQL_RESOLVER_EXPR_RAW_EXPR_
#define OCEANBASE_SQL_RESOLVER_EXPR_RAW_EXPR_
#include "share/ob_define.h"
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/list/ob_obj_store.h"
#include "lib/rc/context.h"
#include "common/ob_range.h"
#include "common/ob_accuracy.h"
#include "objit/expr/ob_iraw_expr.h"
#include "objit/expr/ob_const_expr.h"
#include "objit/expr/ob_var_expr.h"
#include "objit/expr/ob_op_expr.h"
#include "objit/expr/ob_column_ref_expr.h"
#include "objit/expr/ob_case_op_expr.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/expr/ob_expr_info_flag.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/schema/ob_udf.h"
#include "lib/worker.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "pl/ob_pl_type.h"
#include "share/schema/ob_trigger_info.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/resolver/dml/ob_raw_expr_sets.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace pl
{
class ObPLCodeGenerator;
}
namespace sql
{
class ObStmt;
class ObCallProcedureInfo;
class ObSQLSessionInfo;
class ObExprOperator;
class ObRawExprFactory;
class ObIRawExprCopier;
class ObSelectStmt;
class ObRTDatumArith;
class ObLogicalOperator;
extern ObRawExpr *USELESS_POINTER;

// If is_stack_overflow is true, the printing will not continue
#ifndef DEFINE_VIRTUAL_TO_STRING_CHECK_STACK_OVERFLOW
#define DEFINE_VIRTUAL_TO_STRING_CHECK_STACK_OVERFLOW(body) DECLARE_VIRTUAL_TO_STRING     \
  {                                                                                       \
    int ret = common::OB_SUCCESS;                                                         \
    int64_t pos = 0;                                                                      \
    bool is_stack_overflow = false;                                                       \
    if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {                               \
      SQL_RESV_LOG(WARN, "failed to check stack overflow", K(ret), K(is_stack_overflow)); \
    } else if (is_stack_overflow) {                                                       \
      SQL_RESV_LOG(DEBUG, "too deep recursive", K(ret), K(is_stack_overflow));            \
    } else {                                                                              \
      J_OBJ_START();                                                                      \
      body;                                                                               \
      J_OBJ_END();                                                                        \
    }                                                                                     \
    return pos;                                                                           \
  }

#define VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(args...) DEFINE_VIRTUAL_TO_STRING_CHECK_STACK_OVERFLOW(J_KV(args))
#endif

#define IS_SPATIAL_OP(op) \
  (((op) == T_FUN_SYS_ST_INTERSECTS) \
    || ((op) == T_FUN_SYS_ST_COVERS) \
    || ((op) == T_FUN_SYS_ST_DWITHIN) \
    || ((op) == T_FUN_SYS_ST_WITHIN) \
    || ((op) == T_FUN_SYS_ST_CONTAINS)) \

#define IS_MYSQL_GEO_OP(op) \
  (((op) == T_FUN_SYS_ST_GEOMFROMTEXT) \
    || ((op) == T_FUN_SYS_ST_INTERSECTION) \
    || ((op) == T_FUN_SYS_ST_AREA) \
    || ((op) == T_FUN_SYS_ST_INTERSECTS) \
    || ((op) == T_FUN_SYS_ST_X) \
    || ((op) == T_FUN_SYS_ST_Y) \
    || ((op) == T_FUN_SYS_ST_LATITUDE) \
    || ((op) == T_FUN_SYS_ST_LONGITUDE) \
    || ((op) == T_FUN_SYS_ST_TRANSFORM) \
    || ((op) == T_FUN_SYS_POINT) \
    || ((op) == T_FUN_SYS_LINESTRING) \
    || ((op) == T_FUN_SYS_MULTIPOINT) \
    || ((op) == T_FUN_SYS_MULTILINESTRING) \
    || ((op) == T_FUN_SYS_POLYGON) \
    || ((op) == T_FUN_SYS_MULTIPOLYGON) \
    || ((op) == T_FUN_SYS_GEOMCOLLECTION) \
    || ((op) == T_FUN_SYS_ST_COVERS) \
    || ((op) == T_FUN_SYS_ST_ASTEXT) \
    || ((op) == T_FUN_SYS_ST_BUFFER_STRATEGY) \
    || ((op) == T_FUN_SYS_ST_BUFFER) \
    || ((op) == T_FUN_SYS_SPATIAL_CELLID) \
    || ((op) == T_FUN_SYS_SPATIAL_MBR) \
    || ((op) == T_FUN_SYS_ST_GEOMFROMEWKB) \
    || ((op) == T_FUN_SYS_ST_GEOMFROMWKB) \
    || ((op) == T_FUN_SYS_ST_GEOMETRYFROMWKB) \
    || ((op) == T_FUN_SYS_ST_GEOMFROMEWKT) \
    || ((op) == T_FUN_SYS_ST_SRID) \
    || ((op) == T_FUN_SYS_ST_ASWKT) \
    || ((op) == T_FUN_SYS_ST_DISTANCE) \
    || ((op) == T_FUN_SYS_ST_GEOMETRYFROMTEXT) \
    || ((op) == T_FUN_SYS_ST_ISVALID) \
    || ((op) == T_FUN_SYS_ST_ASWKB) \
    || ((op) == T_FUN_SYS_ST_ASBINARY) \
    || ((op) == T_FUN_SYS_ST_DISTANCE_SPHERE) \
    || ((op) == T_FUN_SYS_ST_DWITHIN) \
    || ((op) == T_FUN_SYS_ST_WITHIN) \
    || ((op) == T_FUN_SYS_ST_CONTAINS)) \


#define IS_PRIV_GEO_OP(op) \
  (((op) == T_FUN_SYS_PRIV_ST_BUFFER) \
    || ((op) == T_FUN_SYS_PRIV_ST_ASEWKB) \
    || ((op) == T_FUN_SYS_PRIV_ST_TRANSFORM) \
    || ((op) == T_FUN_SYS_PRIV_ST_SETSRID) \
    || ((op) == T_FUN_SYS_PRIV_ST_BESTSRID) \
    || ((op) == T_FUN_SYS_PRIV_ST_POINT) \
    || ((op) == T_FUN_SYS_PRIV_ST_GEOGFROMTEXT) \
    || ((op) == T_FUN_SYS_PRIV_ST_GEOGRAPHYFROMTEXT) \
    || ((op) == T_FUN_SYS_PRIV_ST_ASEWKT)) \

#define IS_GEO_OP(op) ((IS_MYSQL_GEO_OP(op)) || IS_PRIV_GEO_OP(op))

#define IS_XML_OP(op) \
  (((op) == T_FUN_SYS_XML_ELEMENT) \
    || ((op) == T_FUN_SYS_XMLPARSE)) \

#define IS_SPATIAL_EXPR(op) \
  ((op) >= T_FUN_SYS_ST_LONGITUDE && (op) <= T_FUN_SYS_ST_LATITUDE)

// ObSqlBitSet is a simple bitset, in order to avoid memory exposure
// ObBitSet is too large just for a simple bitset
const static int64_t DEFAULT_SQL_BITSET_SIZE = 32;
template<int64_t N = DEFAULT_SQL_BITSET_SIZE,
         typename FlagType = int64_t,
         bool auto_free = false>
class ObSqlBitSet
{
public:
  typedef uint32_t BitSetWord;

  ObSqlBitSet()
    :block_allocator_(NULL), bit_set_word_array_(NULL), desc_()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(init_block_allocator())) {
      desc_.init_errcode_ = ret;
      SQL_RESV_LOG(WARN, "failed to init block allocator", K(ret));
    } else if (OB_ISNULL(block_allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      desc_.init_errcode_ = ret;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret));
    } else {
      int64_t words_size = sizeof(BitSetWord) * MAX_BITSETWORD;
      if (OB_ISNULL(bit_set_word_array_ = (BitSetWord *)block_allocator_->alloc(words_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        desc_.init_errcode_ = ret;
        SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        MEMSET(bit_set_word_array_, 0, words_size);
        desc_.cap_ = static_cast<int16_t>(MAX_BITSETWORD);
        desc_.len_ = 0;
        desc_.inited_ = true;
      }
    }
  }

  ObSqlBitSet(const ObSqlBitSet<N, FlagType, auto_free> &other)
    : block_allocator_(NULL), bit_set_word_array_(NULL), desc_()
  {
    int ret = OB_SUCCESS;
    if (!other.is_valid()) {
      desc_.init_errcode_ = other.desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "other not initied", K(other.desc_.init_errcode_));
    } else if (OB_FAIL(init_block_allocator())) {
      desc_.init_errcode_ = ret;
      SQL_RESV_LOG(WARN, "failed to init block allocator", K(ret));
    } else if (OB_ISNULL(block_allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      desc_.init_errcode_ = ret;
      SQL_RESV_LOG(WARN, "block_allocator_ is null", K(ret));
    } else {
      int64_t cap = other.bitset_word_count() * 2;
      if (cap <= 0) {
        cap = MAX_BITSETWORD;
      }
      int64_t words_size = sizeof(BitSetWord) * cap;
      if (OB_ISNULL(bit_set_word_array_ = (BitSetWord *)block_allocator_->alloc(words_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        desc_.init_errcode_ = ret;
        SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        MEMSET(bit_set_word_array_, 0, words_size);
        for (int64_t i = 0; i < other.bitset_word_count(); i++) {
          bit_set_word_array_[i] = other.get_bitset_word(i);
        }
        desc_.len_ = static_cast<int16_t>(other.bitset_word_count());
        desc_.cap_ = static_cast<int16_t>(cap);
        desc_.inited_ = true;
      }
    }
  }
  explicit ObSqlBitSet(const int64_t bit_size)
    :block_allocator_(NULL), bit_set_word_array_(NULL), desc_()
  {
    int ret = OB_SUCCESS;
    if (bit_size < 0) {
      ret = OB_INVALID_ARGUMENT;
      desc_.init_errcode_ = ret;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_FAIL(init_block_allocator())) {
      desc_.init_errcode_ = ret;
      SQL_RESV_LOG(WARN, "failed to init block allocator", K(ret));
    } else {
      int64_t bitset_word_cnt = (bit_size <= N ? MAX_BITSETWORD
                                 : ((bit_size - 1) / PER_BITSETWORD_BITS + 1));
      int64_t words_size = sizeof(BitSetWord) * bitset_word_cnt;
      if (OB_ISNULL(bit_set_word_array_ = (BitSetWord *)block_allocator_->alloc(words_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        desc_.init_errcode_ = ret;
        SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        MEMSET(bit_set_word_array_, 0, words_size);
        desc_.len_ = 0;
        desc_.cap_ = static_cast<int16_t>(bitset_word_cnt);
        desc_.inited_ = true;
      }
    }
  }

  ~ObSqlBitSet()
  {
    destroy();
  }

  void reset()
  {
    reuse();
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    for (int64_t i = 0; i < desc_.len_; i++) {
      hash_val = murmurhash(&bit_set_word_array_[i],
                            sizeof(bit_set_word_array_[i]),
                            hash_val);
    }
    return hash_val;
  }

  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  void reuse()
  {
    if (is_valid()) {
      if (NULL != bit_set_word_array_ && desc_.len_ > 0) {
        MEMSET(bit_set_word_array_, 0, desc_.len_ * sizeof(BitSetWord));
      }
      desc_.len_ = 0;
    }
  }

  void destroy()
  {
    if (is_valid()) {
      if (auto_free) {
        // auto_free = true, context allocator is used,
        // free memory is just useless
        // do nothing
      } else {
        // free memory
        if (NULL != block_allocator_ && NULL != bit_set_word_array_) {
          block_allocator_->free(bit_set_word_array_);
        }
        if (NULL != block_allocator_) {
          block_allocator_->reset();
          ob_free(block_allocator_);
        }
        block_allocator_ = NULL;
        bit_set_word_array_ = NULL;
      }
      desc_.len_ = 0;
      desc_.cap_ = 0;
      desc_.inited_ = false;
    }
  }

  int64_t bitset_word_count() const { return static_cast<int64_t>(desc_.len_); }
  int64_t bit_count() const { return static_cast<int64_t>(desc_.len_) * PER_BITSETWORD_BITS; }
  bool is_empty() const { return 0 == num_members(); }
  bool is_valid() const { return desc_.inited_; }
  int get_init_err() const { return desc_.inited_ ? OB_SUCCESS : desc_.init_errcode_;  }
  void clear_all()
  {
    if (!is_valid()) {
      // do nothing
    } else {
      MEMSET(bit_set_word_array_, 0, desc_.len_ * sizeof(BitSetWord));
    }
  }
  BitSetWord get_bitset_word(int64_t index) const
  {
    BitSetWord word = 0;
    if (!is_valid()) {
      SQL_RESV_LOG_RET(WARN, OB_NOT_INIT, "not inited", K(desc_.init_errcode_));
    } else if (index < 0 || index >= desc_.len_) {
      SQL_RESV_LOG_RET(WARN, OB_ERR_UNEXPECTED, "bitmap word index exceeds scope", K(index), K(desc_.len_));
    } else {
      word = bit_set_word_array_[index];
    }
    return word;
  }

  int64_t num_members() const
  {
    int64_t num = 0;
    BitSetWord word = 0;
    if (!is_valid()) {
      // do nothing
    } else {
      for (int64_t i = 0; i < desc_.len_; i++) {
        word = bit_set_word_array_[i];
        if (0 == word) {
          // do nothing
        } else {
          word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
          word = (word & UINT32_C(0x33333333)) + ((word >> 2) & UINT32_C(0x33333333));
          word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 4) & UINT32_C(0x0f0f0f0f));
          word = (word & UINT32_C(0x00ff00ff)) + ((word >> 8) & UINT32_C(0x00ff00ff));
          word = (word & UINT32_C(0x0000ffff)) + ((word >> 16) & UINT32_C(0x0000ffff));
          num += (int64_t)word;
        }
      }
    }
    return num;
  }
  bool has_member(int64_t index) const
  {
    bool bool_ret = false;
    if (!is_valid()) {
      // do nothing
    } else if (OB_UNLIKELY(index < 0)) {
      SQL_RESV_LOG_RET(WARN, OB_INVALID_ARGUMENT, "negative bitmap member not allowed");
    } else {
      int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
      if (pos >= desc_.len_) {
        // dp nothing
      } else {
        bool_ret = (bit_set_word_array_[pos]
                    & ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK))) != 0;
      }
    }
    return bool_ret;
  }

  int add_member(int64_t index)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "got init error", K(desc_.init_errcode_));
    } else if (OB_UNLIKELY (index < 0)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "negative bitmap member not allowed", K(ret), K(index));
    } else {
      int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
      if (OB_UNLIKELY(pos >= desc_.cap_)) {
        int64_t new_word_cnt = pos * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buf", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (pos >= desc_.len_) {
          desc_.len_ = static_cast<int16_t>(pos) + 1;
        }
        bit_set_word_array_[pos] |= ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
      }
    }
    return ret;
  }
  int del_member(int64_t index)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "got init error", K(desc_.init_errcode_));
    } else if (OB_UNLIKELY(index < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "negative bitmap member not allowed", K(ret), K(index));
    } else {
      int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
      if (OB_UNLIKELY(pos >= desc_.len_)) {
        // do nothing
      } else {
        bit_set_word_array_[pos] &= ~((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
      }
    }
    return ret;
  }

  int do_mask(int64_t begin_index, int64_t end_index)
  {
    int ret = OB_SUCCESS;
    int64_t max_bit_count = static_cast<int64_t>(desc_.len_) * PER_BITSETWORD_BITS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "got init error", K(desc_.init_errcode_));
    } else if (begin_index < 0 || begin_index >= max_bit_count
               || end_index < 0 || end_index >= max_bit_count) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(begin_index), K(end_index), K(max_bit_count));
    } else {
      int64_t begin_word = begin_index / PER_BITSETWORD_BITS;
      int64_t end_word = end_index / PER_BITSETWORD_BITS;
      int64_t begin_pos = begin_index % PER_BITSETWORD_BITS;
      int64_t end_pos = end_index % PER_BITSETWORD_BITS;

      for (int64_t i = 0; i < begin_word; i++) {
        bit_set_word_array_[i] = 0;
      }
      for (int64_t i = desc_.len_ - 1; i > end_word; i--) {
        bit_set_word_array_[i] = 0;
      }
      for (int64_t i = 0; i < begin_pos; i++) {
        bit_set_word_array_[begin_word] &= ~((BitSetWord) 1 << i);
      }
      for (int64_t i = 1 + end_pos; i < PER_BITSETWORD_BITS; i++) {
        bit_set_word_array_[end_word] &= ~((BitSetWord) 1 << i);
      }
    }
    return ret;
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  int add_members(const ObSqlBitSet<M, FlagType2, auto_free2> &other)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "got init error", K(desc_.init_errcode_));
    } else if (OB_ISNULL(bit_set_word_array_)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(bit_set_word_array_));
    } else {
      int64_t this_count = desc_.len_;
      int64_t that_count = other.bitset_word_count();

      if (this_count < that_count) {
        if (desc_.cap_ >= that_count) {
          // do nothing
        } else {
          int64_t new_word_cnt = that_count * 2;
          if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
            SQL_RESV_LOG(WARN, "failed to alloc new buffer", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && that_count >= desc_.len_) {
        desc_.len_ = static_cast<int16_t>(that_count);
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        for (int64_t i = 0; i < that_count; i++) {
          bit_set_word_array_[i] |= other.get_bitset_word(i);
        }
      }
    }
    return ret;
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  int add_members2(const ObSqlBitSet<M, FlagType2, auto_free2> &other)
  {
    return add_members(other);
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  int del_members(const ObSqlBitSet<M, FlagType2, auto_free2> &other)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "got init error", K(desc_.init_errcode_));
    } else {
      for (int64_t i = 0; i < desc_.len_; i++) {
        bit_set_word_array_[i] &= ~(other.get_bitset_word(i));
      }
    }
    return ret;
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  int del_members2(const ObSqlBitSet<M, FlagType2, auto_free2> &other)
  {
    return del_members(other);
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  int intersect_members(const ObSqlBitSet<M, FlagType2, auto_free2> &other)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else {
      for (int64_t i = 0; i < desc_.len_; i++) {
        bit_set_word_array_[i] &= (other.get_bitset_word(i));
      }
    }
    return ret;
  }

  int reserve_first() {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = OB_NOT_INIT;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else {
      bool find = false;
      int64_t num = 0;
      for (int64_t i = 0; i < desc_.len_; i++) {
        BitSetWord& word = bit_set_word_array_[i];
        if (word == 0) {
          // do nothing
        } else if (find) {
          word &= 0;
        } else {
          for (int64_t j = 0; !find && j < PER_BITSETWORD_BITS; j ++) {
            if (word & ((BitSetWord) 1 << j)) {
              word &= ((BitSetWord) 1 << j);
              find = true;
            }
          }
        }
      }
    }
    return ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_subset(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    bool bool_ret = true;
    if (!is_valid()) {
      // do nothing
    } else if (is_empty()) {
      bool_ret = true;
    } else if (other.is_empty()) {
      bool_ret = false;
    } else if (desc_.len_ > other.bitset_word_count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < desc_.len_; i++) {
        if ((bit_set_word_array_[i] & (~(other.get_bitset_word(i)))) != 0) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_subset2(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    return is_subset(other);
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_superset(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    bool bool_ret = false;
    if (!is_valid()) {
      // do nothing
    } else if (is_empty()) {
      bool_ret = false;
    } else if (other.is_empty()) {
      bool_ret = true;
    } else if (desc_.len_ < other.bitset_word_count()) {
      bool_ret = false;
    } else {
      bool_ret = true;
      for (int64_t i = 0; bool_ret && i < other.bitset_word_count(); i++) {
        if (((~bit_set_word_array_[i]) & other.get_bitset_word(i)) != 0) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  template <int64_t M, typename FlagType2, bool auto_free2>
  bool is_superset2(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    return is_superset(other);
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  bool overlap(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    bool bool_ret = false;
    if (!is_valid()) {
      // do nothing
    } else {
      int64_t min_bitset_word_count = std::min(static_cast<int64_t>(desc_.len_), other.bitset_word_count());
      for (int64_t i = 0; !bool_ret && i < min_bitset_word_count; i++) {
        if ((bit_set_word_array_[i] & (other.get_bitset_word(i))) != 0) {
          bool_ret = true;
        }
      }
    }
    return bool_ret;
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  bool overlap2(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    return overlap(other);
  }

  int to_array(ObIArray<int64_t> &arr) const
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "got init error", K(desc_.init_errcode_));
    } else {
      arr.reuse();
      int64_t num = num_members();
      int64_t count = 0;
      int64_t max_bit_count = desc_.len_ * PER_BITSETWORD_BITS;
      for (int64_t i = 0; OB_SUCC(ret) && count < num && i < max_bit_count; i++) {
        if (has_member(i)) {
          if (OB_FAIL(arr.push_back(i))) {
            SQL_RESV_LOG(WARN, "failed to push back element", K(ret));
          } else {
            count++;
          }
        }
      } // for end
    }
    return ret;
  }

  template<int64_t M, typename FlagType2, bool auto_free2>
  bool equal(const ObSqlBitSet<M, FlagType2, auto_free2> &other) const
  {
    bool bool_ret = true;
    if (!is_valid() || !other.is_valid()) {
      bool_ret = false;
    } else if (bitset_word_count() != other.bitset_word_count()) {
      bool_ret = false;
    } else {
      for (int64_t i = 0; bool_ret && i < bitset_word_count(); i++) {
        if (get_bitset_word(i) != other.get_bitset_word(i)) {
          bool_ret = false;
        }
      }
    }
    return bool_ret;
  }

  bool operator==(const ObSqlBitSet<N, FlagType, auto_free> &other) const
  {
    return this->equal(other);
  }

  bool operator!=(const ObSqlBitSet<N, FlagType, auto_free> &other) const
  {
    return !(*this == other);
  }

  template<int64_t M, typename FlagType2, bool auto_free2,
           int64_t L, typename FlagType3, bool auto_free3 >
  int intersect(const ObSqlBitSet<M, FlagType2, auto_free2> &left,
                const ObSqlBitSet<L, FlagType3, auto_free3> &right)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "init error", K(desc_.init_errcode_));
    } else if (!left.is_valid()) {
      ret = left.desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "left init error", K(left.desc_.init_errcode_));
    } else if (!right.is_valid()) {
      ret = right.desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "right init error", K(right.desc_.init_errcode_));
    } else if (OB_ISNULL(bit_set_word_array_)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(bit_set_word_array_));
    } else {
      reset();
      int64_t that_count = left.bitset_word_count() < right.bitset_word_count() ?
                           left.bitset_word_count() : right.bitset_word_count() ;
      if (desc_.cap_ < that_count) {
        int64_t new_word_cnt = that_count * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buffer", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        for (int64_t i = 0; i < that_count; i++) {
          bit_set_word_array_[i] = left.get_bitset_word(i) & right.get_bitset_word(i);
        }
        desc_.len_ = static_cast<int16_t>(that_count);
      }
    }
    return ret;
  }

  template<int64_t M, typename FlagType2, bool auto_free2,
           int64_t L, typename FlagType3, bool auto_free3 >
  int except(const ObSqlBitSet<M, FlagType2, auto_free2> &left,
            const ObSqlBitSet<L, FlagType3, auto_free3> &right)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "init error", K(desc_.init_errcode_));
    } else if (!left.is_valid()) {
      ret = left.desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "left init error", K(left.desc_.init_errcode_));
    } else if (!right.is_valid()) {
      ret = right.desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "right init error", K(right.desc_.init_errcode_));
    } else if (OB_ISNULL(bit_set_word_array_)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(bit_set_word_array_));
    } else {
      reset();
      int64_t that_count = left.bitset_word_count();
      if (desc_.cap_ < that_count) {
        int64_t new_word_cnt = that_count * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buffer", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        int64_t i = 0;
        for (; i < left.bitset_word_count() && i < right.bitset_word_count(); i++) {
          bit_set_word_array_[i] = left.get_bitset_word(i) ^ (left.get_bitset_word(i) & right.get_bitset_word(i));
        }
        for (; i < left.bitset_word_count(); i++) {
          bit_set_word_array_[i] = left.get_bitset_word(i);
        }
        desc_.len_ = static_cast<int16_t>(that_count);
      }
    }
    return ret;
  }

  static int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int64_t &obj)
  {
    return common::databuff_print_obj(buf, buf_len, pos, obj);
  }

  static int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const ObExprInfoFlag &flag)
  {
    return common::databuff_printf(buf, buf_len, pos, "\"%s\"", get_expr_info_flag_str(flag));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    J_ARRAY_START();
    int64_t num = num_members();
    int64_t count = 0;
    for (int64_t i = 0; i < bit_count(); i++) {
      if (has_member(i)) {
        if (count != 0 && count < num) {
          J_COMMA();
        }
        FlagType flag = static_cast<FlagType>(i);
        if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, flag))) {
          SQL_RESV_LOG(WARN, "databuff print obj failed", K(ret));
        }
        ++count;
      }
    }
    J_ARRAY_END();
    return pos;
  }

  ObSqlBitSet<N, FlagType, auto_free>& operator=(const ObSqlBitSet<N, FlagType, auto_free> &other)
  {
    int ret = OB_SUCCESS;
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "init error", K(desc_.init_errcode_));
    } else if (!other.is_valid()) {
      ret = other.desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "init error", K(other.desc_.init_errcode_));
    } else if (&other == this) {
      // do nothing
    } else {
      if (other.bitset_word_count() > desc_.cap_) {
        int64_t new_word_cnt = other.bitset_word_count() * 2;
        if (OB_FAIL(alloc_new_buf(new_word_cnt))) {
          SQL_RESV_LOG(WARN, "failed to alloc new buf", K(ret));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < other.bitset_word_count(); i++) {
        bit_set_word_array_[i] = other.get_bitset_word(i);
      }
      desc_.len_ = static_cast<int16_t>(other.bitset_word_count());
    }
    if (OB_FAIL(ret)) {
      // error happened, set inited flag to be false
      desc_.init_errcode_ = ret;
      desc_.inited_ = false;
    }
    return *this;
  }
private:
  int alloc_new_buf(int64_t word_cnt)
  {
    int ret = OB_SUCCESS;
    int64_t words_size = sizeof(BitSetWord) * word_cnt;
    BitSetWord *new_buf = NULL;
    ObIAllocator *allocator = get_block_allocator();
    if (!is_valid()) {
      ret = desc_.init_errcode_;
      SQL_RESV_LOG(WARN, "not inited", K(ret));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid allocator", K(ret));
    } else if (OB_ISNULL(new_buf = (BitSetWord *)allocator->alloc(words_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      MEMSET(new_buf, 0, words_size);
      MEMCPY(new_buf, bit_set_word_array_, desc_.len_ * sizeof(BitSetWord));
      // set word array to new buffer
      allocator->free(bit_set_word_array_);
      bit_set_word_array_ = new_buf;
      desc_.cap_ = static_cast<int16_t>(word_cnt);
    }
    return ret;
  }

  ObIAllocator *get_block_allocator()
  {
    ObIAllocator *ret_alloc = NULL;
    if (!is_valid()) {
      // do nothing
    } else {
      ret_alloc = block_allocator_;
    }
    return ret_alloc;
  }

  int init_block_allocator()
  {
    int ret = OB_SUCCESS;
    if (is_valid()) {
      ret = OB_INIT_TWICE;
      SQL_RESV_LOG(WARN, "init twice", K(ret));
    } else if (auto_free) {
      block_allocator_ = &CURRENT_CONTEXT->get_arena_allocator();
    } else {
      void *alloc_buf = NULL;
      if (OB_ISNULL(alloc_buf = ob_malloc(sizeof(ObArenaAllocator), ObModIds::OB_BIT_SET))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        block_allocator_ = new(alloc_buf)ObArenaAllocator(ObModIds::OB_BIT_SET);
      }
    }
    return ret;
  }
private:
  template<int64_t N2, typename FlagType2, bool auto_free2>
  friend struct ObSqlBitSet;
  static const int64_t PER_BITSETWORD_BITS = 32;
  static const int64_t PER_BITSETWORD_MOD_BITS = 5;
  static const int64_t PER_BITSETWORD_MASK = PER_BITSETWORD_BITS - 1;
  static const int64_t MAX_BITSETWORD = ((N <= 0 ? DEFAULT_SQL_BITSET_SIZE : N) - 1)
                                          / PER_BITSETWORD_BITS + 1;

  struct SqlBitSetDesc
  {
    union
    {
      int32_t init_errcode_;
      struct
      {
        int16_t len_;
        int16_t cap_;
      };
    };
    bool inited_;

    SqlBitSetDesc() : len_(0), cap_(0), inited_(false)
    {}
  };

private:
  ObIAllocator *block_allocator_;
  BitSetWord *bit_set_word_array_;
  SqlBitSetDesc desc_;
};

typedef ObSqlBitSet<96, ObExprInfoFlag, true> ObExprInfo;

class ObUDFRawExpr;
struct ObUDFInfo
{
  ObUDFInfo() :
    udf_name_(),
    udf_package_(),
    udf_database_(),
    param_names_(),
    param_exprs_(),
    udf_param_num_(0),
    ref_expr_(NULL),
    is_udt_udf_(false),
    is_contain_self_param_(false),
    is_udt_udf_inside_pkg_(false),
    is_new_keyword_used_(false),
    flag_(0) {}

  void set_is_udf_udt_static() {
    flag_ |= UDF_UDT_STATIC;
  }
  bool is_udf_udt_member() const {
    return is_udt_udf_ && !(flag_ & UDF_UDT_STATIC);
  }

  void set_is_udf_udt_cons() {
    is_udt_udf_ = true;
    flag_ |= UDF_UDT_CONS;
  }

  bool is_udf_udt_cons() const {
    return is_udt_udf_ && !!(flag_ & UDF_UDT_CONS);
  }

  void clear_is_udf_udt_cons() {
    flag_ &= ~UDF_UDT_CONS;
  }

  void set_is_udt_overload_default_cons() {
    flag_ |= UDF_UDT_CONS_OVERLOAD_DEFAULT;
  }
  bool is_udt_overload_default_cons() {
    return is_udf_udt_cons() && !!(flag_ & UDF_UDT_CONS_OVERLOAD_DEFAULT);
  }
  void clear_udt_overload_default_cons() {
    flag_ &= ~UDF_UDT_CONS_OVERLOAD_DEFAULT;
  }

  static constexpr uint64_t UDF_UDT_STATIC = 1;
  static constexpr uint64_t UDF_UDT_CONS = 2;
  static constexpr uint64_t UDF_UDT_CONS_OVERLOAD_DEFAULT = 4;

  TO_STRING_KV(K_(udf_name),
               K_(udf_package),
               K_(udf_database),
               K_(param_names),
               K_(param_exprs),
               K_(udf_param_num),
               K_(is_udt_udf),
               K_(is_contain_self_param),
               K_(is_udt_udf_inside_pkg),
               K_(is_new_keyword_used),
               K_(flag));

  common::ObString udf_name_;
  common::ObString udf_package_;
  common::ObString udf_database_;
  common::ObArray<common::ObString> param_names_;
  common::ObArray<ObRawExpr*> param_exprs_;
	int64_t	udf_param_num_;
  ObUDFRawExpr *ref_expr_;
  bool is_udt_udf_; // if this udf is udt object routine
  bool is_contain_self_param_; // self param is mocked.
  bool is_udt_udf_inside_pkg_;
  bool is_new_keyword_used_;  // if in NEW obj(...) form
  uint64_t flag_;
};

enum AccessNameType
{
  UNKNOWN = -1,
  SYS_FUNC,
  DLL_UDF,
  PL_UDF,
  PL_VAR,
  DB_NS,
  PKG_NS,
  REC_ELEM,
  TYPE_METHOD,
  CURSOR_ATTR,
  UDT_NS,
  LOCAL_TYPE,
  PKG_TYPE,
};

class ObRawExpr;
class ObSysFunRawExpr;
class ObObjAccessIdent
{
public:
  ObObjAccessIdent()
      : type_(UNKNOWN),
        access_name_(),
        access_index_(common::OB_INVALID_INDEX),
        udf_info_(),
        sys_func_expr_(NULL),
        params_(),
        has_brackets_(false) {}
  ObObjAccessIdent(const common::ObString &name, int64_t index = common::OB_INVALID_INDEX)
    : type_(UNKNOWN),
      access_name_(name),
      access_index_(index),
      udf_info_(),
      sys_func_expr_(NULL),
      params_(),
      has_brackets_(false) {}
  virtual ~ObObjAccessIdent() {}

  int assign(const ObObjAccessIdent &other)
  {
    type_ = other.type_;
    access_name_ = other.access_name_;
    access_index_ = other.access_index_;
    udf_info_ = other.udf_info_;
    sys_func_expr_ = other.sys_func_expr_;
    has_brackets_ = other.has_brackets_;
    return params_.assign(other.params_);
  }
  ObObjAccessIdent &operator =(const ObObjAccessIdent &other)
  {
    assign(other);
    return *this;
  }

public:
  inline void set_type(AccessNameType type) { type_ = type; }
  inline AccessNameType get_type() { return type_; }
  inline void set_sys_func() { type_ = SYS_FUNC; }
  inline void set_dll_udf() { type_ = DLL_UDF; }
  inline void set_pl_udf() { type_ = PL_UDF; }
  inline void set_pl_var() { type_ = PL_VAR; }
  inline void set_db_ns() { type_ = DB_NS; }
  inline void set_pkg_ns() { type_ = PKG_NS; }
  inline void set_rec_elem() { type_ = REC_ELEM; }
  inline void set_type_method() { type_ = TYPE_METHOD; }
  inline void set_cursor_attr() { type_ = CURSOR_ATTR; }
  inline void set_udt_ns() { type_ = UDT_NS; }
  inline bool is_unknown() const { return UNKNOWN == type_; }
  inline bool is_sys_func() const { return SYS_FUNC == type_; }
  inline bool is_dll_udf() const { return DLL_UDF == type_; }
  inline bool is_pl_udf() const { return PL_UDF == type_; }
  inline bool is_pl_var() const { return PL_VAR == type_; }
  inline bool is_db_ns() const { return DB_NS == type_; }
  inline bool is_pkg_ns() const { return PKG_NS == type_; }
  inline bool is_rec_elem() const { return REC_ELEM == type_; }
  inline bool is_type_method() const { return TYPE_METHOD == type_; }
  inline bool is_cursor_attr() const { return CURSOR_ATTR == type_; }
  inline bool is_udt_ns() const { return UDT_NS == type_; }
  inline bool is_local_type() const { return LOCAL_TYPE == type_; }
  inline bool is_pkg_type() const { return PKG_TYPE == type_; }
  inline bool is_udt_type() const { return UDT_NS == type_; }
  inline bool is_type() const { return is_local_type() || is_pkg_type() || is_udt_type(); }

  int extract_params(int64_t level, common::ObIArray<ObRawExpr*> &params) const;
  int replace_params(ObRawExpr *from, ObRawExpr *to);

  TO_STRING_KV(K_(access_name), K_(access_index), K_(type), K_(params));

  AccessNameType type_;
  common::ObString access_name_;
  int64_t access_index_;
  ObUDFInfo udf_info_;
  ObSysFunRawExpr *sys_func_expr_;
  //a.f(x,y)(m,n)里的x、y、m、n都是f的参数，但是x、y的param_level_是0，m、n是1
  common::ObSEArray<std::pair<ObRawExpr*, int64_t>, 4, common::ModulePageAllocator, true> params_;
  bool has_brackets_; // may has empty (), record it.
};

class ObColumnRefRawExpr;
struct ObQualifiedName
{
public:
  ObQualifiedName()
      : database_name_(),
        tbl_name_(),
        col_name_(),
        dblink_name_(),
        is_star_(false),
        ref_expr_(NULL),
        parents_expr_info_(),
        parent_aggr_level_(-1),
        access_idents_(),
        current_resolve_level_(-1),
        is_access_root_(true)
  {
  }
  virtual ~ObQualifiedName() {}

  int assign(const ObQualifiedName &other)
  {
    database_name_ = other.database_name_;
    tbl_name_ = other.tbl_name_;
    col_name_ = other.col_name_;
    dblink_name_ = other.dblink_name_;
    is_star_ = other.is_star_;
    ref_expr_ = other.ref_expr_;
    parents_expr_info_ = other.parents_expr_info_;
    parent_aggr_level_ = other.parent_aggr_level_;
    current_resolve_level_ = other.current_resolve_level_;
    is_access_root_ = other.is_access_root_;
    return access_idents_.assign(other.access_idents_);
  }
  ObQualifiedName &operator =(const ObQualifiedName &other)
  {
    assign(other);
    return *this;
  }

  void format_qualified_name(common::ObNameCaseMode mode);
  inline bool is_unknown() const
  {
    bool bret = true;
    for (int64_t i = 0; bret && i < access_idents_.count(); ++i) {
      if (!access_idents_.at(i).is_unknown()) {
        bret = false;
        break;
      }
    }
    return bret;
  }
  inline bool is_sys_func() const
  {
    return 1 == access_idents_.count() && access_idents_.at(0).is_sys_func();
  }
  inline bool is_pl_udf() const
  {
    bool bret = !access_idents_.empty()
        && access_idents_.at(access_idents_.count() - 1).is_pl_udf();
    //如果最后的UDF有多层参数，那么说明不是UDF
    for (int64_t i = 0;
        bret && i < access_idents_.at(access_idents_.count() - 1).params_.count();
        ++i) {
      bret = 0 == access_idents_.at(access_idents_.count() - 1).params_.at(i).second;
    }
    return bret;
  }
  inline bool is_udf_return_access() const
  {
    bool bret = !access_idents_.empty()
        && access_idents_.at(access_idents_.count() - 1).is_pl_udf();
    //如果最后的UDF有多层参数，那么说明是对UDF返回值的访问
    bool multi_level = false;
    for (int64_t i = 0;
        bret && !multi_level && i < access_idents_.at(access_idents_.count() - 1).params_.count();
        ++i) {
      multi_level = 0 != access_idents_.at(access_idents_.count() - 1).params_.at(i).second;
    }
    return bret && multi_level;
  }
  inline bool is_dll_udf() const { return false; }
  inline bool is_pl_var() const
  {
    bool is_true = false;
    if (!is_sys_func() && !is_pl_udf() && !is_dll_udf()) {
      for (int64_t i = 0; !is_true && i < access_idents_.count(); ++i) {
        if (access_idents_.at(i).is_pl_var()) {
          is_true = true;
        }
      }
      is_true = is_true || access_idents_.count() > 3;
    }
    return is_true;
  }

  inline bool is_type_method() const
  {
    return access_idents_.at(access_idents_.count() - 1).is_type_method();
  }

  inline bool is_access_root() const { return is_access_root_; }

  int replace_access_ident_params(ObRawExpr *from, ObRawExpr *to);

  TO_STRING_KV(N_DATABASE_NAME, database_name_,
               N_TABLE_NAME, tbl_name_,
               N_COLUMN, col_name_,
               K_(dblink_name),
               K_(is_star),
               K_(ref_expr),
               K_(parents_expr_info),
               K_(parent_aggr_level),
               K_(access_idents),
               K_(current_resolve_level),
               K_(is_access_root));
public:
  common::ObString database_name_;
  common::ObString tbl_name_; //当用于UDF的时候，表示package name
  common::ObString col_name_; //当用于UDF的时候，表示function name
  common::ObString dblink_name_;
  bool is_star_;
  ObColumnRefRawExpr *ref_expr_;
  ObExprInfo parents_expr_info_;
  int64_t parent_aggr_level_;
  //通过'.'的方式访问的序列都存在这里，如a.f(x,y).c里的a、f、c
  common::ObSEArray<ObObjAccessIdent, 4, common::ModulePageAllocator, true> access_idents_;
  // the depth of resolve level
  int64_t current_resolve_level_;
  bool is_access_root_; //a(b(c))会被递归解析出c、b(c)、a(b(c))三个ObQualifiedName，只有a(b(c))是root
};

// bug 6349933: for most of the cases, 8 tables should be more than enough
typedef ObSqlBitSet<8, int64_t, true> ObRelIds;
typedef ObSqlBitSet<common::OB_MAX_SUBQUERY_LAYER_NUM, int64_t, true> ObExprLevels;

/**
 * @brief The ExprCopyPolicy enum
 * Share 表达式：column, query, aggregation, window function, 伪列以及被打上 IS_SHARED_REF 的表达式
 * COPY_REF_DEFAULT: 共享表达式采用浅拷贝，其他表达式采用深拷贝
 * COPY_REF_SHARE: 共享表达式也采用深拷贝。
 * e.g. min(c1),
 *   default mode: 直接返回指针
 *   share mode: 深拷 min，浅拷贝 c1，返回深拷贝的指针。
 * 每个 share 表达式只能深拷贝自己。不能触发其他 share 表达式的深拷
 */
enum ExprCopyPolicy {
  COPY_REF_DEFAULT    = 0,
  COPY_REF_SHARED     = 1 << 0,
};

struct OrderItem
{
  OrderItem()
  {
    reset();
  }
  explicit OrderItem(ObRawExpr *expr)
    : expr_(expr), order_type_(default_asc_direction()) {}
  OrderItem(ObRawExpr *expr, ObOrderDirection order_type)
    : expr_(expr), order_type_(order_type) {}
public:
  virtual ~OrderItem() {}
  void reset()
  {
    expr_ = NULL;
    order_type_ = default_asc_direction();
  }
  inline bool operator ==(const OrderItem &other) const
  {
    return (expr_ == other.expr_
            && order_type_ == other.order_type_);
  }
  inline bool operator !=(const OrderItem &other) const
  {
    return !(*this == other);
  }
  uint64_t hash(uint64_t seed) const
  {
    if (NULL != expr_) {
      seed = common::do_hash(*expr_, seed);
    }
    seed = common::do_hash(order_type_, seed);

    return seed;
  }

  int hash(uint64_t &hash_val, uint64_t seed) const
  {
    hash_val = hash(seed);
    return OB_SUCCESS;
  }

  bool is_null_first() const {
    return NULLS_FIRST_ASC == order_type_ || NULLS_FIRST_DESC == order_type_;
  }

  bool is_ascending() const {
    return NULLS_FIRST_ASC == order_type_ || NULLS_LAST_ASC == order_type_;
  }

  bool is_descending() const {
    return NULLS_FIRST_DESC == order_type_ || NULLS_LAST_DESC == order_type_;
  }

  int deep_copy(ObIRawExprCopier &copier, const OrderItem &other);

  static const char* order_type2name(const ObOrderDirection direction)
  {
    const char* name = NULL;
    switch (direction) {
    case NULLS_FIRST_ASC:
      name = N_NULLS_FIRST_ASC;
      break;
    case NULLS_LAST_ASC:
      name = N_NULLS_LAST_ASC;
      break;
    case NULLS_FIRST_DESC:
      name = N_NULLS_FIRST_DESC;
      break;
    case NULLS_LAST_DESC:
      name = N_NULLS_LAST_DESC;
      break;
    default:
      break;
    }
    return name;
  }

  TO_STRING_KV(N_EXPR, expr_,
               N_ASCENDING, order_type2name(order_type_));

  ObRawExpr* expr_;
  ObOrderDirection order_type_;
};

class ObQueryRefRawExpr;
class ObPlQueryRefRawExpr;
class ObSetOpRawExpr;
struct ObExprEqualCheckContext
{
  ObExprEqualCheckContext()
  : override_const_compare_(false),
    override_column_compare_(false),
    override_query_compare_(false),
    ignore_implicit_cast_(false),
    recursion_level_(0),
    override_set_op_compare_(false),
    err_code_(common::OB_SUCCESS),
    param_expr_(),
    need_check_deterministic_(false),
    ignore_param_(false)
  { }
  ObExprEqualCheckContext(bool need_check_deterministic)
  : override_const_compare_(false),
    override_column_compare_(false),
    override_query_compare_(false),
    ignore_implicit_cast_(false),
    recursion_level_(0),
    override_set_op_compare_(false),
    err_code_(common::OB_SUCCESS),
    param_expr_(),
    need_check_deterministic_(need_check_deterministic),
    ignore_param_(false)
  { }
  virtual ~ObExprEqualCheckContext() {}
  struct ParamExprPair
  {
    ParamExprPair(int64_t param_idx,
                  const ObRawExpr *expr)
        : param_idx_(param_idx), expr_(expr)
    { }
    ParamExprPair() : param_idx_(-1), expr_(NULL)
    { }
    TO_STRING_KV(K_(param_idx), K_(expr));
    int64_t param_idx_;
    const ObRawExpr *expr_;
  };
  inline int add_param_pair(int64_t param_idx, const ObRawExpr *expr)
  {
    return param_expr_.push_back(ParamExprPair(param_idx, expr));
  }

  // only compare the result type of two columns
  virtual bool compare_column(const ObColumnRefRawExpr &left,
                              const ObColumnRefRawExpr &right);

  virtual bool compare_const(const ObConstRawExpr &left,
                             const ObConstRawExpr &right);

  virtual bool compare_query(const ObQueryRefRawExpr &left,
                             const ObQueryRefRawExpr &right);

  virtual bool compare_query(const ObPlQueryRefRawExpr &left,
                             const ObPlQueryRefRawExpr &right);

  virtual bool compare_set_op_expr(const ObSetOpRawExpr& left,
                                   const ObSetOpRawExpr& right);

  void reset() {
    override_const_compare_ = false;
    override_column_compare_ = false;
    override_query_compare_ = false;
    ignore_implicit_cast_ = false;
    recursion_level_ = 0;
    override_set_op_compare_ = false;
    err_code_ = OB_SUCCESS;
    param_expr_.reset();
    need_check_deterministic_ = false;
    ignore_param_ = false;
  }
  bool override_const_compare_;
  bool override_column_compare_;
  bool override_query_compare_;
  bool ignore_implicit_cast_;
  int recursion_level_;
  bool override_set_op_compare_;
  int err_code_;
  //when compare with T_QUESTIONMARK, as T_QUESTIONMARK is unkown, record this first.
  common::ObSEArray<ParamExprPair, 3, common::ModulePageAllocator, true> param_expr_;
  bool need_check_deterministic_;
  bool ignore_param_; // only compare structure of expr
};

struct ObExprParamCheckContext : ObExprEqualCheckContext
{
  ObExprParamCheckContext() :
    ObExprEqualCheckContext(),
    calculable_items_(NULL),
    equal_param_constraints_(NULL)
  {
    override_column_compare_ = true;
    override_const_compare_ = true;
    override_query_compare_ = false;
  }
  virtual ~ObExprParamCheckContext() {}
  void init(const ObIArray<ObHiddenColumnItem> *calculable_items,
            const common::ObIArray<ObPCParamEqualInfo> *equal_param_constraints,
            EqualSets *equal_sets = NULL);

  virtual bool compare_column(const ObColumnRefRawExpr &left,
                              const ObColumnRefRawExpr &right)override;
  /**
   * 比较两个常量表达式：
   * 如果两个表达式都没有参数化，比较实际值
   * 如果两个表达式都参数化了，并且是预计算表达式，需要进一步
   * 获取预计算表达式比较；
   * 比较参数idx是否一致，如果一致，返回相等
   * 否则在all_equal_param_constraints_中查找是否存在等值约束，
   * 如果存在则返回相等，否则返回不相等
   */
  bool compare_const(const ObConstRawExpr &left, const ObConstRawExpr &right) override;

  int get_calc_expr(const int64_t param_idx, const ObRawExpr *&expr);

  int is_pre_calc_item(const ObConstRawExpr &const_expr, bool &is_calc);

  const ObIArray<ObHiddenColumnItem> *calculable_items_; // from query context
  const common::ObIArray<ObPCParamEqualInfo> *equal_param_constraints_;
  EqualSets *equal_sets_;
};

enum ObVarType
{
  INVALID_VAR = -1,
  SYS_VAR = 0,
  USER_VAR = 1,
};

struct ObVarInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObVarInfo() : type_(INVALID_VAR), name_() {}
  int deep_copy(common::ObIAllocator &allocator, ObVarInfo &var_info) const;
  bool operator==(const ObVarInfo &other) const
  {
    return (type_ == other.type_ && name_ == other.name_);
  }
  TO_STRING_KV(K_(type), K_(name));
  ObVarType type_;
  common::ObString name_;
};

class ObQueryRefRawExpr;
struct ObSubQueryInfo
{
  ObSubQueryInfo()
  {
    sub_query_ = NULL;
    ref_expr_ = NULL;
  }
  TO_STRING_KV(K_(ref_expr));

  const ParseNode *sub_query_;
  ObQueryRefRawExpr *ref_expr_;
  ObExprInfo parents_expr_info_;
};
class ObAggFunRawExpr;
class ObPseudoColumnRawExpr;
class ObOpRawExpr;
class ObWinFunRawExpr;
class ObUserVarIdentRawExpr;
struct ObUDFInfo;
template <typename ExprFactoryT>
struct ObResolveContext
{
  struct ObAggResolveLinkNode : public common::ObDLinkBase<ObAggResolveLinkNode>
  {
    ObAggResolveLinkNode()
      : is_win_agg_(false) {}
    bool is_win_agg_;
  };
  ObResolveContext(ExprFactoryT &expr_factory,
                   const common::ObTimeZoneInfo *tz_info,
                   const common::ObNameCaseMode mode)
  : expr_factory_(expr_factory),
    stmt_(NULL),
    connection_charset_(common::CHARSET_INVALID),
    dest_collation_(common::CS_TYPE_INVALID),
    tz_info_(tz_info),
    case_mode_(mode),
    columns_(NULL),
    sys_vars_(NULL),
    sub_query_info_(NULL),
    aggr_exprs_(NULL),
    win_exprs_(NULL),
    udf_info_(NULL),
    op_exprs_(NULL),
    user_var_exprs_(nullptr),
    is_extract_param_type_(true),
    param_list_(NULL),
    prepare_param_count_(0),
    external_param_info_(NULL),
    current_scope_(T_NONE_SCOPE),
    is_win_agg_(false),
    schema_checker_(NULL),
    session_info_(NULL),
    secondary_namespace_(NULL),
    query_ctx_(NULL),
    is_for_pivot_(false),
    is_for_dynamic_sql_(false),
    is_for_dbms_sql_(false),
    tg_timing_event_(TG_TIMING_EVENT_INVALID),
    view_ref_id_(OB_INVALID_ID),
    is_variable_allowed_(true)
  {
  }

  ExprFactoryT &expr_factory_;
  ObStmt *stmt_;
  common::ObCharsetType connection_charset_;
  common::ObCollationType dest_collation_;
  const common::ObTimeZoneInfo *tz_info_;
  common::ObNameCaseMode case_mode_;
  //标记该表达式的上层表达式的一些属性，
  //比如count(c1), c1的上层是count()，所以c1的parents_expr_info 含有IS_AGG
  ObExprInfo parents_expr_info_;
  common::ObIArray<ObQualifiedName> *columns_;
  common::ObIArray<ObVarInfo> *sys_vars_;
  common::ObIArray<ObSubQueryInfo> *sub_query_info_;
  common::ObIArray<ObAggFunRawExpr*> *aggr_exprs_;
  common::ObIArray<ObWinFunRawExpr*> *win_exprs_;
  common::ObIArray<ObUDFInfo> *udf_info_;
  common::ObIArray<ObOpRawExpr*> *op_exprs_;
  common::ObIArray<ObUserVarIdentRawExpr*> *user_var_exprs_;
  //由于单测expr resolver中包含一些带？的表达式case，
  //所以为expr resolver ctx增添一个配置变量isextract_param_type
  //如果配置该参数为true，那么遇到？将为其填上真实的参数类型，
  //如果没有对应的参数将报错，如果配置该参数为false的时候，
  //参数列表指定了也将为其填上真实的参数类型，
  //如果没有指定参数列表，那么expr resolver将忽略参数类型的填充
  bool is_extract_param_type_;
  const ParamStore *param_list_;
  int64_t prepare_param_count_;
  ExternalParams *external_param_info_;//for anonymous + ps
  ObStmtScope current_scope_;
  common::ObArenaAllocator local_allocator_;
  typedef common::ObDList<ObAggResolveLinkNode> ObAggResolveLink;
  ObAggResolveLink agg_resolve_link_;
  bool is_win_agg_;
  ObSchemaChecker *schema_checker_;// we use checker to get udf function name.
  const ObSQLSessionInfo *session_info_;// we use to get tenant id
  pl::ObPLBlockNS *secondary_namespace_;
  ObQueryCtx *query_ctx_;
  bool is_for_pivot_;
  bool is_for_dynamic_sql_;
  bool is_for_dbms_sql_;
  TgTimingEvent tg_timing_event_; // for mysql trigger
  uint64_t view_ref_id_;
  bool is_variable_allowed_;
};

typedef ObResolveContext<ObRawExprFactory> ObExprResolveContext;
class ObRawExprVisitor;
struct ObHiddenColumnItem;

enum ExplicitedRefType {
  NONE_REF = 0,
  REF_BY_NORMAL = 1 << 0,
  REF_BY_PART_EXPR = 1 << 1,
  REF_BY_VIRTUAL_GEN_COL = 1<< 2,
  REF_BY_STORED_GEN_COL = 1 << 3
};
class ObRawExpr : virtual public jit::expr::ObIRawExpr
{
public:
  friend sql::ObExpr *ObStaticEngineExprCG::get_rt_expr(const ObRawExpr &raw_expr);
  friend sql::ObExpr *ObExprOperator::get_rt_expr(const ObRawExpr &raw_expr) const;
  friend class pl::ObPLCodeGenerator;
  friend class sql::ObCallProcedureInfo;
  friend class sql::ObRTDatumArith;

  explicit ObRawExpr(ObItemType expr_type = T_INVALID)
     : ObIRawExpr(expr_type),
       magic_num_(0x13572468),
       info_(),
       rel_ids_(),
       inner_alloc_(NULL),
       expr_factory_(NULL),
       reference_type_(ExplicitedRefType::NONE_REF),
       ref_count_(0),
       is_for_generated_column_(false),
       rt_expr_(NULL),
       extra_(0),
       is_called_in_sql_(true),
       is_calculated_(false),
       is_deterministic_(true),
       partition_id_calc_type_(CALC_INVALID)
  {
  }

  explicit ObRawExpr(common::ObIAllocator &alloc, ObItemType expr_type = T_INVALID)
     : ObIRawExpr(alloc, expr_type),
       magic_num_(0x13572468),
       info_(),
       rel_ids_(),
       inner_alloc_(&alloc),
       expr_factory_(NULL),
       reference_type_(ExplicitedRefType::NONE_REF),
       ref_count_(0),
       is_for_generated_column_(false),
       rt_expr_(NULL),
       extra_(0),
       is_called_in_sql_(true),
       is_calculated_(false),
       is_deterministic_(true),
       partition_id_calc_type_(CALC_INVALID),
       may_add_interval_part_(MayAddIntervalPart::NO),
       runtime_filter_type_(NOT_INIT_RUNTIME_FILTER_TYPE)
  {
  }
  virtual ~ObRawExpr();

  int deep_copy(ObIRawExprCopier &copier, const ObRawExpr &other);
  virtual int assign(const ObRawExpr &other);
  virtual int inner_deep_copy(ObIRawExprCopier &copier);
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) = 0;
  virtual void clear_child() = 0;

  virtual void reset();
  /// whether is the same expression.
  /// Compare the two expression tree.
  bool has_generalized_column() const;
  bool has_enum_set_column() const;
  bool has_specified_pseudocolumn() const;
  bool same_as(const ObRawExpr &expr,
               ObExprEqualCheckContext *check_context = NULL) const;

  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const = 0;

  inline bool is_generalized_column() const
  {
    return is_column_ref_expr() || is_query_ref_expr() || is_aggr_expr() || is_set_op_expr()
          || is_win_func_expr() || has_flag(IS_ROWNUM) || has_flag(IS_PSEUDO_COLUMN)
          || has_flag(IS_SEQ_EXPR) || has_flag(IS_SYS_CONNECT_BY_PATH)
          || has_flag(IS_CONNECT_BY_ROOT) || has_flag(IS_OP_PSEUDO_COLUMN);
  }

  // The expr result is vectorized, the batch result is the same if not vectorized result e.g:
  //  c1 + c1: is vectorized
  //  ? + ?: is not vectorized
  bool is_vectorize_result() const;

  void unset_result_flag(uint32_t result_flag);
  //void set_op_factory(ObExprOperatorFactory &factory) { op_factory_ = &factory; }
  void set_allocator(common::ObIAllocator &alloc);
  void set_expr_factory(ObRawExprFactory &factory) { expr_factory_ = &factory; }
  ObRawExprFactory *get_expr_factory() { return expr_factory_; }

  void set_expr_info(const ObExprInfo &info);
  int add_flag(int32_t flag);
  int add_flags(const ObExprInfo &flags);
  int add_child_flags(const ObExprInfo &flags);
  bool has_flag(ObExprInfoFlag flag) const;
  int clear_flag(int32_t flag);
  /**                                                   +-is_immutable_const_expr-+
   *                               （1、1+2、sysdate）   ｜        (1、1+2)
   *                             +-is_static_const_expr-+
   *                             |
   * is_const_or_calculable_expr-+
   *    (1、1+2、2+？、sysdate）   |
   *                             +-is_dynamic_const_expr
   *                                 （2 + ？）
   * immutable const: is const for all queries
   * static const: is const in a query sql
   * dynamic const: is const in a query block
   */
  bool is_param_expr() const;
  bool is_const_expr() const;
  bool is_immutable_const_expr() const;
  bool is_static_const_expr() const;
  bool is_static_scalar_const_expr() const;
  bool is_dynamic_const_expr() const;
  bool has_hierarchical_query_flag() const;
  const ObExprInfo &get_expr_info() const;
  ObExprInfo &get_expr_info();

  int add_relation_id(int64_t rel_idx);
  int add_relation_ids(const ObRelIds &rel_ids);
  ObRelIds &get_relation_ids();
  const ObRelIds &get_relation_ids() const;

  // implemented base on get_param_count() and get_param_expr() interface,
  // children are visited in get_param_expr() return order.
  int preorder_accept(ObRawExprVisitor &visitor);
  int postorder_accept(ObRawExprVisitor &visitor);

  virtual int do_visit(ObRawExprVisitor &visitor) = 0;

  // skip visit child for expr visitor. (ObSetIterRawExpr)
  virtual bool skip_visit_child() const { return false; }

  virtual int64_t get_param_count() const = 0;
  virtual const ObRawExpr *get_param_expr(int64_t index) const = 0;
  virtual ObRawExpr *&get_param_expr(int64_t index) = 0;
  virtual int64_t get_output_column() const {return -1;}

  inline bool is_not_null_for_read() const { return get_result_type().has_result_flag(NOT_NULL_FLAG); }
  inline bool is_not_null_for_write() const { return get_result_type().has_result_flag(NOT_NULL_WRITE_FLAG); }
  inline bool is_auto_increment() const { return get_result_type().has_result_flag(AUTO_INCREMENT_FLAG); }
  inline bool is_rand_func_expr() const { return has_flag(IS_RAND_FUNC); }
  inline bool is_obj_access_expr() const { return T_OBJ_ACCESS_REF == get_expr_type(); }
  inline bool is_assoc_index_expr() const { return T_FUN_PL_ASSOCIATIVE_INDEX == get_expr_type(); }
  bool is_not_calculable_expr() const;
  bool cnt_not_calculable_expr() const;
  int is_const_inherit_expr(bool &is_const_inherit, const bool param_need_replace = false) const;
  int is_non_pure_sys_func_expr(bool &is_non_pure) const;
  bool is_specified_pseudocolumn_expr() const;
  void set_alias_column_name(const common::ObString &alias_name) { alias_column_name_ = alias_name; }
  const common::ObString &get_alias_column_name() const { return alias_column_name_; }
  int set_expr_name(const common::ObString &expr_name);
  const common::ObString &get_expr_name() const { return expr_name_; }
  inline uint64_t hash(uint64_t seed) const
  {
    seed = common::do_hash(type_, seed);
    seed = common::do_hash(expr_class_, seed);
    seed = result_type_.hash(seed);
    seed = hash_internal(seed);
    return seed;
  }
  inline int hash(uint64_t &hash_val, uint64_t seed) const
  {
    hash_val = hash(seed);
    return OB_SUCCESS;
  }
  inline bool is_type_to_str_expr() const
  {
    return (T_FUN_ENUM_TO_STR == type_ || T_FUN_SET_TO_STR == type_
        || T_FUN_ENUM_TO_INNER_TYPE == type_ || T_FUN_SET_TO_INNER_TYPE == type_);
  }
  virtual uint64_t hash_internal(uint64_t seed) const = 0;

  int get_name(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type = EXPLAIN_UNINITIALIZED) const;
  int get_type_and_length(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const = 0;

  // post-processing for expressions
  int formalize(const ObSQLSessionInfo *my_session);
  int pull_relation_id();
  int extract_info();
  int deduce_type(const ObSQLSessionInfo *my_session = NULL);
  inline ObExprInfo &get_flags() { return info_; }
  int set_enum_set_values(const common::ObIArray<common::ObString> &values);
  const common::ObIArray<common::ObString> &get_enum_set_values() const { return enum_set_values_; }
  bool is_explicited_reference() const { return reference_type_ != ExplicitedRefType::NONE_REF; }
  bool is_referred_by_normal() const { return (reference_type_ & ExplicitedRefType::REF_BY_NORMAL) != 0; }
  bool is_only_referred_by_stored_gen_col() const { return reference_type_ == ExplicitedRefType::REF_BY_STORED_GEN_COL; }
  int32_t get_explicited_reftype() const { return reference_type_; }
  void set_explicited_reference()
  {
    ref_count_++;
    reference_type_ |= ExplicitedRefType::REF_BY_NORMAL;
  }
  void set_part_key_reference() {
    ref_count_++;
    reference_type_ |= ExplicitedRefType::REF_BY_PART_EXPR;
  }
  void set_explicited_reference(ExplicitedRefType ref_type)
  {
    ref_count_++;
    reference_type_ |= ref_type;
  }
  void clear_explicited_referece()
  {
    ref_count_ = 0;
    reference_type_ = ExplicitedRefType::NONE_REF;
  }
  int64_t get_ref_count()  const {
    return ref_count_;
  }
  bool is_for_generated_column() const { return is_for_generated_column_; }
  void set_for_generated_column()
  {
    is_for_generated_column_ = true;
  }
  void clear_for_generated_column()
  {
    is_for_generated_column_ = false;
  }
  void set_rt_expr(sql::ObExpr *expr) { rt_expr_ = expr; }
  void reset_rt_expr() { rt_expr_ = NULL; }
  void set_extra(uint64_t extra) { extra_ = extra; }
  void set_is_called_in_sql(bool is_called_in_sql) { is_called_in_sql_ = is_called_in_sql; }
  void set_is_calculated(bool is_calculated) { is_calculated_ = is_calculated; }
  uint64_t get_extra() const { return extra_; }
  bool is_called_in_sql() const { return is_called_in_sql_; }
  bool is_calculated() const { return is_calculated_; }
  bool is_deterministic() const { return is_deterministic_; }
  bool is_bool_expr() const;
  bool is_spatial_expr() const;
  bool is_geo_expr() const;
  bool is_mysql_geo_expr() const;
  bool is_priv_geo_expr() const;
  bool is_xml_expr() const;
  ObGeoType get_geo_expr_result_type() const;
  void set_is_deterministic(bool is_deterministic) { is_deterministic_ = is_deterministic; }
  int get_geo_cast_result_type(ObGeoType& geo_type) const;
  void set_partition_id_calc_type(PartitionIdCalcType calc_type) {
    partition_id_calc_type_ = calc_type; }
  bool is_json_expr() const;
  bool is_multiset_expr() const;
  PartitionIdCalcType get_partition_id_calc_type() const { return partition_id_calc_type_; }
  void set_may_add_interval_part(MayAddIntervalPart flag) {
    may_add_interval_part_ = flag;
  }
  MayAddIntervalPart get_may_add_interval_part() const
  { return may_add_interval_part_;}
  RuntimeFilterType get_runtime_filter_type() const { return runtime_filter_type_; }
  void set_runtime_filter_type(RuntimeFilterType type) { runtime_filter_type_ = type; }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_,
                       N_RESULT_TYPE, result_type_,
                       N_EXPR_INFO, info_,
                       N_REL_ID, rel_ids_,
                       K_(enum_set_values),
                       K_(reference_type),
                       K_(ref_count),
                       K_(is_for_generated_column),
                       K_(extra),
                       K_(is_called_in_sql),
                       K_(is_calculated),
                       K_(is_deterministic),
                       K_(partition_id_calc_type),
                       K_(may_add_interval_part));

private:
  const ObRawExpr *get_same_identify(const ObRawExpr *e,
                                     const ObExprEqualCheckContext *check_ctx) const;

public:
  uint32_t magic_num_;
protected:
  static const int64_t COMMON_MULTI_NUM = 16;
  static const int64_t COMMON_ENUM_SET_VALUE_NUM = 4;
protected:
  ObExprInfo  info_;    // flags
  ObRelIds    rel_ids_;  // related table idx
  common::ObIAllocator *inner_alloc_;
  ObRawExprFactory *expr_factory_;
  common::ObString alias_column_name_;
  common::ObSEArray<common::ObString, 1, common::ModulePageAllocator, true> enum_set_values_;//string_map
  //在mysql中表达式都有自己的自己名字，例如，cast('1' as unsigned)，这个
  //表达式解析出来这一整串会作为这个表达式的名字。
  //在udf中，需要将udf_func(expr1, expr2)中expr1和expr2的名字作为参数传递
  //给user defined function。
  common::ObString expr_name_;
  // for column expr, agg expr, window function expr and query ref exprs
  int32_t reference_type_;
  int64_t ref_count_;
  bool is_for_generated_column_;
  sql::ObExpr *rt_expr_;

  // 每个raw expr有自己的解释
  uint64_t extra_;
  bool is_called_in_sql_; // 用于区分是被 pl 还是 sql 调用
  bool is_calculated_; // 用于在新引擎 cg 中检查 raw expr 是否被重复计算
  bool is_deterministic_; //expr is deterministic, given the same inputs, returns the same result
  PartitionIdCalcType partition_id_calc_type_; //for calc_partition_id func to mark calc part type
  MayAddIntervalPart may_add_interval_part_; // for calc_partition_id
  RuntimeFilterType runtime_filter_type_; // for runtime filter
private:
  DISALLOW_COPY_AND_ASSIGN(ObRawExpr);
};

inline void ObRawExpr::set_allocator(ObIAllocator &alloc)
{
  inner_alloc_ = &alloc;
  result_type_.set_allocator(&alloc);
}

inline void ObRawExpr::unset_result_flag(uint32_t result_flag)
{
  result_type_.unset_result_flag(result_flag);
}

inline int ObRawExpr::add_relation_id(int64_t rel_idx)
{
  int ret = common::OB_SUCCESS;
  if (rel_idx < 0) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = rel_ids_.add_member(rel_idx);
  }
  return ret;
}

inline int ObRawExpr::add_relation_ids(const ObRelIds &rel_ids)
{
  return rel_ids_.add_members(rel_ids);
}

inline int ObRawExpr::add_flag(int32_t flag)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(info_.add_member(flag))) {
    //add_member will print log,here no need
  } else if (flag <= IS_INFO_MASK_END) {
    if (OB_FAIL(info_.add_member(CNT_INFO_MASK_BEGIN + flag))) {
      //add_member will print log,here no need
    }
  } else {}
  return ret;
}

inline int ObRawExpr::clear_flag(int32_t flag)
{
  return info_.del_member(flag);
}

inline bool ObRawExpr::is_param_expr() const
{
  return has_flag(IS_STATIC_PARAM) || has_flag(IS_DYNAMIC_PARAM);
}

inline bool ObRawExpr::is_const_expr() const
{
  return has_flag(IS_CONST) || has_flag(IS_CONST_EXPR);
}

inline bool ObRawExpr::is_immutable_const_expr() const
{
  // todo: support recognize 1+1 by introducing new expr flag like IS_MUTABLE_FUNC
  return is_const_raw_expr() && !is_param_expr();
}

inline bool ObRawExpr::is_static_const_expr() const
{
  return is_const_expr() &&
          !has_flag(CNT_DYNAMIC_PARAM);
}

inline bool ObRawExpr::is_static_scalar_const_expr() const
{
  return is_static_const_expr() && T_OP_ROW != get_expr_type();
}

inline bool ObRawExpr::is_dynamic_const_expr() const
{
  return is_const_expr() && has_flag(CNT_DYNAMIC_PARAM);
}

inline bool ObRawExpr::has_hierarchical_query_flag() const
{
  return has_flag(CNT_PRIOR) || has_flag(CNT_LEVEL)
         || has_flag(CNT_CONNECT_BY_ISLEAF)
         || has_flag(CNT_CONNECT_BY_ISCYCLE)
         || has_flag(CNT_CONNECT_BY_ROOT)
         || has_flag(CNT_SYS_CONNECT_BY_PATH);;
}

inline int ObRawExpr::add_flags(const ObExprInfo &flags)
{
  return info_.add_members(flags);
}

inline bool ObRawExpr::has_flag(ObExprInfoFlag flag) const
{
  return info_.has_member(flag);
}
inline bool ObRawExpr::inner_same_as(
    const ObRawExpr &expr,
    ObExprEqualCheckContext *check_context) const
{
  UNUSED(check_context);
  return (get_expr_type() == expr.get_expr_type() && get_result_type() == expr.get_result_type());
}

inline void ObRawExpr::set_expr_info(const ObExprInfo &info)
{
  info_ = info;
}
inline ObExprInfo &ObRawExpr::get_expr_info()
{
  return info_;
}
inline const ObExprInfo &ObRawExpr::get_expr_info() const
{
  return info_;
}
inline ObRelIds &ObRawExpr::get_relation_ids()
{
  return rel_ids_;
}
inline const ObRelIds &ObRawExpr::get_relation_ids() const
{
  return rel_ids_;
}

////////////////////////////////////////////////////////////////
class ObTerminalRawExpr: public ObRawExpr
{
public:
  explicit ObTerminalRawExpr(ObItemType expr_type = T_INVALID)
      : ObRawExpr(expr_type)
  {}
  explicit ObTerminalRawExpr(common::ObIAllocator &alloc, ObItemType expr_type = T_INVALID)
      : ObRawExpr(alloc, expr_type)
  {}
  virtual ~ObTerminalRawExpr() {}
  virtual void clear_child() {}

  virtual int64_t get_param_count() const { return 0; }
  virtual const ObRawExpr *get_param_expr(int64_t index) const { UNUSED(index); return NULL; }
  virtual ObRawExpr *&get_param_expr(int64_t index);
  virtual uint64_t hash_internal(uint64_t seed) const { return seed; }
protected:
private:
  DISALLOW_COPY_AND_ASSIGN(ObTerminalRawExpr);
};

////////////////////////////////////////////////////////////////
class ObConstRawExpr :
    public ObTerminalRawExpr,
    public jit::expr::ObConstExpr
{
public:
  ObConstRawExpr()
    :is_date_unit_(false),
     is_literal_bool_(false),
     is_batch_stmt_parameter_(false),/*: precalc_expr_(NULL)*/
     array_param_group_id_(-1)
  { ObRawExpr::set_expr_class(ObRawExpr::EXPR_CONST); }
  ObConstRawExpr(common::ObIAllocator &alloc)
    : ObIRawExpr(alloc),
      ObTerminalRawExpr(alloc),
      ObConstExpr(),
      is_date_unit_(false),
      is_literal_bool_(false),
      is_batch_stmt_parameter_(false),
      array_param_group_id_(-1)
  { ObIRawExpr::set_expr_class(ObIRawExpr::EXPR_CONST); }
  ObConstRawExpr(const oceanbase::common::ObObj &val, ObItemType expr_type = T_INVALID)
    : ObIRawExpr(expr_type),
      ObTerminalRawExpr(expr_type),
      ObConstExpr(),
      is_date_unit_(false),
      is_literal_bool_(false),
      is_batch_stmt_parameter_(false),
      array_param_group_id_(-1)
  {
    set_value(val);
    set_expr_class(ObIRawExpr::EXPR_CONST);
  }
  virtual ~ObConstRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  void set_value(const oceanbase::common::ObObj &val);
  void set_literal_prefix(const common::ObString &name);
  void set_expr_obj_meta(const common::ObObjMeta &meta) { obj_meta_ = meta; }
  const common::ObObjMeta &get_expr_obj_meta() const { return obj_meta_; }
  const common::ObString &get_literal_prefix() const { return literal_prefix_; }
  void set_is_date_unit();
  void reset_is_date_unit();
  bool is_date_unit() {return true == is_date_unit_; }
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  void set_is_literal_bool(const bool is_literal_bool) { is_literal_bool_ = is_literal_bool; }
  bool is_literal_bool() const { return is_literal_bool_; }
  void set_is_batch_stmt_parameter() { is_batch_stmt_parameter_ = true; }
  bool is_batch_stmt_parameter() { return is_batch_stmt_parameter_; }
  void set_array_param_group_id(int64_t id) { array_param_group_id_ = id; }
  int64_t get_array_param_group_id() const { return array_param_group_id_; }
  DECLARE_VIRTUAL_TO_STRING;

private:
  common::ObString literal_prefix_; //仅在编译期使用, 执行期无关
  common::ObObjMeta obj_meta_;
  bool is_date_unit_;
  // for mysql mode to distinguish tinyint and literal bool
  bool is_literal_bool_;
  // is_batch_stmt_parameter_ only used for array_binding batch_execution optimization
  // Indicates that the current parameter is the batch parameter
  bool is_batch_stmt_parameter_;
  int64_t array_param_group_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConstRawExpr);
};

////////////////////////////////////////////////////////////////
/// \brief The ObVarRawExpr class
///  designed for deducing calc type
///  only used by nullif, least, greatest, from_unixtime
class ObVarRawExpr :
    public ObTerminalRawExpr,
    public jit::expr::ObVarExpr
{
public:
  ObVarRawExpr() { ObIRawExpr::set_expr_class(ObIRawExpr::EXPR_VAR); }
  ObVarRawExpr(common::ObIAllocator &alloc)
    : ObIRawExpr(alloc),
      ObTerminalRawExpr(alloc),
      ObVarExpr(),
      result_type_assigned_(false)
  { ObIRawExpr::set_expr_class(ObIRawExpr::EXPR_VAR); }
  ObVarRawExpr(ObItemType expr_type = T_INVALID)
    : ObIRawExpr(expr_type),
      ObTerminalRawExpr(expr_type),
      ObVarExpr(),
      result_type_assigned_(false)
  { set_expr_class(ObIRawExpr::EXPR_VAR); }
  virtual ~ObVarRawExpr() {}

  int assign(const ObRawExpr &other) override;

  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  void set_result_type_assigned(bool v) { result_type_assigned_ = v; }
  bool get_result_type_assigned() { return result_type_assigned_; }

private:
  bool result_type_assigned_;
  DISALLOW_COPY_AND_ASSIGN(ObVarRawExpr);
};

////////////////////////////////////////////////////////////////
class ObUserVarIdentRawExpr : public ObConstRawExpr
{
public:
  ObUserVarIdentRawExpr() : is_contain_assign_(false), query_has_udf_(false) {}
  ObUserVarIdentRawExpr(common::ObIAllocator &alloc)
    : ObConstRawExpr(alloc), is_contain_assign_(false), query_has_udf_(false) {}
  ObUserVarIdentRawExpr(const oceanbase::common::ObObj &val, ObItemType expr_type = T_INVALID)
    : ObConstRawExpr(val, expr_type), is_contain_assign_(false), query_has_udf_(false) {}
  virtual ~ObUserVarIdentRawExpr() {}
  int assign(const ObRawExpr &other) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;

  bool get_is_contain_assign() const { return is_contain_assign_; }
  void set_is_contain_assign(bool is_contain_assign) { is_contain_assign_ = is_contain_assign; }
  bool get_query_has_udf() const { return query_has_udf_; }
  void set_query_has_udf(bool query_has_udf) { query_has_udf_ = query_has_udf; }
  bool is_same_variable(const ObObj &obj) const;
  DECLARE_VIRTUAL_TO_STRING;

private:
  bool is_contain_assign_; // 用户变量在整个query中是否存在赋值操作
  bool query_has_udf_;    // 整个query中是否包含UDF
private:
  DISALLOW_COPY_AND_ASSIGN(ObUserVarIdentRawExpr);
};

////////////////////////////////////////////////////////////////

class ObExecParamRawExpr : public ObConstRawExpr
{
public:
  ObExecParamRawExpr() :
    ObConstRawExpr(),
    ref_same_dblink_(false)
  {
    set_expr_class(ObIRawExpr::EXPR_EXEC_PARAM);
  }

  ObExecParamRawExpr(common::ObIAllocator &alloc)
    : ObConstRawExpr(alloc),
      ref_same_dblink_(false)
  {
    set_expr_class(ObIRawExpr::EXPR_EXEC_PARAM);
  }

  virtual ~ObExecParamRawExpr() {}

  void set_param_index(int64_t index);
  int64_t get_param_index() const;

  void set_ref_expr(ObRawExpr *expr, bool is_onetime = false)
  {
    outer_expr_ = expr;
    is_onetime_ = is_onetime;
  }
  const ObRawExpr* get_ref_expr() const { return outer_expr_; }
  ObRawExpr*& get_ref_expr() { return outer_expr_; }

  bool is_onetime() const { return is_onetime_; }
  bool is_ref_same_dblink() const { return ref_same_dblink_; }
  void set_ref_same_dblink(bool ref_same_dblink) { ref_same_dblink_ = ref_same_dblink; }
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context) const override;
  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;
  virtual int get_name_internal(char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                ExplainType type) const;
  DECLARE_VIRTUAL_TO_STRING;
private:
  // the refered expr in the outer stmt
  ObRawExpr *outer_expr_;
  bool is_onetime_;
  bool ref_same_dblink_;
};

class ObQueryRefRawExpr : public ObRawExpr
{
public:
  ObQueryRefRawExpr()
    : ObRawExpr(),
      ref_id_(common::OB_INVALID_ID),
      output_column_(0),
      is_set_(false),
      is_cursor_(false),
      has_nl_param_(false),
      is_multiset_(false)
  {
    //匿名union对象的初始化只能放到函数体里面，不然会报多次初始化同一个对象的编译错误
    ref_stmt_ = NULL;
    set_expr_class(ObIRawExpr::EXPR_QUERY_REF);
  }

  ObQueryRefRawExpr(common::ObIAllocator &alloc)
    : ObRawExpr(alloc),
      ref_id_(common::OB_INVALID_ID),
      output_column_(0),
      is_set_(false),
      is_cursor_(false),
      has_nl_param_(false),
      is_multiset_(false)
  {
    //匿名union对象的初始化只能放到函数体里面，不然会报多次初始化同一个对象的编译错误
    ref_stmt_ = NULL;
    set_expr_class(ObIRawExpr::EXPR_QUERY_REF);
  }
  ObQueryRefRawExpr(int64_t id, ObItemType expr_type = T_INVALID)
    : ObRawExpr(expr_type),
      ref_id_(id),
      output_column_(0),
      is_set_(false),
      is_cursor_(false),
      has_nl_param_(false),
      is_multiset_(false)
  {
    //匿名union对象的初始化只能放到函数体里面，不然会报多次初始化同一个对象的编译错误
    ref_stmt_ = NULL;
    set_expr_class(ObIRawExpr::EXPR_QUERY_REF);
  }
  virtual ~ObQueryRefRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;

  virtual void clear_child() override;
  int add_param_expr(ObRawExpr *expr);
  int add_exec_param_expr(ObExecParamRawExpr *expr);
  int add_exec_param_exprs(const ObIArray<ObExecParamRawExpr *> &exprs);
  bool has_exec_param() const { return !exec_params_.empty(); }
  virtual int64_t get_param_count() const override;
  virtual const ObRawExpr *get_param_expr(int64_t index) const override;
  virtual ObRawExpr *&get_param_expr(int64_t index) override;
  ObExecParamRawExpr *get_exec_param(int64_t index);
  const ObIArray<ObExecParamRawExpr *> &get_exec_params() const { return exec_params_; }
  ObIArray<ObExecParamRawExpr *> &get_exec_params() { return exec_params_; }

  int64_t get_ref_id() const;
  void set_ref_id(int64_t id);
  ObSelectStmt *&get_ref_stmt() { return ref_stmt_; }
  const ObSelectStmt *get_ref_stmt() const { return ref_stmt_; }
  void set_ref_stmt(ObSelectStmt *ref_stmt)
  {
    ref_stmt_ = ref_stmt;
  }
  void set_output_column(int64_t output_column);
  int64_t get_output_column() const;
  int add_column_type(const ObExprResType &type) { return column_types_.push_back(type); }
  const common::ObIArray<ObExprResType> &get_column_types() const { return column_types_; }
  common::ObIArray<ObExprResType> &get_column_types() { return column_types_; }
  void set_is_set(bool is_set) { is_set_ = is_set; }
  bool is_set() const { return is_set_; }
  void set_cursor(bool is_cursor) { is_cursor_ = is_cursor; }
  bool is_cursor() const { return is_cursor_; }
  void set_has_nl_param(bool has_nl_param) { has_nl_param_ = has_nl_param; }
  bool has_nl_param() const { return has_nl_param_; }
  void set_is_multiset(bool is_multiset) { is_multiset_ = is_multiset; }
  bool is_multiset() const {return is_multiset_; }
  bool is_scalar() const { return !is_set_ && !is_multiset_ && get_output_column() == 1; }
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const { return common::do_hash(ref_id_, seed); }

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;

  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_ID, ref_id_,
                                            K_(output_column),
                                            K_(is_set),
                                            K_(is_cursor),
                                            K_(is_multiset),
                                            K_(column_types),
                                            K_(enum_set_values),
                                            N_CHILDREN, exec_params_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRefRawExpr);
  //ObUnaryRefExpr是表示对一个stmt或者logical plan的引用，
  //引用都是指针，对显示不够友好，所以加一个ref_id，用来展示给人看
  int64_t ref_id_;
  ObSelectStmt *ref_stmt_;
  int64_t output_column_;
  bool is_set_;
  bool is_cursor_;
  // Given a query_ref_expr in a function table,
  // an exec param in the subquery may not belong to the query_ref_expr
  // it may be a nlparam of a nest loop join
  bool has_nl_param_;
  bool is_multiset_;
  //子查询的输出列类型
  common::ObSEArray<ObExprResType, 64, common::ModulePageAllocator, true> column_types_;
  common::ObSEArray<ObExecParamRawExpr *, 4, common::ModulePageAllocator, true> exec_params_;
};

inline int64_t ObQueryRefRawExpr::get_ref_id() const
{
  return ref_id_;
}
inline void ObQueryRefRawExpr::set_ref_id(int64_t id)
{
  ref_id_ = id;
}

inline void ObQueryRefRawExpr::set_output_column(int64_t output_column)
{
  output_column_ = output_column;
}

inline int64_t ObQueryRefRawExpr::get_output_column() const
{
  return output_column_;
}

////////////////////////////////////////////////////////////////
class ObColumnRefRawExpr : public ObTerminalRawExpr, public jit::expr::ObColumnRefExpr
{
public:
  ObColumnRefRawExpr()
    : ObIRawExpr(),
      ObTerminalRawExpr(),
      jit::expr::ObColumnRefExpr(),
      table_id_(common::OB_INVALID_ID),
      column_id_(common::OB_INVALID_ID),
      database_name_(),
      table_name_(),
      synonym_name_(),
      synonym_db_name_(),
      column_name_(),
      column_flags_(0),
      dependant_expr_(NULL),
      is_lob_column_(false),
      is_joined_dup_column_(false),
      is_unpivot_mocked_column_(false),
      is_hidden_(false),
      from_alias_table_(false),
      is_rowkey_column_(false),
      is_unique_key_column_(false),
      is_mul_key_column_(false),
      is_strict_json_column_(0),
      srs_id_(UINT64_MAX),
      udt_set_id_(0)
  {
    set_expr_class(ObIRawExpr::EXPR_COLUMN_REF);
  }

  ObColumnRefRawExpr(common::ObIAllocator &alloc)
    : ObIRawExpr(alloc),
      ObTerminalRawExpr(alloc),
      ObColumnRefExpr(alloc),
      table_id_(common::OB_INVALID_ID),
      column_id_(common::OB_INVALID_ID),
      database_name_(),
      table_name_(),
      synonym_name_(),
      synonym_db_name_(),
      column_name_(),
      column_flags_(0),
      dependant_expr_(NULL),
      is_lob_column_(false),
      is_joined_dup_column_(false),
      is_unpivot_mocked_column_(false),
      is_hidden_(false),
      from_alias_table_(false),
      is_rowkey_column_(false),
      is_unique_key_column_(false),
      is_mul_key_column_(false),
      is_strict_json_column_(0),
      srs_id_(UINT64_MAX),
      udt_set_id_(0)
  {
    set_expr_class(ObIRawExpr::EXPR_COLUMN_REF);
  }

  ObColumnRefRawExpr(uint64_t first_id, uint64_t second_id, ObItemType expr_type = T_INVALID)
    : ObIRawExpr(expr_type),
      ObTerminalRawExpr(expr_type),
      ObColumnRefExpr(expr_type),
      table_id_(first_id),
      column_id_(second_id),
      database_name_(),
      table_name_(),
      synonym_name_(),
      synonym_db_name_(),
      column_name_(),
      column_flags_(0),
      dependant_expr_(NULL),
      is_lob_column_(false),
      is_joined_dup_column_(false),
      is_unpivot_mocked_column_(false),
      is_hidden_(false),
      from_alias_table_(false),
      is_rowkey_column_(false),
      is_unique_key_column_(false),
      is_mul_key_column_(false),
      is_strict_json_column_(0),
      srs_id_(UINT64_MAX),
      udt_set_id_(0)
  {
    set_expr_class(ObIRawExpr::EXPR_COLUMN_REF);
  }

  virtual ~ObColumnRefRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  uint64_t get_table_id() const;
  uint64_t get_column_id() const;
  uint64_t &get_table_id();
  uint64_t &get_column_id();
  void set_ref_id(uint64_t table_id, uint64_t column_id);
  void set_table_id(uint64_t table_id);
  void set_hidden_id(uint64_t hidden_id);
  void set_column_attr(const common::ObString &table_name,
                       const common::ObString &column_name);
  inline void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  inline common::ObString &get_table_name() { return table_name_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  inline void set_synonym_name(const common::ObString &synonym_name) { synonym_name_ = synonym_name;  }
  inline common::ObString &get_synonym_name() { return synonym_name_; }
  inline const common::ObString &get_synonym_name() const { return synonym_name_; }
  inline void set_synonym_db_name(const common::ObString &synonym_db_name)
  {
    synonym_db_name_ = synonym_db_name;
  }
  inline common::ObString &get_synonym_db_name() { return synonym_db_name_; }
  inline const common::ObString &get_synonym_db_name() const { return synonym_db_name_; }
  inline void set_column_name(const common::ObString &column_name) { column_name_ = column_name; }
  inline common::ObString &get_column_name() { return column_name_; }
  inline const common::ObString &get_column_name() const { return column_name_; }
  inline void set_database_name(const common::ObString &db_name) { database_name_ = db_name; }
  inline const common::ObString &get_database_name() const { return database_name_; }
  inline common::ObString &get_database_name() { return database_name_; }
  inline int64_t get_cte_generate_column_projector_offset() const { return get_column_id() - common::OB_APP_MIN_COLUMN_ID; }
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;
  inline bool is_generated_column() const { return share::schema::ObSchemaUtils::is_generated_column(column_flags_); }
  inline bool is_identity_column() const { return share::schema::ObSchemaUtils::is_identity_column(column_flags_); }
  inline bool is_default_expr_v2_column() const { return share::schema::ObSchemaUtils::is_default_expr_v2_column(column_flags_); }
  inline bool is_virtual_generated_column() const { return share::schema::ObSchemaUtils::is_virtual_generated_column(column_flags_); }
  inline bool is_stored_generated_column() const { return share::schema::ObSchemaUtils::is_stored_generated_column(column_flags_); }
  inline bool is_always_identity_column() const { return share::schema::ObSchemaUtils::is_always_identity_column(column_flags_); }
  inline bool is_default_identity_column() const { return share::schema::ObSchemaUtils::is_default_identity_column(column_flags_); }
  inline bool is_default_on_null_identity_column() const { return share::schema::ObSchemaUtils::is_default_on_null_identity_column(column_flags_); }
  inline bool is_fulltext_column() const { return share::schema::ObSchemaUtils::is_fulltext_column(column_flags_); }
  inline bool is_spatial_generated_column() const { return share::schema::ObSchemaUtils::is_spatial_generated_column(column_flags_); }
  inline bool is_cte_generated_column() const { return share::schema::ObSchemaUtils::is_cte_generated_column(column_flags_); }
  inline bool has_generated_column_deps() const { return column_flags_ & GENERATED_DEPS_CASCADE_FLAG; }
  inline bool is_table_part_key_column() const { return column_flags_ & TABLE_PART_KEY_COLUMN_FLAG; }
  inline bool is_table_part_key_org_column() const { return column_flags_ & TABLE_PART_KEY_COLUMN_ORG_FLAG; }
  inline bool has_table_alias_name() const { return column_flags_ & TABLE_ALIAS_NAME_FLAG; }
  void set_column_flags(uint64_t column_flags) { column_flags_ = column_flags; }
  void set_table_alias_name() { column_flags_ |= TABLE_ALIAS_NAME_FLAG; }
  inline uint64_t get_column_flags() const { return column_flags_; }
  inline const ObRawExpr *get_dependant_expr() const { return dependant_expr_; }
  inline ObRawExpr *&get_dependant_expr() { return dependant_expr_; }
  inline void set_dependant_expr(ObRawExpr *expr) { dependant_expr_ = expr; }
  bool is_lob_column() const { return is_lob_column_; }
  void set_lob_column(bool is_lob_column) { is_lob_column_ = is_lob_column; }
  bool is_unique_key_column() const { return is_unique_key_column_; }
  void set_unique_key_column(bool v) { is_unique_key_column_ = v; }
  bool is_mul_key_column() const { return is_mul_key_column_; }
  void set_mul_key_column(bool v) { is_mul_key_column_ = v; }
  int8_t is_strict_json_column() const { return is_strict_json_column_; }
  void set_strict_json_column(int8_t v) { is_strict_json_column_ = v; }
  bool is_joined_dup_column() const { return is_joined_dup_column_; }
  void set_joined_dup_column(bool is_joined_dup_column) { is_joined_dup_column_ = is_joined_dup_column; }
  bool is_unpivot_mocked_column() const { return is_unpivot_mocked_column_; }
  void set_unpivot_mocked_column(const bool value) { is_unpivot_mocked_column_ = value; }
  bool is_hidden_column() const { return is_hidden_; }
  void set_hidden_column(const bool value) { is_hidden_ = value; }
  bool is_from_alias_table() const { return from_alias_table_; }
  void set_from_alias_table(bool value) { from_alias_table_ = value; }
  bool is_rowkey_column() const { return is_rowkey_column_; }
  void set_is_rowkey_column(bool value) { is_rowkey_column_ = value; }

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  inline uint64_t get_srs_id() const { return srs_id_; };
  inline void set_srs_id(uint64_t srs_id) { srs_id_ = srs_id; };

  inline uint64_t get_udt_set_id() const { return udt_set_id_; };
  inline void set_udt_set_id(uint64_t udt_set_id) { udt_set_id_ = udt_set_id; };

  bool is_xml_column() const { return ob_is_xml_pl_type(get_data_type(), get_udt_id())
                                      || ob_is_xml_sql_type(get_data_type(), get_subschema_id()); }

  bool is_udt_hidden_column() const { return is_hidden_column() && get_udt_set_id() > 0;}

  inline common::ObGeoType get_geo_type() const { return static_cast<common::ObGeoType>(srs_info_.geo_type_); }

  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_,
                       N_RESULT_TYPE, result_type_,
                       N_EXPR_INFO, info_,
                       N_REL_ID, rel_ids_,
                       N_TID, table_id_,
                       N_CID, column_id_,
                       K_(database_name),
                       K_(table_name),
                       K_(synonym_name),
                       K_(synonym_db_name),
                       K_(column_name),
                       K_(column_flags),
                       K_(enum_set_values),
                       K_(is_lob_column),
                       K_(is_joined_dup_column),
                       K_(is_unpivot_mocked_column),
                       K_(is_hidden),
                       K_(from_alias_table),
                       K_(is_rowkey_column),
                       K_(is_unique_key_column),
                       K_(is_mul_key_column),
                       K_(is_strict_json_column),
                       K_(srs_id),
                       K_(udt_set_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObColumnRefRawExpr);
  uint64_t table_id_;
  uint64_t column_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString synonym_name_;
  common::ObString synonym_db_name_;
  common::ObString column_name_;
  uint64_t column_flags_; //same as flags in ObColumnSchemaV2
  ObRawExpr *dependant_expr_; //TODO: @yuming.wyc @ryan.ly
  bool is_lob_column_; //TODO @hanhui add lob column
  bool is_joined_dup_column_; //is duplicated column in join (not in using). e.g., t1 (c1, c2) cross join t2 (c2,c3), c2 in t1 and t2 are joined_dup_column_
  bool is_unpivot_mocked_column_; //used for unpivot
  bool is_hidden_; //used for print hidden column
  bool from_alias_table_;
  bool is_rowkey_column_;
  bool is_unique_key_column_;
  bool is_mul_key_column_;
  int8_t is_strict_json_column_;
  union { // for geometry column
    struct {
      uint32_t geo_type_ : 5;
      uint32_t reserved_: 27;
      uint32_t srid_ : 32;
    } srs_info_;
    uint64_t srs_id_;
  };
  uint64_t udt_set_id_;
};

inline void ObColumnRefRawExpr::set_ref_id(uint64_t table_id, uint64_t column_id)
{
  table_id_ = table_id;
  column_id_ = column_id;
}

inline void ObColumnRefRawExpr::set_table_id(uint64_t table_id)
{
  table_id_ = table_id;
}

inline uint64_t ObColumnRefRawExpr::get_table_id() const
{
  return table_id_;
}

inline uint64_t ObColumnRefRawExpr::get_column_id() const
{
  return column_id_;
}

inline uint64_t &ObColumnRefRawExpr::get_table_id()
{
  return table_id_;
}

inline uint64_t &ObColumnRefRawExpr::get_column_id()
{
  return column_id_;
}

////////////////////////////////////////////////////////////////
class ObSetOpRawExpr : public ObTerminalRawExpr
{
public:
  ObSetOpRawExpr()
    : ObTerminalRawExpr(), idx_(-1) {
      set_expr_class(ObIRawExpr::EXPR_SET_OP);
    }
  ObSetOpRawExpr(common::ObIAllocator &alloc)
    : ObTerminalRawExpr(alloc), idx_(-1) {
      set_expr_class(ObIRawExpr::EXPR_SET_OP);
    }
  virtual ~ObSetOpRawExpr() {}
  virtual int do_visit(ObRawExprVisitor &visitor) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  int assign(const ObRawExpr &other) override;

  void set_idx(int64_t idx) { idx_ = idx; }
  int64_t get_idx() const { return idx_; }
  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_,
                       N_RESULT_TYPE, result_type_,
                       N_EXPR_INFO, info_,
                       N_REL_ID, rel_ids_,
                       K_(idx));
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetOpRawExpr);
  int64_t idx_; // set op expr 对应 child stmt select expr 的 index
};

////////////////////////////////////////////////////////////////
class ObAliasRefRawExpr : public ObRawExpr
{
public:
  ObAliasRefRawExpr()
    : ObRawExpr(),
      ref_expr_(NULL)
  {
    set_expr_class(ObIRawExpr::EXPR_ALIAS_REF);
  }
  ObAliasRefRawExpr(common::ObIAllocator &alloc)
      : ObRawExpr(alloc),
      ref_expr_(NULL),
      project_index_(OB_INVALID_INDEX)
  {
    set_expr_class(ObIRawExpr::EXPR_ALIAS_REF);
  }

  virtual ~ObAliasRefRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  const ObRawExpr *get_ref_expr() const;
  ObRawExpr *get_ref_expr();
  void set_ref_expr(ObRawExpr *ref_expr) { ref_expr_ = ref_expr; }
  bool is_ref_query_output() const
  { return ref_expr_->is_query_ref_expr() && project_index_ != OB_INVALID_INDEX; }
  int64_t get_project_index() const { return project_index_; }
  void set_query_output(ObQueryRefRawExpr *query_ref,
                        int64_t project_index)
  { ref_expr_ = query_ref; project_index_ = project_index; }
  virtual void clear_child() override { ref_expr_ = NULL; project_index_ = OB_INVALID_INDEX; }
  virtual int64_t get_param_count() const override { return 1; }
  virtual const ObRawExpr *get_param_expr(int64_t index) const override;
  virtual ObRawExpr *&get_param_expr(int64_t index) override;
  virtual int do_visit(ObRawExprVisitor &visitor) override;
  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_VALUE, ref_expr_,
                                            K_(enum_set_values));
private:
  DISALLOW_COPY_AND_ASSIGN(ObAliasRefRawExpr);
  ObRawExpr *ref_expr_;
  int64_t project_index_; // project index of the subquery
};

////////////////////////////////////////////////////////////////
class ObNonTerminalRawExpr : public ObRawExpr
{
public:
  ObNonTerminalRawExpr() : ObRawExpr(), op_(NULL), input_types_()
  {}
  ObNonTerminalRawExpr(common::ObIAllocator &alloc) : ObRawExpr(alloc), op_(NULL), input_types_()
  {}
  virtual ~ObNonTerminalRawExpr() { free_op(); }

  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  virtual void reset() { free_op(); input_types_.reset(); }

  virtual ObExprOperator *get_op();
  void free_op();
  /*
   * 为了在Resolve阶段记录下函数操作数的目标类型，引入input_types_。
   * input_types_在CG阶段会通过get_input_types取出并设置到ObExprOperator中
   * 用于计算阶段对操作数进行动态类型转换
   */
  int set_input_types(const ObIExprResTypes &input_types);

  inline const ObExprResTypes &get_input_types() { return input_types_; }
  inline int64_t get_input_types_count() const { return input_types_.count(); }

  virtual uint64_t hash_internal(uint64_t seed) const { return seed; }
protected:
  // data members
  ObExprOperator *op_;
  ObExprResTypes input_types_;
  DISALLOW_COPY_AND_ASSIGN(ObNonTerminalRawExpr);
};

////////////////////////////////////////////////////////////////
class ObOpRawExpr :
  public ObNonTerminalRawExpr,
  public jit::expr::ObOpExpr
{
public:
  ObOpRawExpr() : ObIRawExpr(),
                  ObNonTerminalRawExpr(),
                  ObOpExpr(),
                  exprs_(),
                  subquery_key_(T_WITH_NONE),
                  deduce_type_adding_implicit_cast_(true)
  { set_expr_class(ObIRawExpr::EXPR_OPERATOR);}

  ObOpRawExpr(common::ObIAllocator &alloc) : ObIRawExpr(alloc),
                                             ObNonTerminalRawExpr(alloc),
                                             ObOpExpr(alloc),
                                             exprs_(),
                                             subquery_key_(T_WITH_NONE),
                                             deduce_type_adding_implicit_cast_(true)
  { set_expr_class(ObIRawExpr::EXPR_OPERATOR); }

  ObOpRawExpr(ObRawExpr *first_expr, ObRawExpr *second_expr, ObItemType type); //binary op
  virtual ~ObOpRawExpr() {}
  int assign(const ObRawExpr &other) override;

  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  int set_param_expr(ObRawExpr *expr);  // unary op
  int set_param_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr); // binary op
  int set_param_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr, ObRawExpr *third_expr); // triple op
  int add_param_expr(ObRawExpr *expr);
  int remove_param_expr(int64_t index);
  int replace_param_expr(int64_t index, ObRawExpr *expr);

  bool deduce_type_adding_implicit_cast() const { return deduce_type_adding_implicit_cast_; }
  void set_deduce_type_adding_implicit_cast(const bool v) { deduce_type_adding_implicit_cast_ = v; }

  void set_expr_type(ObItemType type);

  common::ObIArray<ObRawExpr *> &get_param_exprs() { return exprs_; }
  const common::ObIArray<ObRawExpr *> &get_param_exprs() const { return exprs_; }

  int64_t get_param_count() const;
  const ObRawExpr *get_param_expr(int64_t index) const;
  ObRawExpr *&get_param_expr(int64_t index);
  virtual int64_t get_output_column() const
  {
    return T_OP_ROW == get_expr_type() ? get_param_count() : -1;
  }

  virtual void clear_child() override;
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  //used for jit expr
  virtual int64_t get_children_count() const
  {
    return exprs_.count();
  }
  //used for jit expr
  virtual int get_children(jit::expr::ExprArray &jit_exprs) const;

  //如果操作符中后面跟随的是一个带关键字的子查询，需要记录下该关键字
  void set_subquery_key(ObSubQueryKey &key) { subquery_key_ = key; }
  ObSubQueryKey get_subquery_key() { return subquery_key_; }

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  int get_subquery_comparison_name(const common::ObString &symbol,
                                   char *buf,
                                   int64_t buf_len,
                                   int64_t &pos,
                                   ExplainType type) const;
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_CHILDREN, exprs_);
protected:
  common::ObSEArray<ObRawExpr *, COMMON_MULTI_NUM, common::ModulePageAllocator, true> exprs_;
  ObSubQueryKey subquery_key_;

  bool deduce_type_adding_implicit_cast_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOpRawExpr);
};


inline const ObRawExpr *ObOpRawExpr::get_param_expr(int64_t index) const
{
  const ObRawExpr *expr = NULL;
  if (index >= 0 && index < exprs_.count()) {
    expr = exprs_.at(index);
  }
  return expr;
}

//here must return in two branch, because ret value is a *&
inline ObRawExpr *&ObOpRawExpr::get_param_expr(int64_t index)
{
  if (index >= 0 && index < exprs_.count()) {
    return exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}

inline int ObOpRawExpr::add_param_expr(ObRawExpr *expr)
{
  return exprs_.push_back(expr);
}

inline int ObOpRawExpr::remove_param_expr(int64_t index)
{
  int ret = common::OB_SUCCESS;
  if (index < 0 || index >= exprs_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = exprs_.remove(index);
  }
  return ret;
}

inline int ObOpRawExpr::replace_param_expr(int64_t index, ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (index < 0 || index >= exprs_.count()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObRawExpr *&target_expr = exprs_.at(index);
    target_expr = expr;
  }
  return ret;
}

inline int64_t ObOpRawExpr::get_param_count() const
{
  return exprs_.count();
}

inline uint64_t ObOpRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_value = seed;
  for (int64_t i = 0; i < exprs_.count(); ++i) {
    if (NULL != exprs_.at(i)) {
      hash_value = common::do_hash(*(exprs_.at(i)), hash_value);
    }
  }
  return hash_value;
}

////////////////////////////////////////////////////////////////
class ObCaseOpRawExpr : public ObNonTerminalRawExpr, public jit::expr::ObCaseOpExpr
{
public:
  ObCaseOpRawExpr()
      : ObIRawExpr(),
      ObNonTerminalRawExpr(),
      ObCaseOpExpr(),
      arg_expr_(NULL),
      when_exprs_(),
      then_exprs_(),
      default_expr_(NULL),
      is_decode_func_(false)
  {
    set_expr_class(ObIRawExpr::EXPR_CASE_OPERATOR);
  }
  ObCaseOpRawExpr(common::ObIAllocator &alloc)
      : ObIRawExpr(alloc),
      ObNonTerminalRawExpr(alloc),
      ObCaseOpExpr(),
      arg_expr_(NULL),
      when_exprs_(),
      then_exprs_(),
      default_expr_(NULL),
      is_decode_func_(false)
  {
    set_expr_class(ObIRawExpr::EXPR_CASE_OPERATOR);
  }
  virtual ~ObCaseOpRawExpr() {}
  int assign(const ObRawExpr &other) override;

  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  const ObRawExpr *get_arg_param_expr() const;
  const ObRawExpr *get_default_param_expr() const;
  const ObRawExpr *get_when_param_expr(int64_t index) const;
  const ObRawExpr *get_then_param_expr(int64_t index) const;
  ObRawExpr *&get_arg_param_expr();
  inline common::ObIArray<ObRawExpr*> &get_when_param_exprs() { return when_exprs_; }
  inline const common::ObIArray<ObRawExpr*> &get_when_param_exprs() const { return when_exprs_; }
  inline common::ObIArray<ObRawExpr*> &get_then_param_exprs() { return then_exprs_; }
  inline const common::ObIArray<ObRawExpr*> &get_then_param_exprs() const { return then_exprs_; }
  ObRawExpr *&get_default_param_expr();
  ObRawExpr *&get_when_param_expr(int64_t index);
  ObRawExpr *&get_then_param_expr(int64_t index);
  void set_arg_param_expr(ObRawExpr *expr);
  void set_default_param_expr(ObRawExpr *expr);
  int add_when_param_expr(ObRawExpr *expr);
  int add_then_param_expr(ObRawExpr *expr);
  int replace_when_param_expr(int64_t index, ObRawExpr *expr);
  int replace_then_param_expr(int64_t index, ObRawExpr *expr);
  int replace_param_expr(int64_t index, ObRawExpr *new_expr);
  int64_t get_when_expr_size() const;
  int64_t get_then_expr_size() const;
  bool is_arg_case() const { return NULL != arg_expr_; }
  bool is_decode_func() const { return is_decode_func_; }

  virtual void clear_child() override;
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual int64_t get_param_count() const;
  virtual const ObRawExpr *get_param_expr(int64_t index) const;
  virtual ObRawExpr *&get_param_expr(int64_t index);

  //used for jit
  virtual int64_t get_children_count() const
  {
    return get_param_count();
  }

  virtual int get_children(jit::expr::ExprArray &jit_exprs) const;

  virtual uint64_t hash_internal(uint64_t seed) const;

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_ARG_CASE, arg_expr_,
                                            N_DEFAULT, default_expr_,
                                            N_WHEN, when_exprs_,
                                            N_THEN, then_exprs_,
                                            N_DECODE, is_decode_func_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCaseOpRawExpr);
  ObRawExpr *arg_expr_;
  common::ObSEArray<ObRawExpr *, COMMON_MULTI_NUM, common::ModulePageAllocator, true> when_exprs_;
  common::ObSEArray<ObRawExpr *, COMMON_MULTI_NUM, common::ModulePageAllocator, true> then_exprs_;
  ObRawExpr *default_expr_;
  bool is_decode_func_;
};

inline const ObRawExpr *ObCaseOpRawExpr::get_arg_param_expr() const
{
  return arg_expr_;
}
inline const ObRawExpr *ObCaseOpRawExpr::get_default_param_expr() const
{
  return default_expr_;
}
inline const ObRawExpr *ObCaseOpRawExpr::get_when_param_expr(int64_t index) const
{
  ObRawExpr *expr = NULL;
  if (OB_LIKELY(index >= 0 && index < when_exprs_.count())) {
    expr = when_exprs_.at(index);
  } else {}
  return expr;
}
inline const ObRawExpr *ObCaseOpRawExpr::get_then_param_expr(int64_t index) const
{
  ObRawExpr *expr = NULL;
  if (OB_LIKELY(index >= 0 || index < then_exprs_.count())) {
    expr = then_exprs_.at(index);
  } else {}
  return expr;
}
inline ObRawExpr *&ObCaseOpRawExpr::get_arg_param_expr()
{
  return arg_expr_;
}
inline ObRawExpr *&ObCaseOpRawExpr::get_default_param_expr()
{
  return default_expr_;
}

//here must return in two branch, because ret value is a *&
inline ObRawExpr *&ObCaseOpRawExpr::get_when_param_expr(int64_t index)
{
  if (OB_LIKELY(index >= 0 && index < when_exprs_.count())) {
    return when_exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}

//here must return in two branch, because ret value is a *&
inline ObRawExpr *&ObCaseOpRawExpr::get_then_param_expr(int64_t index)
{
  if (OB_LIKELY(index >= 0 && index < then_exprs_.count())) {
    return then_exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}
inline void ObCaseOpRawExpr::set_arg_param_expr(ObRawExpr *expr)
{
  arg_expr_ = expr;
}
inline void ObCaseOpRawExpr::set_default_param_expr(ObRawExpr *expr)
{
  default_expr_ = expr;
}
inline int ObCaseOpRawExpr::add_when_param_expr(ObRawExpr *expr)
{
  int ret = when_exprs_.push_back(expr);
  return ret;
}
inline int ObCaseOpRawExpr::add_then_param_expr(ObRawExpr *expr)
{
  int ret = then_exprs_.push_back(expr);
  return ret;
}

inline int ObCaseOpRawExpr::replace_when_param_expr(int64_t index, ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY((index < 0 || index >= when_exprs_.count()))) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    when_exprs_.at(index) = expr;
  }
  return ret;
}

inline int ObCaseOpRawExpr::replace_then_param_expr(int64_t index,
                                                    ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY((index < 0 || index >= then_exprs_.count()))) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    then_exprs_.at(index) = expr;
  }
  return ret;
}

inline int64_t ObCaseOpRawExpr::get_when_expr_size() const
{
  return when_exprs_.count();
}
inline int64_t ObCaseOpRawExpr::get_then_expr_size() const
{
  return then_exprs_.count();
}
inline int64_t ObCaseOpRawExpr::get_param_count() const
{
  return when_exprs_.count() + then_exprs_.count()
      + (NULL == arg_expr_ ? 0 : 1)
      + (NULL == default_expr_ ? 0 : 1);
}
inline uint64_t ObCaseOpRawExpr::hash_internal(uint64_t seed) const
{
  if (arg_expr_) {
    seed = common::do_hash(*arg_expr_, seed);
  }

  for (int64_t i = 0; i < when_exprs_.count(); ++i) {
    if (OB_LIKELY(NULL != when_exprs_.at(i))) {
      seed = common::do_hash(*(when_exprs_.at(i)), seed);
    }
  }
  for (int64_t i = 0; i < then_exprs_.count(); ++i) {
    if (OB_LIKELY(NULL != then_exprs_.at(i))) {
      seed = common::do_hash(*(then_exprs_.at(i)), seed);
    }
  }

  if (NULL != default_expr_) {
    seed = common::do_hash(*default_expr_, seed);
  }

  seed = common::do_hash(is_decode_func_, seed);

  return seed;
}

////////////////////////////////////////////////////////////////
class ObAggFunRawExpr : public ObRawExpr
{
public:
  ObAggFunRawExpr()
    : ObRawExpr(),
    real_param_exprs_(),
    distinct_(false),
    order_items_(),
    separator_param_expr_(NULL),
    udf_meta_(),
    is_nested_aggr_(false),
    is_need_deserialize_row_(false),
    pl_agg_udf_expr_(NULL)
  {
    set_expr_class(ObIRawExpr::EXPR_AGGR);
    order_items_.set_label(common::ObModIds::OB_SQL_AGGR_FUNC_ARR);
  }
  ObAggFunRawExpr(common::ObIAllocator &alloc)
    : ObRawExpr(alloc),
    real_param_exprs_(),
    distinct_(false),
    order_items_(),
    separator_param_expr_(NULL),
    udf_meta_(),
    is_nested_aggr_(false),
    is_need_deserialize_row_(false),
    pl_agg_udf_expr_(NULL)
  {
    set_expr_class(ObIRawExpr::EXPR_AGGR);
    order_items_.set_label(common::ObModIds::OB_SQL_AGGR_FUNC_ARR);
  }
  ObAggFunRawExpr(const common::ObSEArray<ObRawExpr *, 1, common::ModulePageAllocator, true> &real_param_exprs,
                  bool is_distinct, ObItemType expr_type = T_INVALID)
    : ObRawExpr(expr_type),
    real_param_exprs_(real_param_exprs),
    distinct_(is_distinct),
    order_items_(),
    separator_param_expr_(NULL),
    udf_meta_(),
    is_nested_aggr_(false),
    is_need_deserialize_row_(false),
    pl_agg_udf_expr_(NULL)
  {
    set_expr_class(ObIRawExpr::EXPR_AGGR);
    order_items_.set_label(common::ObModIds::OB_SQL_AGGR_FUNC_ARR);
  }
  virtual ~ObAggFunRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  int add_real_param_expr(ObRawExpr *expr);
  int replace_real_param_expr(int64_t index, ObRawExpr *expr);
  int replace_param_expr(int64_t index, ObRawExpr *expr);
  bool contain_nested_aggr() const;
  bool is_param_distinct() const;
  void set_param_distinct(bool is_distinct);
  void set_separator_param_expr(ObRawExpr *separator_param_expr);
  bool is_nested_aggr() const;
  void set_in_nested_aggr(bool is_nested);
  int add_order_item(const OrderItem &order_item);
  virtual void clear_child() override;
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int64_t get_param_count() const;
  virtual const ObRawExpr *get_param_expr(int64_t index) const;
  virtual ObRawExpr *&get_param_expr(int64_t index);
  inline int64_t get_real_param_count() const { return real_param_exprs_.count(); }
  inline const common::ObIArray<ObRawExpr*> &get_real_param_exprs() const { return real_param_exprs_; }
  inline common::ObIArray<ObRawExpr*> &get_real_param_exprs_for_update() { return real_param_exprs_; }
  virtual int do_visit(ObRawExprVisitor &visitor) override;
  inline ObRawExpr *get_separator_param_expr() const { return separator_param_expr_; }
  inline const common::ObIArray<OrderItem> &get_order_items() const { return order_items_; }
  //可能在外面修改order_items_，慎用
  inline common::ObIArray<OrderItem> &get_order_items_for_update() { return order_items_; }
  inline bool is_need_deserialize_row() const { return is_need_deserialize_row_; }
  void set_is_need_deserialize_row(bool is_need) { is_need_deserialize_row_ = is_need; }

  inline void set_pl_agg_udf_expr(ObRawExpr *udf_expr) { pl_agg_udf_expr_ = udf_expr; }
  inline ObRawExpr *get_pl_agg_udf_expr() const { return pl_agg_udf_expr_; }

  virtual uint64_t hash_internal(uint64_t seed) const
  {
    for (int64_t i = 0; i < real_param_exprs_.count(); ++i) {
      if (OB_LIKELY(NULL != real_param_exprs_.at(i))) {
        seed = common::do_hash(*real_param_exprs_.at(i), seed);
      }
    }
    seed = common::do_hash(distinct_, seed);
    for (int64_t i = 0; i < order_items_.count(); ++i) {
      seed = common::do_hash(order_items_.at(i), seed);
    }
    if (NULL != separator_param_expr_) {
      seed = common::do_hash(*separator_param_expr_, seed);
    }
    seed = common::do_hash(is_need_deserialize_row_, seed);
    return seed;
  }

  //set udf meta to this expr
  int set_udf_meta(const share::schema::ObUDF &udf);
  const share::schema::ObUDFMeta get_udf_meta() { return udf_meta_; }

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  const char *get_name_dblink(ObItemType expr_type) const;
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_CHILDREN, real_param_exprs_,
                                            N_DISTINCT, distinct_,
                                            N_ORDER_BY, order_items_,
                                            N_SEPARATOR_PARAM_EXPR, separator_param_expr_,
                                            K_(udf_meta),
                                            K_(is_nested_aggr),
                                            K_(pl_agg_udf_expr));
private:
  DISALLOW_COPY_AND_ASSIGN(ObAggFunRawExpr);
  // real_param_exprs_.count() == 0 means '*'
  common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> real_param_exprs_;
  bool distinct_;
  // used for group_concat/rank/percent rank/dense rank/cume dist
  common::ObArray<OrderItem, common::ModulePageAllocator, true> order_items_;
  ObRawExpr *separator_param_expr_;
  //use for udf function info
  share::schema::ObUDFMeta udf_meta_;
  bool is_nested_aggr_;
  bool is_need_deserialize_row_;// for topk histogram and hybrid histogram computation
  ObRawExpr *pl_agg_udf_expr_;//for pl agg udf expr
};

inline int ObAggFunRawExpr::add_real_param_expr(ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = real_param_exprs_.push_back(expr);
  }
  return ret;
}

inline int ObAggFunRawExpr::replace_real_param_expr(int64_t index, ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= real_param_exprs_.count())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObRawExpr *&target_expr = real_param_exprs_.at(index);
    target_expr = expr;
  }
  return ret;
}

inline int ObAggFunRawExpr::replace_param_expr(int64_t index, ObRawExpr *expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= get_param_count())) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(NULL == expr)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (index >= real_param_exprs_.count()) {
    ObRawExpr *&target_expr = order_items_.at(index - real_param_exprs_.count()).expr_;
    target_expr = expr;
  } else {
    ObRawExpr *&target_expr = real_param_exprs_.at(index);
    target_expr = expr;
  }
  return ret;
}

inline bool ObAggFunRawExpr::is_param_distinct() const
{
  return distinct_;
}
inline bool ObAggFunRawExpr::is_nested_aggr() const
{
  return is_nested_aggr_;
}
inline bool ObAggFunRawExpr::contain_nested_aggr() const
{
  bool ret = false;
  for (int64 i = 0; !ret && i < get_param_count(); i++) {
    if (get_param_expr(i)->has_flag(CNT_AGG)) {
      ret = true;
    } else { /*do nothing.*/ }
  }
  return ret;
}
inline void ObAggFunRawExpr::set_param_distinct(bool is_distinct) { distinct_ = is_distinct; }
inline void ObAggFunRawExpr::set_in_nested_aggr(bool is_nested) { is_nested_aggr_ = is_nested; }
inline void ObAggFunRawExpr::set_separator_param_expr(ObRawExpr *separator_param_expr)
{
  separator_param_expr_ = separator_param_expr;
}
inline int ObAggFunRawExpr::add_order_item(const OrderItem &order_item)
{
  return order_items_.push_back(order_item);
}

////////////////////////////////////////////////////////////////
// for normal system function, func_name_ is used to distinguish them.
// for special system function, ObRawExpr::type_ can be reset. Such function may not need name
class ObSysFunRawExpr : public ObOpRawExpr
{
public:
  ObSysFunRawExpr(common::ObIAllocator &alloc)
    : ObOpRawExpr(alloc),
      func_name_(),
      operator_id_(common::OB_INVALID_ID),
      dblink_id_(common::OB_INVALID_ID)
      { set_expr_class(ObIRawExpr::EXPR_SYS_FUNC); }
  ObSysFunRawExpr()
    : ObOpRawExpr(),
      func_name_(),
      operator_id_(common::OB_INVALID_ID),
      dblink_id_(common::OB_INVALID_ID)
    { set_expr_class(ObIRawExpr::EXPR_SYS_FUNC); }
  virtual ~ObSysFunRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  void set_func_name(const common::ObString &name);
  const common::ObString &get_func_name() const;
  virtual void clear_child() override;
  int check_param_num();
  int check_param_num(int param_count);
  virtual ObExprOperator *get_op();
  virtual void reset();
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const
  {
    uint64_t hash_ret = common::do_hash(func_name_, seed);
    return hash_ret;
  }

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  int get_cast_type_name(char *buf, int64_t buf_len, int64_t &pos) const;
  int get_column_conv_name(char *buf, int64_t buf_len, int64_t &pos, ExplainType type) const;
  int get_autoinc_nextval_name(char *buf, int64_t buf_len, int64_t &pos) const;
  void set_op_id(int64_t operator_id) { operator_id_ = operator_id; }
  int64_t get_op_id() const { return operator_id_; }
  void set_dblink_name(const common::ObString &name) { dblink_name_ = name; }
  const common::ObString &get_dblink_name() const { return dblink_name_; }
  void set_dblink_id(int64_t dblink_id) { dblink_id_ = dblink_id; }
  int64_t get_dblink_id() const { return dblink_id_; }
  bool is_dblink_sys_func() const { return common::OB_INVALID_ID != dblink_id_; }

  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_FUNC, func_name_,
                                            N_CHILDREN, exprs_,
                                            K_(enum_set_values),
                                            K_(dblink_name),
                                            K_(dblink_id));
private:
  int check_param_num_internal(int32_t param_num, int32_t param_count, ObExprOperatorType type);
  DISALLOW_COPY_AND_ASSIGN(ObSysFunRawExpr);
  common::ObString func_name_;
  common::ObString dblink_name_;
  //用于记录rownum表达式归属的count算子的op_id_
  uint64_t operator_id_;
  uint64_t dblink_id_;
};

inline void ObSysFunRawExpr::set_func_name(const common::ObString &name)
{
  func_name_ = name;
}
inline const common::ObString &ObSysFunRawExpr::get_func_name() const
{
  return func_name_;
}

class ObSequenceRawExpr : public ObSysFunRawExpr
{
public:
  ObSequenceRawExpr(common::ObIAllocator &alloc)
      : ObSysFunRawExpr(alloc), database_name_(), name_(), action_(), sequence_id_(0) {}
  ObSequenceRawExpr()
      : ObSysFunRawExpr(), database_name_(), name_(), action_(), sequence_id_(0) {}
  virtual ~ObSequenceRawExpr() = default;
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  int set_sequence_meta(const common::ObString &database_name,
                        const common::ObString &name,
                        const common::ObString &action,
                        uint64_t sequence_id);
  const common::ObString &get_database_name() { return database_name_; }
  const common::ObString &get_name() { return name_; }
  const common::ObString &get_action() const { return action_; }
  uint64_t get_sequence_id() const { return sequence_id_; }
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;
  virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const override;
private:
  common::ObString database_name_; // sequence database name
  common::ObString name_; // sequence object name
  common::ObString action_; // NEXTVAL or CURRVAL
  uint64_t sequence_id_; // 这个值也包装成 expr 放到 ObSysFunRawExpr 的 param 中了
};

class ObNormalDllUdfRawExpr : public ObSysFunRawExpr
{
public:
  ObNormalDllUdfRawExpr(common::ObIAllocator &alloc) : ObSysFunRawExpr(alloc), udf_meta_(), udf_attributes_() {}
  ObNormalDllUdfRawExpr() : ObSysFunRawExpr(), udf_meta_(), udf_attributes_() {}
  virtual ~ObNormalDllUdfRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  int set_udf_meta(const share::schema::ObUDF &udf);
  int add_udf_attribute_name(const common::ObString &name);
  int add_udf_attribute(const ObRawExpr *expr, const ParseNode *node);
  const share::schema::ObUDFMeta &get_udf_meta() const { return udf_meta_; }
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;
private:
  //for udf function info
  share::schema::ObUDFMeta udf_meta_;
  common::ObSEArray<common::ObString, 16> udf_attributes_;// name of input expr
};

class ObCollectionConstructRawExpr : public ObSysFunRawExpr
{
public:
  ObCollectionConstructRawExpr(common::ObIAllocator &alloc)
    : ObSysFunRawExpr(alloc),
      type_(pl::ObPLType::PL_INVALID_TYPE),
      elem_type_(),
      capacity_(OB_INVALID_SIZE),
      udt_id_(OB_INVALID_ID),
      database_id_(OB_INVALID_ID),
      coll_schema_version_(common::OB_INVALID_VERSION) {}
  ObCollectionConstructRawExpr()
    : ObSysFunRawExpr(),
      type_(pl::ObPLType::PL_INVALID_TYPE),
      elem_type_(),
      capacity_(OB_INVALID_SIZE),
      udt_id_(OB_INVALID_ID),
      database_id_(OB_INVALID_ID),
      coll_schema_version_(common::OB_INVALID_VERSION) {}
  virtual ~ObCollectionConstructRawExpr() {}

  inline void set_type(pl::ObPLType type) { type_ = type; }
  inline void set_elem_type(const pl::ObPLDataType &type) { new(&elem_type_)pl::ObPLDataType(type); }
  inline void set_capacity(int64_t capacity) { capacity_ = capacity; }
  inline void set_udt_id(uint64_t udt_id) { udt_id_ = udt_id; }

  int set_access_names(const common::ObIArray<ObObjAccessIdent> &access_idents);
  const common::ObIArray<ObString>& get_access_names() const { return access_names_; }

  pl::ObPLType get_type() const { return type_; }
  bool is_not_null() const { return elem_type_.is_not_null(); }
  const pl::ObPLDataType& get_elem_type() const { return elem_type_; }
  int64_t get_capacity() const { return capacity_; }
  uint64_t get_udt_id() const { return udt_id_; }

  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;

  virtual ObExprOperator *get_op() override;

  inline void set_database_id(int64_t database_id)
  {
    database_id_ = database_id;
  }

  OB_INLINE uint64_t get_database_id() const { return database_id_; }

  inline void set_coll_schema_version(int64_t schema_version)
  {
    coll_schema_version_ = schema_version;
  }

  inline bool need_add_dependency()
  {
    return coll_schema_version_ != common::OB_INVALID_VERSION;
  }

  int get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version);

  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_FUNC, get_func_name(),
                                            N_CHILDREN, exprs_,
                                            K_(coll_schema_version));
private:
  pl::ObPLType type_; // PL_NESTED_TABLE_TYPE|PL_ASSOCIATIVE_ARRAY_TYPE|PL_VARRAY_TYPE
  pl::ObPLDataType elem_type_; // 记录复杂数据类型的元素类型
  int64_t capacity_; //记录VArray的容量，对于NestedTable为-1
  uint64_t udt_id_; // 记录复杂类型的ID
  // 用于打印构造函数的名字
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> access_names_;
  int64_t database_id_;
  int64_t coll_schema_version_;
};

class ObObjectConstructRawExpr : public ObSysFunRawExpr
{
public:
  ObObjectConstructRawExpr(common::ObIAllocator &alloc)
    : ObSysFunRawExpr(alloc),
      rowsize_(0),
      udt_id_(OB_INVALID_ID),
      elem_types_(),
      access_names_(),
      database_id_(OB_INVALID_ID),
      object_schema_version_(common::OB_INVALID_VERSION) {}
  ObObjectConstructRawExpr()
    : ObSysFunRawExpr(),
      rowsize_(0),
      udt_id_(OB_INVALID_ID),
      elem_types_(),
      access_names_(),
      database_id_(OB_INVALID_ID),
      object_schema_version_(common::OB_INVALID_VERSION) {}

  virtual ~ObObjectConstructRawExpr() {}

  inline void set_rowsize(int64_t rowsize) { rowsize_ = rowsize; }
  int64_t get_rowsize() { return rowsize_; }

  inline void set_udt_id(uint64_t udt_id) { udt_id_ = udt_id; }
  uint64_t get_udt_id() { return udt_id_; }

  inline int add_elem_type(ObExprResType &elem_type)
  {
    return elem_types_.push_back(elem_type);
  }
  inline int set_elem_types(common::ObIArray<ObExprResType> &elem_types)
  {
    return elem_types_.assign(elem_types);
  }
  inline const common::ObIArray<ObExprResType>& get_elem_types()
  {
    return elem_types_;
  }

  int set_access_names(const common::ObIArray<ObObjAccessIdent> &access_idents);
  int add_access_name(const common::ObString &access_name) { return access_names_.push_back(access_name); }
  const common::ObIArray<ObString>& get_access_names() const { return access_names_; }

  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;

  inline void set_database_id(int64_t database_id)
  {
    database_id_ = database_id;
  }

  OB_INLINE uint64_t get_database_id() const { return database_id_; }

  inline void set_coll_schema_version(int64_t schema_version)
  {
    object_schema_version_ = schema_version;
  }

  inline bool need_add_dependency()
  {
    return object_schema_version_ != common::OB_INVALID_VERSION;
  }

  int get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version);

  virtual ObExprOperator *get_op() override;

  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_FUNC, get_func_name(),
                                            N_CHILDREN, exprs_,
                                            K_(database_id),
                                            K_(object_schema_version));
private:
  int64_t rowsize_;
  uint64_t udt_id_;
  // 记录Object每个元素的类型
  common::ObSEArray<ObExprResType, 5, common::ModulePageAllocator, true> elem_types_;
  // 用于打印构造函数的名字
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> access_names_;
  int64_t database_id_;
  int64_t object_schema_version_;
};

class ObUDFParamDesc
{
public:
  enum OutType {
    NOT_OUT,
    OBJ_ACCESS_OUT,
    LOCAL_OUT,
    PACKAGE_VAR_OUT,
    SUBPROGRAM_VAR_OUT
  };

  OutType type_;
  int64_t id1_; // variable index
  int64_t id2_; // subprogram id
  int64_t id3_; // package id

  ObUDFParamDesc()
    : type_(OutType::NOT_OUT), id1_(OB_INVALID_ID), id2_(OB_INVALID_ID), id3_(OB_INVALID_ID) {}
  ObUDFParamDesc(OutType type,
                 int64_t id1 = OB_INVALID_ID,
                 int64_t id2 = OB_INVALID_ID,
                 int64_t id3 = OB_INVALID_ID)
    : type_(type), id1_(id1), id2_(id2), id3_(id3) {}
  OB_INLINE bool operator==(const ObUDFParamDesc &other) const {
    return type_ == other.type_ && id1_ == other.id1_ &&
           id2_ == other.id2_ && id3_ == other.id3_; }
  OB_INLINE bool is_out() const { return type_ != NOT_OUT; }
  OB_INLINE bool is_local_out() const { return OutType::LOCAL_OUT == type_; }
  OB_INLINE bool is_package_var_out() const { return OutType::PACKAGE_VAR_OUT == type_; }
  OB_INLINE bool is_subprogram_var_out() const { return OutType::SUBPROGRAM_VAR_OUT == type_; }
  OB_INLINE bool is_obj_access_out() const { return OutType::OBJ_ACCESS_OUT == type_; }

  OB_INLINE int64_t get_index() const { return id1_; }
  OB_INLINE int64_t get_subprogram_id() const { return id2_; }
  OB_INLINE int64_t get_package_id() const { return id3_; }

  TO_STRING_KV(K_(type), K_(id1), K_(id2), K_(id3));
  NEED_SERIALIZE_AND_DESERIALIZE;
};
class ObUDFRawExpr : public ObSysFunRawExpr
{
public:
  ObUDFRawExpr(common::ObIAllocator &alloc)
    : ObSysFunRawExpr(alloc),
      udf_id_(common::OB_INVALID_ID),
      pkg_id_(common::OB_INVALID_ID),
      type_id_(common::OB_INVALID_ID),
      subprogram_path_(),
      udf_schema_version_(common::OB_INVALID_VERSION),
      pkg_schema_version_(common::OB_INVALID_VERSION),
      pls_type_(pl::PL_INTEGER_INVALID),
      params_type_(),
      database_name_(),
      package_name_(),
      is_deterministic_(false),
      is_parallel_enable_(false),
      is_udt_udf_(false),
      is_pkg_body_udf_(false),
      is_return_sys_cursor_(false),
      is_aggregate_udf_(false),
      is_aggr_udf_distinct_(false),
      nocopy_params_(),
      loc_(0),
      is_udt_cons_(false),
      params_name_(),
      params_desc_v2_() {
    set_expr_class(ObIRawExpr::EXPR_UDF);
  }

  ObUDFRawExpr()
    : ObSysFunRawExpr(),
      udf_id_(common::OB_INVALID_ID),
      pkg_id_(common::OB_INVALID_ID),
      type_id_(common::OB_INVALID_ID),
      subprogram_path_(),
      udf_schema_version_(common::OB_INVALID_VERSION),
      pkg_schema_version_(common::OB_INVALID_VERSION),
      pls_type_(pl::PL_INTEGER_INVALID),
      params_type_(),
      database_name_(),
      package_name_(),
      is_deterministic_(false),
      is_parallel_enable_(false),
      is_udt_udf_(false),
      is_pkg_body_udf_(false),
      is_return_sys_cursor_(false),
      is_aggregate_udf_(false),
      is_aggr_udf_distinct_(false),
      nocopy_params_(),
      loc_(0),
      is_udt_cons_(false),
      params_name_(),
      params_desc_v2_() {
    set_expr_class(ObIRawExpr::EXPR_UDF);
  }

  virtual ~ObUDFRawExpr() {}

  inline void set_udf_id(uint64_t udf_id){ udf_id_ = udf_id; }
  inline int set_subprogram_path(const ObIArray<int64_t> &path)
  {
    return subprogram_path_.assign(path);
  }
  inline void set_udf_schema_version(int64_t schema_version)
  {
    udf_schema_version_ = schema_version;
  }
  inline void set_pkg_schema_version(int64_t schema_version)
  {
    pkg_schema_version_ = schema_version;
  }
  inline void set_pls_type(const pl::ObPLIntegerType pls_type)
  {
    pls_type_ = pls_type;
  }
  inline int set_params_type(common::ObIArray<ObExprResType> &params_type)
  {
    return params_type_.assign(params_type);
  }
  inline void set_database_name(const common::ObString &database_name)
  {
    database_name_ = database_name;
  }
  inline void set_package_name(const common::ObString &package_name)
  {
    package_name_ = package_name;
  }
  inline int add_param_desc(ObUDFParamDesc desc)
  {
    return params_desc_v2_.push_back(desc);
  }
  inline bool is_param_out(int64_t i) const
  {
    return params_desc_v2_.at(i).is_out();
  }
  inline int64_t get_param_position(int64_t i) const
  {
    return params_desc_v2_.at(i).get_index();
  }
  inline bool has_param_out() const
  {
    for (int64_t i = 0; i < params_desc_v2_.count(); ++i) {
      if (is_param_out(i)) {
        return true;
      }
    }
    return false;
  }
  inline int add_param_name(common::ObString &name)
  {
    return params_name_.push_back(name);
  }
  inline common::ObIArray<common::ObString> &get_params_name()
  {
    return params_name_;
  }
  inline const common::ObIArray<common::ObString> &get_params_name() const
  {
    return params_name_;
  }
  inline void set_pkg_id(uint64_t pkg_id){ pkg_id_ = pkg_id; }
  inline uint64_t get_pkg_id() const { return pkg_id_; }
  inline uint64_t get_udf_id() const { return udf_id_; }
  inline const ObIArray<int64_t> &get_subprogram_path() const { return subprogram_path_; }
  inline pl::ObPLIntegerType get_pls_type() const { return pls_type_; }
  inline common::ObIArray<ObExprResType> &get_params_type() { return params_type_; }
  inline const common::ObIArray<ObExprResType> &get_params_type() const { return params_type_; }
  inline common::ObString get_database_name() const { return database_name_; }
  inline common::ObString get_package_name() const { return package_name_; }
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;
  virtual ObExprOperator *get_op() override;

  int check_param() { return common::OB_SUCCESS; }

  virtual uint64_t hash_internal(uint64_t seed) const override
  {
    uint64_t hash_ret = common::do_hash(udf_id_, seed);
    return hash_ret;
  }

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const override;

  inline void set_parallel_enable(bool is_parallel_enable) { is_parallel_enable_ = is_parallel_enable; }
  inline bool is_parallel_enable() const { return is_parallel_enable_; }

  inline void set_is_udt_udf(bool is_udt_udf) { is_udt_udf_ = is_udt_udf; }
  inline bool get_is_udt_udf() const { return is_udt_udf_; }
  inline void set_is_return_sys_cursor(bool is_ret_cursor) { is_return_sys_cursor_ = is_ret_cursor; }
  inline bool get_is_return_sys_cursor() const { return is_return_sys_cursor_; }

  inline void set_type_id(uint64_t type_id) { type_id_ = type_id; }
  inline uint64_t get_type_id() const { return type_id_; }
  inline void set_is_aggregate_udf(bool is_aggregate_udf) { is_aggregate_udf_ = is_aggregate_udf; }
  inline bool get_is_aggregate_udf() const { return is_aggregate_udf_; }
  inline void set_is_aggr_udf_distinct(bool is_aggr_udf_distinct) {
    is_aggr_udf_distinct_ = is_aggr_udf_distinct; }
  inline bool get_is_aggr_udf_distinct() const { return is_aggr_udf_distinct_; }

  inline void set_loc(uint64_t loc) { loc_ = loc; }
  inline uint64_t get_loc() const { return loc_; }
  inline void set_is_udt_cons(bool flag) { is_udt_cons_ = flag; }
  inline bool get_is_udt_cons() const { return is_udt_cons_; }
  inline ObIArray<ObUDFParamDesc>& get_params_desc() { return params_desc_v2_; }
  inline const ObIArray<ObUDFParamDesc>& get_params_desc() const { return params_desc_v2_; }
  ObIArray<int64_t>& get_nocopy_params() { return nocopy_params_; }
  const ObIArray<int64_t>& get_nocopy_params() const { return nocopy_params_; }

  inline bool need_add_dependency()
  {
    return udf_schema_version_ != common::OB_INVALID_VERSION
           || pkg_schema_version_ != common::OB_INVALID_VERSION;
  }

  int get_schema_object_version(share::schema::ObSchemaObjVersion &obj_version);

  inline void set_pkg_body_udf(bool v) { is_pkg_body_udf_ = v; }
  inline bool is_pkg_body_udf() const { return is_pkg_body_udf_; }

  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            N_DATABASE, get_database_name(),
                                            K_(package_name),
                                            N_FUNC, get_func_name(),
                                            K_(udf_id),
                                            K_(pkg_id),
                                            K_(type_id),
                                            K_(subprogram_path),
                                            K_(is_deterministic),
                                            K_(is_udt_udf),
                                            K_(is_return_sys_cursor),
                                            K_(loc),
                                            K_(udf_schema_version),
                                            K_(pkg_schema_version),
                                            K_(is_pkg_body_udf),
                                            K_(is_return_sys_cursor),
                                            K_(is_aggregate_udf),
                                            K_(is_parallel_enable),
                                            K_(is_aggr_udf_distinct),
                                            K_(loc),
                                            K_(is_udt_cons),
                                            K_(params_desc_v2),
                                            N_CHILDREN, exprs_);
private:
  uint64_t udf_id_;
  uint64_t pkg_id_;
  uint64_t type_id_;
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> subprogram_path_;
  int64_t udf_schema_version_;
  int64_t pkg_schema_version_;
  pl::ObPLIntegerType pls_type_; // 当返回PLS类型时, 该字段记录返回的PLS类型
  common::ObSEArray<ObExprResType, 5, common::ModulePageAllocator, true> params_type_;
  common::ObString database_name_;
  common::ObString package_name_;
  bool is_deterministic_;
  bool is_parallel_enable_;
  bool is_udt_udf_;
  bool is_pkg_body_udf_;
  bool is_return_sys_cursor_;
  bool is_aggregate_udf_;
  bool is_aggr_udf_distinct_;
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> nocopy_params_;
  uint64_t loc_; // line 和 column 组合，主要是为call_stack准备
  bool is_udt_cons_;
  common::ObSEArray<common::ObString, 5, common::ModulePageAllocator, true> params_name_;
  common::ObSEArray<ObUDFParamDesc, 5, common::ModulePageAllocator, true> params_desc_v2_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDFRawExpr);
};

class ObPLIntegerCheckerRawExpr : public ObOpRawExpr
{
public:
  ObPLIntegerCheckerRawExpr(common::ObIAllocator &alloc)
    : ObOpRawExpr(alloc),
      pl_integer_type_(pl::PL_INTEGER_INVALID),
      pl_integer_range_() {}
  ObPLIntegerCheckerRawExpr()
    : ObOpRawExpr(),
      pl_integer_type_(pl::PL_INTEGER_INVALID),
      pl_integer_range_() {}
  virtual ~ObPLIntegerCheckerRawExpr() {}
  void set_pl_integer_type(pl::ObPLIntegerType type) { pl_integer_type_ = type; }
  pl::ObPLIntegerType get_pl_integer_type() const { return pl_integer_type_; }
  void set_range(int32_t lower, int32_t upper)
  {
    pl_integer_range_.set_range(lower, upper);
  }
  inline int32_t get_lower() const { return pl_integer_range_.get_lower(); }
  inline int32_t get_upper() const { return pl_integer_range_.get_upper(); }
  int assign(const ObRawExpr &other) override;

  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            K_(pl_integer_type),
                                            K_(pl_integer_range_.range),
                                            N_CHILDREN, exprs_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLIntegerCheckerRawExpr);
private:
  pl::ObPLIntegerType pl_integer_type_;
  pl::ObPLIntegerRange pl_integer_range_;
};

class ObPLGetCursorAttrRawExpr : public ObSysFunRawExpr
{
public:
  ObPLGetCursorAttrRawExpr(common::ObIAllocator &alloc)
    : ObSysFunRawExpr(alloc), cursor_info_() {}
  ObPLGetCursorAttrRawExpr()
    : ObSysFunRawExpr(), cursor_info_() {}
  virtual ~ObPLGetCursorAttrRawExpr() {}
  virtual ObExprOperator *get_op() override;
  void set_pl_get_cursor_attr_info(pl::ObPLGetCursorAttrInfo info) { cursor_info_ = info; }
  const pl::ObPLGetCursorAttrInfo& get_pl_get_cursor_attr_info() const { return cursor_info_; }
  int assign(const ObRawExpr &other) override;
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            K_(cursor_info),
                                            N_CHILDREN, exprs_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLGetCursorAttrRawExpr);
private:
  pl::ObPLGetCursorAttrInfo cursor_info_;
};

class ObPLSQLCodeSQLErrmRawExpr : public ObSysFunRawExpr
{
public:
  ObPLSQLCodeSQLErrmRawExpr(common::ObIAllocator &alloc)
    : ObSysFunRawExpr(alloc), is_sqlcode_(true) {}
  ObPLSQLCodeSQLErrmRawExpr()
    : ObSysFunRawExpr(), is_sqlcode_(true) {}
  virtual ~ObPLSQLCodeSQLErrmRawExpr() {}
  int assign(const ObRawExpr &other) override;
  virtual ObExprOperator *get_op() override;
  void set_is_sqlcode(bool is_sqlcode) { is_sqlcode_ = is_sqlcode; }
  bool get_is_sqlcode() const { return is_sqlcode_; }
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            K_(is_sqlcode),
                                            N_CHILDREN, exprs_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLSQLCodeSQLErrmRawExpr);
private:
  bool is_sqlcode_;
};

class ObPLSQLVariableRawExpr : public ObSysFunRawExpr
{
public:
  ObPLSQLVariableRawExpr(common::ObIAllocator &alloc)
    : ObSysFunRawExpr(alloc), plsql_line_(OB_INVALID_INDEX), plsql_variable_() {}
  ObPLSQLVariableRawExpr()
    : ObSysFunRawExpr(), plsql_line_(OB_INVALID_INDEX) {}
  virtual ~ObPLSQLVariableRawExpr() {}

  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual ObExprOperator *get_op() override;

  void set_plsql_line(int64_t v) { plsql_line_ = v; }
  int64_t get_plsql_line() { return plsql_line_; }

  void set_plsql_variable(ObString &v) { plsql_variable_ = v; }
  ObString& get_plsql_variable() { return plsql_variable_; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLSQLVariableRawExpr);
private:
  int64_t plsql_line_; // for $$PLSQL_LINE
  ObString plsql_variable_; // for $$PLSQL_UNIT, $$PLSQL_CCFLAGS etc...
};

class ObCallParamRawExpr : public ObOpRawExpr
{
public:
  ObCallParamRawExpr(common::ObIAllocator &alloc)
    : ObOpRawExpr(alloc), expr_(NULL), name_() {}
  ObCallParamRawExpr()
    : ObOpRawExpr(), expr_(NULL), name_() {}

  virtual ~ObCallParamRawExpr() {}

  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;

  ObRawExpr* get_expr() { return expr_; }
  ObString& get_name() { return name_; }

  inline void set_expr(ObRawExpr *expr) { expr_ = expr; }
  inline void set_name(ObString &name) { name_ = name; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObCallParamRawExpr);
private:
  ObRawExpr *expr_;
  ObString name_;
};

class ObPLAssocIndexRawExpr : public ObOpRawExpr
{
public:
  ObPLAssocIndexRawExpr(common::ObIAllocator &alloc)
    : ObOpRawExpr(alloc),
    parent_type_(pl::parent_expr_type::EXPR_UNKNOWN),
    for_write_(false),
    out_of_range_set_err_(true),
    is_index_by_varchar_(false) {}
  ObPLAssocIndexRawExpr()
    : ObOpRawExpr(), for_write_(false) {}
  virtual ~ObPLAssocIndexRawExpr() {}
  int assign(const ObRawExpr &other) override;
  inline void set_write(bool for_write) { for_write_ = for_write; }
  inline bool get_write() const { return for_write_; }
  inline bool get_out_of_range_set_err() const { return out_of_range_set_err_; }
  inline void set_out_of_range_set_err(bool is_set_err) { out_of_range_set_err_ = is_set_err; }
  inline bool is_index_by_varchar() const { return is_index_by_varchar_; }
  inline void set_is_index_by_varchar(bool index_by_varchar) { is_index_by_varchar_ = index_by_varchar; }
  inline void set_parent_type(pl::parent_expr_type type) { parent_type_ = type; }
  inline pl::parent_expr_type get_parent_type() const { return parent_type_; }
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            K_(for_write),
                                            N_CHILDREN, exprs_,
                                            K_(out_of_range_set_err),
                                            K_(parent_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObPLAssocIndexRawExpr);
private:
  // hack,  0: parent expr is prior, 1 parent expr is
  pl::parent_expr_type parent_type_;
  union {
    uint64_t expr_flag_;
    struct {
      uint64_t for_write_ : 1;
      uint64_t out_of_range_set_err_ : 1; // set ret to out of range or not.
      uint64_t is_index_by_varchar_ : 1; // assoc type index type is varchar
      uint64_t reserved_:61;
    };
  };
};

class ObObjAccessRawExpr : public ObOpRawExpr
{
public:
  ObObjAccessRawExpr(common::ObIAllocator &alloc)
    : ObOpRawExpr(alloc),
      get_attr_func_(0),
      func_name_(),
      access_indexs_(),
      var_indexs_(),
      for_write_(false),
      property_type_(pl::ObCollectionType::INVALID_PROPERTY),
      orig_access_indexs_() {}
  ObObjAccessRawExpr()
    : ObOpRawExpr(),
      get_attr_func_(0),
      func_name_(),
      access_indexs_(),
      var_indexs_(),
      for_write_(false),
      property_type_(pl::ObCollectionType::INVALID_PROPERTY),
      orig_access_indexs_() {}
  virtual ~ObObjAccessRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  int add_access_indexs(const common::ObIArray<pl::ObObjAccessIdx> &access_idxs);
  common::ObIArray<pl::ObObjAccessIdx> &get_access_idxs() { return access_indexs_; }
  const common::ObIArray<pl::ObObjAccessIdx> &get_access_idxs() const { return access_indexs_; }
  const common::ObIArray<int64_t> &get_var_indexs() const { return var_indexs_; }
  int get_final_type(pl::ObPLDataType &type) const;
  void set_get_attr_func_addr(uint64_t get_attr_func) { get_attr_func_ = get_attr_func; }
  uint64_t get_get_attr_func_addr() const { return get_attr_func_; }
  void set_func_name(const common::ObString &func_name) { func_name_ = func_name; }
  const common::ObString &get_func_name() const { return func_name_; }
  bool for_write() const { return for_write_; }
  void set_write(bool for_write) { for_write_ = for_write; }
  bool is_property() const { return pl::ObCollectionType::INVALID_PROPERTY != property_type_; }
  pl::ObCollectionType::PropertyType get_property() const { return property_type_; }
  void set_property(pl::ObCollectionType::PropertyType property_type) { property_type_ = property_type; }
  common::ObIArray<pl::ObObjAccessIdx> &get_orig_access_idxs() { return orig_access_indexs_; }
  const common::ObIArray<pl::ObObjAccessIdx> &get_orig_access_idxs() const { return orig_access_indexs_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObObjAccessRawExpr);
  uint64_t get_attr_func_; //获取用户自定义类型数据的函数指针
  common::ObString func_name_;
  common::ObSEArray<pl::ObObjAccessIdx, 4, common::ModulePageAllocator, true> access_indexs_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> var_indexs_;
  bool for_write_;
  pl::ObCollectionType::PropertyType property_type_;
  common::ObSEArray<pl::ObObjAccessIdx, 4, common::ModulePageAllocator, true> orig_access_indexs_;
};

enum ObMultiSetType {
  MULTISET_TYPE_INVALID = -1,
  MULTISET_TYPE_UNION,
  MULTISET_TYPE_INTERSECT,
  MULTISET_TYPE_EXCEPT,
  MULTISET_TYPE_SUBMULTISET,
  MULTISET_TYPE_MEMBER_OF,
  MULTISET_TYPE_IS_SET,
  MULTISET_TYPE_EMPTY,
};

enum ObMultiSetModifier {
  MULTISET_MODIFIER_INVALID = -1,
  MULTISET_MODIFIER_ALL,
  MULTISET_MODIFIER_DISTINCT,
  MULTISET_MODIFIER_NOT,
};

class ObMultiSetRawExpr :
  public ObOpRawExpr
{
public:
  ObMultiSetRawExpr(common::ObIAllocator &alloc)
  : ObOpRawExpr(alloc),
    ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID),
    ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID) {}
  ObMultiSetRawExpr()
  : ObOpRawExpr(),
    ms_modifier_(ObMultiSetModifier::MULTISET_MODIFIER_INVALID),
    ms_type_(ObMultiSetType::MULTISET_TYPE_INVALID) {}

  virtual ~ObMultiSetRawExpr(){}

  int assign(const ObRawExpr &other) override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  inline ObMultiSetModifier get_multiset_modifier() const { return ms_modifier_; }
  inline ObMultiSetType get_multiset_type() const { return ms_type_; }

  void set_multiset_modifier(ObMultiSetModifier modifier) { ms_modifier_ = modifier; }
  void set_multiset_type(ObMultiSetType type) { ms_type_ = type; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiSetRawExpr);
  ObMultiSetModifier ms_modifier_;
  ObMultiSetType ms_type_;
};

class ObCollPredRawExpr : public ObMultiSetRawExpr
{
public:
  ObCollPredRawExpr(common::ObIAllocator &alloc) : ObMultiSetRawExpr(alloc) {}
  virtual ~ObCollPredRawExpr() {}

  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCollPredRawExpr);
};

////////////////////////////////////////////////////////////////
// eg :
//  partition by class order by score rows between 5 preceding and 5 following : 学生成绩排名前5后5
//  partition by class order by score range between 5 preceding and 5 following : 学生成绩上下相差5分
// new
enum WindowType
{
  // 物理偏移, eg: rows between 1 preceding and 1 following, 按上下偏移1行得到窗口
  WINDOW_ROWS,
  // 逻辑偏移, eg: range between 1 preceding and 1 following, 按value - 1, value + 1得到窗口
  WINDOW_RANGE,
  // 标识未定义状态
  WINDOW_MAX,
};
enum BoundType
{
  // 按partition界作为窗口界
  BOUND_UNBOUNDED,
  // 对于rows表示按当前行作为窗口界, 对于range表示按当前行的value作为窗口界
  BOUND_CURRENT_ROW,
  // 偏移值
  BOUND_INTERVAL,
};

enum BoundExprIdx {
  BOUND_EXPR_ADD = 0,
  BOUND_EXPR_SUB,
  BOUND_EXPR_MAX
};

struct Bound
{
  OB_UNIS_VERSION_V(1);
public:
  Bound()
    : type_(BOUND_UNBOUNDED),
      is_preceding_(false),
      is_nmb_literal_(false),
      interval_expr_(NULL),
      date_unit_expr_(NULL)
  {
    MEMSET(exprs_, 0, sizeof(ObRawExpr*) * BOUND_EXPR_MAX);
  }

  int assign(const Bound &other);
  int inner_deep_copy(ObIRawExprCopier &copier);

  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);

  bool same_as(const Bound &other, ObExprEqualCheckContext *check_context) const;

  BoundType type_;
  bool is_preceding_;
  bool is_nmb_literal_;
  ObRawExpr *interval_expr_;
  ObRawExpr *date_unit_expr_;

  ObRawExpr *exprs_[BOUND_EXPR_MAX];
  TO_STRING_KV(K_(type), K_(is_preceding), K_(is_nmb_literal), KPC_(interval_expr),
               K_(date_unit_expr));
};

////////////////////////////////////////////////////////////////
struct ObFrame
{
public:
  ObFrame()
    : win_type_(WINDOW_MAX),
      is_between_(false)
  {}
  inline void set_window_type(WindowType win_type)
  { win_type_ = win_type; }
  inline void set_is_between(bool is_between)
  { is_between_ = is_between; }
  inline void set_upper(const Bound &upper)
  { upper_ = upper; }
  inline void set_lower(const Bound &lower)
  { lower_ = lower; }
  inline WindowType get_window_type() const { return win_type_; }
  inline bool is_between() const { return is_between_; }
  inline Bound &get_upper() { return upper_; }
  inline Bound &get_lower() { return lower_; }

  int assign(const ObFrame &other);

  WindowType win_type_;
  bool is_between_;
  Bound upper_;
  Bound lower_;
};

struct ObWindow : public ObFrame
{
public:
  ObWindow()
    : has_frame_orig_(false)
  {
    partition_exprs_.set_label(common::ObModIds::OB_SQL_WINDOW_FUNC);
    order_items_.set_label(common::ObModIds::OB_SQL_WINDOW_FUNC);
  }
  inline int set_partition_exprs(const common::ObIArray<ObRawExpr *> &exprs)
  { return partition_exprs_.assign(exprs); }
  inline int set_order_items(const common::ObIArray<OrderItem> &items)
  { return order_items_.assign(items); }
  inline void set_win_name(common::ObString &win_name)
  { win_name_ = win_name; }
  inline void set_has_frame_orig(int64_t has_frame_orig)
  { has_frame_orig_ = has_frame_orig; }
  inline common::ObString &get_win_name() { return win_name_; }
  inline bool has_frame_orig() { return has_frame_orig_; }
  inline bool has_order_items() { return order_items_.count() > 0; }
  inline const common::ObIArray<ObRawExpr *> &get_partition_exprs() const { return partition_exprs_; }
  inline common::ObIArray<ObRawExpr *> &get_partition_exprs() { return partition_exprs_; }
  inline const common::ObIArray<OrderItem> &get_order_items() const { return order_items_; }
  inline common::ObIArray<OrderItem> &get_order_items() { return order_items_; }

  int assign(const ObWindow &other);

  common::ObArray<ObRawExpr *, common::ModulePageAllocator, true> partition_exprs_;
  common::ObArray<OrderItem, common::ModulePageAllocator, true> order_items_;
  // used in resolver
  common::ObString win_name_;
  bool has_frame_orig_;
};

class ObWinFunRawExpr : public ObRawExpr, public ObWindow
{
public:
  ObWinFunRawExpr()
    : ObRawExpr(),
      ObWindow(),
      func_type_(T_MAX),
      is_ignore_null_(false),
      is_from_first_(false),
      agg_expr_(NULL),
      pl_agg_udf_expr_(NULL)
  {
    set_expr_class(ObIRawExpr::EXPR_WINDOW);
  }
  ObWinFunRawExpr(common::ObIAllocator &alloc)
    : ObRawExpr(alloc),
      ObWindow(),
      func_type_(T_MAX),
      is_ignore_null_(false),
      is_from_first_(false),
      agg_expr_(NULL),
      pl_agg_udf_expr_(NULL)
  {
    set_expr_class(ObIRawExpr::EXPR_WINDOW);
  }
  virtual ~ObWinFunRawExpr() {}

  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;
  int replace_param_expr(int64_t partition_expr_index, ObRawExpr *expr);
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  inline void set_func_type(ObItemType func_type)
  { func_type_ = func_type; }
  inline void set_is_ignore_null(bool is_ignore_null)
  { is_ignore_null_ = is_ignore_null; }
  inline void set_is_from_first(bool is_from_first)
  { is_from_first_ = is_from_first; }
  inline int set_func_params(const common::ObIArray<ObRawExpr *> &params)
  { return func_params_.assign(params); }
  inline void set_agg_expr(ObAggFunRawExpr *agg_expr)
  { agg_expr_ = agg_expr; }
  inline ObItemType get_func_type() const { return func_type_; }
  inline bool is_ignore_null() { return is_ignore_null_; }
  inline bool is_from_first() { return is_from_first_; }
  inline const common::ObIArray<ObRawExpr *> &get_func_params() const { return func_params_; }
  inline common::ObIArray<ObRawExpr *> &get_func_params() { return func_params_; }
  inline ObAggFunRawExpr *get_agg_expr() { return agg_expr_; }
  inline ObAggFunRawExpr *get_agg_expr() const { return agg_expr_; }
  inline void set_pl_agg_udf_expr(ObRawExpr *udf_expr) { pl_agg_udf_expr_ = udf_expr; }
  inline ObRawExpr *get_pl_agg_udf_expr() const { return pl_agg_udf_expr_; }

  virtual void clear_child() override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int64_t get_param_count() const
  {
    int64_t cnt = (agg_expr_ != NULL ? agg_expr_->get_param_count() : 0)
                  + func_params_.count()
                  + partition_exprs_.count()
                  + order_items_.count()
                  + (upper_.interval_expr_ != NULL ? 1 : 0)
                  + (lower_.interval_expr_ != NULL ? 1 : 0)
                  + (pl_agg_udf_expr_ != NULL ? 1 : 0);
    for (int64_t i = 0; i < 2; ++i) {
      const Bound *bound = 0 == i ? &upper_ : &lower_;
      for (int64_t j = 0; j < BOUND_EXPR_MAX; ++j) {
        if (NULL != bound->exprs_[j]) {
          cnt++;
        }
      }
    }
    return cnt;
  }
  virtual const ObRawExpr *get_param_expr(int64_t index) const;
  virtual ObRawExpr *&get_param_expr(int64_t index);
  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;
  int64_t get_partition_param_index(int64_t index);

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  VIRTUAL_TO_STRING_KV_CHECK_STACK_OVERFLOW(N_ITEM_TYPE, type_,
                                            N_RESULT_TYPE, result_type_,
                                            N_EXPR_INFO, info_,
                                            N_REL_ID, rel_ids_,
                                            K_(func_type),
                                            K_(func_params),
                                            K_(partition_exprs),
                                            K_(order_items),
                                            K_(win_type),
                                            K_(is_between),
                                            K_(upper),
                                            K_(lower),
                                            KPC_(agg_expr),
                                            KPC_(pl_agg_udf_expr));
public:
  common::ObString sort_str_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObWinFunRawExpr);
  ObItemType func_type_;
  bool is_ignore_null_;
  bool is_from_first_;
  common::ObArray<ObRawExpr *, common::ModulePageAllocator, true> func_params_;
  ObAggFunRawExpr *agg_expr_;
  ObRawExpr *pl_agg_udf_expr_;//for pl agg udf expr
};

////////////////////////////////////////////////////////////////
class ObPseudoColumnRawExpr : public ObTerminalRawExpr
{
public:
  ObPseudoColumnRawExpr() :
    ObTerminalRawExpr(),
    table_id_(common::OB_INVALID_ID)
  {
    set_expr_class(ObIRawExpr::EXPR_PSEUDO_COLUMN);
  }
  ObPseudoColumnRawExpr(common::ObIAllocator &alloc) :
    ObTerminalRawExpr(alloc),
    table_id_(common::OB_INVALID_ID)
  {
    set_expr_class(ObIRawExpr::EXPR_PSEUDO_COLUMN);
  }
  virtual ~ObPseudoColumnRawExpr() {}
  int assign(const ObRawExpr &other) override;
  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs);
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;
  virtual uint64_t hash_internal(uint64_t seed) const;
  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;
  bool is_hierarchical_query_type() const { return type_ == T_LEVEL || type_ == T_CONNECT_BY_ISCYCLE || type_ == T_CONNECT_BY_ISLEAF; }
  bool is_cte_query_type() const { return T_CTE_SEARCH_COLUMN == type_ || T_CTE_CYCLE_COLUMN == type_; }
  void set_cte_cycle_value(ObRawExpr *v, ObRawExpr *d_v) {cte_cycle_value_ = v; cte_cycle_default_value_ = d_v; };
  void get_cte_cycle_value(ObRawExpr *&v, ObRawExpr *&d_v) {v = cte_cycle_value_; d_v = cte_cycle_default_value_; };
  void set_table_id(int64_t table_id) { table_id_ = table_id; }
  int64_t get_table_id() const { return table_id_; }
  void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  const common::ObString & get_table_name() const { return table_name_; }

  VIRTUAL_TO_STRING_KV(N_ITEM_TYPE, type_,
                       N_RESULT_TYPE, result_type_,
                       N_EXPR_INFO, info_,
                       N_REL_ID, rel_ids_,
                       N_TABLE_ID, table_id_,
                       N_TABLE_NAME, table_name_);
private:
  ObRawExpr *cte_cycle_value_;
  ObRawExpr *cte_cycle_default_value_;
  int64_t table_id_;
  common::ObString table_name_;
  DISALLOW_COPY_AND_ASSIGN(ObPseudoColumnRawExpr);
};


// Operator pseudo column to carry operator data to above operators.
// e.g.:
//   T_PDML_PARTITION_ID: carry partition id of row to above PDML operators.
//   T_PSEUDO_GROUP_ID: carry the batch group id for DAS batch rescan.
//   T_INNER_AGGR_CODE: carry aggregate code for 3-stage aggregation.
//   T_PSEUDO_ROLLUP_ID: carry aggregate code for rollup distributor and collector.
class ObOpPseudoColumnRawExpr : public ObTerminalRawExpr
{
public:
  ObOpPseudoColumnRawExpr();
  ObOpPseudoColumnRawExpr(ObItemType expr_type = T_INVALID);
  ObOpPseudoColumnRawExpr(common::ObIAllocator &alloc);

  virtual ~ObOpPseudoColumnRawExpr();

  int assign(const ObOpPseudoColumnRawExpr &other);
  int inner_deep_copy(ObIRawExprCopier &copier) override;

  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;
  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;
  virtual int do_visit(ObRawExprVisitor &visitor) override;
  virtual int get_name_internal(char *buf,
                                const int64_t buf_len,
                                int64_t &pos,
                                ExplainType type) const override;

  void set_name(const char *name) { name_ = name; }
  const char *get_name() const { return name_; }

private:
  const char *name_;
};

/// visitor interface
class ObRawExprVisitor
{
public:
  ObRawExprVisitor() {}
  virtual ~ObRawExprVisitor() {}

  // OP types: constants, ? etc.
  virtual int visit(ObConstRawExpr &expr) = 0;
  // OP types: ObObjtypes
  virtual int visit(ObVarRawExpr &expr) = 0;
  // OP types: operator pseudo column expr
  virtual int visit(ObOpPseudoColumnRawExpr &expr) = 0;

  virtual int visit(ObPlQueryRefRawExpr &expr) = 0;

  virtual int visit(ObExecParamRawExpr &expr) = 0;
  // OP types: subquery, cell index
  virtual int visit(ObQueryRefRawExpr &expr) = 0;
  // OP types: identify, table.column
  virtual int visit(ObColumnRefRawExpr &expr) = 0;
  // unary OP types: exists, not, negative, positive
  // binary OP types: +, -, *, /, >, <, =, <=>, IS, IN etc.
  // triple OP types: like, not like, btw, not btw
  // multi OP types: and, or, ROW
  virtual int visit(ObOpRawExpr &expr) = 0;
  // OP types: case, arg case
  virtual int visit(ObCaseOpRawExpr &expr) = 0;
  // OP types: aggregate functions e.g. max, min, avg, count, sum
  virtual int visit(ObAggFunRawExpr &expr) = 0;
  // OP types: system functions
  virtual int visit(ObSysFunRawExpr &expr) = 0;
  virtual int visit(ObSetOpRawExpr &expr) = 0;
  virtual int visit(ObAliasRefRawExpr &expr) { UNUSED(expr); return common::OB_SUCCESS; }
  virtual int visit(ObWinFunRawExpr &expr) { UNUSED(expr); return common::OB_SUCCESS; }
  virtual int visit(ObPseudoColumnRawExpr &expr) { UNUSED(expr); return common::OB_SUCCESS; }
  virtual bool skip_child(ObRawExpr &expr) { UNUSED(expr); return false; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprVisitor);
};

class ObRawExprFactory
{
public:
  explicit ObRawExprFactory(common::ObIAllocator &alloc)
    : allocator_(alloc),
      expr_store_(alloc),
      is_called_sql_(true),
      proxy_(nullptr)
  {
  }
  ObRawExprFactory(ObRawExprFactory &expr_factory) : allocator_(expr_factory.allocator_),
                                                     expr_store_(allocator_),
                                                     proxy_(&expr_factory)
  {
  }
  ~ObRawExprFactory() {
    // 对于非工作线程, 需要调用其析构函数,
    // 避免因未调用析构函数而导致raw_expr中
    // ObSEArray的内存泄漏
    if (!THIS_WORKER.has_req_flag() && OB_ISNULL(proxy_)) {
      destory();
    }
  }
  //~ObRawExprFactory() { }

  int create_raw_expr(ObRawExpr::ExprClass expr_class,
                      ObItemType expr_type,
                      ObRawExpr *&new_expr);

  template <typename ExprType>
  inline int create_raw_expr_inner(ObItemType expr_type, ExprType *&raw_expr)
  {
    int ret = common::OB_SUCCESS;
    void *ptr = allocator_.alloc(sizeof(ExprType));
    raw_expr = NULL;
    bool is_overflow = false;
    if (OB_UNLIKELY(NULL == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "no more memory to create raw expr");
    } else if (OB_NOT_NULL(proxy_)) {
      if (OB_FAIL(check_stack_overflow(is_overflow))) {
        SQL_RESV_LOG(WARN, "failed to check stack overflow", K(ret));
      } else if (is_overflow) {
        ret = OB_SIZE_OVERFLOW;
        SQL_RESV_LOG(WARN, "too deep recursive", K(ret));
      } else if (OB_FAIL(proxy_->create_raw_expr(expr_type, raw_expr))) {
        SQL_RESV_LOG(WARN, "failed to create raw expr by pl factory", K(ret));
      } else {
        raw_expr->set_is_called_in_sql(is_called_sql_);
      }
    } else {
      raw_expr = new(ptr) ExprType(allocator_);
      raw_expr->set_allocator(allocator_);
      raw_expr->set_expr_factory(*this);
      raw_expr->set_expr_type(expr_type);
      if (OB_FAIL(raw_expr->get_expr_info().get_init_err()) ||
          OB_FAIL(raw_expr->get_relation_ids().get_init_err())) {
        SQL_RESV_LOG(WARN, "failed to init ObSqlBitSet", K(ret));
        raw_expr->~ExprType();
        raw_expr = NULL;
      } else if (OB_FAIL(expr_store_.store_obj(raw_expr))) {
        SQL_RESV_LOG(WARN, "store raw expr failed", K(ret));
        raw_expr->~ExprType();
        raw_expr = NULL;
      } else {
        SQL_RESV_LOG(DEBUG, "create_raw_expr", K(expr_type), K(raw_expr),
                     "expr_type", get_type_name(expr_type), K(lbt()));
      }
    }
    return ret;
  }

  template <typename ExprType>
  int create_raw_expr(ObItemType expr_type, ExprType *&raw_expr)
  {
    return create_raw_expr_inner(expr_type, raw_expr);
  }


  inline void destory()
  {
    if (OB_NOT_NULL(proxy_)) {
      proxy_ = nullptr;
      expr_store_.destroy();
    } else {
      DLIST_FOREACH_NORET(node, expr_store_.get_obj_list()) {
        if (node != NULL && node->get_obj() != NULL) {
          node->get_obj()->~ObRawExpr();
        }
      }
      expr_store_.destroy();
    }
  }

  inline common::ObIAllocator &get_allocator() { return allocator_; }
  common::ObObjStore<ObRawExpr*, common::ObIAllocator&, true> &get_expr_store() { return expr_store_; }
  void set_is_called_sql(const bool is_called_sql) { is_called_sql_ = is_called_sql; }
  TO_STRING_KV("", "");
private:
  common::ObIAllocator &allocator_;
  common::ObObjStore<ObRawExpr*, common::ObIAllocator&, true> expr_store_;
  bool is_called_sql_;
  //if not null, raw_expr is create by pl resolver
  ObRawExprFactory *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRawExprFactory);
};

template <>
int ObRawExprFactory::create_raw_expr<ObSysFunRawExpr>(ObItemType expr_type, ObSysFunRawExpr *&raw_expr);

template <>
int ObRawExprFactory::create_raw_expr<ObOpRawExpr>(ObItemType expr_type, ObOpRawExpr *&raw_expr);


class ObRawExprPointer {
public:
  ObRawExprPointer();

  virtual ~ObRawExprPointer();
  int get(ObRawExpr *&expr) const;
  int set(ObRawExpr *expr);
  int add_ref(ObRawExpr **expr);
  int64_t ref_count() const { return expr_group_.count(); }
  int assign(const ObRawExprPointer &other);
  TO_STRING_KV("", "");
private:
  common::ObSEArray<ObRawExpr **, 1> expr_group_;
};

class ObPlQueryRefRawExpr : public ObRawExpr
{
public:
  ObPlQueryRefRawExpr()
    : ObRawExpr(),
      ps_sql_(ObString()),
      type_(stmt::T_NONE),
      route_sql_(ObString()),
      subquery_result_type_(),
      is_ignore_fail_(false),
      exprs_()
  {
    set_expr_class(ObIRawExpr::EXPR_PL_QUERY_REF);
  }

  ObPlQueryRefRawExpr(common::ObIAllocator &alloc)
    : ObRawExpr(alloc),
      ps_sql_(ObString()),
      type_(stmt::T_NONE),
      route_sql_(ObString()),
      subquery_result_type_(),
      is_ignore_fail_(false),
      exprs_()
  {
    set_expr_class(ObIRawExpr::EXPR_PL_QUERY_REF);
  }

  virtual ~ObPlQueryRefRawExpr() {}
  int assign(const ObRawExpr &other) override;
  int inner_deep_copy(ObIRawExprCopier &copier) override;

  int add_param_expr(ObRawExpr *expr);
  int64_t get_param_count() const;
  const ObRawExpr *get_param_expr(int64_t index) const;
  ObRawExpr *&get_param_expr(int64_t index);

  inline void set_ps_sql(const common::ObString &sql) { ps_sql_ = sql; }
  inline void set_stmt_type(stmt::StmtType type) { type_ = type; }
  inline void set_route_sql(const common::ObString &sql) { route_sql_ = sql; }
  inline void set_subquery_result_type(const sql::ObExprResType &type)
  {
    subquery_result_type_ = type;
  }
  inline const common::ObString &get_ps_sql() const { return ps_sql_; }
  inline stmt::StmtType get_stmt_type() const { return type_; }
  inline const common::ObString &get_route_sql() const { return route_sql_; }
  inline const sql::ObExprResType &get_subquery_result_type() const
  {
    return subquery_result_type_;
  }

  inline void set_ignore_fail() { is_ignore_fail_ = true; }
  inline bool is_ignore_fail() const { return is_ignore_fail_; }

  virtual int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs) override;

  virtual void clear_child() override;

  virtual bool inner_same_as(const ObRawExpr &expr,
                             ObExprEqualCheckContext *check_context = NULL) const override;

  virtual int do_visit(ObRawExprVisitor &visitor) override;

  virtual uint64_t hash_internal(uint64_t seed) const;

  int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos, ExplainType type) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlQueryRefRawExpr);

  common::ObString ps_sql_; //prepare后的参数化sql
  stmt::StmtType type_; //prepare的语句类型

  common::ObString route_sql_;
  sql::ObExprResType subquery_result_type_;

  bool is_ignore_fail_;

  common::ObSEArray<ObRawExpr *, COMMON_MULTI_NUM, common::ModulePageAllocator, true> exprs_;
};

inline const ObRawExpr *ObPlQueryRefRawExpr::get_param_expr(int64_t index) const
{
  const ObRawExpr *expr = NULL;
  if (index >= 0 && index < exprs_.count()) {
    expr = exprs_.at(index);
  }
  return expr;
}

//here must return in two branch, because ret value is a *&
inline ObRawExpr *&ObPlQueryRefRawExpr::get_param_expr(int64_t index)
{
  if (index >= 0 && index < exprs_.count()) {
    return exprs_.at(index);
  } else {
    return USELESS_POINTER;
  }
}

inline int ObPlQueryRefRawExpr::add_param_expr(ObRawExpr *expr)
{
  return exprs_.push_back(expr);
}

inline int64_t ObPlQueryRefRawExpr::get_param_count() const
{
  return exprs_.count();
}

inline uint64_t ObPlQueryRefRawExpr::hash_internal(uint64_t seed) const
{
  uint64_t hash_value = seed;
  for (int64_t i = 0; i < exprs_.count(); ++i) {
    if (NULL != exprs_.at(i)) {
      hash_value = common::do_hash(*(exprs_.at(i)), hash_value);
    }
  }
  return hash_value;
}

}// end sql
}// end oceanbase

#endif //OCEANBASE_SQL_RESOLVER_EXPR_RAW_EXPR_
