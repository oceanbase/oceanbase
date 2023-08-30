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

#ifndef OCEANBASE_COMMON_OB_COMMON_TYPES_
#define OCEANBASE_COMMON_OB_COMMON_TYPES_

#include <cstdint>
#include "lib/utility/ob_print_utils.h"
//#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace common
{
struct ObQueryFlag
{
#define OBSF_BIT_SCAN_ORDER           2
#define OBSF_BIT_DAILY_MERGE          1
#define OBSF_BIT_RMMB_OPTIMIZE        1
#define OBSF_BIT_WHOLE_MACRO_SCAN     2
#define OBSF_BIT_FULL_ROW             1
#define OBSF_BIT_INDEX_BACK           1
#define OBSF_BIT_QUERY_STAT           1
#define OBSF_BIT_SQL_MODE             2
#define OBSF_BIT_READ_LATEST          1
#define OBSF_BIT_PREWARM              1
#define OBSF_BIT_INDEX_INVALID        1
#define OBSF_BIT_ROW_CACHE            1
#define OBSF_BIT_JOIN_TYPE            4
#define OBSF_BIT_BLOCK_INDEX_CACHE    1
#define OBSF_BIT_BLOCK_CACHE          1
#define OBSF_BIT_BLOOMFILTER_CACHE    1
#define OBSF_BIT_MULTI_VERSION_MERGE  1
#define OBSF_BIT_NEED_FEEDBACK        1
#define OBSF_BIT_FUSE_ROW_CACHE       1
#define OBSF_BIT_FAST_AGG             1
#define OBSF_BIT_ITER_UNCOMMITTED_ROW 1
#define OBSF_BIT_IGNORE_TRANS_STAT    1
#define OBSF_BIT_IS_LARGE_QUERY       1
#define OBSF_BIT_IS_SSTABLE_CUT       1
#define OBSF_BIT_IS_SHOW_SEED         1
#define OBSF_BIT_SKIP_READ_LOB        1
#define OBSF_BIT_IS_LOOKUP_FOR_4377   1
#define OBSF_BIT_FOR_FOREING_KEY_CHECK 1
#define OBSF_BIT_RESERVED             31

  static const uint64_t OBSF_MASK_SCAN_ORDER = (0x1UL << OBSF_BIT_SCAN_ORDER) - 1;
  static const uint64_t OBSF_MASK_DAILY_MERGE =  (0x1UL << OBSF_BIT_DAILY_MERGE) - 1;
  static const uint64_t OBSF_MASK_RMMB_OPTIMIZE = (0x1UL << OBSF_BIT_RMMB_OPTIMIZE) - 1;
  static const uint64_t OBSF_MASK_WHOLE_MACRO_SCAN = (0x1UL << OBSF_BIT_WHOLE_MACRO_SCAN) - 1;
  static const uint64_t OBSF_MASK_FULL_ROW = (0x1UL << OBSF_BIT_FULL_ROW) - 1;
  static const uint64_t OBSF_MASK_INDEX_BACK = (0x1UL << OBSF_BIT_INDEX_BACK) - 1;
  static const uint64_t OBSF_MASK_QUERY_STAT = (0x1UL << OBSF_BIT_QUERY_STAT) - 1;
  static const uint64_t OBSF_MASK_RESERVED = (0x1UL << OBSF_BIT_RESERVED) - 1;
  static const uint64_t OBSF_MASK_SQL_MODE = (0x1UL << OBSF_BIT_SQL_MODE) - 1;
  static const uint64_t OBSF_MASK_READ_LATEST = (0x1UL << OBSF_BIT_READ_LATEST) - 1;
  static const uint64_t OBSF_MASK_PREWARM = (0x1UL << OBSF_BIT_PREWARM) - 1;
  static const uint64_t OBSF_MASK_INDEX_INVALID = (0x1UL << OBSF_BIT_INDEX_INVALID) - 1;
  static const uint64_t OBSF_MASK_JOIN_TYPE = (0x1UL << OBSF_BIT_JOIN_TYPE) - 1;
  static const uint64_t OBSF_MASK_MULTI_VERSION_MERGE = (0x1UL << OBSF_BIT_MULTI_VERSION_MERGE) - 1;
  static const uint64_t OBSF_MASK_NEED_FEEDBACK = (0x1UL << OBSF_BIT_NEED_FEEDBACK) - 1;
  static const uint64_t OBSF_MASK_FUSE_ROW_CACHE = (0x1UL << OBSF_BIT_FUSE_ROW_CACHE) - 1;
  static const uint64_t OBSF_MASK_FAST_AGG = (0x1UL << OBSF_BIT_FAST_AGG) - 1;
  static const uint64_t OBSF_MASK_ITER_UNCOMMITTED_ROW = (0x1UL << OBSF_BIT_ITER_UNCOMMITTED_ROW) - 1;
  static const uint64_t OBSF_MASK_IGNORE_TRANS_STAT = (0x1UL << OBSF_BIT_IGNORE_TRANS_STAT) - 1;
  static const uint64_t OBSF_MASK_IS_LARGE_QUERY = (0x1UL << OBSF_BIT_IS_LARGE_QUERY) - 1;
  static const uint64_t OBSF_MASK_IS_SSTABLE_CUT = (0x1UL << OBSF_BIT_IS_SSTABLE_CUT) - 1;
  static const uint64_t OBSF_MASK_SKIP_READ_LOB = (0x1UL << OBSF_BIT_SKIP_READ_LOB) - 1;
  static const uint64_t OBSF_MASK_FOR_FOREING_KEY_CHECK = (0x1UL << OBSF_BIT_FOR_FOREING_KEY_CHECK) - 1;

  enum ScanOrder
  {
    ImplementedOrder = 0,
    Forward = 1,
    Reverse = 2,
    KeepOrder = 3,
  };

  enum CachePolicy
  {
    UseCache = 0,
    DoNotUseCache = 1
  };

  enum AggMode
  {
    DoNotUseFastAgg = 0,
    UseFastAgg = 1
  };

  enum SqlMode
  {
    MysqlMode = 0,
    AnsiMode = 1,
    ReservedMode = 2,
  };

  union
  {
    uint64_t flag_;
    struct
    {
      uint64_t scan_order_     : OBSF_BIT_SCAN_ORDER;        // 1: forward(default), 2: reverse
      uint64_t daily_merge_    : OBSF_BIT_DAILY_MERGE;       // 0: normal scan(default), 1: daily merge scan
      uint64_t rmmb_optimize_ : OBSF_BIT_RMMB_OPTIMIZE;     // 0: donot optimize(default), 1: optimize
      uint64_t whole_macro_scan_: OBSF_BIT_WHOLE_MACRO_SCAN;  // 0: normal scan 1:whole macro scan, like daily merge or build index, will read one macro block in single io request
      uint64_t full_row_       : OBSF_BIT_FULL_ROW;          // 0: partial columns(default), 1: all columns
      uint64_t index_back_     : OBSF_BIT_INDEX_BACK;        // 0: access index only, 1: index can not cover result column(s), data table need to be accessed too
      uint64_t query_stat_     : OBSF_BIT_QUERY_STAT;        // 0: close ObIOStat statistics, 1: open ObIOStat statistics
      uint64_t sql_mode_       : OBSF_BIT_SQL_MODE;          // 0: mysql mode, 1: standard mod, 2: reserved mode
      uint64_t read_latest_    : OBSF_BIT_READ_LATEST;
      uint64_t prewarm_      : OBSF_BIT_PREWARM;             //0: is not prewarm, 1: prewarm
      uint64_t index_invalid_ : OBSF_BIT_INDEX_INVALID; //0: index is valid, 1: index is invalid
      uint64_t use_row_cache_ : OBSF_BIT_ROW_CACHE;
      uint64_t join_type_     : OBSF_BIT_JOIN_TYPE;
      uint64_t use_block_index_cache_: OBSF_BIT_BLOCK_INDEX_CACHE;
      uint64_t use_block_cache_ : OBSF_BIT_BLOCK_CACHE;
      uint64_t use_bloomfilter_cache_: OBSF_BIT_BLOOMFILTER_CACHE;
      uint64_t multi_version_minor_merge_ : OBSF_BIT_MULTI_VERSION_MERGE; // 1: multi_version_minor_merge_
      uint64_t is_need_feedback_ : OBSF_BIT_NEED_FEEDBACK;
      uint64_t use_fuse_row_cache_ : OBSF_BIT_FUSE_ROW_CACHE;
      uint64_t use_fast_agg_ : OBSF_BIT_FAST_AGG;
      uint64_t iter_uncommitted_row_ : OBSF_BIT_ITER_UNCOMMITTED_ROW;
      uint64_t ignore_trans_stat_: OBSF_BIT_IGNORE_TRANS_STAT;
      uint64_t is_large_query_ : OBSF_BIT_IS_LARGE_QUERY;
      uint64_t is_sstable_cut_ : OBSF_BIT_IS_SSTABLE_CUT; //0:sstable no need cut, 1: sstable need cut
      uint64_t is_show_seed_   : OBSF_BIT_IS_SHOW_SEED;
      uint64_t skip_read_lob_   : OBSF_BIT_SKIP_READ_LOB;
      uint64_t is_lookup_for_4377_ : OBSF_BIT_IS_LOOKUP_FOR_4377;
      uint64_t for_foreign_key_check_ : OBSF_BIT_FOR_FOREING_KEY_CHECK;
      uint64_t reserved_       : OBSF_BIT_RESERVED;
    };
  };

  ObQueryFlag() : flag_(0) {}
  ObQueryFlag(const ScanOrder order,
              const bool daily_merge,
              const bool optimize,
              const bool whole_macro_scan,
              const bool full_row,
              const bool index_back,
              const bool query_stat,
              const SqlMode sql_mode = MysqlMode,
              const bool read_latest = false,
              const bool prewarm = false,
              const uint64_t join_type = 0,
              const bool multi_version_minor_merge = false,
              const bool need_feedback = false,
              const bool is_large_query = false,
              const bool is_sstable_cut = false)
  {
    flag_ = 0;
    scan_order_ = order & OBSF_MASK_SCAN_ORDER;
    daily_merge_ = daily_merge & OBSF_MASK_DAILY_MERGE;
    // read multiple macro blocks optimize
    rmmb_optimize_ = optimize & OBSF_MASK_RMMB_OPTIMIZE;
    whole_macro_scan_ = whole_macro_scan & OBSF_MASK_WHOLE_MACRO_SCAN;
    full_row_ = full_row & OBSF_MASK_FULL_ROW;
    index_back_ = index_back & OBSF_MASK_INDEX_BACK;
    query_stat_ = query_stat & OBSF_MASK_QUERY_STAT;
    sql_mode_ = sql_mode & OBSF_MASK_SQL_MODE;
    read_latest_ = read_latest & OBSF_MASK_READ_LATEST;
    prewarm_ = prewarm & OBSF_MASK_PREWARM;
    reserved_ = 0;
    join_type_ = join_type & OBSF_MASK_JOIN_TYPE;
    multi_version_minor_merge_ = multi_version_minor_merge & OBSF_MASK_MULTI_VERSION_MERGE;
    is_need_feedback_ = need_feedback & OBSF_MASK_NEED_FEEDBACK;
    is_large_query_ = is_large_query & OBSF_MASK_IS_LARGE_QUERY;
    is_sstable_cut_ = is_sstable_cut & OBSF_MASK_IS_SSTABLE_CUT;
  }
  void reset() { flag_ = 0; }
  inline bool is_reverse_scan() const { return scan_order_ == Reverse; }
  inline bool is_ordered_scan() const { return scan_order_ == ObQueryFlag::Forward || scan_order_ == ObQueryFlag::Reverse; }
  inline bool is_daily_merge() const { return daily_merge_; }
  inline bool is_rmmb_optimized() const { return rmmb_optimize_; }
  inline bool is_whole_macro_scan() const { return whole_macro_scan_; }
  inline bool is_full_row() const { return full_row_; }
  inline bool is_index_back() const { return index_back_; }
  inline bool is_query_stat() const { return query_stat_; }
  inline bool is_mysql_mode() const { return sql_mode_ == MysqlMode; }
  inline bool is_read_latest() const { return read_latest_; }
  inline bool is_lookup_for_4377() const { return is_lookup_for_4377_; }
  inline bool is_prewarm() const { return prewarm_; }
  inline bool is_index_invalid() const { return index_invalid_; }
  inline bool is_use_row_cache() const { return !is_whole_macro_scan() && use_row_cache_ == UseCache; }
  inline bool is_use_block_index_cache() const { return !is_whole_macro_scan() && use_block_index_cache_ == UseCache; }
  inline bool is_use_block_cache() const { return !is_whole_macro_scan() && use_block_cache_ == UseCache; }
  inline bool is_use_bloomfilter_cache() const { return !is_whole_macro_scan() && use_bloomfilter_cache_ == UseCache; }
  inline bool is_use_fuse_row_cache() const { return !is_whole_macro_scan() && use_fuse_row_cache_ == UseCache; }
  inline bool is_use_fast_agg() { return use_fast_agg_ == UseFastAgg; }
  inline uint64_t get_join_type() const { return join_type_; }
  inline bool is_multi_version_minor_merge() const { return multi_version_minor_merge_; }
  inline bool is_need_feedback() const { return is_need_feedback_; }
  inline bool is_large_query() const { return is_large_query_; }
  inline void set_not_use_row_cache() { use_row_cache_ = DoNotUseCache; }
  inline void set_not_use_block_cache() { use_block_cache_ = DoNotUseCache; }
  inline void set_not_use_block_index_cache() { use_block_index_cache_ = DoNotUseCache; }
  inline void set_not_use_bloomfilter_cache() { use_bloomfilter_cache_ = DoNotUseCache; }
  inline void set_not_use_fuse_row_cache() { use_fuse_row_cache_ = DoNotUseCache; }
  inline void set_not_use_fast_agg() { use_fast_agg_ = DoNotUseFastAgg; }
  inline void set_use_row_cache() { use_row_cache_ = UseCache; }
  inline void set_use_block_cache() { use_block_cache_ = UseCache; }
  inline void set_use_block_index_cache() { use_block_index_cache_ = UseCache; }
  inline void set_use_bloomfilter_cache() { use_bloomfilter_cache_ = UseCache; }
  inline void set_use_fuse_row_cache() { use_fuse_row_cache_ = UseCache; }
  inline void set_use_fast_agg() { use_fast_agg_ = UseFastAgg; }
  inline void set_iter_uncommitted_row() { iter_uncommitted_row_ = true; }
  inline void set_not_iter_uncommitted_row() { iter_uncommitted_row_ = false; }
  inline void set_for_foreign_key_check() { for_foreign_key_check_ = true; }
  inline void set_ignore_trans_stat() { ignore_trans_stat_ = true; }
  inline void set_not_ignore_trans_stat() { ignore_trans_stat_ = false; }
  inline bool iter_uncommitted_row() const { return iter_uncommitted_row_; }
  inline bool is_for_foreign_key_check() const { return for_foreign_key_check_; }
  inline bool is_ignore_trans_stat() const { return ignore_trans_stat_; }
  inline bool is_sstable_cut() const { return is_sstable_cut_; }
  inline bool is_skip_read_lob() const { return skip_read_lob_; }
  inline void disable_cache()
  {
    set_not_use_row_cache();
    set_not_use_fuse_row_cache();
    set_not_use_block_cache();
    set_not_use_bloomfilter_cache();
  }

  TO_STRING_KV("scan_order", scan_order_,
               "daily_merge", daily_merge_,
               "rmmb_optimize", rmmb_optimize_,
               "whole_macro_scan", whole_macro_scan_,
               "full_row", full_row_,
               "index_back", index_back_,
               "query_stat", query_stat_,
               "sql_mode", sql_mode_,
               "read_latest", read_latest_,
               "prewarm", prewarm_,
               "join_type", join_type_,
               "use_row_cache", use_row_cache_,
               "use_block_index_cache", use_block_index_cache_,
               "use_bloomfilter_cache", use_bloomfilter_cache_,
               "multi_version_minor_merge", multi_version_minor_merge_,
               "is_need_feedback", is_need_feedback_,
               "use_fuse_row_cache", use_fuse_row_cache_,
               "use_fast_agg", use_fast_agg_,
               "iter_uncommitted_row", iter_uncommitted_row_,
               "ignore_trans_stat", ignore_trans_stat_,
               "is_large_query", is_large_query_,
               "is_sstable_cut", is_sstable_cut_,
               "skip_read_lob", skip_read_lob_,
               "is_lookup_for_4377", is_lookup_for_4377_,
               "is_for_foreign_key_check", for_foreign_key_check_,
               "reserved", reserved_);
  OB_UNIS_VERSION(1);
};

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_COMMON_TYPES_ */
