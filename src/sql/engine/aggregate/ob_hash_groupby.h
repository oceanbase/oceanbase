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

#ifndef _OB_HASH_GROUPBY_H
#define _OB_HASH_GROUPBY_H 1
#include "sql/engine/aggregate/ob_groupby.h"

namespace oceanbase {
namespace sql {

class ObGbyBloomFilter;
class ObGbyPartition;

class ObHashGroupBy : public ObGroupBy {
private:
  class ObHashGroupByCtx;

public:
  static const int64_t MIN_PARTITION_CNT = 8;
  static const int64_t MAX_PARTITION_CNT = 256;

  // min in memory groups
  static const int64_t MIN_INMEM_GROUPS = 4;

  static const int64_t MIN_GROUP_HT_INIT_SIZE = 1024;
  static const int64_t MAX_GROUP_HT_INIT_SIZE = 1 << 20;  // 1048576
  static const int64_t DISTINCT_SET_NUM = 100000;
  static const int64_t PREPARE_ROW_NUM = 100000;
  static constexpr const double MAX_PART_MEM_RATIO = 0.5;
  static constexpr const double EXTRA_MEM_RATIO = 0.25;
  explicit ObHashGroupBy(common::ObIAllocator& alloc);
  virtual ~ObHashGroupBy();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;

  int get_hash_groupby_row_count(ObExecContext& exec_ctx, int64_t& hash_groupby_row_count) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupBy);

private:
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  /**
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief load data to hash group by map
   */
  virtual int load_data(ObExecContext& ctx) const;
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

  int init_sql_mem_mgr(ObHashGroupByCtx* gby_ctx, int64_t input_size) const;
  int update_mem_status_periodically(ObHashGroupByCtx* gby_ctx, int64_t nth_cnt, const int64_t input_row,
      int64_t& est_part_cnt, bool& need_dump) const;
  int64_t detect_part_cnt(const int64_t rows, ObHashGroupByCtx& gby_ctx) const;

  // check is dump needed right now, update %est_part_cnt on demand
  OB_INLINE bool need_start_dump(
      ObHashGroupByCtx& gby_ctx, const int64_t input_rows, int64_t& est_part_cnt, bool check_dump) const;

  // Setup: memory entity, bloom filter, spill partitions
  int setup_dump_env(const int64_t level, const int64_t input_rows, ObGbyPartition** parts, int64_t& part_cnt,
      ObGbyBloomFilter*& bloom_filter, ObHashGroupByCtx& gby_ctx) const;

  int cleanup_dump_env(const bool dump_success, const int64_t level, ObGbyPartition** parts, int64_t& part_cnt,
      ObGbyBloomFilter*& bloom_filter, ObHashGroupByCtx& gby_ctx) const;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif /* _OB_HASH_GROUPBY_H */
