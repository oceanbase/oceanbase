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

#ifndef _OB_OPT_DS_STAT_H_
#define _OB_OPT_DS_STAT_H_

#include <stdint.h>
#include <cstddef>
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

// enum ObOptDSType
// {
//    TABLE_DYNAMIC_SAMPLE,  //table dynamic sampling
//    JOIN_DYNAMIC_SAMPLE, //join dynamic sampling
//    MAX_DYNAMIC_SAMPLE
// };
enum ObDynamicSamplingLevel
{
  NO_DYNAMIC_SAMPLING = 0,
  BASIC_DYNAMIC_SAMPLING//dynamic sampling basic table use basic way
  //ADS_DYNAMIC_SAMPLING//dynamic sampling basic table use ADS way
};

struct ObOptDSColStat
{
  ObOptDSColStat() :
    column_id_(0),
    num_distinct_(0),
    num_null_(0),
    degree_(1) {}
  TO_STRING_KV(K(column_id_),
               K(num_distinct_),
               K(num_null_),
               K(degree_));
  uint64_t column_id_;
  int64_t num_distinct_;
  int64_t num_null_;
  int64_t degree_;
};

class ObOptDSStat : public common::ObIKVCacheValue
{
public:
  typedef ObArrayWrap<ObOptDSColStat> DSColStats;
  struct Key : public common::ObIKVCacheKey
  {
    Key() : tenant_id_(0),
            table_id_(OB_INVALID_ID),
            partition_hash_(0),
            ds_level_(ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING),
            sample_block_(0),
            expression_hash_(0)
    {
    }
    uint64_t hash() const { return common::murmurhash(this, sizeof(Key), 0); }
    bool operator==(const ObIKVCacheKey &other) const
    {
      const Key &other_key = reinterpret_cast<const Key&>(other);
      return tenant_id_ == other_key.tenant_id_ &&
             table_id_ == other_key.table_id_ &&
             partition_hash_ == other_key.partition_hash_ &&
             ds_level_ == other_key.ds_level_ &&
             sample_block_ == other_key.sample_block_ &&
             expression_hash_ == other_key.expression_hash_;
    }
    uint64_t get_tenant_id() const
    {
      return tenant_id_;
    }
    int64_t size() const { return sizeof(*this); }
    int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
    {
      int ret = OB_SUCCESS;
      Key *tmp = NULL;
      if (NULL == buf || buf_len < size()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument.",
            K(ret), KP(buf), K(buf_len), K(size()));
      } else {
        tmp = new (buf) Key();
        *tmp = *this;
        key = tmp;
      }
      return ret;
    }
    bool is_valid() const
    {
      return tenant_id_ != 0 &&
             table_id_ != OB_INVALID_ID &&
             ds_level_ != ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING &&
             sample_block_ > 0;
    }
    void reset()
    {
      tenant_id_ = 0;
      table_id_ = OB_INVALID_ID;
      partition_hash_ = 0;
      ds_level_ = ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
      sample_block_ = 0;
      expression_hash_ = 0;
    }
    TO_STRING_KV(K(tenant_id_),
                 K(table_id_),
                 K(partition_hash_),
                 K(ds_level_),
                 K(sample_block_),
                 K(expression_hash_));
    uint64_t tenant_id_;//tenant id
    uint64_t table_id_;//sample table id
    uint64_t partition_hash_;//sample table partition hash
    uint64_t ds_level_; //dynamic sampling level
    uint64_t sample_block_;//sample block num
    uint64_t expression_hash_;//the expression hash value
  };

  ObOptDSStat()
    : tenant_id_(0),
      table_id_(OB_INVALID_ID),
      partition_hash_(0),
      ds_level_(ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING),
      dml_cnt_(0),
      expression_hash_(0),
      rowcount_(0),
      macro_block_num_(0),
      micro_block_num_(0),
      sample_block_ratio_(0.0),
      ds_degree_(1),
      col_stats_(),
      stat_expired_time_(-1)
  {}
  virtual ~ObOptDSStat() { reset(); }
  void init(const ObOptDSStat::Key &key);
  uint64_t get_ds_level() const { return ds_level_; }
  bool is_arrived_expired_time() const {
    return stat_expired_time_ != -1 && stat_expired_time_ <= ObTimeUtility::current_time(); }
  void set_stat_expired_time(int64_t expired_time) {  stat_expired_time_ = expired_time; }
  DSColStats &get_ds_col_stats() { return col_stats_; }
  const DSColStats &get_ds_col_stats() const { return col_stats_; }
  int prepare_allocate_col_stats(ObIAllocator &allocator, int64_t row_cnt);
  virtual int64_t size() const override;
  int assign(const ObOptDSStat& other);
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  int deep_copy(const ObOptDSStat &src, char *buf, const int64_t buf_len, int64_t &pos);
  int deep_copy(ObIAllocator &allocate, ObOptDSStat *&ds_stat) const;
  void set_rowcount(int64_t rowcount) { rowcount_ = rowcount; }
  int64_t get_rowcount() const { return rowcount_; }
  void set_macro_block_num(int64_t macro_block_num) { macro_block_num_ = macro_block_num; }
  int64_t get_macro_block_num() const { return macro_block_num_; }
  void set_micro_block_num(int64_t micro_block_num) { micro_block_num_ = micro_block_num; }
  int64_t get_micro_block_num() const { return micro_block_num_; }
  void set_sample_block_ratio(double sample_block_ratio) { sample_block_ratio_ = sample_block_ratio; }
  double get_sample_block_ratio() const { return sample_block_ratio_; }
  void set_dml_cnt(int64_t dml_cnt) { dml_cnt_ = dml_cnt; }
  int64_t get_dml_cnt() const { return dml_cnt_; }
  void set_ds_degree(int64_t ds_degree) { ds_degree_ = ds_degree; }
  int64_t get_ds_degree() const { return ds_degree_; }
  bool is_valid() const
  {
    return tenant_id_ > 0 &&
           table_id_ != OB_INVALID_ID &&
           ds_level_ != ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
  }
  void reset() {
    tenant_id_ = 0;
    table_id_ = OB_INVALID_ID;
    partition_hash_ = 0;
    ds_level_ = ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
    dml_cnt_ = 0;
    expression_hash_ = 0;
    rowcount_ = 0;
    macro_block_num_ = 0;
    micro_block_num_ = false;
    sample_block_ratio_ = 0.0;
    ds_degree_ = 1;
    col_stats_.reset();
    stat_expired_time_ = -1;
  }
  TO_STRING_KV(K(tenant_id_),
               K(table_id_),
               K(partition_hash_),
               K(ds_level_),
               K(dml_cnt_),
               K(expression_hash_),
               K(rowcount_),
               K(macro_block_num_),
               K(micro_block_num_),
               K(sample_block_ratio_),
               K(ds_degree_),
               K(col_stats_),
               K(stat_expired_time_));

private:
  uint64_t tenant_id_;//tenant id
  uint64_t table_id_;//table id
  uint64_t partition_hash_;//partition hash
  uint64_t ds_level_;//dynamic sampling level
  uint64_t dml_cnt_; //dynamic sampling dml info
  uint64_t expression_hash_;//the expression hash value
  int64_t rowcount_;//row count
  int64_t macro_block_num_;
  int64_t micro_block_num_;
  double sample_block_ratio_;
  int64_t ds_degree_;
  DSColStats col_stats_;//table dynamic sampling column stats
  int64_t stat_expired_time_;//mark the stat in cache is arrived expired time, if arrived at expired time need reload.
};

}
}

#endif /* _OB_OPT_TABLE_STAT_H_ */
