/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H
#define OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H

#include "lib/container/ob_array.h"   // ObArray
#include "lib/hash/ob_hashmap.h"      // ObHashmap
#include "rootserver/balance/ob_balance_group_define.h" // ObBalanceGroupID, ObPartGroupInfo

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
}
}

namespace rootserver
{
typedef common::ObArray<ObPartGroupInfo *> ObPartGroupBucket;

class ObBalanceGroupUnit
{
public:
  explicit ObBalanceGroupUnit(common::ObIAllocator &alloc)
      : inited_(false),
        bg_unit_id_(OB_INVALID_ID),
        alloc_(alloc),
        pg_buckets_() {}
  ~ObBalanceGroupUnit();
  int init(const uint64_t bg_unit_id, const int64_t bucket_num);
  ObArray<ObPartGroupBucket> &get_pg_buckets() { return pg_buckets_; }
  bool is_valid() const { return inited_ && OB_INVALID_ID != bg_unit_id_; }
  ObObjectID get_bg_unit_id() const { return bg_unit_id_; }
  int64_t get_pg_count() const;
  int64_t get_data_size() const;
  int64_t get_balance_weight() const;
  int append_part_group(ObPartGroupInfo *const part_group);
  int remove_part_group(ObPartGroupInfo *const part_group);
  int get_transfer_out_part_group(
      const ObBalanceGroupUnit &dst_unit,
      ObPartGroupInfo *&part_group) const;

  TO_STRING_KV(K_(inited), K_(bg_unit_id), "pg_count", get_pg_count(), "pg_data_size", get_data_size(),
      "pg_balance_weight", get_balance_weight(), K_(pg_buckets));

private:
  int get_transfer_out_bucket_(const ObBalanceGroupUnit &dst_unit, int64_t &bucket_idx) const;

private:
  bool inited_;
  ObObjectID bg_unit_id_;
  ObIAllocator &alloc_;
  ObArray<ObPartGroupBucket> pg_buckets_;
};

// the part group container for round robin partitition distribution
class ObPartGroupContainer
{
public:
  ObPartGroupContainer(ObIAllocator &alloc)
      : is_inited_(false),
        bucket_num_(0),
        alloc_(alloc),
        bg_units_(alloc) {}
  ~ObPartGroupContainer();
  int init(const ObBalanceGroupID &bg_id, const int64_t ls_num);
  bool is_inited() const { return is_inited_; }
  int append_part_group(ObPartGroupInfo *const part_group);
  int remove_part_group(ObPartGroupInfo *const part_group);
  int select(
      const int64_t balance_weight,
      ObPartGroupContainer &dst_pg,
      ObPartGroupInfo *&part_group) const;
  int get_largest_part_group(ObPartGroupInfo *&pg) const;
  int get_smallest_part_group(ObPartGroupInfo *&pg) const;
  int64_t get_part_group_count() const;
  int64_t get_data_size() const;
  int64_t get_balance_weight() const;
  int get_balance_weight_array(ObIArray<int64_t> &weight_arr) const;
  int split_out_weighted_part_groups(ObPartGroupContainer &pg_container);

  TO_STRING_KV(K_(is_inited), K_(bucket_num), "pg_count", get_part_group_count(),
      "pg_data_size", get_data_size(), "pg_balance_weight", get_balance_weight());

private:
  int get_bg_unit_(const uint64_t bg_unit_id, ObBalanceGroupUnit *&bg_unit) const;
  int get_or_create_bg_unit_(const uint64_t bg_unit_id, ObBalanceGroupUnit *&bg_unit);
  int get_transfer_out_unit_(const ObPartGroupContainer &dst_pgs, ObBalanceGroupUnit *&bg_unit) const;
  DISALLOW_COPY_AND_ASSIGN(ObPartGroupContainer);

private:
  class Iterator
  {
  public:
    Iterator(const ObPartGroupContainer &container)
      : is_end_(false),
        container_(container),
        bg_unit_iter_(container.bg_units_.begin()),
        bucket_idx_(0),
        pg_idx_(0) {}
    ~Iterator() {}
    void reset();
    int next(ObPartGroupInfo *&pg);
    TO_STRING_KV(K_(is_end), K_(container), K_(bucket_idx), K_(pg_idx));
  private:
    bool is_end_;
    const ObPartGroupContainer &container_;
    ObList<ObBalanceGroupUnit *, ObIAllocator>::const_iterator bg_unit_iter_;
    int64_t bucket_idx_;
    int64_t pg_idx_;
  };

  #define ITERATE_PG(PROCESS)                                \
  {                                                          \
    if (IS_NOT_INIT) {                                       \
      ret = OB_NOT_INIT;                                     \
      LOG_WARN("not init", KR(ret));                         \
    } else {                                                 \
      Iterator iter(*this);                                  \
      ObPartGroupInfo *tmp_pg = nullptr;                     \
      while (OB_SUCC(ret)) {                                 \
        if (OB_FAIL(iter.next(tmp_pg))) {                    \
          if (OB_ITER_END == ret) {                          \
            ret = OB_SUCCESS;                                \
            break;                                           \
          } else {                                           \
            LOG_WARN("iterator next failed", KR(ret));       \
          }                                                  \
        } else if (OB_ISNULL(tmp_pg)) {                      \
          ret = OB_ERR_UNEXPECTED;                           \
          LOG_WARN("null pg_info", KR(ret), KP(tmp_pg));     \
        } else {                                             \
          PROCESS;                                           \
        }                                                    \
      }                                                      \
    }                                                        \
  }

  template<typename Func>
  int for_each_(Func &func) const {
    int ret = OB_SUCCESS;
    ITERATE_PG(
      if (OB_FAIL(func(*tmp_pg))) {
        LOG_WARN("process pg failed", KR(ret));
      }
    );
    return ret;
  }

  template<typename Func>
  int find_(ObPartGroupInfo *&pg, Func &func) const {
    int ret = OB_SUCCESS;
    pg = nullptr;
    ITERATE_PG(
      if (func(*tmp_pg)) {
        pg = tmp_pg;
        ret = OB_SUCCESS;
        break; // end while
      }
    );
    if (OB_SUCC(ret) && OB_ISNULL(pg)) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  class PGFunctor
  {
  public:
    // pg balance weight > 0
    struct IsWeightedPG
    {
      bool operator()(const ObPartGroupInfo &tmp_pg) const {
        return tmp_pg.get_weight() > 0;
      }
    };

    // pg balance weight == target balance weight
    struct IsSameWeightPG
    {
      IsSameWeightPG(const int64_t weight) : balance_weight_(weight) {}
      bool operator()(const ObPartGroupInfo &tmp_pg) const {
        return tmp_pg.get_weight() == balance_weight_;
      }
      int64_t balance_weight_;
    };

    // get max data size pg
    struct GetMaxSizePG
    {
      GetMaxSizePG(int64_t &max_size, ObPartGroupInfo *&pg) : max_size_(max_size), pg_(pg) {}
      int operator()(ObPartGroupInfo &tmp_pg) {
        if (tmp_pg.get_data_size() > max_size_) {
          pg_ = &tmp_pg;
          max_size_ = tmp_pg.get_data_size();
        }
        return OB_SUCCESS;
      }
      int64_t &max_size_;
      ObPartGroupInfo *&pg_;
    };

    // get min data size pg
    struct GetMinSizePG
    {
      GetMinSizePG(int64_t &min_size, ObPartGroupInfo *&pg) : min_size_(min_size), pg_(pg) {}
      int operator()(ObPartGroupInfo &tmp_pg) {
        if (tmp_pg.get_data_size() < min_size_) {
          pg_ = &tmp_pg;
          min_size_ = tmp_pg.get_data_size();
        }
        return OB_SUCCESS;
      }
      int64_t &min_size_;
      ObPartGroupInfo *&pg_;
    };

    // get array of balance weight > 0
    struct GetWeightArray
    {
      GetWeightArray(ObIArray<int64_t> &weight_arr) : weight_arr_(weight_arr) {}
      int operator()(const ObPartGroupInfo &tmp_pg) {
        int ret = OB_SUCCESS;
        if (tmp_pg.get_weight() > 0) {
          if (OB_FAIL(weight_arr_.push_back(tmp_pg.get_weight()))) {
            LOG_WARN("push back failed", KR(ret), K(tmp_pg));
          }
        }
        return ret;
      }
      ObIArray<int64_t> &weight_arr_;
    };
  }; // end PGFunctor

private:
  const int64_t MAP_BUCKET_NUM = 4096;

private:
  bool is_inited_;
  int64_t bucket_num_;
  ObIAllocator &alloc_;
  ObList<ObBalanceGroupUnit *, ObIAllocator> bg_units_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_PART_GROUP_CONTAINER_H */
