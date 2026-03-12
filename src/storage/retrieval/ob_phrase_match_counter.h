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

#ifndef OB_PHRASE_MATCH_COUNTER_H_
#define OB_PHRASE_MATCH_COUNTER_H_

#include "share/ob_define.h"
#include "lib/container/ob_heap.h"

namespace oceanbase
{
namespace storage
{

class ObPhraseMatchCounter
{
public:
  ObPhraseMatchCounter();
  virtual ~ObPhraseMatchCounter() {}
  static int create(
      ObIAllocator &alloc,
      const ObIArray<int64_t> &ids,
      const ObIArray<int64_t> &offsets,
      const int64_t slop,
      const bool has_duplicate_tokens,
      ObPhraseMatchCounter *&counter);
  void reset();
  virtual int count_matches(
      const ObIArray<ObArray<int64_t>> &abs_pos_lists,
      double &total_match_count) = 0;
protected:
  inline int64_t get_pos(const int64_t idx) const
  {
    return pos_lists_.at(idx).at(leads_.at(idx));
  }
protected:
  int init(
      ObIAllocator &alloc,
      const ObIArray<int64_t> &ids,
      const ObIArray<int64_t> &offsets,
      const int64_t slop);
  virtual int inner_init(ObIAllocator &alloc) = 0;
  virtual void inner_reset() = 0;
  int prepare(const ObIArray<ObArray<int64_t>> &abs_pos_lists, bool &early_stop);
protected:
  int64_t slop_;
  ObFixedArray<int64_t, ObIAllocator> token_ids_;
  ObFixedArray<int64_t, ObIAllocator> token_offsets_;
  ObFixedArray<ObArray<int64_t>, ObIAllocator> pos_lists_;
  ObFixedArray<int64_t, ObIAllocator> leads_;
  double total_match_count_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPhraseMatchCounter);
};

class ObExactPhraseMatchCounter final : public ObPhraseMatchCounter
{
public:
  ObExactPhraseMatchCounter();
  virtual ~ObExactPhraseMatchCounter() {}
  int count_matches(
      const ObIArray<ObArray<int64_t>> &abs_pos_lists,
      double &total_match_count) override;
protected:
  int inner_init(ObIAllocator &alloc) override;
  void inner_reset() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExactPhraseMatchCounter);
};

class ObSloppyPhraseMatchCounter : public ObPhraseMatchCounter
{
public:
  ObSloppyPhraseMatchCounter();
  virtual ~ObSloppyPhraseMatchCounter() {}
  int count_matches(
      const ObIArray<ObArray<int64_t>> &abs_pos_lists,
      double &total_match_count) override;
protected:
  static inline double effective_match_count(const int64_t dist)
  {
    return 1.0 / (1 + std::abs(dist));
  }
protected:
  virtual int extra_prepare(const ObIArray<ObArray<int64_t>> &abs_pos_lists, bool &early_stop) = 0;
  virtual int choose(int64_t &target, int64_t &bound) = 0;
  virtual int advance(const int64_t target) = 0;
  virtual int evaluate(const int64_t target) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSloppyPhraseMatchCounter);
};

class ObUniqueTokensSloppyPhraseMatchCounter final : public ObSloppyPhraseMatchCounter
{
public:
  ObUniqueTokensSloppyPhraseMatchCounter();
  virtual ~ObUniqueTokensSloppyPhraseMatchCounter() {}
protected:
  int inner_init(ObIAllocator &alloc) override;
  void inner_reset() override;
  int extra_prepare(const ObIArray<ObArray<int64_t>> &abs_pos_lists, bool &early_stop) override;
  int choose(int64_t &target, int64_t &bound) override;
  int advance(const int64_t target) override;
  int evaluate(const int64_t target) override;
protected:
  struct PosItem
  {
    PosItem() : pos_(0), idx_(0) {}
    PosItem(const int64_t pos, const int64_t idx) : pos_(pos), idx_(idx) {}
    ~PosItem() = default;
    TO_STRING_KV(K_(pos), K_(idx));
    int64_t pos_;
    int64_t idx_;
  };
  struct PosItemCmp
  {
    bool operator()(const PosItem &l, const PosItem &r) const
    {
      return l.pos_ > r.pos_ || (l.pos_ == r.pos_ && l.idx_ > r.idx_);
    }
    int get_error_code() { return OB_SUCCESS; }
  };
  PosItemCmp cmp_;
  ObBinaryHeap<PosItem, PosItemCmp> movables_;
  int64_t immovable_min_pos_;
  int64_t max_pos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUniqueTokensSloppyPhraseMatchCounter);
};

class ObRepeatableTokensSloppyPhraseMatchCounter final : public ObSloppyPhraseMatchCounter
{
public:
  ObRepeatableTokensSloppyPhraseMatchCounter();
  virtual ~ObRepeatableTokensSloppyPhraseMatchCounter() {}
protected:
  int inner_init(ObIAllocator &alloc) override;
  void inner_reset() override;
  int extra_prepare(const ObIArray<ObArray<int64_t>> &abs_pos_lists, bool &early_stop) override;
  int choose(int64_t &target, int64_t &bound) override;
  int advance(const int64_t target) override;
  int evaluate(const int64_t target) override;
protected:
  ObFixedArray<ObSEArray<int64_t, 1>, ObIAllocator> token_groups_;
  ObFixedArray<int64_t, ObIAllocator> ingroup_indices_;
  int64_t rest_min_pos_;
  int64_t max_pos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRepeatableTokensSloppyPhraseMatchCounter);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_PHRASE_MATCH_COUNTER_H_
