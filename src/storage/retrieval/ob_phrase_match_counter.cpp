/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_phrase_match_counter.h"

namespace oceanbase
{
namespace storage
{

ObPhraseMatchCounter::ObPhraseMatchCounter()
  : slop_(0),
    token_ids_(),
    token_offsets_(),
    pos_lists_(),
    leads_(),
    total_match_count_(0.0),
    is_inited_(false)
{}

int ObPhraseMatchCounter::create(
    ObIAllocator &alloc,
    const ObIArray<int64_t> &ids,
    const ObIArray<int64_t> &offsets,
    const int64_t slop,
    const bool has_duplicate_tokens,
    ObPhraseMatchCounter *&counter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slop < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected negative slop", K(ret), K(slop));
  } else if (0 == slop) {
    if (OB_ISNULL(counter = OB_NEWx(ObExactPhraseMatchCounter, &alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for exact phrase match counter", K(ret));
    }
  } else if (!has_duplicate_tokens) {
    if (OB_ISNULL(counter = OB_NEWx(ObUniqueTokensSloppyPhraseMatchCounter, &alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for unique tokens phrase match counter", K(ret));
    }
  } else {
    if (OB_ISNULL(counter = OB_NEWx(ObRepeatableTokensSloppyPhraseMatchCounter, &alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for repeatable tokens phrase match counter", K(ret));
    }
  }
  if (FAILEDx(counter->init(alloc, ids, offsets, slop))) {
    LOG_WARN("failed to init phrase match counter", K(ret));
  }
  return ret;
}

int ObPhraseMatchCounter::init(
    ObIAllocator &alloc,
    const ObIArray<int64_t> &ids,
    const ObIArray<int64_t> &offsets,
    const int64_t slop)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ids.count() != offsets.count() || ids.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array size", K(ret), K(ids.count()), K(offsets.count()));
  } else if (OB_UNLIKELY(slop < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected slop", K(ret), K(slop));
  } else {
    slop_ = slop;
    token_ids_.set_allocator(&alloc);
    token_offsets_.set_allocator(&alloc);
    pos_lists_.set_allocator(&alloc);
    leads_.set_allocator(&alloc);
    if (OB_FAIL(token_ids_.assign(ids))) {
      LOG_WARN("failed to assign token indices", K(ret));
    } else if (OB_FAIL(token_offsets_.assign(offsets))) {
      LOG_WARN("failed to assign token offsets", K(ret));
    } else if (OB_FAIL(pos_lists_.init(ids.count()))) {
      LOG_WARN("failed to init pos lists", K(ret));
    } else if (OB_FAIL(pos_lists_.prepare_allocate(ids.count()))) {
      LOG_WARN("failed to prepare allcoate pos lists", K(ret));
    } else if (OB_FAIL(leads_.init(ids.count()))) {
      LOG_WARN("failed to init leads", K(ret));
    } else if (OB_FAIL(leads_.prepare_allocate(ids.count()))) {
      LOG_WARN("failed to prepare allocate leads", K(ret));
    } else if (OB_FAIL(inner_init(alloc))) {}
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObPhraseMatchCounter::reset()
{
  token_ids_.reset();
  token_offsets_.reset();
  for (int64_t i = 0; i < pos_lists_.count(); ++i) {
    pos_lists_[i].reset();
  }
  pos_lists_.reset();
  leads_.reset();
  inner_reset();
  is_inited_ = false;
}

int ObPhraseMatchCounter::prepare(
    const ObIArray<ObArray<int64_t>> &abs_pos_lists,
    bool &early_stop)
{
  int ret = OB_SUCCESS;
  early_stop = false;
  for (int64_t i = 0; OB_SUCC(ret) && !early_stop && i < token_ids_.count(); ++i) {
    if (OB_UNLIKELY(token_ids_.at(i) < 0 || token_ids_.at(i) >= abs_pos_lists.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected token id", K(ret), K(token_ids_.at(i)), K(abs_pos_lists.count()));
    } else {
      ObArray<int64_t> &pos_list = pos_lists_.at(i);
      pos_list.reuse();
      const ObArray<int64_t> &abs_pos_list = abs_pos_lists.at(token_ids_.at(i));
      if (OB_UNLIKELY(abs_pos_list.empty())) {
        early_stop = true;
      } else if (OB_FAIL(pos_list.prepare_allocate(abs_pos_list.count()))) {
        LOG_WARN("failed to prepare allocate pos list", K(ret));
      } else {
        for (int64_t j = 0; j < pos_list.count(); ++j) {
          pos_list.at(j) = abs_pos_list.at(j) - token_offsets_.at(i);
        }
        leads_.at(i) = 0;
      }
    }
  }
  total_match_count_ = 0.0;
  return ret;
}

ObExactPhraseMatchCounter::ObExactPhraseMatchCounter()
  : ObPhraseMatchCounter()
{}

int ObExactPhraseMatchCounter::inner_init(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != slop_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected slop", K(ret), K_(slop));
  }
  return ret;
}

void ObExactPhraseMatchCounter::inner_reset()
{}

int ObExactPhraseMatchCounter::count_matches(
    const ObIArray<ObArray<int64_t>> &abs_pos_lists,
    double &total_match_count)
{
  int ret = OB_SUCCESS;
  bool early_stop = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(prepare(abs_pos_lists, early_stop))) {
    LOG_WARN("failed to prepare", K(ret));
  } else if (!early_stop) {
    while (OB_SUCC(ret)) {
      bool end = false;
      for (int64_t i = 1; OB_SUCC(ret) && !end && i < token_ids_.count(); ++i) {
        while (leads_.at(i) < pos_lists_.at(i).count() && get_pos(i) < get_pos(0)) {
          ++leads_.at(i);
        }
        if (leads_.at(i) >= pos_lists_.at(i).count()) {
          ret = OB_ITER_END;
        } else if (get_pos(i) > get_pos(0)) {
          while (leads_.at(0) < pos_lists_.at(0).count() && get_pos(0) < get_pos(i)) {
            ++leads_.at(0);
          }
          if (leads_.at(0) >= pos_lists_.at(0).count()) {
            ret = OB_ITER_END;
          } else {
            end = true;
          }
        }
      }
      if (OB_SUCC(ret) && !end) {
        ++total_match_count_;
        ++leads_.at(0);
        if (leads_.at(0) >= pos_lists_.at(0).count()) {
          ret = OB_ITER_END;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  total_match_count = total_match_count_;
  return ret;
}

ObSloppyPhraseMatchCounter::ObSloppyPhraseMatchCounter()
  : ObPhraseMatchCounter()
{}

int ObSloppyPhraseMatchCounter::count_matches(
    const ObIArray<ObArray<int64_t>> &abs_pos_lists,
    double &total_match_count)
{
  int ret = OB_SUCCESS;
  bool early_stop = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(prepare(abs_pos_lists, early_stop))) {
    LOG_WARN("failed to prepare", K(ret));
  } else if (early_stop) {
    // skip the following
  } else if (OB_FAIL(extra_prepare(abs_pos_lists, early_stop))) {
    LOG_WARN("failed to extra prepare", K(ret));
  } else if (early_stop) {
    // skip the following
  } else if (OB_FAIL(evaluate(-1))) {
    LOG_WARN("failed to evaluate", K(ret));
  } else {
    int64_t target = -1;
    int64_t bound = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(choose(target, bound))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to choose target", K(ret));
        }
      } else {
        while (OB_SUCC(ret) && get_pos(target) <= bound) {
          if (OB_FAIL(advance(target))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to advance target", K(ret));
            }
          } else if (OB_FAIL(evaluate(target))) {
            LOG_WARN("failed to evaluate", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  total_match_count = total_match_count_;
  return ret;
}

ObUniqueTokensSloppyPhraseMatchCounter::ObUniqueTokensSloppyPhraseMatchCounter()
  : ObSloppyPhraseMatchCounter(),
    cmp_(),
    movables_(cmp_, nullptr),
    immovable_min_pos_(0),
    max_pos_(0)
{}

int ObUniqueTokensSloppyPhraseMatchCounter::inner_init(ObIAllocator &alloc)
{
  return OB_SUCCESS;
}

void ObUniqueTokensSloppyPhraseMatchCounter::inner_reset()
{
  movables_.reset();
}

int ObUniqueTokensSloppyPhraseMatchCounter::extra_prepare(
    const ObIArray<ObArray<int64_t>> &abs_pos_lists,
    bool &early_stop)
{
  int ret = OB_SUCCESS;
  early_stop = false;
  while (OB_SUCC(ret) && !movables_.empty()) {
    if (OB_FAIL(movables_.pop())) {
      LOG_WARN("failed to pop from movables", K(ret));
    }
  }
  max_pos_ = INT64_MIN;
  immovable_min_pos_ = INT64_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < token_ids_.count(); ++i) {
    if (leads_.at(i) + 1 < pos_lists_.at(i).count()) {
      if (OB_FAIL(movables_.push(PosItem(get_pos(i), i)))) {
        LOG_WARN("failed to push into movables", K(ret));
      }
    } else {
      immovable_min_pos_ = MIN(immovable_min_pos_, get_pos(i));
    }
    max_pos_ = MAX(max_pos_, get_pos(i));
  }
  return ret;
}

int ObUniqueTokensSloppyPhraseMatchCounter::choose(int64_t &target, int64_t &bound)
{
  int ret = OB_SUCCESS;
  const int64_t last_target = target;
  if (last_target >= 0) {
    if (leads_.at(last_target) + 1 < pos_lists_.at(last_target).count()) {
      if (OB_FAIL(movables_.push(PosItem(get_pos(last_target), last_target)))) {
        LOG_WARN("failed to push into movables", K(ret));
      }
    } else {
      immovable_min_pos_ = MIN(immovable_min_pos_, get_pos(last_target));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (movables_.empty()) {
    ret = OB_ITER_END;
  } else {
    target = movables_.top().idx_;
    if (OB_FAIL(movables_.pop())) {
      LOG_WARN("failed to pop from movables");
    } else if (!movables_.empty()) {
      bound = movables_.top().pos_;
    } else {
      bound = INT64_MAX;
    }
  }
  return ret;
}

int ObUniqueTokensSloppyPhraseMatchCounter::advance(const int64_t target)
{
  int ret = OB_SUCCESS;
  if (leads_.at(target) + 1 >= pos_lists_.at(target).count()) {
    ret = OB_ITER_END;
  } else {
    ++leads_.at(target);
    max_pos_ = MAX(max_pos_, get_pos(target));
  }
  return ret;
}

int ObUniqueTokensSloppyPhraseMatchCounter::evaluate(const int64_t target)
{
  int ret = OB_SUCCESS;
  int64_t min_pos = immovable_min_pos_;
  if (target >= 0) {
    min_pos = MIN(min_pos, get_pos(target));
  }
  if (!movables_.empty()) {
    min_pos = MIN(min_pos, movables_.top().pos_);
  }
  const int64_t dist = max_pos_ - min_pos;
  if (dist <= slop_) {
    total_match_count_ += effective_match_count(dist);
  }
  return ret;
}

ObRepeatableTokensSloppyPhraseMatchCounter::ObRepeatableTokensSloppyPhraseMatchCounter()
  : ObSloppyPhraseMatchCounter(),
    token_groups_(),
    ingroup_indices_(),
    rest_min_pos_(0),
    max_pos_(0)
{}

int ObRepeatableTokensSloppyPhraseMatchCounter::inner_init(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  token_groups_.set_allocator(&alloc);
  ingroup_indices_.set_allocator(&alloc);
  if (OB_FAIL(token_groups_.init(token_ids_.count()))) {
    LOG_WARN("failed to init token groups", K(ret));
  } else if (OB_FAIL(token_groups_.prepare_allocate(token_ids_.count()))) {
    LOG_WARN("failed to prepare allocate token groups", K(ret));
  } else if (OB_FAIL(ingroup_indices_.init(token_ids_.count()))) {
    LOG_WARN("failed to init ingroup indices", K(ret));
  } else if (OB_FAIL(ingroup_indices_.prepare_allocate(token_ids_.count()))) {
    LOG_WARN("failed to prepare allocate ingroup indices", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < token_ids_.count(); ++i) {
    if (OB_UNLIKELY(token_ids_.at(i) < 0 || token_ids_.at(i) >= token_ids_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected token id", K(token_ids_.at(i)), K(token_ids_.count()));
    } else {
      ingroup_indices_.at(i) = token_groups_.at(token_ids_.at(i)).count();
      if (OB_FAIL(token_groups_.at(token_ids_.at(i)).push_back(i))) {
        LOG_WARN("failed to push back into token ids", K(ret));
      }
    }
  }
  return ret;
}

void ObRepeatableTokensSloppyPhraseMatchCounter::inner_reset()
{
  for (int64_t i = 0; i < token_groups_.count(); ++i) {
    token_groups_.at(i).reset();
  }
  token_groups_.reset();
  ingroup_indices_.reset();
}

int ObRepeatableTokensSloppyPhraseMatchCounter::extra_prepare(
    const ObIArray<ObArray<int64_t>> &abs_pos_lists,
    bool &early_stop)
{
  int ret = OB_SUCCESS;
  early_stop = false;
  for (int64_t id = 0; OB_SUCC(ret) && !early_stop && id < token_groups_.count(); ++id) {
    const ObIArray<int64_t> &token_group = token_groups_.at(id);
    if (token_group.empty()) {
      // skip
    } else if (token_group.count() > abs_pos_lists.at(id).count()) {
      early_stop = true;
    } else if (token_group.count() > 1) {
      for (int64_t i = 0; i < token_group.count(); ++i) {
        leads_.at(token_group.at(i)) = i;
      }
    }
  }
  if (OB_SUCC(ret) && !early_stop) {
    max_pos_ = INT64_MIN;
    for (int64_t i = 0; i < token_ids_.count(); ++i) {
      max_pos_ = MAX(max_pos_, get_pos(i));
    }
  }
  return ret;
}

int ObRepeatableTokensSloppyPhraseMatchCounter::choose(int64_t &target, int64_t &bound)
{
  int ret = OB_SUCCESS;
  const int64_t last_target = target;
  target = -1;
  int64_t first_min_pos = INT64_MAX;
  int64_t second_min_pos = INT64_MAX;
  for (int64_t i = 0; i < token_ids_.count(); ++i) {
    const ObIArray<int64_t> &token_group = token_groups_.at(token_ids_.at(i));
    const int64_t rev_ingroup_idx = token_group.count() - ingroup_indices_.at(i);
    if (leads_.at(i) + rev_ingroup_idx < pos_lists_.at(i).count()) {
      if (get_pos(i) < first_min_pos) {
        second_min_pos = first_min_pos;
        first_min_pos = get_pos(i);
        target = i;
      } else if (get_pos(i) < second_min_pos) {
        second_min_pos = get_pos(i);
      }
    }
  }
  if (target < 0) {
    ret = OB_ITER_END;
  } else {
    bound = second_min_pos;
    if (last_target < 0 || token_ids_.at(target) != token_ids_.at(last_target)) {
      rest_min_pos_ = INT64_MAX;
      for (int64_t i = 0; i < token_ids_.count(); ++i) {
        if (token_ids_.at(target) != token_ids_.at(i)) {
          rest_min_pos_ = MIN(rest_min_pos_, get_pos(i));
        }
      }
    }
  }
  return ret;
}

int ObRepeatableTokensSloppyPhraseMatchCounter::advance(const int64_t target)
{
  int ret = OB_SUCCESS;
  const ObIArray<int64_t> &token_group = token_groups_.at(token_ids_.at(target));
  const int64_t rev_ingroup_idx = token_group.count() - ingroup_indices_.at(target);
  if (leads_.at(target) + rev_ingroup_idx >= pos_lists_.at(target).count()) {
    ret = OB_ITER_END;
  } else {
    ++leads_.at(target);
    max_pos_ = MAX(max_pos_, get_pos(target));
    bool end = false;
    for (int64_t i = ingroup_indices_.at(target) + 1; !end && i < token_group.count(); ++i) {
      if (leads_.at(token_group.at(i)) == leads_.at(token_group.at(i - 1))) {
        ++leads_.at(token_group.at(i));
        max_pos_ = MAX(max_pos_, get_pos(token_group.at(i)));
      } else {
        end = true;
      }
    }
  }
  return ret;
}

int ObRepeatableTokensSloppyPhraseMatchCounter::evaluate(const int64_t target)
{
  int ret = OB_SUCCESS;
  int64_t min_pos = INT64_MAX;
  if (target >= 0) {
    min_pos = rest_min_pos_;
    const ObIArray<int64_t> &token_group = token_groups_.at(token_ids_.at(target));
    for (int64_t i = 0; i < token_group.count(); ++i) {
      min_pos = MIN(min_pos, get_pos(token_group.at(i)));
    }
  } else {
    for (int64_t i = 0; i < token_ids_.count(); ++i) {
      min_pos = MIN(min_pos, get_pos(i));
    }
  }
  const int64_t dist = max_pos_ - min_pos;
  if (dist <= slop_) {
    total_match_count_ += effective_match_count(dist);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
