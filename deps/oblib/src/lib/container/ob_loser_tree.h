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

#ifndef OB_LOSER_TREE_H_
#define OB_LOSER_TREE_H_
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

enum ObRowMergerType
{
  SIMPLE_MERGER,
  LOSER_TREE,
  MAJOR_ROWS_MERGE
};

template <typename T, typename Comparator>
class ObRowsMerger
{
public:
  virtual ~ObRowsMerger() = default;
  virtual ObRowMergerType type() = 0;
  virtual int init(const int64_t table_cnt, common::ObIAllocator &allocator) = 0;
  virtual bool is_inited() const = 0;
  virtual int open(const int64_t table_cnt) = 0;
  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual int top(const T *&row) = 0;
  virtual int pop() = 0;
  virtual int push(const T &row) = 0;
  virtual int push_top(const T &row) = 0;
  virtual int rebuild() = 0;
  virtual int count() const = 0;
  virtual bool empty() const = 0;
  virtual bool is_unique_champion() const = 0;
  TO_STRING_KV("name", "ObRowsMerger")
};

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
class ObLoserTree : public ObRowsMerger<T, CompareFunctor>
{
public:
  virtual ObRowMergerType type() override { return ObRowMergerType::LOSER_TREE; }
  explicit ObLoserTree(CompareFunctor &cmp);
  virtual ~ObLoserTree() { reset(); };

  virtual int init(const int64_t total_player_cnt, common::ObIAllocator &allocator);
  virtual bool is_inited() const { return is_inited_; }
  virtual int open(const int64_t total_player_cnt);
  virtual void reset();
  virtual void reuse();

  virtual int top(const T *&player);
  virtual int pop();
  virtual int push(const T &player);
  virtual int push_top(const T &player);
  virtual int rebuild();

  virtual OB_INLINE int count() const { return player_cnt_ - cur_free_cnt_; }
  virtual OB_INLINE bool empty() const { return player_cnt_ == cur_free_cnt_; }
  virtual OB_INLINE bool is_unique_champion() const { return is_unique_champion_; }

protected:
  enum class ReplayType
  {
    NO_CHANGE = 0,
    LOSER_CHANGE = 1,
    WINNER_CHANGE = 2,
    BOTH_CHANGE = 3
  };

  struct MatchResult
  {
  public:
    MatchResult() { reset(); };
    ~MatchResult() = default;
    void reset()
    {
      winner_idx_ = INVALID_IDX;
      loser_idx_ = INVALID_IDX;
      is_draw_ = false;
      replay_ = ReplayType::NO_CHANGE;
    }
    int64_t winner_idx_;
    int64_t loser_idx_;
    bool is_draw_;
    ReplayType replay_;

    TO_STRING_KV(K(winner_idx_), K(loser_idx_), K(is_draw_), K(replay_));

    int set_replay_type(const int64_t player);
  };

  int alloc_max_space();
  int inner_rebuild();
  int update_champion_path(const int64_t idx);
  int get_match_result(
      const int64_t offender,
      const int64_t defender,
      const int64_t match_idx);
  // return TRUE if offender win
  virtual int duel(T &offender, T &defender, const int64_t match_idx, bool &is_offender_win);
  void set_unique_champion();

  OB_INLINE bool is_single_player() const { return 1 == player_cnt_; }
  OB_INLINE size_t get_leaf(const size_t player_idx) { return leaf_offset_ + player_idx; }
  static int64_t get_match_cnt(const int64_t player_cnt);
  static OB_INLINE size_t get_parent(const size_t index) { return (index - 1) / 2; }
  static OB_INLINE size_t get_left(const size_t index) { return 2 * index + 1; }
  static OB_INLINE size_t get_right(const size_t index) { return 2 * index + 2; }

private:
  int init_basic_info(const int64_t total_player_cnt);
  ObLoserTree() = delete;
  ObLoserTree(const ObLoserTree&) = delete;
  void operator=(const ObLoserTree&) = delete;

protected:
  static const int64_t INVALID_IDX = -1;

  bool is_inited_;
  int64_t player_cnt_;
  int64_t match_cnt_;
  int64_t leaf_offset_;
  T *players_;
  MatchResult *matches_;
  int64_t *free_players_; //record the idx of players who have quit
  int64_t cur_free_cnt_;
  bool need_rebuild_;
  bool is_unique_champion_; //if the champion has draw game in the tree
  CompareFunctor &cmp_;
  common::ObIAllocator *allocator_;
};

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::MatchResult::set_replay_type(
    const int64_t player)
{
  int ret = OB_SUCCESS;
  if (winner_idx_ == INVALID_IDX && loser_idx_ == INVALID_IDX) {
    replay_ = ReplayType::BOTH_CHANGE;
  } else if (replay_ == ReplayType::NO_CHANGE) {
    if (winner_idx_ == player) {
      replay_ = ReplayType::WINNER_CHANGE;
    } else if (loser_idx_ == player || loser_idx_ == INVALID_IDX) {
      replay_ = ReplayType::LOSER_CHANGE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "player mismatch", K(ret), K(player), K(winner_idx_), K(loser_idx_));
    }
  } else if (replay_ == ReplayType::BOTH_CHANGE) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "previous match not finish", K(ret), K(player), K(replay_));
  } else if (replay_ == ReplayType::WINNER_CHANGE) {
    if (winner_idx_ == player) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "duplicated winner", K(ret), K(player), K(replay_));
    } else if (loser_idx_ == player) {
      replay_ = ReplayType::BOTH_CHANGE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "player mismatch", K(ret), K(player), K(*this));
    }
  } else if (replay_ == ReplayType::LOSER_CHANGE) {
    if (loser_idx_ == player) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "duplicated loser", K(ret), K(player), K(replay_));
    } else if (winner_idx_ == player) {
      replay_ = ReplayType::BOTH_CHANGE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "player mismatch", K(ret), K(player), K(winner_idx_), K(loser_idx_));
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::ObLoserTree(CompareFunctor &cmp)
  :is_inited_(false), player_cnt_(0), match_cnt_(0), leaf_offset_(0),
   players_(nullptr), matches_(nullptr), free_players_(nullptr), cur_free_cnt_(0),
   need_rebuild_(false), is_unique_champion_(true), cmp_(cmp), allocator_(nullptr)
{
  STATIC_ASSERT(MAX_PLAYER_CNT > 0, "invalid player count");
  STATIC_ASSERT(std::is_trivially_copyable<T>::value, "class is not supported");
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int64_t ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::get_match_cnt(
    const int64_t player_cnt)
{
  int64_t match_cnt = 0;
  if (player_cnt < 0) {
    match_cnt = 0;
  } else if (player_cnt <= 1) {
    match_cnt = player_cnt;
  } else {
    match_cnt = next_pow2(player_cnt) * 2 - 1;
  }
  return match_cnt;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::alloc_max_space()
{
  int ret = OB_SUCCESS;
  int64_t player_cnt = MAX_PLAYER_CNT;
  int64_t match_cnt = get_match_cnt(MAX_PLAYER_CNT);
  if (nullptr == (players_ = static_cast<T*>(allocator_->alloc(sizeof(T) * player_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate players failed", K(ret), K(player_cnt), K(match_cnt));
  } else if (nullptr == (matches_ = static_cast<MatchResult*>(allocator_->alloc(sizeof(MatchResult) * match_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate matches failed", K(ret), K(player_cnt), K(match_cnt));
  } else if (nullptr == (free_players_ = static_cast<int64_t*>(allocator_->alloc(sizeof(int64_t) * player_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "allocate free players failed", K(ret), K(player_cnt), K(match_cnt));
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::init(
    const int64_t total_player_cnt,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "init twice", K(ret));
  } else {
    allocator_ = &allocator;
    if (OB_FAIL(alloc_max_space())) {
      LIB_LOG(WARN, "alloc space fail,", K(ret));
    } else if (OB_FAIL(init_basic_info(total_player_cnt))) {
      LIB_LOG(WARN, "fail to init basic info", K(ret), K(total_player_cnt));
    } else {
      is_inited_ = true;
    }
  }

  if (!IS_INIT) {
    reset();
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::open(const int64_t total_player_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(init_basic_info(total_player_cnt))) {
    LIB_LOG(WARN, "fail to init basic info", K(ret), K(total_player_cnt));
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::init_basic_info(const int64_t total_player_cnt)
{
  int ret = OB_SUCCESS;
  if (total_player_cnt <= 0 || total_player_cnt > MAX_PLAYER_CNT) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "total player count is invalid", K(ret), K(total_player_cnt));
  } else {
    player_cnt_ = total_player_cnt;
    cur_free_cnt_ = player_cnt_;
    match_cnt_ = get_match_cnt(player_cnt_);
    leaf_offset_ = (match_cnt_ - 1) / 2;

    for (int64_t i = 0; i < match_cnt_; ++i) {
      matches_[i].reset();
    }
    for (int64_t i = 0; i < player_cnt_; ++i) {
      free_players_[i] = player_cnt_ - 1 - i;
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
void ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::reuse()
{
  player_cnt_ = 0;
  match_cnt_ = 0;
  leaf_offset_ = 0;
  cur_free_cnt_ = 0;
  need_rebuild_ = false;
  is_unique_champion_= true;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
void ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::reset()
{
  if (nullptr != allocator_) {
    if (nullptr != players_) {
      allocator_->free(players_);
    }
    if (nullptr != matches_) {
      allocator_->free(matches_);
    }
    if (nullptr != free_players_) {
      allocator_->free(free_players_);
    }
  }
  player_cnt_ = 0;
  match_cnt_ = 0;
  leaf_offset_ = 0;
  cur_free_cnt_ = 0;
  need_rebuild_ = false;
  is_unique_champion_= true;
  allocator_ = nullptr;
  players_ = nullptr;
  matches_ = nullptr;
  free_players_ = nullptr;
  is_inited_ = false;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::get_match_result(
    const int64_t offender,
    const int64_t defender,
    const int64_t match_idx)
{
  int ret = OB_SUCCESS;
  bool is_offender_win = false;
  if (offender >= player_cnt_ || defender >= player_cnt_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LIB_LOG(WARN, "out of range", K(ret), K(offender), K(defender), K(match_idx));
  } else if (INVALID_IDX == offender) {
    matches_[match_idx].winner_idx_ = defender;
    matches_[match_idx].loser_idx_ = offender;
    matches_[match_idx].is_draw_ = false;
  } else if (INVALID_IDX == defender) {
    matches_[match_idx].winner_idx_ = offender;
    matches_[match_idx].loser_idx_ = defender;
    matches_[match_idx].is_draw_ = false;
  } else if (OB_FAIL(duel(players_[offender], players_[defender], match_idx, is_offender_win))) {
    LIB_LOG(WARN, "duel fail", K(ret), K(match_idx), K(is_offender_win));
  } else {
    matches_[match_idx].winner_idx_ = is_offender_win ? offender : defender;
    matches_[match_idx].loser_idx_ = is_offender_win ? defender : offender;
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::duel(
    T &offender, T &defender, const int64_t match_idx, bool &is_offender_win)
{
  int ret = OB_SUCCESS;
  int64_t cmp_ret = 0;
  if (OB_FAIL(cmp_.cmp(offender, defender, cmp_ret))) {
    LIB_LOG(WARN, "compare fail", K(ret), K(match_idx), K(is_offender_win));
  } else {
    matches_[match_idx].is_draw_ = (0 == cmp_ret);
    is_offender_win = cmp_ret < 0;
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
void ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::set_unique_champion()
{
  is_unique_champion_ = true;
  const int64_t champion = matches_[0].winner_idx_;
  if (champion >= 0 && champion < player_cnt_) {
    int64_t child = get_leaf(champion);
    int64_t parent = INVALID_IDX;
    while (child > 0 && is_unique_champion_) {
      parent = get_parent(child);
      if (matches_[parent].is_draw_) {
        is_unique_champion_ = false;
      }
      child = parent;
    }
  }
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::rebuild()
{
  int ret = OB_SUCCESS;
  if (is_single_player()) {
    need_rebuild_ = false;
  } else if (!need_rebuild_) {
    // do nothing
  } else if (empty()) {
    ret = OB_EMPTY_RESULT;
    LIB_LOG(WARN, "the tree is already empty", K(ret));
  } else if (OB_FAIL(inner_rebuild())) {
    LIB_LOG(WARN, "fail to build", K(ret), K(need_rebuild_), K(player_cnt_),
        K(cur_free_cnt_), K(match_cnt_), K(matches_[0]));
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::inner_rebuild()
{
  int ret = OB_SUCCESS;
  int64_t l_player = INVALID_IDX;
  int64_t r_player = INVALID_IDX;
  int64_t old_winner = INVALID_IDX;
  for (int64_t j = leaf_offset_ - 1; OB_SUCC(ret) && j >= 0; --j) {
    l_player = get_left(j);
    r_player = get_right(j);
    old_winner = matches_[j].winner_idx_;

    if (ReplayType::NO_CHANGE == matches_[j].replay_) {
      // do nothing
    } else if (OB_FAIL(get_match_result(matches_[l_player].winner_idx_,
                                        matches_[r_player].winner_idx_,
                                        j))) {
      LIB_LOG(WARN, "get match result fail", K(ret), K(l_player), K(r_player), K(j));
    } else {
      if (matches_[j].replay_ == ReplayType::LOSER_CHANGE && old_winner == matches_[j].winner_idx_) {
        // loser still be loser, no need to change parent match
      } else if (j > 0 && OB_FAIL(matches_[get_parent(j)].set_replay_type(old_winner))) {
        LIB_LOG(WARN, "set parent replay type fail", K(ret), K(j));
      }

      if (OB_SUCC(ret)) {
        matches_[j].replay_ = ReplayType::NO_CHANGE;
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_unique_champion();
    need_rebuild_ = false;
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::update_champion_path(
    const int64_t idx)
{
  int ret = OB_SUCCESS;

  // set invalid player
  int64_t child = get_leaf(idx);
  matches_[child].winner_idx_ = INVALID_IDX;
  matches_[child].loser_idx_ = INVALID_IDX;

  int64_t parent = get_parent(child);
  int64_t winner_idx = INVALID_IDX;
  int64_t loser_idx = INVALID_IDX;
  while(OB_SUCC(ret) && child > 0) {
    winner_idx = matches_[child].winner_idx_;
    loser_idx = matches_[parent].loser_idx_;
    if (winner_idx == loser_idx && INVALID_IDX != winner_idx) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(INFO, "candidate is not winner", K(ret), K(child),
          K(parent), K(matches_[parent]), K(matches_[child]));
    } else if (OB_FAIL(get_match_result(winner_idx, loser_idx, parent))) {
      LIB_LOG(WARN, "get match result fail", K(ret), K(winner_idx), K(loser_idx), K(parent));
    } else {
      child = parent;
      parent = get_parent(child);
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::top(const T *&player)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "not init", K(ret));
  } else if (need_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "new players has been push, please rebuild", K(ret));
  } else if (empty()) {
    ret = OB_EMPTY_RESULT;
    LIB_LOG(WARN, "tree is empty or not start", K(ret));
  } else if (is_single_player()) {
    player = &(players_[0]);
  } else {
    const MatchResult result = matches_[0];
    if (result.winner_idx_ < 0 || result.winner_idx_ >= player_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "unexpected player", K(ret), K(result), K(player_cnt_), K(cur_free_cnt_));
    } else {
      player = &(players_[result.winner_idx_]);
    }
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::push(const T &player)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "not init", K(ret));
  } else if (cur_free_cnt_ <= 0){
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "player is full", K(ret), K(player_cnt_), K(cur_free_cnt_));
  } else if (is_single_player()) {
    players_[0] = player;
  } else {
    int64_t idx = free_players_[cur_free_cnt_ - 1];
    int64_t leaf = get_leaf(idx);
    int64_t parent = get_parent(leaf);
    if (OB_FAIL(matches_[parent].set_replay_type(idx))) {
      LIB_LOG(WARN, "fail to set replay type", K(ret), K(parent), K(leaf), K(player_cnt_));
    } else {
      players_[idx] = player;
      matches_[leaf].winner_idx_ = idx;
      matches_[leaf].loser_idx_ = idx;
    }
  }

  if (OB_SUCC(ret)) {
    --cur_free_cnt_;
    need_rebuild_ = true;
  }
  return ret;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::push_top(const T &player)
{
  UNUSED(player);
  return OB_NOT_SUPPORTED;
}

template <typename T, typename CompareFunctor, int64_t MAX_PLAYER_CNT>
int ObLoserTree<T, CompareFunctor, MAX_PLAYER_CNT>::pop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "not init", K(ret));
  } else if (need_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "new players has been push, please rebuild", K(ret));
  } else if (empty()) {
    ret = OB_EMPTY_RESULT;
    LIB_LOG(WARN, "the tree is already empty", K(ret));
  } else if (is_single_player()) {
    cur_free_cnt_ = player_cnt_;
  } else {
    const int64_t champion = matches_[0].winner_idx_;
    if (champion < 0 || champion >= player_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "champion is invalid", K(ret), K(matches_[0]));
    } else if (OB_FAIL(update_champion_path(champion))){
      LIB_LOG(WARN, "update_champion_path fail", K(ret), K(matches_[0]));
    } else {
      set_unique_champion();
      free_players_[cur_free_cnt_++] = champion;
    }
  }
  return ret;
}
} //namespace oceanbase
} //namespace common
#endif /* OB_LOSER_TREE_H_ */
