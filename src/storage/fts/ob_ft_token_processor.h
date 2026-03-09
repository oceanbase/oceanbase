/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_FTS_TOKEN_PROCESSOR_H_
#define OB_FTS_TOKEN_PROCESSOR_H_

#include "object/ob_object.h"
#include "storage/fts/ob_fts_struct.h"
#include "storage/fts/ob_fts_stop_token_check.h"

namespace oceanbase
{
namespace storage
{

class ObFTParserProperty;

class ObFTTokenProcessor final
{
public:
  ObFTTokenProcessor(ObIAllocator &scratch_allocator) :
      is_inited_(false), token_meta_(), token_map_(nullptr), min_max_token_cnt_(0),
      non_stop_token_cnt_(0), stop_token_cnt_(0), min_token_size_(0),
      max_token_size_(0), flag_(), hash_func_(nullptr), cmp_func_(nullptr),
      stop_token_checker_(), scratch_allocator_(scratch_allocator) { }
  ~ObFTTokenProcessor() = default;
  int init(const ObFTParserProperty &property,
           const ObObjMeta &meta,
           const ObProcessTokenFlag &flag,
           ObFTTokenMap *token_map);
  void reset();
  void reuse();
  int process_token(const bool need_pos_list,
                    const char *token,
                    const int64_t token_len,
                    const int64_t char_cnt,
                    const int64_t position);
  OB_INLINE int64_t get_non_stop_token_count() const { return non_stop_token_cnt_; }
  VIRTUAL_TO_STRING_KV(
      K_(token_meta),
      K_(min_max_token_cnt),
      K_(non_stop_token_cnt),
      K_(stop_token_cnt),
      K_(min_token_size),
      K_(max_token_size),
      KP_(token_map));

private:
  static constexpr int64_t MAX_CHAR_COUNT_PER_TOKEN = 1024;
  class UpdateTokenCallBack final
  {
  public:
    UpdateTokenCallBack(ObIAllocator &allocator, const int64_t position)
    : allocator_(allocator), position_(position) { }
    ~UpdateTokenCallBack() = default;
    int operator()(common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> &pair);

  private:
    ObIAllocator &allocator_;
    int64_t position_;
  };
  class UpdateTokenWithoutPosListCallBack final
  {
  public:
    UpdateTokenWithoutPosListCallBack() = default;
    ~UpdateTokenWithoutPosListCallBack() = default;
    int operator()(common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> &pair);
  };
private:
  bool is_min_max_token(const int64_t c_len) const;
  int groupby_token(const bool need_pos_list, const ObFTToken &token, const int64_t position);

private:
  bool is_inited_;
  ObObjMeta token_meta_;
  ObFTTokenMap *token_map_;
  int64_t min_max_token_cnt_;
  int64_t non_stop_token_cnt_;
  int64_t stop_token_cnt_;
  int64_t min_token_size_;
  int64_t max_token_size_;
  ObProcessTokenFlag flag_;
  sql::ObExprHashFuncType hash_func_;
  ObDatumCmpFuncType cmp_func_;
  ObStopTokenChecker stop_token_checker_;
  ObIAllocator &scratch_allocator_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_TOKEN_PROCESSOR_H_
