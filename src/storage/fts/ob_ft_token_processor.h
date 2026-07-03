/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_FTS_TOKEN_PROCESSOR_H_
#define OB_FTS_TOKEN_PROCESSOR_H_

#include "object/ob_object.h"
#include "storage/fts/ob_fts_struct.h"

namespace oceanbase
{
namespace storage
{

class ObFTParserProperty;

class ObFTTokenProcessor final
{
public:
  ObFTTokenProcessor(ObIAllocator &scratch_allocator) :
      is_inited_(false), token_meta_(), token_map_(nullptr),
      non_stop_token_cnt_(0), flag_(), hash_func_(nullptr), cmp_func_(nullptr),
      scratch_allocator_(scratch_allocator) { }
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
                    const int64_t position);
  OB_INLINE int64_t get_non_stop_token_count() const { return non_stop_token_cnt_; }
  VIRTUAL_TO_STRING_KV(
      K_(token_meta),
      K_(non_stop_token_cnt),
      KP_(token_map));

private:
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
  int groupby_token(const bool need_pos_list, const ObFTToken &token, const int64_t position);

private:
  bool is_inited_;
  ObObjMeta token_meta_;
  ObFTTokenMap *token_map_;
  int64_t non_stop_token_cnt_;
  ObProcessTokenFlag flag_;
  sql::ObExprHashFuncType hash_func_;
  ObDatumCmpFuncType cmp_func_;
  ObIAllocator &scratch_allocator_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OB_FTS_TOKEN_PROCESSOR_H_
