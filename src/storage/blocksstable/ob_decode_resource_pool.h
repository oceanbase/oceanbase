/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_DECODE_RESOURCE_H_
#define OCEANBASE_BLOCKSSTABLE_OB_DECODE_RESOURCE_H_

#include "lib/objectpool/ob_small_obj_pool.h"
#include "storage/blocksstable/encoding/ob_encoding_allocator.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_allocator.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObColumnDecoderCtxBlock
{
  static const int64_t CTX_NUMS = 64;
  ObColumnDecoderCtxBlock():ctxs_(){}
  ObColumnDecoderCtx ctxs_[CTX_NUMS];
  TO_STRING_KV(KP(ctxs_));
};

struct ObColumnCSDecoderCtxBlock
{
  static const int64_t CS_CTX_NUMS = 32;
  ObColumnCSDecoderCtxBlock():ctxs_(){}
  ObColumnCSDecoderCtx ctxs_[CS_CTX_NUMS];
  TO_STRING_KV(KP(ctxs_));
};

class ObDecodeResourcePool
{
public:
  static ObDecodeResourcePool &get_instance();

  ObDecodeResourcePool(): raw_pool_(), dict_pool_(), rle_pool_(), const_pool_(), int_diff_pool_(),
                   str_diff_pool_(), hex_str_pool_(), str_prefix_pool_(),
                   column_equal_pool_(), column_substr_pool_(), ctx_block_pool_(),
                   cs_integer_pool_(), cs_string_pool_(), cs_int_dict_pool_(),
                   cs_str_dict_pool_(), cs_ctx_block_pool_(), is_inited_(false) {}
  ~ObDecodeResourcePool();
  static int mtl_init(ObDecodeResourcePool *&ctx_array_pool);
  void destroy();
  int init();
  int reload_config();
  template <typename T>
  int alloc(T *&item);
  template <typename T>
  int free(T *item);
private:
  uint64_t get_adaptive_factor(const uint64_t tenant_id) const;
  template<typename T>
  ObSmallObjPool<T> &get_pool();
private:
  static const uint64_t MIN_FACTOR = 1; // 5.5M
  static const uint64_t MAX_FACTOR = 50; // 275.5M
  static const int64_t MAX_DECODER_CNT = 4096;
  static const int64_t MID_DECODER_CNT = MAX_DECODER_CNT / 2;
  static const int64_t MIN_DECODER_CNT = MAX_DECODER_CNT / 4;
  static const int64_t MAX_CS_DECODER_CNT = 4096;
  static const int64_t MAX_CTX_BLOCK_CNT =
      MAX_DECODER_CNT * ObColumnHeader::MAX_TYPE / ObColumnDecoderCtxBlock::CTX_NUMS;
  static const int64_t MAX_CS_CTX_BLOCK_CNT =
      MAX_CS_DECODER_CNT * ObCSColumnHeader::MAX_TYPE / ObColumnCSDecoderCtxBlock::CS_CTX_NUMS;
  ObSmallObjPool<ObRawDecoder> raw_pool_;
  ObSmallObjPool<ObDictDecoder> dict_pool_;
  ObSmallObjPool<ObRLEDecoder> rle_pool_;
  ObSmallObjPool<ObConstDecoder> const_pool_;
  ObSmallObjPool<ObIntegerBaseDiffDecoder> int_diff_pool_;
  ObSmallObjPool<ObStringDiffDecoder> str_diff_pool_;
  ObSmallObjPool<ObHexStringDecoder> hex_str_pool_;
  ObSmallObjPool<ObStringPrefixDecoder> str_prefix_pool_;
  ObSmallObjPool<ObColumnEqualDecoder> column_equal_pool_;
  ObSmallObjPool<ObInterColSubStrDecoder> column_substr_pool_;
  ObSmallObjPool<ObColumnDecoderCtxBlock> ctx_block_pool_;
  // cs decoding need
  ObSmallObjPool<ObIntegerColumnDecoder> cs_integer_pool_;
  ObSmallObjPool<ObStringColumnDecoder> cs_string_pool_;
  ObSmallObjPool<ObIntDictColumnDecoder> cs_int_dict_pool_;
  ObSmallObjPool<ObStrDictColumnDecoder> cs_str_dict_pool_;
  ObSmallObjPool<ObColumnCSDecoderCtxBlock> cs_ctx_block_pool_;
  bool is_inited_;
};


class ObDecoderPool
{
  static const int16_t MAX_FREE_CNT = 128;
  static const int16_t MID_FREE_CNT = MAX_FREE_CNT / 2;
  static const int16_t MIN_FREE_CNT = MAX_FREE_CNT / 4;
public:
  ObDecoderPool();
  virtual ~ObDecoderPool() { reset(); }
  template <typename T>
  inline int alloc(T *&item);
  template <typename T>
  inline int free(T *item);
  void reset();
private:
  constexpr static int16_t MAX_CNTS[] = {MAX_FREE_CNT, MAX_FREE_CNT,
      MID_FREE_CNT, MAX_FREE_CNT, MID_FREE_CNT, MIN_FREE_CNT,
      MIN_FREE_CNT, MIN_FREE_CNT, MIN_FREE_CNT, MIN_FREE_CNT};
  template <typename T>
  inline int alloc_miss_cache(T *&item);
  inline bool has_decoder(const ObColumnHeader::Type &type) const;
  inline bool not_reach_limit(const ObColumnHeader::Type &type) const;
  template <typename T>
  inline void pop_decoder(const ObColumnHeader::Type &type, T *&item);
  inline void push_decoder(const ObColumnHeader::Type &type, ObIColumnDecoder *item);
  template <typename T>
  inline int free_decoders(ObDecodeResourcePool &decode_res_pool,
                            const ObColumnHeader::Type &type);
  ObIColumnDecoder* raw_pool_[MAX_CNTS[ObColumnHeader::RAW]];
  ObIColumnDecoder* dict_pool_[MAX_CNTS[ObColumnHeader::DICT]];
  ObIColumnDecoder* rle_pool_[MAX_CNTS[ObColumnHeader::RLE]];
  ObIColumnDecoder* const_pool_[MAX_CNTS[ObColumnHeader::CONST]];
  ObIColumnDecoder* int_diff_pool_[MAX_CNTS[ObColumnHeader::INTEGER_BASE_DIFF]];
  ObIColumnDecoder* str_diff_pool_[MAX_CNTS[ObColumnHeader::STRING_DIFF]];
  ObIColumnDecoder* hex_str_pool_[MAX_CNTS[ObColumnHeader::HEX_PACKING]];
  ObIColumnDecoder* str_prefix_pool_[MAX_CNTS[ObColumnHeader::STRING_PREFIX]];
  ObIColumnDecoder* column_equal_pool_[MAX_CNTS[ObColumnHeader::COLUMN_EQUAL]];
  ObIColumnDecoder* column_substr_pool_[MAX_CNTS[ObColumnHeader::COLUMN_SUBSTR]];
  ObIColumnDecoder** pools_[ObColumnHeader::MAX_TYPE];
  int16_t free_cnts_[ObColumnHeader::MAX_TYPE];
};

////////////////////////// CSDecoder///////////////////////////////////////
class ObCSDecoderPool
{
  static const int16_t MAX_CS_FREE_CNT = 128;
public:
  ObCSDecoderPool();
  virtual ~ObCSDecoderPool() { reset(); }
  template <typename T>
  inline int alloc(T *&item);
  template <typename T>
  inline int free(T *item);
  void reset();
private:
  constexpr static int16_t MAX_CS_CNTS[ObCSColumnHeader::MAX_TYPE] =
      {MAX_CS_FREE_CNT, MAX_CS_FREE_CNT, MAX_CS_FREE_CNT, MAX_CS_FREE_CNT};
  template <typename T>
  inline int alloc_miss_cache(T *&item);
  inline bool has_decoder(const ObCSColumnHeader::Type &type) const;
  inline bool not_reach_limit(const ObCSColumnHeader::Type &type) const;
  template <typename T>
  inline void pop_decoder(const ObCSColumnHeader::Type &type, T *&item);
  inline void push_decoder(const ObCSColumnHeader::Type &type, ObIColumnCSDecoder *item);
  template <typename T>
  inline int free_decoders(ObDecodeResourcePool &decode_res_pool,
                            const ObCSColumnHeader::Type &type);
  ObIColumnCSDecoder* cs_integer_pool_[MAX_CS_CNTS[ObCSColumnHeader::INTEGER]];
  ObIColumnCSDecoder* cs_string_pool_[MAX_CS_CNTS[ObCSColumnHeader::STRING]];
  ObIColumnCSDecoder* cs_int_dict_pool_[MAX_CS_CNTS[ObCSColumnHeader::INT_DICT]];
  ObIColumnCSDecoder* cs_str_dict_pool_[MAX_CS_CNTS[ObCSColumnHeader::INT_DICT]];
  ObIColumnCSDecoder** pools_[ObCSColumnHeader::MAX_TYPE];
  int16_t free_cnts_[ObCSColumnHeader::MAX_TYPE];
};

#include "ob_decode_resource_pool.ipp"

}
}

#endif //OCEANBASE_BLOCKSSTABLE_OB_DECODE_RESOURCE_H_
