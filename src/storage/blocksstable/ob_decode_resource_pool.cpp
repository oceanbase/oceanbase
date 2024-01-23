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

#include "storage/blocksstable/ob_decode_resource_pool.h"

namespace oceanbase
{
namespace blocksstable
{
ObDecodeResourcePool::~ObDecodeResourcePool()
{
  destroy();
}

int ObDecodeResourcePool::mtl_init(ObDecodeResourcePool *&decode_res_pool)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(decode_res_pool->init())) {
    STORAGE_LOG(WARN, "failed to init tenant decode resource pool", K(ret));
  }
  return ret;
}

ObDecodeResourcePool &ObDecodeResourcePool::get_instance()
{
  static ObDecodeResourcePool instance;
  return instance;
}

void ObDecodeResourcePool::destroy()
{
  if (is_inited_) {
    raw_pool_.destroy();
    dict_pool_.destroy();
    rle_pool_.destroy();
    const_pool_.destroy();
    int_diff_pool_.destroy();
    str_diff_pool_.destroy();
    hex_str_pool_.destroy();
    str_prefix_pool_.destroy();
    column_equal_pool_.destroy();
    column_substr_pool_.destroy();
    ctx_block_pool_.destroy();
    cs_integer_pool_.destroy();
    cs_string_pool_.destroy();
    cs_int_dict_pool_.destroy();
    cs_str_dict_pool_.destroy();
    cs_ctx_block_pool_.destroy();
    is_inited_ = false;
  }
}

int ObDecodeResourcePool::init() {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    uint64_t tenant_id = MTL_ID();
    //empiric value (raw, dict, const):(rle, int_diff):else = 4:2:1
    if(OB_FAIL(raw_pool_.init(MAX_DECODER_CNT, "RawPl", tenant_id))
        || OB_FAIL(dict_pool_.init(MAX_DECODER_CNT, "DictPl", tenant_id))
        || OB_FAIL(rle_pool_.init(MID_DECODER_CNT, "RlePl", tenant_id))
        || OB_FAIL(const_pool_.init(MAX_DECODER_CNT, "ConstPl", tenant_id))
        || OB_FAIL(int_diff_pool_.init(MID_DECODER_CNT, "IntDiffPl", tenant_id))
        || OB_FAIL(str_diff_pool_.init(MIN_DECODER_CNT, "StrDiffPl", tenant_id))
        || OB_FAIL(hex_str_pool_.init(MIN_DECODER_CNT, "HexStrPl", tenant_id))
        || OB_FAIL(str_prefix_pool_.init(MIN_DECODER_CNT, "StrPrefixPl", tenant_id))
        || OB_FAIL(column_equal_pool_.init(MIN_DECODER_CNT, "ColEqualPl", tenant_id))
        || OB_FAIL(column_substr_pool_.init(MIN_DECODER_CNT, "ColSubStrPl", tenant_id))
        || OB_FAIL(ctx_block_pool_.init(MAX_CTX_BLOCK_CNT, "CtxBlockPl", tenant_id)
        || OB_FAIL(cs_integer_pool_.init(MAX_CS_DECODER_CNT, "CsIntPl", tenant_id))
        || OB_FAIL(cs_string_pool_.init(MAX_CS_DECODER_CNT, "CsStrPl", tenant_id))
        || OB_FAIL(cs_int_dict_pool_.init(MAX_CS_DECODER_CNT, "CsDictPl", tenant_id))
        || OB_FAIL(cs_str_dict_pool_.init(MAX_CS_DECODER_CNT, "CsDictPl", tenant_id))
        || OB_FAIL(cs_ctx_block_pool_.init(MAX_CS_CTX_BLOCK_CNT, "CsCtxBlockPl", tenant_id))
        )) {
      STORAGE_LOG(WARN, "failed to init decode resource pool", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

template<>
ObSmallObjPool<ObRawDecoder>& ObDecodeResourcePool::get_pool()
{
  return raw_pool_;
}

template<>
ObSmallObjPool<ObDictDecoder>& ObDecodeResourcePool::get_pool()
{
  return dict_pool_;
}

template<>
ObSmallObjPool<ObRLEDecoder>& ObDecodeResourcePool::get_pool()
{
  return rle_pool_;
}

template<>
ObSmallObjPool<ObConstDecoder>& ObDecodeResourcePool::get_pool()
{
  return const_pool_;
}

template<>
ObSmallObjPool<ObIntegerBaseDiffDecoder>& ObDecodeResourcePool::get_pool()
{
  return int_diff_pool_;
}

template<>
ObSmallObjPool<ObStringDiffDecoder>& ObDecodeResourcePool::get_pool()
{
  return str_diff_pool_;
}

template<>
ObSmallObjPool<ObHexStringDecoder>& ObDecodeResourcePool::get_pool()
{
  return hex_str_pool_;
}

template<>
ObSmallObjPool<ObStringPrefixDecoder>& ObDecodeResourcePool::get_pool()
{
  return str_prefix_pool_;
}

template<>
ObSmallObjPool<ObColumnEqualDecoder>& ObDecodeResourcePool::get_pool()
{
  return column_equal_pool_;
}

template<>
ObSmallObjPool<ObInterColSubStrDecoder>& ObDecodeResourcePool::get_pool()
{
  return column_substr_pool_;
}

template<>
ObSmallObjPool<ObColumnDecoderCtxBlock>& ObDecodeResourcePool::get_pool()
{
  return ctx_block_pool_;
}

///////////////////////////// cs decoding  /////////////////////////////////////
template<>
ObSmallObjPool<ObIntegerColumnDecoder>& ObDecodeResourcePool::get_pool()
{
  return cs_integer_pool_;
}

template<>
ObSmallObjPool<ObStringColumnDecoder>& ObDecodeResourcePool::get_pool()
{
  return cs_string_pool_;
}

template<>
ObSmallObjPool<ObIntDictColumnDecoder>& ObDecodeResourcePool::get_pool()
{
  return cs_int_dict_pool_;
}

template<>
ObSmallObjPool<ObStrDictColumnDecoder>& ObDecodeResourcePool::get_pool()
{
  return cs_str_dict_pool_;
}

template<>
ObSmallObjPool<ObColumnCSDecoderCtxBlock>& ObDecodeResourcePool::get_pool()
{
  return cs_ctx_block_pool_;
}

///////////////////////////// decode pool  /////////////////////////////////////
ObDecoderPool::ObDecoderPool()
  : raw_pool_(),
    dict_pool_(),
    rle_pool_(),
    const_pool_(),
    int_diff_pool_(),
    str_diff_pool_(),
    hex_str_pool_(),
    str_prefix_pool_(),
    column_equal_pool_(),
    column_substr_pool_(),
    pools_{raw_pool_, dict_pool_, rle_pool_, const_pool_, int_diff_pool_, str_diff_pool_,
        hex_str_pool_, str_prefix_pool_, column_equal_pool_, column_substr_pool_}
{
  memset(free_cnts_, 0, sizeof(free_cnts_));
}

void ObDecoderPool::reset()
{
  ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
  if (OB_ISNULL(decode_res_pool)) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "NULL tenant decode resource pool", K(ret));
  } else {
    (void)free_decoders<ObRawDecoder>(*decode_res_pool, ObColumnHeader::RAW);
    (void)free_decoders<ObDictDecoder>(*decode_res_pool, ObColumnHeader::DICT);
    (void)free_decoders<ObRLEDecoder>(*decode_res_pool, ObColumnHeader::RLE);
    (void)free_decoders<ObConstDecoder>(*decode_res_pool, ObColumnHeader::CONST);
    (void)free_decoders<ObIntegerBaseDiffDecoder>(*decode_res_pool, ObColumnHeader::INTEGER_BASE_DIFF);
    (void)free_decoders<ObStringDiffDecoder>(*decode_res_pool, ObColumnHeader::STRING_DIFF);
    (void)free_decoders<ObHexStringDecoder>(*decode_res_pool, ObColumnHeader::HEX_PACKING);
    (void)free_decoders<ObStringPrefixDecoder>(*decode_res_pool, ObColumnHeader::STRING_PREFIX);
    (void)free_decoders<ObColumnEqualDecoder>(*decode_res_pool, ObColumnHeader::COLUMN_EQUAL);
    (void)free_decoders<ObInterColSubStrDecoder>(*decode_res_pool, ObColumnHeader::COLUMN_SUBSTR);
  }
}



///////////////////////////// cs decoder pool  /////////////////////////////////////
ObCSDecoderPool::ObCSDecoderPool()
  : cs_integer_pool_(),
    cs_string_pool_(),
    cs_int_dict_pool_(),
    cs_str_dict_pool_(),
    pools_{cs_integer_pool_, cs_string_pool_, cs_int_dict_pool_, cs_str_dict_pool_}
{
  memset(free_cnts_, 0, sizeof(free_cnts_));
}

void ObCSDecoderPool::reset()
{
  ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
  if (OB_ISNULL(decode_res_pool)) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "NULL tenant decode resource pool", K(ret));
  } else {
    (void)free_decoders<ObIntegerColumnDecoder>(*decode_res_pool, ObCSColumnHeader::INTEGER);
    (void)free_decoders<ObStringColumnDecoder>(*decode_res_pool, ObCSColumnHeader::STRING);
    (void)free_decoders<ObIntDictColumnDecoder>(*decode_res_pool, ObCSColumnHeader::INT_DICT);
    (void)free_decoders<ObStrDictColumnDecoder>(*decode_res_pool, ObCSColumnHeader::STR_DICT);
  }
}

}//end namespace blocksstable
}//end namespace oceanbase