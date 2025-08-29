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

#define USING_LOG_PREFIX STORAGE

#include "ob_semistruct_encoding_util.h"
#include "storage/blocksstable/cs_encoding/ob_integer_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_int_dict_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_string_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_str_dict_column_encoder.h"
#include "sql/engine/expr/ob_json_param_type.h"

namespace oceanbase
{
namespace blocksstable
{

int ObSemiStructColumnEncodeCtx::scan()
{
  int ret = OB_SUCCESS;
  bool has_outrow = false;
  is_enable_ = true;

  if (schema_not_match_block_count_ > MAX_SCHEMA_NOT_MATCH_COUNT) {
    ++not_encode_block_count_;
    if (not_encode_block_count_ > MAX_NOT_ENCODE_COUNT) {
      is_enable_ = true;
      not_encode_block_count_ = 0;
      schema_not_match_block_count_ = 0;
    } else {
      disable_encoding();
    }
  }

  if (! is_enable_) {
  } else if (OB_FAIL(check_has_outrow(has_outrow))) {
    LOG_WARN("check has outrow fail", K(ret));
  } else if (has_outrow) {
    ++encounter_outrow_block_cnt_;
    disable_encoding();
  } else if (OB_FAIL(do_scan())) {
    LOG_WARN("scan fail", K(ret));
  }

  if (OB_SEMISTRUCT_SCHEMA_NOT_MATCH == ret) {
    ++schema_not_match_block_count_;
    sub_schema_.reset();
    previous_cs_encoding_.reset();
    reuse();
    if (OB_FAIL(do_scan())) {
      LOG_WARN("do scan fail", K(ret));
    }
  }

  if (OB_NOT_SUPPORT_SEMISTRUCT_ENCODE == ret) {
    disable_encoding();
    sub_schema_.reset();
    previous_cs_encoding_.reset();
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret) && is_enable_ ) {
    schema_not_match_block_count_ = 0;
    not_encode_block_count_ = 0;
    if (sub_schema_.get_freq_column_count() <= 0) {
      LOG_TRACE("local sub schema don't have frequent column, so don't semistruct encoding", KPC(this));
      disable_encoding();
      sub_schema_.reset();
      previous_cs_encoding_.reset();
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::do_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_sub_schema())) {
    LOG_WARN("init sub schema fail", K(ret));
  } else if (OB_FAIL(init_sub_column_datums())) {
    LOG_WARN("init sub column datums fail", K(ret));
  } else if (OB_FAIL(fill_sub_column_datums())) {
    LOG_WARN("fill sub column datums fail", K(ret));
  } else if (OB_FAIL(init_sub_column_encode_ctxs())) {
    LOG_WARN("init_sub_column_encode_ctxs fail", K(ret));
  } else if (OB_FAIL(scan_sub_column_datums())) {
    LOG_WARN("scan sub column datums fail", K(ret));
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::check_has_outrow(bool &has_outrow)
{
  int ret = OB_SUCCESS;
  has_outrow = false;
  if (OB_ISNULL(datums_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col datums is null", K(ret));
  } else {
    int64_t row_cnt = datums_->count();
    for (int64_t row_idx = 0; !has_outrow && OB_SUCC(ret) && row_idx < row_cnt; ++row_idx) {
      const ObDatum &datum = datums_->at(row_idx);
      if (datum.is_nop() || datum.is_null()) {
      } else if (datum.len_ < sizeof(ObLobCommon)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected lob datum len", K(ret), K(row_idx), K(datum));
      } else {
        const ObLobCommon &lob_common = datum.get_lob_data();
        has_outrow = !lob_common.in_row_;
      }
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::init_sub_schema()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 1_s);
  // inference local schema
  ObArenaAllocator tmp_allocator("SemiTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObJsonSchemaFlatter json_schema_flatter(&tmp_allocator);
  if (sub_schema_.is_inited()) { // donot infer if is inited
  } else if (OB_ISNULL(datums_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datums is null", K(ret));
  } else if (OB_FAIL(json_schema_flatter.init())) {
    LOG_WARN("init schema flatter fail", K(ret));
  } else if (OB_FAIL(json_schema_flatter.flat_datums(*datums_))) {
    LOG_WARN("flat json fail", K(ret), K(json_schema_flatter));
  } else if (OB_FAIL(json_schema_flatter.build_sub_schema(sub_schema_))) {
    LOG_WARN("build_sub_schema fail", K(ret), K(json_schema_flatter));
  } else if (OB_FAIL(previous_cs_encoding_.init(get_store_column_count()))) {
    LOG_WARN("fail to init previous_cs_encoding_info", K(ret));
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::init_sub_column_datums()
{
  int ret = OB_SUCCESS;
  const int64_t sub_col_count = get_store_column_count();
  const int64_t row_count = datums_->count();
  if (OB_FAIL(sub_col_datums_.reserve(sub_col_count))) {
    LOG_WARN("reserve array fail", K(ret), "size", sub_col_count);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sub_col_count; ++i) {
    ObColDatums *c = nullptr;
    if (OB_ISNULL(c = OB_NEWx(ObColDatums, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (OB_FAIL(c->reserve(row_count))) {
      LOG_WARN("reserve array fail", K(ret), "size", row_count);
    } else if (OB_FAIL(sub_col_datums_.push_back(c))) {
      LOG_WARN("push back sub column datum fail", K(ret), "size", sub_col_datums_.count());
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(c)) {
      c->~ObColDatums();
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::fill_sub_column_datums()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 1_s);
  // flat and split json
  ObJsonDataFlatter json_data_flatter(&allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_data_flatter.init(sub_schema_, sub_col_datums_))) {
    LOG_WARN("init data flatter fail", K(ret), K(sub_schema_));
  } else if (OB_FAIL(json_data_flatter.flat_datums(*datums_))) {
    LOG_WARN("flat json fail", K(ret), K(json_data_flatter));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < sub_col_datums_.count(); ++i) {
      if (sub_col_datums_.at(i)->count() != datums_->count()) {
        ret = OB_SEMISTRUCT_SCHEMA_NOT_MATCH;
        LOG_WARN("sub subema is not match, some frequnent clumn don't have value", KR(ret), K(sub_schema_));
      }
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encoder_allocator_.init())) {
    LOG_WARN("init encoder_allocator_ fail", K(ret));
  }
  return ret;
}

void ObSemiStructColumnEncodeCtx::reset()
{
  int ret = OB_SUCCESS;
  column_index_ = -1;
  col_desc_.reset();
  datums_ = nullptr;
  sub_schema_.reset();
  FOREACH(cv, sub_col_datums_)
  {
    ObColDatums *p = *cv;
    if (nullptr != p) {
      p->~ObColDatums();
      allocator_.free(p);
    }
  }
  sub_col_datums_.reset();

  free_encoders();
  sub_encoders_.reset();
  hashtables_.reset();
  sub_col_ctxs_.reset();
  // encoder_allocator_ no reset method
  // previous_cs_encoding_ no reset method

  encoding_ctx_ = nullptr;
  all_string_buf_writer_ = nullptr;

  schema_not_match_block_count_ = 0;
  not_encode_block_count_ = 0;
  encounter_outrow_block_cnt_ = 0;
  is_enable_ = false;

  // must be last
  allocator_.reset();
}

void ObSemiStructColumnEncodeCtx::reuse()
{
  int ret = OB_SUCCESS;
  // column_index_
  // col_desc_
  // datums_ = nullptr;
  // sub_schema_.reuse();
  FOREACH(cv, sub_col_datums_)
  {
    ObColDatums *p = *cv;
    if (nullptr != p) {
      p->~ObColDatums();
      allocator_.free(p);
    }
  }
  sub_col_datums_.reuse();
  free_encoders();
  sub_col_ctxs_.reuse();
  // previous_cs_encoding_ no need reuse
  // encoding_ctx_ = nullptr;
  // all_string_buf_writer_ = nullptr;

  // schema_not_match_block_count_ keep current value
  // not_encode_block_count_ keep current value
  // encounter_outrow_block_cnt_ keep current value
  // is_enable_ = false;
  // must be last
  allocator_.reset();
}

int ObSemiStructColumnEncodeCtx::get_sub_column_type(const int64_t column_idx, ObObjType &type) const
{
  int ret = OB_SUCCESS;
  const ObSemiStructSubColumn *sub_column = nullptr;
  if (OB_FAIL(sub_schema_.get_store_column(column_idx, sub_column))) {
    LOG_WARN("get sub column fail", K(ret), K(column_idx), K(sub_schema_));
  } else {
    type = sub_column->get_obj_type();
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::get_sub_column_type(const int64_t column_idx, ObObjMeta &type) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type;
  if (OB_FAIL(get_sub_column_type(column_idx, obj_type))) {
    LOG_WARN("get obj type fail", K(ret), K(column_idx));
  } else {
    type.set_type(obj_type);
    if (ob_is_string_type(obj_type))type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
  return ret;
}

int64_t ObSemiStructColumnEncodeCtx::get_store_column_count() const
{
  return sub_schema_.get_store_column_count();
}

int64_t ObSemiStructColumnEncodeCtx::get_sub_schema_serialize_size() const
{
  return sub_schema_.get_encode_size();
}

int ObSemiStructColumnEncodeCtx::serialize_sub_schema(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  const int64_t schema_serialize_size = sub_schema_.get_encode_size();
  char *buf = buf_writer.current();
  int64_t pos = 0;
  if (OB_FAIL(buf_writer.advance(schema_serialize_size))) {
    LOG_WARN("buffer advance failed", K(ret), K(schema_serialize_size), K(sub_schema_));
  } else if (OB_FAIL(sub_schema_.encode(buf, schema_serialize_size, pos))) {
    LOG_WARN("encode sub schema fail", K(ret), K(schema_serialize_size), K(pos), K(sub_schema_));
  } else if (schema_serialize_size != pos) {
    LOG_WARN("serialize sub schema incorrect", K(ret), K(schema_serialize_size), K(pos), K(sub_schema_));
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::init_sub_column_encode_ctxs()
{
  int ret = OB_SUCCESS;
  int64_t sub_col_count = get_store_column_count();
  sub_col_ctxs_.reuse();
  if (OB_FAIL(sub_col_ctxs_.reserve(sub_col_count))) {
    LOG_WARN("reserve fail", K(ret), K(sub_col_count));
  }
  for (int i = 0; OB_SUCC(ret) && i < sub_col_count; ++i) {
    ObColumnCSEncodingCtx cc;
    cc.encoding_ctx_ = encoding_ctx_;
    cc.allocator_ = &allocator_;
    cc.all_string_buf_writer_ = all_string_buf_writer_;
    cc.semistruct_ctx_ = this;
    cc.is_semistruct_sub_col_ = true;
    cc.col_datums_ = sub_col_datums_.at(i);
    if (OB_FAIL(sub_col_ctxs_.push_back(cc))) {
      LOG_WARN("push back fail", K(ret), K(i), K(cc));
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::scan_sub_column_datums()
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard(__func__, 1_s);
  for (int i = 0; OB_SUCC(ret) && i < sub_col_datums_.count(); ++i) {
    const ObColDatums *datums = sub_col_datums_.at(i);
    ObColumnCSEncodingCtx &sub_col_ctx = sub_col_ctxs_.at(i);
    ObObjType obj_type = ObNullType;
    if (OB_ISNULL(datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub dataum is null", K(ret), K(i), K(sub_col_datums_));
    } else if (OB_FAIL(get_sub_column_type(i, obj_type))) {
      LOG_WARN("get sub column type fail", K(ret), K(i));
    } else if (OB_FAIL(scan_sub_column_datums(i, obj_type, *datums, sub_col_ctx))) {
      LOG_WARN("scan sub column data fail", K(ret), K(i));
    }
  }
  return ret;
}

static uint64_t calc_hash_bucket_num(const int64_t row_count)
{
  // next power of 2
  uint64_t bucket_num = row_count << 1;
  if (0 != (bucket_num & (bucket_num - 1))) {
    while (0 != (bucket_num & (bucket_num - 1))) {
      bucket_num = bucket_num & (bucket_num - 1);
    }
    bucket_num = bucket_num << 1;
  }
  return bucket_num;
}

int ObSemiStructColumnEncodeCtx::scan_sub_column_datums(const int64_t column_index, const ObObjType obj_type, const ObColDatums &col_datums, ObColumnCSEncodingCtx &sub_col_ctx)
{
  int ret = OB_SUCCESS;
  ObColDesc sub_col_desc;
  sub_col_desc.col_type_.set_type(obj_type);
  const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(obj_type)];

  // build hashtable
  ObDictEncodingHashTable *ht = nullptr;
  ObDictEncodingHashTableBuilder *builder = nullptr;
  bool need_build_hash_table = true;
  ObPreviousColumnEncoding *previous_encoding = previous_cs_encoding_.get_column_encoding(column_index);
  if (previous_encoding->column_encoding_type_can_be_reused()) {
    // previous column encoding type is used and is not dict encoding, so no need to build hash table
    if (ObCSColumnHeader::INTEGER == previous_encoding->identifier_.column_encoding_type_ ||
        ObCSColumnHeader::STRING == previous_encoding->identifier_.column_encoding_type_) {
      need_build_hash_table = false;
    }
  }

  if (need_build_hash_table) {
    int64_t row_count = datums_->count();
    uint64_t bucket_num = calc_hash_bucket_num(row_count);
    const int64_t node_num = row_count;
    if (OB_FAIL(hashtable_factory_.create(bucket_num, node_num, ht))) {
      LOG_WARN("create hashtable failed", K(ret), K(bucket_num), K(node_num));
    } else if (FALSE_IT(builder = static_cast<ObDictEncodingHashTableBuilder *>(ht))) {
    } else if (OB_FAIL(builder->build(col_datums, sub_col_desc))) {
      LOG_WARN("build hash table failed", K(ret), K(sub_col_desc));
    }
  }

  if (OB_SUCC(ret)) {
    sub_col_ctx.ht_ = ht;
    if (OB_FAIL(ObCSEncodingUtil::build_cs_column_encoding_ctx(store_class, -1/*precision_bytes*/, sub_col_ctx))) {
      LOG_WARN("build_column_encoding_ctx failed", K(ret), K(obj_type), K(sub_col_desc), KP(ht), K(store_class));
    } else if (ht != nullptr && OB_FAIL(hashtables_.push_back(ht))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_FAIL(ret) && ht != nullptr) {
    // avoid overwirte ret
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = hashtable_factory_.recycle(true, ht))) {
      LOG_WARN("recycle hashtable failed", K(temp_ret));
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::init_sub_column_encoders()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < sub_col_datums_.count(); ++i) {
    ObColumnCSEncodingCtx &sub_col_ctx = sub_col_ctxs_.at(i);
    ObObjType obj_type = ObNullType;
    ObIColumnCSEncoder *e = nullptr;
    if (OB_FAIL(get_sub_column_type(i, obj_type))) {
      LOG_WARN("get sub column type fail", K(ret), K(i));
    } else {
      const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(obj_type)];
      if (OB_FAIL(try_use_previous_encoder(i, store_class, e))) {
        LOG_WARN("try_use_previous_encoder fail", K(ret), K(obj_type), K(store_class), K(i));
      } else if (nullptr != e) { // use previose, no need detect
      } else if (ObCSEncodingUtil::is_integer_store_class(store_class)) {
        if (OB_FAIL(choose_encoder_for_integer(i, e))) {
          LOG_WARN("fail to choose encoder for integer", K(ret));
        }
      } else if (ObCSEncodingUtil::is_string_store_class(store_class)) {
        if (OB_FAIL(choose_encoder_for_string(i, e))) {
          LOG_WARN("fail to choose encoder for variable length type", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported store class", K(ret), K(store_class));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_previous_info_before_encoding(i, *e))) {
        LOG_WARN("fail to update previous info before encoding", K(ret), K(i));
      } else if (OB_FAIL(sub_encoders_.push_back(e))) {
        LOG_WARN("push back encoder failed");
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != e) {
        free_encoder(e);
        e = nullptr;
      }
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::try_use_previous_encoder(const int64_t column_idx,
                                                     const ObObjTypeStoreClass store_class,
                                                     ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;
  ObPreviousColumnEncoding *previous_encoding = previous_cs_encoding_.get_column_encoding(column_idx);

  if (previous_encoding->column_encoding_type_can_be_reused()) {
    const ObCSColumnHeader::Type previous_type = previous_encoding->identifier_.column_encoding_type_;
    ObCSColumnHeader::Type curr_type = ObCSColumnHeader::Type::MAX_TYPE;
    ObColumnCSEncodingCtx &col_ctx = sub_col_ctxs_.at(column_idx);

    if (ObCSEncodingUtil::is_integer_store_class(store_class)) {
      if (ObCSColumnHeader::Type::INTEGER  == previous_type || ObCSColumnHeader::Type::INT_DICT == previous_type) {
        curr_type = previous_type;
      } else if (ObCSColumnHeader::Type::STRING == previous_type || ObCSColumnHeader::Type::STR_DICT == previous_type) {
        if (OB_UNLIKELY(store_class != ObDecimalIntSC || col_ctx.is_wide_int_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected store class", K(ret), K(store_class), K(col_ctx));
        } else if (ObCSColumnHeader::Type::STRING == previous_type) {
          curr_type = ObCSColumnHeader::Type::INTEGER;
        } else {
          curr_type = ObCSColumnHeader::Type::INT_DICT;
        }
      }
    } else if (ObCSEncodingUtil::is_string_store_class(store_class)) {
      if (ObCSColumnHeader::Type::STRING == previous_type || ObCSColumnHeader::Type::STR_DICT == previous_type) {
        curr_type = previous_type;
      } else if (ObCSColumnHeader::Type::INTEGER  == previous_type || ObCSColumnHeader::Type::INT_DICT == previous_type) {
        if (OB_UNLIKELY(store_class != ObDecimalIntSC || !col_ctx.is_wide_int_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected store class", K(ret), K(store_class), K(col_ctx));
        } else if (ObCSColumnHeader::Type::INTEGER == previous_type) {
          curr_type = ObCSColumnHeader::Type::STRING;
        } else {
          curr_type = ObCSColumnHeader::Type::STR_DICT;
        }
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("not supported store class", K(ret), K(store_class));
    }

    if (OB_SUCC(ret)) {
      if (ObCSColumnHeader::Type::INTEGER == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder<ObIntegerColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else if (ObCSColumnHeader::Type::INT_DICT == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder<ObIntDictColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else if (ObCSColumnHeader::Type::STRING == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder<ObStringColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else if (ObCSColumnHeader::Type::STR_DICT == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder<ObStrDictColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected econding type", K(ret), K(curr_type), K(store_class), K(col_ctx));
      }
    }
  } else {
    // can't use previous column encoding type
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::update_previous_info_before_encoding(const int32_t col_idx, ObIColumnCSEncoder &e)
{
  int ret = OB_SUCCESS;
  int32_t int_stream_count = 0;
  const ObIntegerStream::EncodingType *types = nullptr;
  ObColumnEncodingIdentifier identifier;
  if (OB_FAIL(e.get_identifier_and_stream_types(identifier, types))) {
    LOG_WARN("fail to get_identifier_and_stream_types", K(ret));
  } else if (OB_FAIL(previous_cs_encoding_.update_column_detect_info(col_idx, identifier,
      encoding_ctx_->micro_block_cnt_, encoding_ctx_->major_working_cluster_version_))) {
    LOG_WARN("fail to check_and_set_valid", K(ret), K(col_idx), K(identifier), K(encoding_ctx_->micro_block_cnt_));
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::choose_encoder_for_integer(const int64_t column_idx, ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;
  ObIColumnCSEncoder *integer_encoder = nullptr;
  ObIColumnCSEncoder *dict_encoder = nullptr;
  if (OB_FAIL(alloc_and_init_encoder<ObIntegerColumnEncoder>(column_idx, integer_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else if (OB_FAIL(alloc_and_init_encoder<ObIntDictColumnEncoder>(column_idx, dict_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else {
    int64_t integer_estimate_size = integer_encoder->estimate_store_size();
    int64_t dict_estimate_size = dict_encoder->estimate_store_size();
    bool use_dict = false;
    const int64_t row_count = dict_encoder->get_row_count();
    const int64_t distinct_cnt = static_cast<ObStrDictColumnEncoder*>(dict_encoder)->get_distinct_cnt();
    use_dict = (dict_estimate_size < integer_estimate_size * 70 / 100) ||
        (dict_estimate_size < integer_estimate_size && distinct_cnt < row_count * 50 / 100);

    if (use_dict) {
      e = dict_encoder;
      free_encoder(integer_encoder);
      integer_encoder = nullptr;
    } else {
      e = integer_encoder;
      free_encoder(dict_encoder);
      dict_encoder = nullptr;
    }
    LOG_TRACE("choose encoder for integer", K(ret), K(column_idx),
       K(integer_estimate_size), K(dict_estimate_size), KPC(integer_encoder), KPC(dict_encoder));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != integer_encoder) {
      free_encoder(integer_encoder);
      integer_encoder = nullptr;
    }
    if (nullptr != dict_encoder) {
      free_encoder(dict_encoder);
      dict_encoder = nullptr;
    }
  }
  return ret;
}

int ObSemiStructColumnEncodeCtx::choose_encoder_for_string(const int64_t column_idx, ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;
  ObIColumnCSEncoder *string_encoder = nullptr;
  ObIColumnCSEncoder *dict_encoder = nullptr;
  if (OB_FAIL(alloc_and_init_encoder<ObStringColumnEncoder>(column_idx, string_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else if (OB_FAIL(alloc_and_init_encoder<ObStrDictColumnEncoder>(column_idx, dict_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else {
    int64_t string_estimate_size = string_encoder->estimate_store_size();
    int64_t dict_estimate_size = dict_encoder->estimate_store_size();
    bool use_dict = false;
    const int64_t row_count = dict_encoder->get_row_count();
    const int64_t distinct_cnt = static_cast<ObStrDictColumnEncoder*>(dict_encoder)->get_distinct_cnt();
    use_dict = (dict_estimate_size < string_estimate_size * 70 / 100) ||
        (dict_estimate_size < string_estimate_size && distinct_cnt < row_count * 50 / 100);

    if (use_dict) {
      e = dict_encoder;
      free_encoder(string_encoder);
      string_encoder = nullptr;
    } else {
      e = string_encoder;
      free_encoder(dict_encoder);
      dict_encoder = nullptr;
    }
    LOG_TRACE("choose encoder for var length type", K(ret), K(column_idx),
      K(string_estimate_size), K(dict_estimate_size), KPC(string_encoder), KPC(dict_encoder));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != string_encoder) {
      free_encoder(string_encoder);
      string_encoder = nullptr;
    }
    if (nullptr != dict_encoder) {
      free_encoder(dict_encoder);
      dict_encoder = nullptr;
    }
  }
  return ret;
}

template <typename T>
int ObSemiStructColumnEncodeCtx::alloc_and_init_encoder(const int64_t column_index, ObIColumnCSEncoder *&encoder)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = ObNullType;
  encoder = nullptr;
  T *e = nullptr;
  if (OB_FAIL(encoder_allocator_.alloc(e))) {
    LOG_WARN("alloc encoder failed", K(ret));
  } else if (OB_FAIL(get_sub_column_type(column_index, obj_type))) {
    LOG_WARN("get sub column type fail", K(ret), K(column_index));
  } else {
    sub_col_ctxs_.at(column_index).try_set_need_sort(e->get_type(), ob_obj_type_class(obj_type),
        false/*has_outrow_lob*/, encoding_ctx_->major_working_cluster_version_);
    if (OB_FAIL(e->init(sub_col_ctxs_.at(column_index), column_index, datums_->count()))) {
      LOG_WARN("init column encoder failed", K(ret), K(column_index));
    }
    if (OB_FAIL(ret)) {
      free_encoder(e);
      e = NULL;
    } else {
      encoder = e;
    }
  }
  return ret;
}

void ObSemiStructColumnEncodeCtx::free_encoder(ObIColumnCSEncoder *encoder)
{
  if (OB_NOT_NULL(encoder)) {
    encoder_allocator_.free(encoder);
  }
}

void ObSemiStructColumnEncodeCtx::free_encoders()
{
  int ret = OB_SUCCESS;
  FOREACH(e, sub_encoders_)
  {
    free_encoder(*e);
  }
  FOREACH(ht, hashtables_)
  {
    // should continue even fail
    if (OB_FAIL(hashtable_factory_.recycle(true, *ht))) {
      LOG_WARN("recycle hashtable failed", K(ret));
    }
  }
  sub_encoders_.reuse();
  hashtables_.reuse();
}

int ObSemiStructEncodeCtx::get_col_ctx(const int64_t column_index, ObSemiStructColumnEncodeCtx *&res)
{
  int ret = OB_SUCCESS;
  InnerWrapper wrapper;
  int64_t cur_cnt = col_ctxs_.count();
  for (int i = 0; -1 == wrapper.col_idx_ && i < cur_cnt; ++i) {
    if (col_ctxs_.at(i).col_idx_ == column_index) {
      wrapper = col_ctxs_.at(i);
    }
  }
  if (wrapper.col_idx_ != -1) {
    res = wrapper.col_ctx_;
  } else {
    if (OB_ISNULL(res = OB_NEWx(ObSemiStructColumnEncodeCtx, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("aloc semistruct_ctx fail", K(ret), "size", sizeof(ObSemiStructColumnEncodeCtx));
    } else if (OB_FAIL(res->init())) {
      LOG_WARN("init semistruct_ctx fail", K(ret));
    } else if (OB_FAIL(col_ctxs_.push_back(wrapper))) {
      LOG_WARN("push back new encoder fail", K(ret), K(col_ctxs_));
    } else {
      col_ctxs_.at(cur_cnt).col_idx_ = column_index;
      col_ctxs_.at(cur_cnt).col_ctx_ = res;
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(res)) {
      res->reset();
      res->~ObSemiStructColumnEncodeCtx();
      res = nullptr;
    }
  }
  return ret;
}

void ObSemiStructEncodeCtx::reset()
{
  for (int i = 0; i < col_ctxs_.count(); ++i) {
    ObSemiStructColumnEncodeCtx *col_ctx = col_ctxs_.at(i).col_ctx_;
    if (OB_NOT_NULL(col_ctx)) {
      col_ctx->reset();
      col_ctx->~ObSemiStructColumnEncodeCtx();
    }
  }
  col_ctxs_.reset();
  allocator_.reset();
}

void ObSemiStructEncodeCtx::reuse()
{
  for (int i = 0; i < col_ctxs_.count(); ++i) {
    ObSemiStructColumnEncodeCtx *col_ctx = col_ctxs_.at(i).col_ctx_;
    if (OB_NOT_NULL(col_ctx)) {
      col_ctx->reuse();
    }
  }
}

int ObSemiStructDecodeHandler::init(ObIAllocator &allocator, const char* sub_schema_data_ptr, const int64_t sub_schema_data_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(sub_schema_ = OB_NEWx(ObSemiStructSubSchema, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc sub schema fail", K(ret), "size", sizeof(ObSemiStructSubSchema));
  } else if (OB_FAIL(sub_schema_->decode(sub_schema_data_ptr, sub_schema_data_len, pos))) {
    LOG_WARN("decode sub schema fail", K(ret), K(sub_schema_data_len), KP(sub_schema_data_ptr));
  } else if (pos != sub_schema_data_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub schema decode incorrect", K(ret), K(pos), K(sub_schema_data_len));
  } else if (OB_ISNULL(reassembler_ = OB_NEWx(ObJsonReassembler, &allocator, sub_schema_, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc reassembler fail", K(ret), "size", sizeof(ObJsonReassembler));
  } else if (OB_FAIL(reassembler_->init())) {
    LOG_WARN("init reassembler fail", K(ret));
  }
  return ret;
}

void ObSemiStructDecodeHandler::reset()
{
  if (OB_NOT_NULL(sub_schema_)) {
    sub_schema_->reset();
    sub_schema_->~ObSemiStructSubSchema();
    sub_schema_ = nullptr;
  }
  if (OB_NOT_NULL(reassembler_)) {
    reassembler_->reset();
    reassembler_->~ObJsonReassembler();
    reassembler_ = nullptr;
  }
}

ObDatumRow& ObSemiStructDecodeHandler::get_sub_row()
{
  return reassembler_->get_sub_row();
}

int ObSemiStructDecodeHandler::serialize(const ObDatumRow &row, ObString &result)
{
  return reassembler_->serialize(row, result);
}

int ObSemiStructDecodeHandler::check_can_pushdown(
    const sql::ObSemiStructWhiteFilterNode &filter_node,
    bool &can_pushdown, int64_t &sub_col_idx) const
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *root_expr = filter_node.expr_;
  const sql::ObExpr *json_expr = root_expr->args_[0];
  const ObSemiStructSubColumn* sub_col = nullptr;
  bool is_spare = false;
  const share::ObSubColumnPath &col_path = filter_node.get_sub_col_path();

  can_pushdown = false;
  if (! sql::is_support_pushdown_json_expr(json_expr->type_)) {
    LOG_INFO("not support pushdown json expr", K(ret), KPC(json_expr));
  } else if (OB_FAIL(sub_schema_->get_column(col_path, sub_col))) {
    if (OB_SEARCH_NOT_FOUND != ret) {
      LOG_WARN("get sub column fail", K(ret), K(col_path), KPC(sub_schema_));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("sub column not found, so not white pushdown", K(col_path), KPC(sub_schema_));
    }
  } else if (OB_ISNULL(sub_col)) {
    LOG_INFO("pushdown not support for not found json sub column", K(col_path), KPC(sub_col), KPC(sub_schema_));
  } else if (sub_col->is_spare_storage()) {
    LOG_INFO("pushdown not support for spare json sub column", K(col_path), KPC(sub_col), KPC(sub_schema_));
  } else if (sub_col->get_col_id() < 0 || sub_col->get_col_id() >= sub_schema_->get_store_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sub_col_idx", K(ret), K(col_path), KPC(sub_col), KPC(sub_schema_));
  } else if (sub_col->get_obj_type() != json_expr->datum_meta_.type_) {
    LOG_INFO("json sub column is different from json expr", K(sub_col->get_obj_type()), K(json_expr->datum_meta_.type_),
        K(col_path), KPC(sub_col), KPC(sub_schema_));
  } else if (ob_is_string_type(json_expr->datum_meta_.type_) && CS_TYPE_UTF8MB4_BIN != json_expr->datum_meta_.cs_type_) {
    LOG_INFO("json expr collation type is utf8mb4_bin", K(json_expr->datum_meta_),
        K(col_path), KPC(sub_col), KPC(sub_schema_));
  } else {
    sub_col_idx = sub_col->get_col_id();
    can_pushdown = true;
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
