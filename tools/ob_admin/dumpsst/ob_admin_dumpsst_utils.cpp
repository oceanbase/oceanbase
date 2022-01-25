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

#include "ob_admin_dumpsst_utils.h"
#include "share/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace serialization;
using namespace blocksstable;
using namespace storage;

namespace tools
{

int parse_string(const char* src, const char del, const char* dst[], int64_t& size)
{
  int ret = OB_SUCCESS;
  int64_t obj_index = 0;

  char* str = (char*) ob_malloc(OB_MAX_FILE_NAME_LENGTH, ObModIds::TEST);
  strncpy(str, src, strlen(src) + 1);

  char* st_ptr = str;
  char* et_ptr = NULL; //use for strchar,the fist position of del in src
  char* last_ptr = str + strlen(str) - 1; //end of the str

  //skip white space
  while (0 != *st_ptr && ' ' == *st_ptr)
    ++st_ptr;

  //parse rowkey_str from st_str division by ';'  ,except the last item
  while (NULL != st_ptr && NULL != (et_ptr = strchr(st_ptr, del))) {
    *et_ptr = 0;

    if (size <= obj_index) {
      ret = OB_SIZE_OVERFLOW;
      break;
    } else {
      dst[obj_index++] = st_ptr;
      st_ptr = et_ptr + 1;
      while (0 != *st_ptr && ' ' == *st_ptr)
        ++st_ptr;

      if (size - 1 == obj_index) {
        dst[obj_index++] = st_ptr;
        break;
      }
    }
  }

  if (OB_SUCCESS == ret && 0 != *st_ptr && size > obj_index) {
    //skip white space of end
    while (last_ptr > st_ptr && ' ' == *last_ptr) {
      *last_ptr = 0;
      --last_ptr;
    }
    dst[obj_index++] = st_ptr;
  }

  if (OB_SUCC(ret)) {
    size = obj_index;
  }

  return ret;
}

int parse_table_key(const char *str, ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  int64_t tmp = 0;
  const int64_t TABLE_KEY_ATTR_CNT = 6;
  int64_t size = TABLE_KEY_ATTR_CNT;
  const char *dst[TABLE_KEY_ATTR_CNT];
  if (OB_FAIL(parse_string(str, ',', dst, size))) {
    LOG_WARN("failed to parse string", K(ret));
  }
  if (OB_SUCC(ret)) {
    // parse table type
    pret = sscanf(dst[0], "%ld", &tmp);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("table type is not valid");
    } else {
      table_key.table_type_ = (ObITable::TableType)tmp;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_partition_key(dst[1], table_key.pkey_))) {
      LOG_WARN("failed to parse partition key", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    pret = sscanf(dst[2], "%lu", &table_key.table_id_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("index id is not valid");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_version_range(dst[3], table_key.trans_version_range_))) {
      LOG_WARN("failed to parse version range");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_log_ts_range(dst[4], table_key.log_ts_range_))) {
      LOG_WARN("failed to parse log ts range");
    }
  }

  if (OB_SUCC(ret)) {
    pret = sscanf(dst[5], "%ld", &tmp);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("major version is not valid");
    } else {
      table_key.version_.version_ = tmp;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("success to parse table key", K(table_key));
  }

  return ret;
}

int parse_partition_key(const char *str, ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  int64_t size = 2;
  const char *dst[2];
  uint64_t table_id = 0;
  int64_t part_id = 0;
  if (OB_FAIL(parse_string(str, ':', dst, size))) {
    STORAGE_LOG(ERROR, "failed to parse string", K(ret));
  } else if (2 != size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "table id and partition id should be provided", K(ret), K(str));
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[0], "%lu", &table_id);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("table id is not valid");
    }
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[1], "%ld", &part_id);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("partition id is not valid");
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pkey.init(table_id, part_id, 0))) {
      STORAGE_LOG(ERROR, "faile to init pkey", K(ret), K(table_id), K(part_id));
    }
  }
  return ret;
}

int parse_version_range(const char *str, ObVersionRange &version_range)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  int64_t size = 3;
  const char *dst[3];
  if (OB_FAIL(parse_string(str, ':', dst, size))) {
    STORAGE_LOG(ERROR, "failed to parse string", K(ret));
  } else if (3 != size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "version range has three member variables", K(ret), K(str));
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[0], "%ld", &version_range.base_version_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("base version is not valid");
    }
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[1], "%ld", &version_range.multi_version_start_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("multi_version_start is not valid");
    }
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[2], "%ld", &version_range.snapshot_version_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("snapshot_version is not valid");
    }
  }
  return ret;
}

int parse_log_ts_range(const char *str, ObLogTsRange &log_ts_range)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  int64_t size = 3;
  const char *dst[3];
  if (OB_FAIL(parse_string(str, ':', dst, size))) {
    STORAGE_LOG(ERROR, "failed to parse string", K(ret));
  } else if (3 != size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "version range has three member variables", K(ret), K(str));
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[0], "%ld", &log_ts_range.start_log_ts_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("start_log_ts is not valid");
    }
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[1], "%ld", &log_ts_range.end_log_ts_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("end_log_ts is not valid");
    }
  }
  if (OB_SUCC(ret)) {
    pret = sscanf(dst[2], "%ld", &log_ts_range.max_log_ts_);
    if (pret != 1) {
      ret = OB_INVALID_ARGUMENT;
      printf("max_log_ts is not valid");
    }
  }
  return ret;
}


static const char* obj_type_name[] = { "ObNullType", "ObTinyIntType", "ObSmallIntType",
                                       "ObMediumIntType", "ObInt32Type", "ObIntType",
                                       "ObUTinyIntType", "ObUSmallIntType", "ObUMediumIntType",
                                       "ObUInt32Type", "ObUInt64Type", "ObFloatType",
                                       "ObDoubleType", "ObUFloatType", "ObUDoubleType",
                                       "ObNumberType", "ObUNumberType", "ObDateTimeType",
                                       "ObTimestampType", "ObDateType", "ObTimeType", "ObYearType",
                                       "ObVarcharType", "ObCharType", "ObHexStringType",
                                       "ObExtendType", "ObUnknowType", "ObTinyTextType",
                                       "ObTextType", "ObMediumTextType", "ObLongTextType", "ObBitType"};

int MacroBlock::setup(const char *data, const int64_t size, const int64_t macro_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(data)
      || OB_UNLIKELY(size <= 0)
      || OB_UNLIKELY(macro_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size), K(macro_id));
  } else {
    data_ = data;
    size_ = size;
    macro_id_ = macro_id;
    if (OB_FAIL(common_header_.deserialize(data_, size_, pos))) {
      LOG_ERROR("deserialize common header fail", K(ret), KP(data), K(size), K(pos));
    } else if (OB_FAIL(common_header_.check_integrity())) {
      LOG_ERROR("invalid common header", K(ret), K_(common_header));
    }
  }
  return ret;
}

int MicroBlock::setup(const char *data, const int64_t size, const int64_t micro_id,
    const ObColumnMap *column_map, const int64_t row_store_type,
    const uint16_t *column_id_list, const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)
      || OB_UNLIKELY(size <= 0)
      || OB_UNLIKELY(micro_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size), K(micro_id));
  } else {
    data_ = data;
    size_ = size;
    micro_id_ = micro_id;
    row_store_type_ = row_store_type;
    column_map_ = column_map;
    column_cnt_ = column_cnt;
    column_id_list_ = column_id_list;
  }
  return ret;
}

int MacroBlockReader::init(const char *data, const int64_t size, const int64_t macro_id,
    const ObMacroBlockMeta *macro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(data) || size <= 0 || macro_id <= 0 || OB_ISNULL(macro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(size), K(macro_id), KP(macro_meta));
  } else if (OB_FAIL(macro_block_.setup(data, size, macro_id))) {
    STORAGE_LOG(ERROR, "failed to setup macro block", K(ret), KP(data), K(size), K(macro_id));
  } else {
    macro_meta_ = macro_meta;
    int64_t pos = 0;
    // parse headers
    pos += macro_block_.common_header_.get_serialize_size();
    sstable_header_ = reinterpret_cast<const ObSSTableMacroBlockHeader*>(macro_block_.data_ + pos);
    pos += sizeof(ObSSTableMacroBlockHeader);

    const int64_t column_cnt = sstable_header_->column_count_;
    column_id_list_ = reinterpret_cast<const uint16_t*>(macro_block_.data_ + pos);
    pos += sizeof(uint16_t) * column_cnt;
    column_type_list_ = reinterpret_cast<const common::ObObjMeta*>(macro_block_.data_ + pos);
    pos += sizeof(common::ObObjMeta) * column_cnt;
    column_checksum_ = reinterpret_cast<const int64_t*>(macro_block_.data_ + pos);
    pos += sizeof(int64_t) * column_cnt;

    if (OB_FAIL(parse_micro_block_index())) {
      LOG_WARN("failed to parse micro index", K(ret));
    } else if (OB_FAIL(build_column_map())) {
      LOG_WARN("failed to build column map", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void MacroBlockReader::reset()
{
  macro_block_.reset();
  macro_meta_ = NULL;
  sstable_header_ = NULL;
  column_id_list_ = NULL;
  column_type_list_ = NULL;
  column_checksum_ = NULL;
  curr_micro_block_id_ = -1;
  curr_micro_block_.reset();
  column_map_.reuse();
  micro_index_pos_.reset();
  micro_index_keys_.reset();
  micro_mark_deletions_.reset();
  cur_mib_rh_ = NULL;
  is_inited_ = false;
}

int64_t MacroBlockReader::count() const
{
  return sstable_header_->micro_block_count_;
}

int MacroBlockReader::set_index(const int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(index < 0)
      || OB_UNLIKELY(index >= count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(index));
  } else {
    curr_micro_block_id_ = index;
    if (OB_FAIL(set_current_micro_block())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to set current micro block", K(ret));
      }
    }
  }
  return ret;
}

int MacroBlockReader::get_value(const MicroBlock *&micro_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    micro_block = NULL;
    if (curr_micro_block_id_ >= micro_index_pos_.count()) {
      ret = OB_ITER_END;
    } else {
      micro_block = &curr_micro_block_;
    }
  }
  return ret;
}

int64_t MacroBlockReader::get_index() const
{
  return curr_micro_block_id_;
}

int MacroBlockReader::set_current_micro_block()
{
  int ret = OB_SUCCESS;
  const char *out = NULL;
  int64_t outsize = 0;
  const char* compressor_name = sstable_header_->compressor_name_;

  if (curr_micro_block_id_ >= micro_index_pos_.count()) {
    ret = OB_ITER_END;
  } else {
    ObPosition datapos = micro_index_pos_.at(curr_micro_block_id_);
    if (OB_FAIL(get_micro_block_payload_buffer(compressor_name,
        macro_block_.data_ + sstable_header_->micro_block_data_offset_ + datapos.offset_,
        datapos.length_, out, outsize, cur_mib_rh_))) {
      LOG_WARN("failed to read micro block", K(ret), K(curr_micro_block_id_), K(macro_block_.macro_id_));
    } else {
      //cur_mib_payload_ = out;
      //cur_mib_payload_size_ = outsize;
      curr_micro_block_.reset();
      if (OB_FAIL(curr_micro_block_.setup(out,
                                          outsize,
                                          curr_micro_block_id_,
                                          &column_map_,
                                          sstable_header_->row_store_type_,
                                          column_id_list_,
                                          sstable_header_->column_count_))) {
        LOG_WARN("failed to setup curr micro block", K(ret), K(curr_micro_block_id_));
      }
    }
  }
  return ret;
}

int MacroBlockReader::get_micro_block_payload_buffer(const char* compressor_name, const char* buf,
    const int64_t size, const char*& out, int64_t& outsize, const blocksstable::ObRecordHeaderV3 *&rh)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t max_size = size;
  char *uncompressed_buf = NULL;
  rh = reinterpret_cast<const ObRecordHeaderV3*>(buf);
  pos += sizeof(ObRecordHeaderV3);
  if (rh->data_length_ != rh->data_zlength_) {
    ObCompressor* compressor = NULL;
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compressor_name, compressor))) {
      LOG_WARN("get compressor failed", K(ret));
    } else if (OB_FAIL(compressor->get_max_overflow_size(rh->data_length_, max_size))) {
      LOG_WARN("get max overflow size failed", K(ret));
    } else if ((NULL == uncompressed_buf) && (NULL == (uncompressed_buf =
        reinterpret_cast<char*>(ob_malloc(max_size + rh->data_length_, ObModIds::OB_SSTABLE_AIO))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for uncompress buf", K(ret), K(size));
    } else if (OB_FAIL(compressor->decompress(buf + pos, rh->data_zlength_,
        uncompressed_buf, max_size + rh->data_length_, outsize))) {
      LOG_WARN("failed to decompress data", K(ret), K(rh->data_zlength_), K(rh->data_length_), K(max_size));
    } else if (outsize != rh->data_length_) {
      ret = OB_ERROR;
      LOG_WARN("decompress error", K(ret), "expect_size", rh->data_length_, "real_size", outsize);
    } else {
      out = uncompressed_buf;
    }
  } else {
    out = reinterpret_cast<const char*>(buf) + pos;
    outsize = rh->data_length_;
  }
  return ret;
}

int MacroBlockReader::parse_micro_block_index()
{
  int ret = OB_SUCCESS;
  const char *index_ptr = NULL;
  int32_t index_size = 0;
  const char* endkey_ptr = NULL;
  blocksstable::ObPosition data_pos;
  blocksstable::ObPosition key_pos;

  micro_index_pos_.reuse();
  micro_index_keys_.reuse();
  micro_mark_deletions_.reuse();

  index_ptr = macro_block_.data_ + sstable_header_->micro_block_index_offset_;
  index_size = sstable_header_->micro_block_index_size_;
  UNUSED(index_size);
  endkey_ptr = macro_block_.data_ + sstable_header_->micro_block_endkey_offset_;
  UNUSED(endkey_ptr);
  for (int32_t i = 0; OB_SUCC(ret) && i < sstable_header_->micro_block_count_; ++i) {
    parse_one_micro_block_index(index_ptr, i, data_pos, key_pos);
    if (OB_FAIL(micro_index_pos_.push_back(data_pos))) {
      LOG_WARN("failed to push back micro index pos", K(ret), K(data_pos));
    } else if (OB_FAIL(micro_index_keys_.push_back(key_pos))) {
      LOG_WARN("failed to push back micro index key", K(ret), K(key_pos));
    }
  }

  if (OB_SUCC(ret)) {
    const char *deletion_ptr = macro_block_.data_ + macro_meta_->micro_block_mark_deletion_offset_;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_header_->micro_block_count_; ++i) {
      uint8_t mark_deletion = deletion_ptr[i];
      LOG_INFO("micro block mark deletion info", K(mark_deletion), K(i));
      if (OB_FAIL(micro_mark_deletions_.push_back(mark_deletion > 0 ? true : false))) {
        LOG_WARN("fail to push back micro mark deletion", K(ret));
      }
    }
  }
  return ret;
}

void MacroBlockReader::parse_one_micro_block_index(const char* index_ptr,
    const int64_t idx, ObPosition& datapos, ObPosition& keypos)
{
  int32_t off1[2];
  int32_t off2[2];

  off1[0] = *reinterpret_cast<const int32_t*>(index_ptr + sizeof(int32_t) * 2 * idx);
  off1[1] = *reinterpret_cast<const int32_t*>(index_ptr + sizeof(int32_t) * 2 * idx + sizeof(int32_t));
  off2[0] = *reinterpret_cast<const int32_t*>(index_ptr + sizeof(int32_t) * 2 * (idx + 1));
  off2[1] = *reinterpret_cast<const int32_t*>(index_ptr + sizeof(int32_t) * 2 * (idx + 1) + sizeof(int32_t));

  datapos.offset_ = off1[0];
  datapos.length_ = off2[0] - off1[0];
  keypos.offset_ = off1[1];
  keypos.length_ = off2[1] - off1[1];
}

int MacroBlockReader::build_column_map()
{
  int ret = OB_SUCCESS;
  hash::ObArrayIndexHashSet<const uint16_t*, uint16_t> column_hash;
  column_hash.reset();
  column_hash.init(column_id_list_);
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_header_->column_count_; ++i) {
    if (OB_FAIL(column_hash.set_index(i))) {
      LOG_WARN("column hash set index failed", K(ret), K(i));
    }
  }

  uint64_t idx = 0;
  if (OB_SUCC(ret)) {
    column_map_.reuse();
    ObArray<share::schema::ObColDesc> out_cols;
    ObArray<int32_t> projector;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_header_->column_count_; ++i) {
      ret = column_hash.get_index(static_cast<uint16_t>(column_id_list_[i]), idx);
      if (OB_ENTRY_NOT_EXIST == ret) {
        idx = -1;
        ret = OB_SUCCESS;
      }
      share::schema::ObColDesc col;
      col.col_id_ = column_id_list_[i];
      col.col_type_ = column_type_list_[i];
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(out_cols.push_back(col))) {
        LOG_WARN("fail to add column to out column array", K(ret), K(col));
      } else if (OB_FAIL(projector.push_back(static_cast<int32_t>(idx)))) {
        LOG_WARN("fail to add index to projector", K(ret), K(idx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(column_map_.init(allocator_,
                                        macro_meta_->schema_version_,
                                        sstable_header_->rowkey_column_count_,
                                        sstable_header_->column_count_,
                                        out_cols,
                                        nullptr,
                                        &projector))) {
      LOG_WARN("failed to init column map", K(ret), K(macro_meta_->schema_version_),
          K(sstable_header_->rowkey_column_count_), K(sstable_header_->column_count_), K(out_cols), K(projector));
    }
  }
  return ret;
}

int MacroBlockReader::dump_header()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    dump_common_header(&macro_block_.common_header_);
    dump_sstable_header(sstable_header_);
    // dump column id, type, checksum list;
    if (sstable_header_->column_count_ > 0) {
      PrintHelper::print_dump_cols_info_start("column_id", "column_type", "column_checksum", "collation type");
      for (int64_t i = 0; i < sstable_header_->column_count_; ++i) {
        PrintHelper::print_dump_cols_info_line(*(column_id_list_ + i),
            obj_type_name[(column_type_list_ + i)->get_type()], *(column_checksum_ + i),
            (column_type_list_ + i)->get_collation_type());
      }
    }
    PrintHelper::print_end_line();

    // dump micro_block_index;
    ObObj objs[sstable_header_->rowkey_column_count_];
    const char *endkey_ptr = macro_block_.data_ + sstable_header_->micro_block_endkey_offset_;
    int64_t endkey_ptr_pos = 0;
    for (int32_t i = 0; i < sstable_header_->micro_block_count_; ++i) {
      parse_micro_block_key(endkey_ptr,
          endkey_ptr_pos,
          endkey_ptr_pos + micro_index_keys_.at(i).length_,
          column_type_list_,
          objs,
          sstable_header_->rowkey_column_count_);
      ObRowkey endkey(objs, sstable_header_->rowkey_column_count_);
      dump_micro_index(endkey, micro_index_pos_.at(i), micro_index_keys_.at(i), micro_mark_deletions_.at(i), i);
    }
  }
  return ret;
}

int MacroBlockReader::parse_micro_block_key(const char* buf, int64_t& pos, int64_t row_end_pos,
    const ObObjMeta* type_list, ObObj* objs, int32_t rowkey_count)
{
  int ret = OB_SUCCESS;
  ObFlatRowReader reader;
  ObNewRow row;
  row.cells_ = objs;
  row.count_ = rowkey_count;

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = reader.read_compact_rowkey(type_list, rowkey_count, buf /*+ pos*/, row_end_pos, pos, row))) {
      fprintf(stderr, "fail to read row."
              "ret = %d, pos = %ld, end_pos = %ld\n",
              ret, pos, row_end_pos);
    } else {
      pos = row_end_pos;
    }
  }
  return ret;
}

void MacroBlockReader::dump_micro_index(const ObRowkey& endkey, const blocksstable::ObPosition& datapos,
    const blocksstable::ObPosition& keypos, const bool mark_deletion, int32_t num)
{
  PrintHelper::print_dump_title("Micro Index", num * 1L, 1);
  PrintHelper::print_dump_line("endkey", to_cstring(endkey));
  PrintHelper::print_dump_line("datapos offset", datapos.offset_);
  PrintHelper::print_dump_line("datapos length", datapos.length_);
  PrintHelper::print_dump_line("keypos offset", keypos.offset_);
  PrintHelper::print_dump_line("keypos length", keypos.length_);
  PrintHelper::print_dump_line("mark deletion", mark_deletion);
  PrintHelper::print_end_line();
}

int MacroBlockReader::dump(const int64_t micro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(dump_header())) {
    LOG_WARN("failed to dump header", K(ret));
  } else if (micro_id < ALL_MINOR_INDEX) {
    LOG_WARN("invalid micro id", K(ret), K(micro_id));
  } else {
    const MicroBlock *micro_block = NULL;
    MicroBlockReader micro_reader;
    for (int64_t i = 0; OB_SUCC(ret) && i < count(); ++i) {
      if (micro_id == ALL_MINOR_INDEX || micro_id == i) {
        micro_block = NULL;
        micro_reader.reset();
        if (OB_FAIL(set_index(i))) {
          LOG_WARN("failed to set index", K(ret), K(i));
        } else if (OB_FAIL(get_value(micro_block))) {
          LOG_WARN("failed to get micro block", K(ret));
        } else if (OB_ISNULL(micro_block)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("micro block is NULL", K(ret));
        } else if (OB_FAIL(micro_reader.init(*micro_block))) {
          LOG_WARN("failed to setup micro reader", K(ret));
        } else if (OB_FAIL(micro_reader.dump(-1))) {
          LOG_WARN("dump micro block failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int MicroBlockReader::init(const MicroBlock &micro_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(micro_block));
  } else {
    micro_block_ = micro_block;
    if (FLAT_ROW_STORE == micro_block_.row_store_type_) {
      micro_reader_ = &flat_micro_reader_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row store type", K(ret), "type", micro_block_.row_store_type_);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(micro_reader_->init(micro_block))) {
        LOG_WARN("failed to init micro reader", K(ret), K(micro_block));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int64_t MicroBlockReader::count() const
{
  return micro_reader_ == NULL ? 0 : micro_reader_->count();
}

int MicroBlockReader::set_index(const int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(micro_reader_->set_index(index))) {
    LOG_WARN("failed to set index", K(ret), K(index));
  }
  return ret;
}

int64_t MicroBlockReader::get_index() const
{
  return micro_reader_ == NULL ? -1 : micro_reader_->get_index();
}

int MicroBlockReader::get_value(const storage::ObStoreRow *&value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(micro_reader_->get_value(value))) {
    LOG_WARN("failed to get value", K(ret));
  }
  return ret;
}

void MicroBlockReader::reset()
{
  micro_block_.reset();
  micro_reader_ = NULL;
  flat_micro_reader_.reset();
  is_inited_ = false;
}

int MicroBlockReader::dump(const int64_t)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(micro_reader_->dump(-1))) {
    LOG_WARN("failed to dump", K(ret));
  }
  return ret;
}

int FlatMicroBlockReader::init(const MicroBlock &micro_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(micro_block));
  } else {
    micro_block_ = micro_block;
    micro_block_header_ = reinterpret_cast<const ObMicroBlockHeader *>(micro_block_.data_);
    index_buffer_ = reinterpret_cast<const int32_t*>(micro_block_.data_ + micro_block_header_->row_index_offset_);
    curr_row_.row_val_.cells_ = columns_;
    curr_row_.row_val_.count_ = micro_block_header_->column_count_;
    is_inited_ = true;
  }
  return ret;
}

int64_t FlatMicroBlockReader::count() const
{
  return micro_block_header_->row_count_;
}

int FlatMicroBlockReader::set_index(const int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(index < 0)
      || OB_UNLIKELY(index >= count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(index));
  } else if (index >= count()) {
    ret = OB_ITER_END;
  } else {
    curr_row_index_ = index;
  }
  return ret;
}

int64_t FlatMicroBlockReader::get_index() const
{
  return curr_row_index_;
}

int FlatMicroBlockReader::get_value(const storage::ObStoreRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int64_t pos = micro_block_header_->header_size_ + *(index_buffer_ + curr_row_index_);
    int64_t end_pos = micro_block_header_->header_size_ + *(index_buffer_ + curr_row_index_ + 1);
    if (curr_row_index_ >= micro_block_header_->row_count_ || end_pos > micro_block_.size_) {
      ret = OB_ITER_END;
    } else {
      reader_.reuse_allocator();
      if (OB_FAIL(
          reader_.read_row(
              micro_block_.data_,
              end_pos,
              pos,
              *micro_block_.column_map_,
              allocator_,
              curr_row_))) {
        LOG_WARN("failed to read meta row", K(ret), K(pos), K(end_pos));
      } else {
        row = &curr_row_;
      }
    }
  }
  return ret;
}

int FlatMicroBlockReader::dump(const int64_t)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    const ObStoreRow *row = NULL;
    dump_micro_header(micro_block_header_);
    PrintHelper::print_dump_title("Micro Block", micro_block_.micro_id_, 1);
    PrintHelper::print_dump_title("Total Rows", count(), 1);

    for (int64_t i = 0; OB_SUCC(ret) && i < count(); ++i) {
      if (OB_FAIL(set_index(i))) {
        LOG_WARN("failed to set index", K(ret), K(i));
      } else if (OB_FAIL(get_value(row))) {
        LOG_WARN("failed to get micro block", K(ret));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is NULL", K(ret));
      } else {
        PrintHelper::print_row_title(false /*use csv*/, row, i);
        dump_row(row);
      }
    }
  }
  return ret;
}

void FlatMicroBlockReader::reset()
{
  micro_block_.reset();
  micro_block_header_ = NULL;
  index_buffer_ = NULL;
  curr_row_index_ = 0;
  curr_row_.row_val_.count_ = 0;
  is_inited_ = false;
}

void FlatMicroBlockReader::dump_micro_header(const ObMicroBlockHeader *micro_block_header)
{
  PrintHelper::print_dump_title("Micro Header");
  PrintHelper::print_dump_line("header_size", micro_block_header->header_size_);
  PrintHelper::print_dump_line("version", micro_block_header->version_);
  PrintHelper::print_dump_line("magic", micro_block_header->magic_);
  PrintHelper::print_dump_line("attr", micro_block_header->attr_);
  PrintHelper::print_dump_line("column_count", micro_block_header->column_count_);
  PrintHelper::print_dump_line("row_index_offset", micro_block_header->row_index_offset_);
  PrintHelper::print_dump_line("row_count", micro_block_header->row_count_);
  PrintHelper::print_end_line();
}

int ObDumpsstPartitionImage::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_PARTITION, this))) {
    STORAGE_LOG(WARN, "failed to register_redo_module", K(ret));
  }
  return ret;
}

ObDumpsstPartitionImage &ObDumpsstPartitionImage::get_instance()
{
  static ObDumpsstPartitionImage pi;
  return pi;
}

}
}
