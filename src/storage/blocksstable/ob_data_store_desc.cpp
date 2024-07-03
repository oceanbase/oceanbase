//Copyright (c) 2023 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace blocksstable
{
/**
 * -------------------------------------------------------------------ObStaticDataStoreDesc-------------------------------------------------------------------
 */
const ObCompressorType ObStaticDataStoreDesc::DEFAULT_MINOR_COMPRESSOR_TYPE;
ObStaticDataStoreDesc::ObStaticDataStoreDesc(const bool is_ddl)
  : is_ddl_(is_ddl),
    merge_type_(compaction::INVALID_MERGE_TYPE),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    ls_id_(),
    tablet_id_(),
    macro_block_size_(0),
    macro_store_size_(0),
    micro_block_size_limit_(0),
    schema_version_(0),
    snapshot_version_(0),
    end_scn_(),
    progressive_merge_round_(0),
    major_working_cluster_version_(0),
    encrypt_id_(0),
    master_key_id_(0)
{
  end_scn_.set_min();
  MEMSET(encrypt_key_, 0, sizeof(encrypt_key_));
}

bool ObStaticDataStoreDesc::is_valid() const
{
  return ls_id_.is_valid()
         && tablet_id_.is_valid()
         && compressor_type_ > ObCompressorType::INVALID_COMPRESSOR
         && snapshot_version_ > 0
         && schema_version_ >= 0;
}

void ObStaticDataStoreDesc::reset()
{
  merge_type_ = compaction::INVALID_MERGE_TYPE;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  ls_id_.reset();
  tablet_id_.reset();
  macro_block_size_ = 0;
  macro_store_size_ = 0;
  micro_block_size_limit_ = 0;
  schema_version_ = 0;
  snapshot_version_ = 0;
  end_scn_.set_min();
  progressive_merge_round_ = 0;
  major_working_cluster_version_ = 0;
  encrypt_id_ = 0;
  master_key_id_ = 0;
  MEMSET(encrypt_key_, 0, sizeof(encrypt_key_));
}

int ObStaticDataStoreDesc::assign(const ObStaticDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  is_ddl_ = desc.is_ddl_;
  merge_type_ = desc.merge_type_;
  compressor_type_ = desc.compressor_type_;
  ls_id_ = desc.ls_id_;
  tablet_id_ = desc.tablet_id_;
  macro_block_size_ = desc.macro_block_size_;
  macro_store_size_ = desc.macro_store_size_;
  micro_block_size_limit_ = desc.micro_block_size_limit_;
  schema_version_ = desc.schema_version_;
  snapshot_version_ = desc.snapshot_version_;
  end_scn_ = desc.end_scn_;
  major_working_cluster_version_ = desc.major_working_cluster_version_;
  encrypt_id_ = desc.encrypt_id_;
  master_key_id_ = desc.master_key_id_;
  MEMCPY(encrypt_key_, desc.encrypt_key_, sizeof(encrypt_key_));
  return ret;
}

int ObStaticDataStoreDesc::init_encryption_info(const ObMergeSchema &merge_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(merge_schema.get_encryption_id(encrypt_id_))) {
    STORAGE_LOG(WARN, "fail to get encrypt id from table schema", K(ret), K(merge_schema));
  } else if (merge_schema.need_encrypt() && merge_schema.get_encrypt_key_len() > 0) {
    const int64_t key_str_len = share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH;
    if (OB_UNLIKELY(merge_schema.get_encrypt_key_len() > key_str_len)) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "encrypt key length overflow", KR(ret),
        K(merge_schema.get_encrypt_key_len()), K(key_str_len));
    } else {
      master_key_id_ = merge_schema.get_master_key_id();
      MEMCPY(encrypt_key_, merge_schema.get_encrypt_key().ptr(),
        merge_schema.get_encrypt_key().length());
    }
  }
  return ret;
}

void ObStaticDataStoreDesc::init_block_size(const ObMergeSchema &merge_schema)
{
  const int64_t pct_free = merge_schema.get_pctfree();
  macro_block_size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  if (pct_free >= 0 && pct_free <= 50) {
    macro_store_size_ = macro_block_size_ * (100 - pct_free) / 100;
  } else {
    macro_store_size_ = macro_block_size_ * DEFAULT_RESERVE_PERCENT / 100;
  }
  micro_block_size_limit_ = macro_block_size_
    - ObMacroBlockCommonHeader::get_serialize_size()
    - ObSSTableMacroBlockHeader::get_fixed_header_size()
    - MIN_RESERVED_SIZE;
}

int ObStaticDataStoreDesc::init(
    const ObMergeSchema &merge_schema,
    const share::ObLSID &ls_id,
    const common::ObTabletID tablet_id,
    const compaction::ObMergeType merge_type,
    const int64_t snapshot_version,
    const share::SCN &end_scn,
    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  const bool is_major = compaction::is_major_or_meta_merge_type(merge_type);
  if (OB_UNLIKELY(!merge_schema.is_valid() || !ls_id.is_valid() || !tablet_id.is_valid() || snapshot_version <= 0
    || (!is_major && !end_scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arguments is invalid", K(ret), K(merge_schema), K(snapshot_version), K(end_scn));
  } else {
    reset();
    merge_type_ = merge_type;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;

    if (!is_major) {
      end_scn_ = end_scn;
    } else if (OB_FAIL(end_scn_.convert_for_tx(snapshot_version))) {
      STORAGE_LOG(WARN, "fail to convert scn", K(ret), K(snapshot_version));
    }

    if (FAILEDx(init_encryption_info(merge_schema))) {
      STORAGE_LOG(WARN, "fail to get encrypt info from table schema", K(ret));
    } else {
      schema_version_ = merge_schema.get_schema_version();
      snapshot_version_ = snapshot_version;
      progressive_merge_round_ = merge_schema.get_progressive_merge_round();
      compressor_type_ = merge_schema.get_compressor_type();
      (void) init_block_size(merge_schema);
      if (is_major) {
        uint64_t compat_version = 0;
        if (cluster_version > 0) {
          major_working_cluster_version_ = cluster_version;
        } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
          STORAGE_LOG(WARN, "fail to get data version", K(ret));
        } else {
          major_working_cluster_version_ = compat_version;
          STORAGE_LOG(INFO, "success to set major working cluster version", K(ret), "merge_type", merge_type_to_str(merge_type),
            K(cluster_version), K(major_working_cluster_version_));
        }
      } else if (compressor_type_ != ObCompressorType::NONE_COMPRESSOR) {
        // for mini/minor, use default compressor
        compressor_type_ = DEFAULT_MINOR_COMPRESSOR_TYPE;
      }
    }
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObColDataStoreDesc-------------------------------------------------------------------
 */
ObColDataStoreDesc::ObColDataStoreDesc()
  : is_row_store_(true),
    default_col_checksum_array_valid_(false),
    table_cg_idx_(0),
    row_column_count_(0),
    rowkey_column_count_(0),
    schema_rowkey_col_cnt_(0),
    full_stored_col_cnt_(0),
    allocator_("DataStoDesc", OB_MALLOC_NORMAL_BLOCK_SIZE),
    col_default_checksum_array_(allocator_),
    agg_meta_array_(allocator_),
    col_desc_array_(allocator_)
{}

bool ObColDataStoreDesc::is_valid() const
{
  return row_column_count_ > 0
         && rowkey_column_count_ >= 0
         && row_column_count_ >= rowkey_column_count_
         && schema_rowkey_col_cnt_ >= 0;
}

void ObColDataStoreDesc::reset()
{
  is_row_store_ = true;
  table_cg_idx_ = 0;
  row_column_count_ = 0;
  rowkey_column_count_ = 0;
  schema_rowkey_col_cnt_ = 0;
  full_stored_col_cnt_ = 0;
  default_col_checksum_array_valid_ = false;
  col_desc_array_.reset();
  col_default_checksum_array_.reset();
  agg_meta_array_.reset();
  datum_utils_.reset();
  allocator_.reset();
}

int ObColDataStoreDesc::assign(const ObColDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  is_row_store_ = desc.is_row_store_;
  table_cg_idx_ = desc.table_cg_idx_;
  row_column_count_ = desc.row_column_count_;
  rowkey_column_count_ = desc.rowkey_column_count_;
  schema_rowkey_col_cnt_ = desc.schema_rowkey_col_cnt_;
  full_stored_col_cnt_ = desc.full_stored_col_cnt_;
  default_col_checksum_array_valid_ = desc.default_col_checksum_array_valid_;
  col_desc_array_.reset();
  col_default_checksum_array_.reset();
  agg_meta_array_.reset();
  datum_utils_.reset();
  if (OB_FAIL(col_desc_array_.assign(desc.col_desc_array_))) {
    STORAGE_LOG(WARN, "Failed to assign column desc array", K(ret));
  } else if (OB_FAIL(col_default_checksum_array_.assign(desc.col_default_checksum_array_))) {
    STORAGE_LOG(WARN, "Failed to assign col default checksum array, ", K(ret));
  } else if (OB_FAIL(agg_meta_array_.assign(desc.agg_meta_array_))) {
    STORAGE_LOG(WARN, "Failed to assign aggregate meta array", K(ret));
  } else if (OB_FAIL(datum_utils_.assign(desc.datum_utils_, allocator_))) {
    STORAGE_LOG(WARN, "Failed to init datum utils", K(ret));
  }
  return ret;
}

int ObColDataStoreDesc::init(
  const bool is_major,
  const ObMergeSchema &merge_schema,
  const uint16_t table_cg_idx,
  const int64_t major_working_cluster_version)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(merge_schema.get_store_column_count(full_stored_col_cnt_, true))) {
    STORAGE_LOG(WARN, "failed to get store column count", K(ret), K(merge_schema));
  } else {
    is_row_store_ = true;
    table_cg_idx_ = table_cg_idx;
    schema_rowkey_col_cnt_ = merge_schema.get_rowkey_column_num();
    rowkey_column_count_ =
      schema_rowkey_col_cnt_ + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    full_stored_col_cnt_ += storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    row_column_count_ = full_stored_col_cnt_;
    if (!merge_schema.is_column_info_simplified()) {
      if (OB_FAIL(col_desc_array_.init(row_column_count_))) {
        STORAGE_LOG(WARN, "Failed to reserve column desc array", K(ret));
      } else if (OB_FAIL(merge_schema.get_multi_version_column_descs(col_desc_array_))) {
        STORAGE_LOG(WARN, "Failed to generate multi version column ids", K(ret));
      } else if (is_major && OB_FAIL(generate_skip_index_meta(merge_schema, nullptr/*cg_schema*/, major_working_cluster_version))) {
        STORAGE_LOG(WARN, "failed to generate skip index meta", K(ret));
      }
    } else {
      if (OB_FAIL(col_desc_array_.init(rowkey_column_count_))) {
        STORAGE_LOG(WARN, "fail to reserve column desc array", K(ret));
      } else if (OB_FAIL(merge_schema.get_mulit_version_rowkey_column_ids(col_desc_array_))) {
        STORAGE_LOG(WARN, "fail to get rowkey column ids", K(ret));
      } else if (is_major && OB_FAIL(generate_skip_index_meta(merge_schema, nullptr/*cg_schema*/, major_working_cluster_version))) {
        STORAGE_LOG(WARN, "failed to generate skip index meta", K(ret));
      }
    }
    if (FAILEDx(gene_col_default_checksum_array(merge_schema))) {
      STORAGE_LOG(WARN, "failed to init default column checksum", KR(ret), K(merge_schema));
    } else if (FALSE_IT(fresh_col_meta(merge_schema))) {
    } else if (OB_FAIL(get_compat_mode_from_schema(merge_schema, is_oracle_mode))) {
      STORAGE_LOG(WARN, "failed to get compat mode", KR(ret), K(merge_schema));
    } else if (OB_FAIL(datum_utils_.init(
        col_desc_array_, schema_rowkey_col_cnt_, is_oracle_mode, allocator_))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret));
    } else {
      STORAGE_LOG(INFO, "success to init data desc", K(ret), KPC(this), K(merge_schema), K(table_cg_idx),
        K(is_oracle_mode), K(col_desc_array_));
    }
  }
  return ret;
}

int ObColDataStoreDesc::get_compat_mode_from_schema(
  const share::schema::ObMergeSchema &merge_schema,
  bool &is_oracle_mode)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
  if (typeid(merge_schema) == typeid(storage::ObStorageSchema)) {
    compat_mode = static_cast<const storage::ObStorageSchema *>(&merge_schema)->get_compat_mode();
  } else if (typeid(merge_schema) == typeid(ObTableSchema)) {
    const int64_t table_id = static_cast<const ObTableSchema *>(&merge_schema)->get_table_id();
    if (is_ls_reserved_table(table_id)) {
      compat_mode = lib::Worker::CompatMode::MYSQL;
    } else if (OB_FAIL(share::ObCompatModeGetter::get_table_compat_mode(MTL_ID(), table_id, compat_mode))) {
      STORAGE_LOG(WARN, "fail to get tenant mode", KR(ret), K(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    is_oracle_mode = lib::Worker::CompatMode::ORACLE == compat_mode;
  }
  return ret;
}

int ObColDataStoreDesc::add_col_desc_from_cg_schema(
  const share::schema::ObMergeSchema &merge_schema,
  const storage::ObStorageColumnGroupSchema &cg_schema)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = cg_schema.column_cnt_;
  common::ObArray<share::schema::ObColDesc> multi_version_column_desc_array;
  if (OB_FAIL(col_desc_array_.init(column_cnt))) {
    STORAGE_LOG(WARN, "Failed to reserve column desc array", K(ret));
  } else if (merge_schema.is_column_info_simplified()) {
    if (OB_FAIL(merge_schema.get_mulit_version_rowkey_column_ids(multi_version_column_desc_array))) {
      STORAGE_LOG(WARN, "failed to get rowkey column ids", K(ret), K(column_cnt), K(cg_schema), K(merge_schema));
    }
  } else if (OB_FAIL(merge_schema.get_multi_version_column_descs(multi_version_column_desc_array))) {
    STORAGE_LOG(WARN, "Failed to generate multi version column ids", K(ret));
  }

  for (uint16_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    const uint16_t idx = cg_schema.column_idxs_[i];
    if (idx >= multi_version_column_desc_array.count()) {
      if (OB_FAIL(add_binary_col_desc(column_cnt))) {
        STORAGE_LOG(WARN, "failed to add fake col desc when merge schema is simplified",
            K(ret), K(column_cnt), K(idx), K(cg_schema), K(multi_version_column_desc_array));
      }
    } else if (OB_FAIL(col_desc_array_.push_back(multi_version_column_desc_array.at(idx)))) {
      STORAGE_LOG(WARN, "failed to push back col desc", K(ret), K(i), K(idx),
                  K(column_cnt), K(multi_version_column_desc_array));
    }
  }
  return ret;
}

int ObColDataStoreDesc::init(const bool is_major,
                             const ObMergeSchema &merge_schema,
                             const storage::ObStorageColumnGroupSchema &cg_schema,
                             const uint16_t table_cg_idx,
                             const int64_t major_working_cluster_version)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(!merge_schema.is_valid() || !cg_schema.is_valid() || cg_schema.is_all_column_group() || !is_major)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arguments is invalid", K(ret), K(merge_schema), K(cg_schema), K(is_major));
  } else {
    reset();
    const int64_t column_cnt = cg_schema.column_cnt_;
    if (OB_FAIL(add_col_desc_from_cg_schema(merge_schema, cg_schema))) {
      STORAGE_LOG(WARN, "Failed to reserve column desc array", K(ret));
    } else {
      (void) fresh_col_meta(merge_schema);
      is_row_store_ = cg_schema.is_rowkey_column_group();
      table_cg_idx_ = table_cg_idx;
      schema_rowkey_col_cnt_ = cg_schema.schema_rowkey_column_cnt_;
      rowkey_column_count_ = cg_schema.rowkey_column_cnt_;
      row_column_count_ = column_cnt;
      full_stored_col_cnt_ = row_column_count_;
    }

    if (FAILEDx(gene_col_default_checksum_array(merge_schema))) {
      STORAGE_LOG(WARN, "failed to init default column checksum", KR(ret), K(merge_schema));
    } else if (OB_FAIL(generate_skip_index_meta(merge_schema, &cg_schema, major_working_cluster_version))) {
      STORAGE_LOG(WARN, "failed to generate skip index meta", K(ret), K(major_working_cluster_version), K(merge_schema), K(cg_schema));
    } else if (OB_FAIL(get_compat_mode_from_schema(merge_schema, is_oracle_mode))) {
      STORAGE_LOG(WARN, "failed to get compat mode", KR(ret), K(merge_schema));
    } else if (OB_FAIL(datum_utils_.init(col_desc_array_, schema_rowkey_col_cnt_,
        is_oracle_mode, allocator_, !is_row_store_))) {
      STORAGE_LOG(WARN, "Failed to init datum utils", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "success to init data desc", K(ret), KPC(this), K(is_oracle_mode), K(cg_schema));
  }
  return ret;
}

int ObColDataStoreDesc::mock_valid_col_default_checksum_array(int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  full_stored_col_cnt_ = column_cnt;
  col_default_checksum_array_.reset();
  if (OB_FAIL(init_col_default_checksum_array(column_cnt))) {
    STORAGE_LOG(WARN, "failed to init col default checksum array", KR(ret));
  } else {
    default_col_checksum_array_valid_ = true;
  }
  return ret;
}

int ObColDataStoreDesc::gene_col_default_checksum_array(
  const share::schema::ObMergeSchema &merge_schema)
{
  int ret = OB_SUCCESS;
  if (is_row_store_) {
    if (merge_schema.is_column_info_simplified()) {
      ret = init_col_default_checksum_array(full_stored_col_cnt_);
    } else {
      ret = init_col_default_checksum_array(merge_schema);
    }
  } else {
    ret = init_col_default_checksum_array(full_stored_col_cnt_);
  }
  return ret;
}

// fill default column checksum
// column_cnt is valid when init_by_schema = true
int ObColDataStoreDesc::init_col_default_checksum_array(
  const share::schema::ObMergeSchema &merge_schema)
{
  int ret = OB_SUCCESS;
  default_col_checksum_array_valid_ = true;
  common::ObArray<ObSSTableColumnMeta> meta_array;
  if (OB_FAIL(merge_schema.init_column_meta_array(meta_array))) {
    STORAGE_LOG(WARN, "fail to init column meta array", K(ret), K(merge_schema));
  } else if (OB_FAIL(col_default_checksum_array_.init(meta_array.count()))) {
    STORAGE_LOG(WARN, "fail to init column default checksum array", K(ret), K(meta_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_array.count(); ++i) {
      if (OB_FAIL(col_default_checksum_array_.push_back(meta_array.at(i).column_default_checksum_))) {
        STORAGE_LOG(WARN, "fail to push default checksum into array", K(ret),
                    K(i), K(meta_array));
      }
    }
  }
  return ret;
}

// for mini & minor, just push default_col_checksum=0
int ObColDataStoreDesc::init_col_default_checksum_array(
  const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(column_cnt));
  } else if (OB_FAIL(col_default_checksum_array_.init(column_cnt))) {
    STORAGE_LOG(WARN, "fail to init column default checksum array", K(ret));
  } else {
    default_col_checksum_array_valid_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (OB_FAIL(col_default_checksum_array_.push_back(0))) {
        STORAGE_LOG(WARN, "fail to push default checksum into array", K(ret), K(i), K(column_cnt));
      }
    }
  }
  return ret;
}

int ObColDataStoreDesc::generate_skip_index_meta(
    const share::schema::ObMergeSchema &schema,
    const storage::ObStorageColumnGroupSchema *cg_schema,
    const int64_t major_working_cluster_version)
{
  int ret = OB_SUCCESS;
  ObArray<ObSkipIndexColumnAttr> skip_idx_attrs;
  const bool is_full_column_sstable = !(nullptr != cg_schema && !cg_schema->is_all_column_group());
  if (OB_UNLIKELY(!schema.is_valid() || (nullptr != cg_schema && !cg_schema->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid schema", K(ret), K(schema), KPC(cg_schema));
  } else if (OB_UNLIKELY(!agg_meta_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected non-empty aggregate meta array", K(ret));
  } else if (schema.is_column_info_simplified()) {
      // simplified do not generate skip index, do not init agg_meta_array
  } else if (OB_FAIL(schema.get_skip_index_col_attr(skip_idx_attrs))) {
    STORAGE_LOG(WARN, "failed to get skip index col attr", K(ret));
  } else if (OB_UNLIKELY(skip_idx_attrs.count() < full_stored_col_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected stored col count and skip attr array count not match",
        K(ret), K(skip_idx_attrs), K_(col_desc_array), K_(full_stored_col_cnt));
  } else if (OB_FAIL(agg_meta_array_.init(ObSkipIndexColMeta::MAX_AGG_COLUMN_PER_ROW * full_stored_col_cnt_))) {
    STORAGE_LOG(WARN, "failed to init agg meta array", K(ret), K_(full_stored_col_cnt));
  } else if (is_full_column_sstable) {
    // generate skip index for row store
    for (int64_t i = 0; OB_SUCC(ret) && i < full_stored_col_cnt_; ++i) {
      if (!skip_idx_attrs.at(i).has_skip_index()) {
      } else if (OB_FAIL(blocksstable::ObSkipIndexColMeta::append_skip_index_meta(
          skip_idx_attrs.at(i), i, agg_meta_array_))) {
        STORAGE_LOG(WARN, "failed to append skip index meta array", K(ret), KPC(cg_schema), K(i));
      }
    }
  } else if (cg_schema->is_single_column_group()) {
    // build min_max and sum aggregate for single column group by default;
    const uint16_t single_cg_column_idx = cg_schema->column_idxs_[0];
    if (OB_FAIL(generate_single_cg_skip_index_meta(
        skip_idx_attrs.at(single_cg_column_idx), *cg_schema, major_working_cluster_version))) {
      STORAGE_LOG(WARN, "failed to generate skip index meta for single column group",
          K(ret), "skip_idx_attr", skip_idx_attrs.at(single_cg_column_idx),
          K(major_working_cluster_version), KPC(cg_schema));
    }
  } else {
    // generate skip index for column in column group
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schema->column_cnt_; ++i) {
      const uint16_t column_idx = cg_schema->column_idxs_[i];
      if (!skip_idx_attrs.at(column_idx).has_skip_index()) {
      } else if (OB_FAIL(blocksstable::ObSkipIndexColMeta::append_skip_index_meta(
          skip_idx_attrs.at(column_idx), i, agg_meta_array_))) {
        STORAGE_LOG(WARN, "failed to append skip index meta array", K(ret), KPC(cg_schema), K(i), K(column_idx));
      }
    }
  }
  STORAGE_LOG(DEBUG, "[SKIP INDEX] generate skip index meta", K(ret), KPC(cg_schema), K_(agg_meta_array), K_(col_desc_array));
  return ret;
}

int ObColDataStoreDesc::generate_single_cg_skip_index_meta(
    const ObSkipIndexColumnAttr &skip_idx_attr_by_user,
    const storage::ObStorageColumnGroupSchema &cg_schema,
    const int64_t major_working_cluster_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!cg_schema.is_single_column_group() || 1 != cg_schema.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(cg_schema));
  } else if (OB_UNLIKELY(1 != col_desc_array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected col desc array count for single column group", K(ret), K_(col_desc_array));
  } else {
    const uint16_t column_idx = cg_schema.column_idxs_[0];
    ObSkipIndexColumnAttr single_cg_skip_idx_attr = skip_idx_attr_by_user;
    single_cg_skip_idx_attr.set_min_max();
    if (major_working_cluster_version < DATA_VERSION_4_3_2_0) {
      single_cg_skip_idx_attr.set_sum();
    }
    if (OB_FAIL(blocksstable::ObSkipIndexColMeta::append_skip_index_meta(
        single_cg_skip_idx_attr, 0, agg_meta_array_))) {
      STORAGE_LOG(WARN, "failed to append skip index meta array", K(ret), K(column_idx), K(cg_schema));
    }

  }
  return ret;
}

void ObColDataStoreDesc::fresh_col_meta(const share::schema::ObMergeSchema &merge_schema)
{
  for (int64_t i = 0; i < col_desc_array_.count(); i++) {
    if (col_desc_array_.at(i).col_type_.is_lob_storage()) {
      col_desc_array_.at(i).col_type_.set_has_lob_header();
    }
  }
  const ObTableSchema *table_schema = nullptr;
  const ObColumnSchemaV2 *col_schema = nullptr;
  if ((table_schema = dynamic_cast<const ObTableSchema *>(&merge_schema)) != NULL) {
    for (int64_t i = 0; i < col_desc_array_.count(); i++) {
      if (col_desc_array_.at(i).col_type_.is_decimal_int()) {
        col_schema = table_schema->get_column_schema(col_desc_array_.at(i).col_id_);
        OB_ASSERT(col_schema != NULL);
        col_desc_array_.at(i).col_type_.set_stored_precision(
          col_schema->get_accuracy().get_precision());
        col_desc_array_.at(i).col_type_.set_scale(
          col_schema->get_accuracy().get_scale());
      }
    }
  }
}

int ObColDataStoreDesc::add_binary_col_desc(int64_t col_idx)
{
  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_BINARY);
  return add_col_desc(meta, col_idx);
}

int ObColDataStoreDesc::add_col_desc(const ObObjMeta meta, int64_t col_idx)
{
  int ret = OB_SUCCESS;
  share::schema::ObColDesc fake_col;
  fake_col.col_id_ = static_cast<uint64_t>(col_idx + OB_APP_MIN_COLUMN_ID);
  fake_col.col_type_ = meta;
  fake_col.col_order_ = DESC;
  if (OB_FAIL(col_desc_array_.push_back(fake_col))) {
    STORAGE_LOG(WARN, "failed to push back fake col desc", K(ret), K(fake_col), K_(col_desc_array));
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObDataStoreDesc-------------------------------------------------------------------
 */
ObDataStoreDesc::ObDataStoreDesc()
  : static_desc_(nullptr),
    col_desc_(nullptr)
{
  reset();
}

ObDataStoreDesc::~ObDataStoreDesc()
{
  reset();
}

const ObTabletID ObDataStoreDesc::EMERGENCY_TABLET_ID_MAGIC = ObTabletID(0);

int ObDataStoreDesc::get_emergency_row_store_type()
{
  int ret = OB_SUCCESS;

  if (GCONF._force_skip_encoding_partition_id.get_value_string().empty()) {
    // no need check emergency row store type
  } else {
    char partition_key[OB_TMP_BUF_SIZE_256];
    if (OB_FAIL(GCONF._force_skip_encoding_partition_id.copy(partition_key, OB_TMP_BUF_SIZE_256))) {
      STORAGE_LOG(WARN, "Failed to deep copy emergency partition key", K(ret));
    } else {
      char *endptr = nullptr;
      ObTabletID emergency_tablet_id(std::strtoull(partition_key, &endptr, 0));
      uint64_t emergency_tenant_id = 0;
      uint64_t emergency_ls_id = 0;
      if (OB_ISNULL(endptr)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid emergency tablet_id for skiping encoding", K(ret), K(partition_key));
      } else {
        // skip space and get ls id
        while ('\0' != *endptr && isspace(*endptr)) {
          endptr++;
        }
        if ('#' != *endptr) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "Invalid emergency partition key for skiping encoding", K(ret), K(partition_key));
        } else {
          endptr++;
          emergency_ls_id = std::strtoull(endptr, &endptr, 0);

          // skip space and get tenant id
          while ('\0' != *endptr && isspace(*endptr)) {
            endptr++;
          }
          if ('@' != *endptr) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "Invalid emergency partition key for skiping encoding", K(ret), K(partition_key));
          } else {
            endptr++;
            emergency_tenant_id = std::strtoull(endptr, &endptr, 0);
          }
          if (OB_SUCC(ret)) {
            oceanbase::share::ObTaskController::get().allow_next_syslog();
            if (EMERGENCY_TENANT_ID_MAGIC == emergency_tenant_id
                && EMERGENCY_LS_ID_MAGIC == emergency_ls_id
                && EMERGENCY_TABLET_ID_MAGIC == emergency_tablet_id) {
              STORAGE_LOG(INFO, "Magic emergency partition key set to skip encoding for all the tablet",
                  K(emergency_tenant_id), K(emergency_ls_id), K(emergency_tablet_id), K(*this));
              row_store_type_ = FLAT_ROW_STORE;
            } else if (get_tablet_id() == emergency_tablet_id
                          && get_ls_id().id() == emergency_ls_id
                          && MTL_ID() == emergency_tenant_id) {
              STORAGE_LOG(INFO, "Succ to find specified emergency partition to skip encoding",
                  K(emergency_tenant_id), K(emergency_ls_id), K(emergency_tablet_id), K(*this));
              row_store_type_ = FLAT_ROW_STORE;
            } else {
              STORAGE_LOG(INFO, "this partition is not the emergency partition to skip encoding",
                  K(emergency_tenant_id), K(emergency_ls_id), K(emergency_tablet_id), K(*this));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDataStoreDesc::cal_row_store_type(const ObRowStoreType row_store_type,
                                        const compaction::ObMergeType merge_type)
{
  int ret = OB_SUCCESS;
  if (!compaction::is_major_or_meta_merge_type(merge_type)) { // not major or meta merge
    row_store_type_ = FLAT_ROW_STORE;
  } else {
    row_store_type_ = row_store_type;
    if (!ObStoreFormat::is_row_store_type_valid(row_store_type_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Unexpected row store type", K(row_store_type_), K_(row_store_type), K(ret));
    } else if (OB_FAIL(get_emergency_row_store_type())) {
      STORAGE_LOG(WARN, "Failed to check and get emergency row store type", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (ObStoreFormat::is_row_store_type_with_encoding(row_store_type_)) {
        encoder_opt_.set_store_type(row_store_type_);
      }
    }
  }

  return ret;
}

int ObDataStoreDesc::init(
  ObStaticDataStoreDesc &static_desc,
  ObColDataStoreDesc &col_desc,
  const share::schema::ObMergeSchema &merge_schema,
  const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!static_desc.is_valid() || !col_desc.is_valid() || !merge_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arguments is invalid", K(ret), K(static_desc), K(col_desc));
  } else {
    static_desc_ = &static_desc;
    col_desc_ = &col_desc;
    if (OB_FAIL(inner_init(merge_schema, row_store_type))) {
      STORAGE_LOG(WARN, "failed inner init", KR(ret), K(merge_schema));
    } else {
      STORAGE_LOG(TRACE, "success to init data desc", K(ret), KPC(this), K(merge_schema));
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int ObDataStoreDesc::inner_init(
    const ObMergeSchema &merge_schema,
    const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cal_row_store_type(row_store_type, get_merge_type()))) {
    STORAGE_LOG(WARN, "Failed to make the row store type", K(ret));
  } else {
    const bool is_major = compaction::is_major_or_meta_merge_type(get_merge_type());
    sstable_index_builder_ = nullptr;
    if (is_major && get_major_working_cluster_version() <= DATA_VERSION_4_0_0_0) {
      micro_block_size_ = merge_schema.get_block_size();
    } else {
      micro_block_size_ = MAX(merge_schema.get_block_size(), MIN_MICRO_BLOCK_SIZE);
    }
  }
  return ret;
}

int ObDataStoreDesc::update_basic_info_from_macro_meta(const ObSSTableBasicMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cur desc is invalid", KR(ret), KPC(this));
  } else {
    row_store_type_ = meta.root_row_store_type_;
    static_desc_->compressor_type_ = meta.compressor_type_;
    static_desc_->master_key_id_ = meta.master_key_id_;
    static_desc_->encrypt_id_ = meta.encrypt_id_;
    encoder_opt_.set_store_type(meta.root_row_store_type_);
    MEMCPY(static_desc_->encrypt_key_, meta.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);

    // since the schema is always newer than the original sstable and new cols can
    // only be added to the tail, it's safe to pop back the default checksum of
    // new cols to keep the consistency of sstable_meta.
    col_desc_->full_stored_col_cnt_ = meta.column_cnt_;
    while (col_desc_->col_default_checksum_array_.count() > meta.column_cnt_) {
      col_desc_->col_default_checksum_array_.pop_back();
    }
  }
  return ret;
}

bool ObDataStoreDesc::is_valid() const
{
  return nullptr != static_desc_ && static_desc_->is_valid()
         && nullptr != col_desc_ && col_desc_->is_valid()
         && micro_block_size_ > 0
         && is_store_type_valid();
}

bool ObDataStoreDesc::is_store_type_valid() const
{
  bool ret = false;
  bool is_mini_or_minor_merge = compaction::is_minor_merge_type(get_merge_type()) || compaction::is_mini_merge(get_merge_type());
  if (!ObStoreFormat::is_row_store_type_valid(row_store_type_)) {
    // invalid row store type
  } else if (is_force_flat_store_type_) {
    ret = ObRowStoreType::FLAT_ROW_STORE == row_store_type_;
  } else if (is_mini_or_minor_merge) {
    ret = (!ObStoreFormat::is_row_store_type_with_encoding(row_store_type_));
  } else {
    ret = true;
  }

  if (!ret) {
    STORAGE_LOG(WARN, "invalid row store type",
        K_(row_store_type), K_(is_force_flat_store_type));
  }
  return ret;
}

void ObDataStoreDesc::reset()
{
  static_desc_ = nullptr;
  col_desc_ = nullptr;
  row_store_type_ = ENCODING_ROW_STORE;
  encoder_opt_.reset();
  merge_info_ = NULL;
  sstable_index_builder_ = nullptr;
  is_force_flat_store_type_ = false;
  micro_block_size_ = 0;
}

int ObDataStoreDesc::shallow_copy(const ObDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  static_desc_ = desc.static_desc_;
  col_desc_ = desc.col_desc_;
  micro_block_size_ = desc.micro_block_size_;
  row_store_type_ = desc.get_row_store_type();
  encoder_opt_ = desc.encoder_opt_;
  merge_info_ = desc.merge_info_;
  is_force_flat_store_type_ = desc.is_force_flat_store_type_;
  sstable_index_builder_ = desc.sstable_index_builder_;
  return ret;
}
/**
 * -------------------------------------------------------------------ObWholeDataStoreDesc-------------------------------------------------------------------
 */
int ObWholeDataStoreDesc::assign(const ObDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!desc.is_valid())) {
    STORAGE_LOG(WARN, "desc is invalid", KR(ret), K(desc));
  } else if (FALSE_IT(reset())) {
  } else if (OB_FAIL(static_desc_.assign(*desc.static_desc_))) {
    STORAGE_LOG(WARN, "failed to assign static desc", KR(ret), K(desc));
  } else if (OB_FAIL(col_desc_.assign(*desc.col_desc_))) {
    STORAGE_LOG(WARN, "failed to assign col desc", KR(ret), K(desc));
  } else if (OB_FAIL(desc_.shallow_copy(desc))) {
    STORAGE_LOG(WARN, "failed to shallow copy", KR(ret), K(desc));
  } else {
    // update all ptr to local variables
    desc_.static_desc_ = &static_desc_;
    desc_.col_desc_ = &col_desc_;
  }
  return ret;
}

int ObWholeDataStoreDesc::init(
    const ObStaticDataStoreDesc &static_desc,
    const ObMergeSchema &merge_schema,
    const storage::ObStorageColumnGroupSchema *cg_schema,
    const uint16_t table_cg_idx)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!static_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(static_desc));
  } else if (OB_FAIL(static_desc_.assign(static_desc))) {
    STORAGE_LOG(WARN, "failed to assign static desc", KR(ret), K(static_desc));
  } else if (OB_FAIL(inner_init(merge_schema, cg_schema, table_cg_idx))) {
    STORAGE_LOG(WARN, "failed to init", KR(ret), K(merge_schema), K(cg_schema), K(table_cg_idx));
  } else {
    STORAGE_LOG(INFO, "success to init data store desc", KR(ret), K(merge_schema), K(cg_schema), K(table_cg_idx), KPC(this));
  }
  return ret;
}

int ObWholeDataStoreDesc::init(
    const ObMergeSchema &merge_schema,
    const share::ObLSID &ls_id,
    const common::ObTabletID tablet_id,
    const compaction::ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t cluster_version,
    const share::SCN &end_scn,
    const storage::ObStorageColumnGroupSchema *cg_schema,
    const uint16_t table_cg_idx)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(static_desc_.init(merge_schema, ls_id, tablet_id, merge_type, snapshot_version, end_scn, cluster_version))) {
    STORAGE_LOG(WARN, "failed to init static desc", KR(ret));
  } else if (OB_FAIL(inner_init(merge_schema, cg_schema, table_cg_idx))) {
    STORAGE_LOG(WARN, "failed to init", KR(ret), K(merge_schema), K(cg_schema), K(table_cg_idx));
  }
  return ret;
}

int ObWholeDataStoreDesc::inner_init(
    const ObMergeSchema &merge_schema,
    const storage::ObStorageColumnGroupSchema *cg_schema,
    const uint16_t table_cg_idx)
{
  int ret = OB_SUCCESS;
  const bool is_major = compaction::is_major_or_meta_merge_type(static_desc_.merge_type_);
  if (nullptr != cg_schema && !cg_schema->is_all_column_group()) {
    if (OB_FAIL(col_desc_.init(is_major, merge_schema, *cg_schema, table_cg_idx, static_desc_.major_working_cluster_version_))) {
      STORAGE_LOG(WARN, "failed to init data store desc for column grouo", K(ret));
    }
  } else if (OB_FAIL(col_desc_.init(is_major, merge_schema, table_cg_idx, static_desc_.major_working_cluster_version_))) {
    STORAGE_LOG(WARN, "failed to inner init data desc", K(ret));
  }
  if (FAILEDx(desc_.init(static_desc_, col_desc_, merge_schema,
      nullptr == cg_schema ? merge_schema.get_row_store_type() : cg_schema->row_store_type_))) {
    STORAGE_LOG(WARN, "failed to init desc", KR(ret), K_(static_desc));
  }
  return ret;
}

int ObWholeDataStoreDesc::gen_index_store_desc(const ObDataStoreDesc &data_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(data_desc))) { // will check input data_desc in assign
    STORAGE_LOG(WARN, "failed to assign static desc", K(ret), K(data_desc));
  } else {
    col_desc_.col_desc_array_.reset();
    col_desc_.agg_meta_array_.reset();
    desc_.sstable_index_builder_ = nullptr;
    desc_.merge_info_ = nullptr;
    if (!data_desc.is_cg()) {
      col_desc_.row_column_count_ = data_desc.get_rowkey_column_count() + 1;
      if (OB_FAIL(col_desc_.col_desc_array_.init(col_desc_.row_column_count_))) {
        STORAGE_LOG(WARN, "Fail to reserve column desc array", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < data_desc.get_rowkey_column_count(); ++i) {
        if (OB_FAIL(col_desc_.col_desc_array_.push_back(data_desc.get_rowkey_col_descs().at(i)))) {
          STORAGE_LOG(WARN, "Fail to copy rowkey column desc", K(ret), K(i), K(data_desc));
        }
      }
    } else {
      // Attention: index_desc.datum_utils has multi-version columns. Do not reset.
      col_desc_.rowkey_column_count_ = 1; // always 1
      col_desc_.schema_rowkey_col_cnt_ = 0; // 0 indicates is_cg() = true
      col_desc_.row_column_count_ = data_desc.get_row_column_count() + 1;
      if (OB_FAIL(col_desc_.col_desc_array_.init(col_desc_.row_column_count_))) {
        STORAGE_LOG(WARN, "Fail to reserve column desc array", K(ret));
      }
      ObObjMeta meta;
      meta.set_int();
      if (FAILEDx(desc_.col_desc_->add_col_desc(meta, desc_.get_row_column_count()))) {
        STORAGE_LOG(WARN, "Fail to push varchar column for index block", K(ret), K(meta), K(desc_));
      }
    }
  }
  if (FAILEDx(desc_.col_desc_->add_binary_col_desc(desc_.get_row_column_count() + 1))) {
    STORAGE_LOG(WARN, "Fail to push varchar column for index block", K(ret), K(desc_));
  } else if (OB_UNLIKELY(!desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid index store descriptor", K(ret), K(desc_), K(data_desc));
  } else {
    STORAGE_LOG(TRACE, "success to gen index desc", K(ret), K(desc_), K(data_desc));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
