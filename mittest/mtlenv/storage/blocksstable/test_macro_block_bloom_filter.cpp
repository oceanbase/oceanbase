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

#include <errno.h>
#include <gtest/gtest.h>
#include <iostream>
#define protected public
#define private public
#include "lib/oblog/ob_log_module.h"
#include "mittest/mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_macro_block_bloom_filter.h"
#include "storage/blocksstable/ob_macro_block_meta.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace blocksstable
{

// Old data block meta val without bloom filter.
class MockDataBlockMetaVal final
{
public:
  static const int32_t DATA_BLOCK_META_VAL_VERSION = 1;
  static const int32_t DATA_BLOCK_META_VAL_VERSION_V2 = 2;

public:
  MockDataBlockMetaVal()
      : version_(DATA_BLOCK_META_VAL_VERSION_V2), length_(0), data_checksum_(0), rowkey_count_(0), column_count_(0),
        micro_block_count_(0), occupy_size_(0), data_size_(0), data_zsize_(0), original_size_(0),
        progressive_merge_round_(0), block_offset_(0), block_size_(0), row_count_(0), row_count_delta_(0),
        max_merged_trans_version_(0), is_encrypted_(false), is_deleted_(false), contain_uncommitted_row_(false),
        is_last_row_last_flag_(false), compressor_type_(ObCompressorType::INVALID_COMPRESSOR), master_key_id_(0),
        encrypt_id_(0), row_store_type_(ObRowStoreType::MAX_ROW_STORE), schema_version_(0), snapshot_version_(0),
        logic_id_(), macro_id_(), column_checksums_(sizeof(int64_t), ModulePageAllocator("MacroMetaChksum", MTL_ID())),
        has_string_out_row_(false), all_lob_in_row_(false), agg_row_len_(0), agg_row_buf_(nullptr),
        ddl_end_row_offset_(-1)
  {
    MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  }
  ~MockDataBlockMetaVal() {}
  bool is_valid() const { return true; }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
    } else {
      int64_t start_pos = pos;
      const_cast<MockDataBlockMetaVal *>(this)->length_ = get_serialize_size();
      if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
        LOG_WARN("fail to encode version", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, length_))) {
        LOG_WARN("fail to encode length", K(ret), K(buf_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect buf_len", K(ret), K(buf_len), K(pos));
      } else {
        MEMCPY(buf + pos, encrypt_key_, sizeof(encrypt_key_)); // do not serialize char[]
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_ENCODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_,
                    agg_row_len_);
        if (OB_SUCC(ret)) {
          MEMCPY(buf + pos, agg_row_buf_, agg_row_len_);
          pos += agg_row_len_;
          if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
            LST_DO_CODE(OB_UNIS_ENCODE, ddl_end_row_offset_);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, serialize may have bug", K(ret), K(pos), K(start_pos), KPC(this));
          }
        }
      }
    }
    return ret;
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
    } else {
      int64_t start_pos = pos;
      if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
        LOG_WARN("fail to decode version", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(version_ != DATA_BLOCK_META_VAL_VERSION && version_ != DATA_BLOCK_META_VAL_VERSION_V2)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("object version mismatch", K(ret), K(version_));
      } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
        LOG_WARN("fail to decode length", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect data_len", K(ret), K(data_len), K(pos));
      } else {
        MEMCPY(encrypt_key_, buf + pos, sizeof(encrypt_key_));
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_DECODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_,
                    agg_row_len_);
        if (OB_SUCC(ret)) {
          if (agg_row_len_ > 0) {
            agg_row_buf_ = buf + pos;
            pos += agg_row_len_;
          }
          if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
            LST_DO_CODE(OB_UNIS_DECODE, ddl_end_row_offset_);
          } else {
            ddl_end_row_offset_ = -1;
          }
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, deserialize may has bug", K(ret), K(pos), K(start_pos), KPC(this));
          }
        }
      }
    }
    return ret;
  }
  int64_t get_serialize_size() const
  {
    int64_t len = 0;
    len += serialization::encoded_length_i32(version_);
    len += serialization::encoded_length_i32(length_);
    len += sizeof(encrypt_key_);
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                data_checksum_,
                rowkey_count_,
                column_count_,
                micro_block_count_,
                occupy_size_,
                data_size_,
                data_zsize_,
                progressive_merge_round_,
                block_offset_,
                block_size_,
                row_count_,
                row_count_delta_,
                max_merged_trans_version_,
                is_encrypted_,
                is_deleted_,
                contain_uncommitted_row_,
                compressor_type_,
                master_key_id_,
                encrypt_id_,
                row_store_type_,
                schema_version_,
                snapshot_version_,
                logic_id_,
                macro_id_,
                column_checksums_,
                original_size_,
                has_string_out_row_,
                all_lob_in_row_,
                is_last_row_last_flag_,
                agg_row_len_);
    len += agg_row_len_;
    if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, ddl_end_row_offset_);
    }
    return len;
  }
  TO_STRING_KV(K_(version),
               K_(length),
               K_(data_checksum),
               K_(rowkey_count),
               K_(column_count),
               K_(micro_block_count),
               K_(occupy_size),
               K_(data_size),
               K_(data_zsize),
               K_(original_size),
               K_(progressive_merge_round),
               K_(block_offset),
               K_(block_size),
               K_(row_count),
               K_(row_count_delta),
               K_(max_merged_trans_version),
               K_(is_encrypted),
               K_(is_deleted),
               K_(contain_uncommitted_row),
               K_(compressor_type),
               K_(master_key_id),
               K_(encrypt_id),
               K_(encrypt_key),
               K_(row_store_type),
               K_(schema_version),
               K_(snapshot_version),
               K_(is_last_row_last_flag),
               K_(logic_id),
               K_(macro_id),
               K_(column_checksums),
               K_(has_string_out_row),
               K_(all_lob_in_row),
               K_(agg_row_len),
               KP_(agg_row_buf),
               K_(ddl_end_row_offset));

private:
  DISALLOW_COPY_AND_ASSIGN(MockDataBlockMetaVal);

public:
  int32_t version_;
  int32_t length_;
  int64_t data_checksum_;
  int64_t rowkey_count_;
  int64_t column_count_;
  int64_t micro_block_count_;
  int64_t occupy_size_;
  int64_t data_size_;
  int64_t data_zsize_;
  int64_t original_size_;
  int64_t progressive_merge_round_;
  int64_t block_offset_;
  int64_t block_size_;
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  bool is_encrypted_;
  bool is_deleted_;
  bool contain_uncommitted_row_;
  bool is_last_row_last_flag_;
  ObCompressorType compressor_type_;
  int64_t master_key_id_;
  int64_t encrypt_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  ObRowStoreType row_store_type_;
  uint64_t schema_version_;
  int64_t snapshot_version_;
  ObLogicMacroBlockId logic_id_;
  MacroBlockId macro_id_;
  common::ObSEArray<int64_t, 4> column_checksums_;
  bool has_string_out_row_;
  bool all_lob_in_row_;
  int64_t agg_row_len_;
  const char *agg_row_buf_;
  int64_t ddl_end_row_offset_;
};

void set_macro_id_to_valid(MacroBlockId & macro_id)
{
  macro_id.id_mode_ = (uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL;
  macro_id.version_ = MacroBlockId::MACRO_BLOCK_ID_VERSION_V2;
  macro_id.second_id_ = 1;
}

class TestMacroBlockBloomFilter : public TestIndexBlockDataPrepare
{
public:
  TestMacroBlockBloomFilter();
  virtual ~TestMacroBlockBloomFilter() {}
  void SetUp();
  void TearDown();
  void prepare_data_store_desc(ObWholeDataStoreDesc &data_desc, ObSSTableIndexBuilder *sstable_index_builder);
};

TestMacroBlockBloomFilter::TestMacroBlockBloomFilter()
    : TestIndexBlockDataPrepare("test_macro_block_bloom_filter",
                                MAJOR_MERGE,
                                OB_DEFAULT_MACRO_BLOCK_SIZE,
                                10000,
                                65535 * 4)
{
}

void TestMacroBlockBloomFilter::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestMacroBlockBloomFilter::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

void TestMacroBlockBloomFilter::prepare_data_store_desc(
    ObWholeDataStoreDesc &data_desc, ObSSTableIndexBuilder *sstable_index_builder)
{
  int ret = OB_SUCCESS;
  ret = data_desc.init(false/*is_ddl*/, table_schema_, ObLSID(ls_id_), ObTabletID(tablet_id_), MAJOR_MERGE,
                       ObTimeUtility::fast_current_time() /*snapshot_version*/,
                       DATA_CURRENT_VERSION,
                       table_schema_.get_micro_index_clustered(),
                       0 /*transfer_seq*/);
  data_desc.get_desc().sstable_index_builder_ = sstable_index_builder;
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMacroBlockBloomFilter, test_serialize_0)
{
  // prepare data store desc and macro block writer
  ObWholeDataStoreDesc data_desc;
  prepare_data_store_desc(data_desc, root_index_builder_);
  data_desc.desc_.static_desc_->enable_macro_block_bloom_filter_ = true;

  ObMacroBlockBloomFilter mb_bf;
  ASSERT_EQ(OB_SUCCESS, mb_bf.alloc_bf(data_desc.desc_));

  mb_bf.row_count_ = 0;
  int64_t val = mb_bf.get_serialize_size();
  ASSERT_EQ(val, 0);
  mb_bf.row_count_ = 1;
  val = mb_bf.get_serialize_size();
  ASSERT_NE(val, 0);
  // TODO(baichangmin): 补齐 false positive prob 不达标以后的 serialize size
}

TEST_F(TestMacroBlockBloomFilter, test_serialize_1)
{
  // prepare data store desc and macro block writer
  ObWholeDataStoreDesc data_desc;
  prepare_data_store_desc(data_desc, root_index_builder_);
  data_desc.desc_.static_desc_->enable_macro_block_bloom_filter_ = true;

  ObMacroBlockBloomFilter mb_bf;
  int64_t max_row_count = mb_bf.calc_max_row_count();
  ASSERT_EQ(OB_SUCCESS, mb_bf.alloc_bf(data_desc.desc_));
  int64_t nbits = mb_bf.bf_.nbit_;
  ASSERT_EQ(64 * 1024, mb_bf.bf_.calc_nbyte(nbits));

  char * buf = new char[2 * 1024 * 1024]; // 2MB buffer
  mb_bf.row_count_ = 1;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, mb_bf.serialize(buf, 2 * 1024 * 1024, pos));
}

TEST_F(TestMacroBlockBloomFilter, test_compat)
{
  const int64_t buf_len = 2 * 1024 * 1024; // 2MB
  char * buf = new char[buf_len];

  MockDataBlockMetaVal mock_macro_meta_val;
  ASSERT_EQ(mock_macro_meta_val.version_, int64_t(MockDataBlockMetaVal::DATA_BLOCK_META_VAL_VERSION_V2));
  ASSERT_EQ(-1, mock_macro_meta_val.ddl_end_row_offset_);
  mock_macro_meta_val.ddl_end_row_offset_ = 10;
  set_macro_id_to_valid(mock_macro_meta_val.macro_id_);
  int64_t mock_pos = 0;
  ASSERT_EQ(OB_SUCCESS, mock_macro_meta_val.serialize(buf, buf_len, mock_pos));
  ASSERT_EQ(mock_pos, mock_macro_meta_val.get_serialize_size());

  ObDataBlockMetaVal macro_meta_val;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_meta_val.deserialize(buf, mock_pos, pos));
  ASSERT_EQ(pos, mock_pos);
  ASSERT_EQ(0, macro_meta_val.macro_block_bf_size_);
  ASSERT_EQ(10, macro_meta_val.ddl_end_row_offset_);
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_macro_block_bloom_filter.log*");
  OB_LOGGER.set_file_name("test_macro_block_bloom_filter.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_macro_block_bloom_filter");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}