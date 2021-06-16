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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_table_dml_param.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

class ObOldTableSchemaParam {
  typedef common::ObFixedArray<common::ObRowkeyInfo*, common::ObIAllocator> RowKeys;
  typedef common::ObFixedArray<ObColumnParam*, common::ObIAllocator> Columns;
  OB_UNIS_VERSION_V(1);

public:
  explicit ObOldTableSchemaParam(common::ObIAllocator& allocator);
  virtual ~ObOldTableSchemaParam();
  void reset();
  common::ObIAllocator& allocator_;
  uint64_t table_id_;
  int64_t schema_version_;
  ObTableType table_type_;
  ObIndexType index_type_;
  ObIndexStatus index_status_;
  int64_t rowkey_column_num_;
  int64_t shadow_rowkey_column_num_;
  uint64_t fulltext_col_id_;
  common::ObString index_name_;
  Columns columns_;
  ColumnMap col_map_;
  DECLARE_TO_STRING;
};

ObOldTableSchemaParam::ObOldTableSchemaParam(ObIAllocator& allocator)
    : allocator_(allocator),
      table_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      table_type_(MAX_TABLE_TYPE),
      index_type_(INDEX_TYPE_MAX),
      index_status_(INDEX_STATUS_MAX),
      rowkey_column_num_(0),
      shadow_rowkey_column_num_(0),
      fulltext_col_id_(OB_INVALID_ID),
      index_name_(),
      columns_(allocator),
      col_map_(allocator)
{}

ObOldTableSchemaParam::~ObOldTableSchemaParam()
{
  reset();
}

void ObOldTableSchemaParam::reset()
{
  table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  table_type_ = MAX_TABLE_TYPE;
  index_type_ = INDEX_TYPE_MAX;
  index_status_ = INDEX_STATUS_MAX;
  rowkey_column_num_ = 0;
  shadow_rowkey_column_num_ = 0;
  fulltext_col_id_ = OB_INVALID_ID;
  index_name_.reset();
  columns_.reset();
  col_map_.clear();
}

int64_t ObOldTableSchemaParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(table_id),
      K_(schema_version),
      K_(table_type),
      K_(index_type),
      K_(index_status),
      K_(rowkey_column_num),
      K_(shadow_rowkey_column_num),
      K_(fulltext_col_id),
      K_(index_name),
      "columns",
      ObArrayWrap<ObColumnParam*>(0 == columns_.count() ? NULL : &columns_.at(0), columns_.count()));
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObOldTableSchemaParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      table_id_,
      schema_version_,
      table_type_,
      index_type_,
      index_status_,
      rowkey_column_num_,
      shadow_rowkey_column_num_,
      fulltext_col_id_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_name_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize index name", K(ret));
    } else if (OB_FAIL(ObTableParam::serialize_columns(columns_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize columns", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObOldTableSchemaParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      table_id_,
      schema_version_,
      table_type_,
      index_type_,
      index_status_,
      rowkey_column_num_,
      shadow_rowkey_column_num_,
      fulltext_col_id_);

  if (OB_SUCC(ret)) {
    ObString tmp_name;
    if (OB_FAIL(tmp_name.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize index name", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tmp_name, index_name_))) {
      LOG_WARN("failed to copy index name", K(ret), K(tmp_name));
    } else if (OB_FAIL(ObTableParam::deserialize_columns(buf, data_len, pos, columns_, allocator_))) {
      LOG_WARN("failed to deserialize columns", K(ret));
    } else if (OB_FAIL(ObTableParam::create_column_map(columns_, col_map_))) {
      LOG_WARN("failed to create column map", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOldTableSchemaParam)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      table_id_,
      schema_version_,
      table_type_,
      index_type_,
      index_status_,
      rowkey_column_num_,
      shadow_rowkey_column_num_,
      fulltext_col_id_);
  len += index_name_.get_serialize_size();

  if (OB_SUCC(ret)) {
    int64_t columns_size = 0;
    if (OB_FAIL(ObTableParam::get_columns_serialize_size(columns_, columns_size))) {
      LOG_WARN("failed to get columns serialize size", K(ret));
    } else {
      len += columns_size;
    }
  }
  return len;
}

TEST(ObTableSchemaParam, test_serialize)
{
  ObArenaAllocator allocator1;
  ObArenaAllocator allocator2;
  ObOldTableSchemaParam old_param1(allocator1);
  ObTableSchemaParam new_param1(allocator1);
  ObTableSchemaParam new_param2(allocator2);

  old_param1.table_id_ = 1;
  old_param1.index_status_ = INDEX_STATUS_AVAILABLE;
  ObArenaAllocator allocator;
  int64_t buf_size = 1024;
  char* buf = (char*)allocator.alloc(buf_size);
  int64_t pos = 0;

  LOG_INFO("dump param", K(old_param1));
  ASSERT_EQ(OB_SUCCESS, old_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, old_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(old_param1.table_id_, new_param1.table_id_);
  ASSERT_EQ(old_param1.table_id_, new_param2.table_id_);
  ASSERT_EQ(false, new_param1.is_dropped_schema_);
  ASSERT_EQ(false, new_param2.is_dropped_schema_);

  new_param1.is_dropped_schema_ = true;
  new_param1.table_id_ = 2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(new_param1.table_id_, old_param1.table_id_);
  ASSERT_EQ(OB_SUCCESS, old_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(new_param2.table_id_, old_param1.table_id_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(true, new_param2.is_dropped_schema_);
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(false, new_param2.is_dropped_schema_);
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
