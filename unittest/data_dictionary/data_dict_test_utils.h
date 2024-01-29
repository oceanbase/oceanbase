/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * This file defines test_ob_cdc_part_trans_resolver.cpp
 */

#include "gtest/gtest.h"

#define private public
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "logservice/data_dictionary/ob_data_dict_struct.h"
#include "logservice/data_dictionary/ob_data_dict_meta_info.h"
#undef private

#include "lib/random/ob_random.h"
#include "lib/random/ob_mysql_random.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;

namespace oceanbase
{
namespace datadict
{

class ObMockSchemaBuilder
{
  friend class ObTableSchema;
  friend class ObColumnSchemaV2;
public:
  ObMockSchemaBuilder() : allocator_(), random_(), my_random_()
  {
    reset();
    my_random_.init(random_.get(), random_.get());
  }
  ~ObMockSchemaBuilder() { reset(); }
  void reset() { allocator_.reset(); }
public:
  int build_tenant_schema(ObTenantSchema &tenant_schema)
  {
    int ret = OB_SUCCESS;
    tenant_schema.tenant_id_ = get_random_uint64_();
    tenant_schema.schema_version_ = get_random_int64_();
    EXPECT_EQ(OB_SUCCESS, get_random_str_(tenant_schema.tenant_name_));
    tenant_schema.charset_type_ = ObCharsetType::CHARSET_GB18030;
    tenant_schema.collation_type_ = ObCollationType::CS_TYPE_GB18030_ZH2_0900_AS_CS;
    tenant_schema.drop_tenant_time_ = get_random_int64_();
    tenant_schema.compatibility_mode_ = ObCompatibilityMode::MYSQL_MODE;
    tenant_schema.status_ = ObTenantStatus::TENANT_STATUS_RESTORE;
    return ret;
  }
  int build_database_schema(ObDatabaseSchema &database_schema)
  {
    int ret = OB_SUCCESS;
    database_schema.tenant_id_ = get_random_uint64_();
    database_schema.database_id_ = get_random_uint64_();
    database_schema.schema_version_ = get_random_int64_();
    EXPECT_EQ(OB_SUCCESS, get_random_str_(database_schema.database_name_));
    return ret;
  }
  int build_table_schema(ObTableSchema &table_schema)
  {
    int ret = OB_SUCCESS;
    int column_cnt = 500;
    table_schema.set_tenant_id(get_random_uint64_());
    table_schema.set_database_id(get_random_uint64_());
    table_schema.set_table_id(get_random_uint64_());
    table_schema.set_schema_version(4367324905);
    ObString tb_name_str;
    EXPECT_EQ(OB_SUCCESS, get_random_str_(tb_name_str));
    table_schema.set_table_name(tb_name_str);
    table_schema.set_rowkey_column_num(1);
    table_schema.set_table_type(ObTableType::USER_TABLE);
    for (int i = 0; i < column_cnt; i++) {
      ObColumnSchemaV2 *column_schema = static_cast<ObColumnSchemaV2*>(allocator_.alloc(sizeof(ObColumnSchemaV2)));
      new(column_schema) ObColumnSchemaV2();
      EXPECT_EQ(OB_SUCCESS, build_column_schema(*column_schema));
      EXPECT_EQ(OB_SUCCESS, table_schema.add_column(*column_schema));
    }
    return ret;
  }
  int build_column_schema(ObColumnSchemaV2 &column_schema)
  {
    int ret = OB_SUCCESS;
    column_schema.tenant_id_ = get_random_uint64_();
    column_schema.table_id_ = get_random_uint64_();
    column_schema.is_nullable_ = false;
    column_schema.is_autoincrement_ = true;
    column_schema.schema_version_ = 2341235;
    ObObjMeta meta;
    meta.set_collation_type(ObCollationType::CS_TYPE_GB18030_BIN);
    common::ObString ext_info("enum {a, b, c}");
    ObArray<common::ObString> extend_info;
    extend_info.push_back(ext_info);
    column_schema.set_extended_type_info(extend_info);
    column_schema.meta_type_ = meta;
    return ret;
  }
private:
  uint64_t get_random_uint64_()
  {
    return my_random_.get_uint64();
  }
  int64_t get_random_int64_()
  {
    return random_.get();
  }
  int32_t get_random_int32_()
  {
    return random_.get_int32();
  }
  int get_random_str_(ObString &string, const int64_t length = 10)
  {
    int ret = OB_SUCCESS;
    char *buf = NULL;
    EXPECT_EQ(OB_SUCCESS, get_random_char_str_(length, buf));
    string.assign_ptr(buf, length);
    return ret;
  }
  int get_random_char_str_(const int64_t length, char *&buf)
  {
    int ret = OB_SUCCESS;
    buf = static_cast<char*>(allocator_.alloc(length));
    ret = my_random_.create_random_string(buf, length);
    return ret;
  }
private:
  ObArenaAllocator allocator_;
  ObRandom random_;
  ObMysqlRandom my_random_;
};

class DictTableMetaBuilder
{
public:
  DictTableMetaBuilder() : allocator_(), random_(), my_random_()
  {
    reset();
    my_random_.init(random_.get(), random_.get());
  }
  ~DictTableMetaBuilder() { reset(); }
  void reset() { allocator_.reset(); }
public:
  int build_rowkey_column(ObRowkeyColumn *rowkey_col)
  {
    int ret = OB_SUCCESS;
    ObObjMeta meta_type;
    rowkey_col->length_ = random_.get();
    rowkey_col->column_id_ = my_random_.get_uint64();
    rowkey_col->type_ = meta_type;
    return ret;
  }

  int build_column_meta(ObDictColumnMeta *col_meta)
  {
    int ret = OB_SUCCESS;
    EXPECT_EQ(OB_SUCCESS, get_random_str_(col_meta->column_name_, 30)); // 1KB
    ObObjMeta meta_type;
    ObAccuracy accuracy;
    accuracy.set_accuracy(random_.get());
    const ObObj orig_default_value;

    col_meta->column_id_ = common::OB_ALL_MAX_COLUMN_ID;
    col_meta->rowkey_position_ = 0;
    col_meta->is_nullable_ = 0;
    col_meta->is_autoincrement_ = true;
    col_meta->is_part_key_col_ = 1;
    col_meta->index_position_ = 0;
    col_meta->meta_type_ = meta_type;
    col_meta->accuracy_ = accuracy;
    col_meta->column_flags_ = random_.get_int32();
    col_meta->charset_type_ = ObCharsetType::CHARSET_ANY;
    col_meta->orig_default_value_ = orig_default_value;
    col_meta->cur_default_value_ = orig_default_value;
    ObString ext_info1;
    EXPECT_EQ(OB_SUCCESS, get_random_str_(ext_info1, 100)); // 100byte
    ObString ext_info2;
    EXPECT_EQ(OB_SUCCESS, get_random_str_(ext_info2, 100)); // 100byte

    //col_meta->extended_type_info_.push_back(ext_info1);
    //col_meta->extended_type_info_.push_back(ext_info2);

    return ret;
  }

  int build_table_meta(
      ObDictTableMeta *&tb_meta,
      const int64_t rowkey_count,
      const int64_t col_count,
      const int64_t index_count)
  {
    int ret = OB_SUCCESS;

    const static int64_t tb_meta_size = sizeof(ObDictTableMeta);
    tb_meta = static_cast<ObDictTableMeta*>(allocator_.alloc(tb_meta_size));
    new (tb_meta) ObDictTableMeta(&allocator_);
    EXPECT_NE(nullptr, tb_meta);
    EXPECT_EQ(OB_SUCCESS, get_random_str_(tb_meta->table_name_, 30));
    tb_meta->tenant_id_ = my_random_.get_uint64();
    tb_meta->database_id_ = my_random_.get_uint64();
    tb_meta->table_id_ = my_random_.get_uint64();
    tb_meta->schema_version_ = random_.get();
    tb_meta->name_case_mode_ = ObNameCaseMode::OB_ORIGIN_AND_SENSITIVE;
    tb_meta->table_type_ = ObTableType::USER_TABLE;
    int64_t tablet_count = 10;
    for (int i = 0; i < tablet_count; i++) {
      ObTabletID tablet_id(50000 + i);
      EXPECT_EQ(OB_SUCCESS, tb_meta->tablet_id_arr_.push_back(tablet_id));
    }
    tb_meta->max_used_column_id_ = my_random_.get_uint64();
    tb_meta->unique_index_tid_arr_.push_back(1);
    tb_meta->charset_type_ = ObCharsetType::CHARSET_GB18030;
    tb_meta->collation_type_ = ObCollationType::CS_TYPE_GB18030_CHINESE_CI;
    tb_meta->index_type_ = ObIndexType::INDEX_TYPE_UNIQUE_GLOBAL;
    tb_meta->data_table_id_ = my_random_.get_uint64();
    tb_meta->aux_lob_meta_tid_ = my_random_.get_uint64();
    tb_meta->aux_lob_piece_tid_ = my_random_.get_uint64();
    tb_meta->column_count_ = col_count;
    tb_meta->rowkey_column_count_ = rowkey_count;
    tb_meta->index_column_count_ = index_count;

    const static int64_t col_meta_size = sizeof(ObDictColumnMeta);
    int64_t col_arr_size = col_count * col_meta_size;
    ObDictColumnMeta *col_meta = static_cast<ObDictColumnMeta*>(allocator_.alloc(col_arr_size));
    if (col_count > 0) EXPECT_NE(nullptr, col_meta);

    const static int64_t rowkey_col_size = sizeof(ObRowkeyColumn);
    int64_t rowkey_arr_size = rowkey_count * rowkey_col_size;
    ObRowkeyColumn *rowkey_cols = static_cast<ObRowkeyColumn*>(allocator_.alloc(rowkey_arr_size));
    if (rowkey_count > 0) EXPECT_NE(nullptr, rowkey_cols);

    const static int64_t index_col_size = sizeof(ObIndexColumn);
    int64_t index_arr_size = index_count * index_col_size;
    ObIndexColumn *index_cols = static_cast<ObIndexColumn*>(allocator_.alloc(index_arr_size));
    if (index_count > 0) EXPECT_NE(nullptr, index_cols);

    tb_meta->col_metas_ = col_meta;
    tb_meta->rowkey_cols_ = rowkey_cols;
    tb_meta->index_cols_ = index_cols;

    for (int i = 0; i < col_count; i++) {
      new(col_meta + i) ObDictColumnMeta(&allocator_);
      EXPECT_EQ(OB_SUCCESS, build_column_meta(&col_meta[i]));
      EXPECT_EQ(OB_SUCCESS, tb_meta->column_id_arr_order_by_table_def_.push_back(my_random_.get_uint64()));
    }

    for (int i = 0; i < rowkey_count; i++) {
      new(rowkey_cols + i) ObRowkeyColumn();
      EXPECT_EQ(OB_SUCCESS, build_rowkey_column(&rowkey_cols[i]));
    }

    for (int i = 0; i < index_count; i++) {
      new(index_cols + i) ObIndexColumn();
      EXPECT_EQ(OB_SUCCESS, build_rowkey_column(&index_cols[i]));
      EXPECT_EQ(OB_SUCCESS, tb_meta->unique_index_tid_arr_.push_back(my_random_.get_uint64()));
    }
    return ret;
  }
private:
  int get_random_str_(ObString &string, const int64_t length = 10)
  {
    int ret = OB_SUCCESS;
    char *buf = NULL;
    EXPECT_EQ(OB_SUCCESS, get_random_char_str_(length, buf));
    string.assign_ptr(buf, length);
    return ret;
  }
  int get_random_char_str_(const int64_t length, char *&buf)
  {
    int ret = OB_SUCCESS;
    buf = static_cast<char*>(allocator_.alloc(length));
    ret = my_random_.create_random_string(buf, length);
    return ret;
  }
private:
  ObArenaAllocator allocator_;
  ObRandom random_;
  ObMysqlRandom my_random_;
};

int64_t get_timestamp() { return ::oceanbase::common::ObTimeUtility::current_time(); }
}
}
