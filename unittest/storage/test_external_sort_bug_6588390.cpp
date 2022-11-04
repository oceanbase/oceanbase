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

#include <gtest/gtest.h>
#include "lib/sort/ob_external_sort.h"
#include "lib/sort/ob_async_external_sorter.h"
#include "lib/random/ob_random.h"
#include "../../unittest/storage/blocksstable/ob_row_generate.h"
#include "storage/ob_store_row_comparer.h"

namespace oceanbase {

using namespace blocksstable;
using namespace blocksstable;
using namespace storage;
using namespace share;
using namespace share::schema;

namespace unittest {

class TestExternalSort: public ::testing::Test {
public:
	static const int64_t rowkey_count = 2;
public:
	TestExternalSort();
	virtual void SetUp();
protected:
	ObArenaAllocator allocator_;
	ObTableSchema table_schema_;
	ObRowGenerate row_generate_;
};

TestExternalSort::TestExternalSort() :
		allocator_(ObModIds::TEST) {

}

void TestExternalSort::SetUp() {
	ObColumnSchemaV2 column;
	//init table schema
	table_schema_.reset();
	ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_extrenal_sort"));
	table_schema_.set_tenant_id(1);
	table_schema_.set_tablegroup_id(1);
	table_schema_.set_database_id(1);
	table_schema_.set_table_id(3001);
	table_schema_.set_rowkey_column_num(2);
	table_schema_.set_max_used_column_id(10);
	table_schema_.set_block_size(16 * 1024);
	table_schema_.set_compress_func_name("none");
	//init column
	char name[OB_MAX_FILE_NAME_LENGTH];
	memset(name, 0, sizeof(name));
	for (int64_t i = 0; i < 5; ++i) {
		column.reset();
		column.set_table_id(3001);
		column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
		sprintf(name, "test%020ld", i);
		ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
		if (0 == i) {
			column.set_data_type(common::ObTimestampType);
			column.set_rowkey_position(1);
		} else if (1 == i) {
			column.set_data_type(common::ObTimestampType);
			column.set_rowkey_position(2);
		} else if (2 == i) {
			column.set_data_type(common::ObIntType);
			column.set_rowkey_position(0);
		} else if (3 == i) {
			column.set_data_type(common::ObIntType);
			column.set_rowkey_position(0);
		} else if (4 == i) {
			column.set_data_type(common::ObVarcharType);
			column.set_rowkey_position(0);
		}
		ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
	}
	//init ObRowGenerate
	ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_, &allocator_));
}

TEST_F(TestExternalSort, normal) {
	int ret = OB_SUCCESS;
	ObExternalSort<ObStoreRow, ObStoreRowComparer> external_sort;
	int sort_ret = OB_SUCCESS;
	ObArray<int64_t> sort_column_indexes;
    ObStoreRowComparer compare(sort_ret, sort_column_indexes);
	const int64_t schema_count = 10;

	for (int64_t i = 0; i < schema_count; ++i) {
		for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_count; ++j) {
			if (OB_SUCCESS != (ret = sort_column_indexes.push_back(j))) {
				STORAGE_LOG(WARN, "Fail to push sort column indexes, ", K(j));
			}
		}

		ret = external_sort.init("test", 1024L, &compare);
		EXPECT_EQ(OB_SUCCESS, ret);

		ObStoreRow row;
		ObObj objs[5];
		row.row_val_.cells_ = objs;
		row.row_val_.count_ = 5;

		for (int64_t i = 0; i < 3000L; ++i) {
			ASSERT_EQ(0, row_generate_.get_next_row(row));
			ret = external_sort.add_item(&row);
			EXPECT_EQ(OB_SUCCESS, ret);
		}

		ret = external_sort.do_sort();
		EXPECT_EQ(OB_SUCCESS, ret);

		const ObStoreRow *get_row = NULL;

		ret = external_sort.get_next_item(get_row);
		EXPECT_EQ(OB_SUCCESS, ret);

		while (OB_SUCCESS == (ret = external_sort.get_next_item(get_row))) {

		}

		EXPECT_EQ(OB_ITER_END, ret);
		external_sort.destroy();
	}

}
//add bug case to async_external_sort
TEST_F(TestExternalSort, test_async) {
	int ret = OB_SUCCESS;
	ObAsyncExternalSorter<ObStoreRow, ObStoreRowComparer> external_sort;
  external_sort.set_skip_del_file_after_error(false);
  int sort_ret = OB_SUCCESS;
    ObArray<int64_t> sort_column_indexes;
    ObStoreRowComparer compare(sort_ret, sort_column_indexes);
	const int64_t schema_count = 10;

	for (int64_t i = 0; i < schema_count; ++i) {
		for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_count; ++j) {
			if (OB_SUCCESS != (ret = sort_column_indexes.push_back(j))) {
				STORAGE_LOG(WARN, "Fail to push sort column indexes, ", K(j));
			}
		}

		const int64_t fragment_mem_limit = 16L * 1024L * 1024L;
		const int64_t file_buf_size = 2 * 1024 * 1024;
		const int64_t expire_timestamp = 0;
		const int64_t buf_length = 256 * 1204;
		char buf[buf_length];
		int64_t buf_pos = 0;
		int64_t buf_len = 0;

		ret = external_sort.init("test_async", fragment_mem_limit,
				file_buf_size, expire_timestamp, &compare);
		EXPECT_EQ(OB_SUCCESS, ret);

		ObStoreRow row;
		ObObj objs[5];
		row.row_val_.cells_ = objs;
		row.row_val_.count_ = 5;

		ASSERT_EQ(0, row_generate_.get_next_row(row));
		STORAGE_LOG(INFO, "", K(row));
		for (int64_t i = 0; i < 300000L; ++i) {
			ASSERT_EQ(0, row_generate_.get_next_row(row));
			buf_pos = 0;
			ASSERT_EQ(OB_SUCCESS, row.serialize(buf, buf_length, buf_pos));
			buf_len = buf_pos;
			buf_pos = 0;
			ASSERT_EQ(OB_SUCCESS, row.deserialize(buf, buf_len, buf_pos));
			if (i == 0) {
				STORAGE_LOG(INFO, "dump", K(buf_pos), K(buf_len), K(row));
			}

			ret = external_sort.add_item(row);
			ASSERT_EQ(OB_SUCCESS, ret);
		}

		ret = external_sort.do_sort();
		ASSERT_EQ(OB_SUCCESS, ret);

		const ObStoreRow *get_row = NULL;

		ret = external_sort.get_next_item(get_row);
		EXPECT_EQ(OB_SUCCESS, ret);

		while (OB_SUCC(ret)) {
			ret = external_sort.get_next_item(get_row);
		}

		EXPECT_EQ(OB_ITER_END, ret);
		external_sort.cleanup();
	}

}
}
}

int main(int argc, char **argv) {
    oceanbase::lib::set_memory_limit(8L * 1024L * 1024L * 1024L);
	oceanbase::common::ObIOManager::get_instance().init(1024L * 1024L * 1024L);
	oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
