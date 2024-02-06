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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
//#include "lib/utility/ob_test_util.h"
//#include "sql/engine/test_engine_util.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

static char *file_path = NULL;

using namespace oceanbase::sql;
//using namespace oceanbase::sql::test;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;


struct FileMeta {
  const char *file_name;
  int64_t column_num;
};

class TestParser : public ::testing::Test
{
public:
  enum tpch_table_names {
    TPCH_LINE_ITEM = 0,
    TPCH_TABLE_CNT
  };
  TestParser();
  ~TestParser();
  virtual void SetUp();
  virtual void TearDown();
  FileMeta tpch_file_metas[TPCH_TABLE_CNT];
};
TestParser::TestParser()
{
}

TestParser::~TestParser()
{
}

void TestParser::SetUp()
{
  tpch_file_metas[TPCH_LINE_ITEM].column_num = 16;
  tpch_file_metas[TPCH_LINE_ITEM].file_name = "lineitem.tbl";

}

void TestParser::TearDown()
{
}
/*
TEST_F(TestParser, csv_parser)
{
  ObCSVFormats format;
  format.field_term_char_ = '|';
  format.line_term_char_ = '\n';
  format.enclose_char_ = INT64_MAX;
  format.escape_char_ = '\\';
  format.null_column_fill_zero_string_ = true;
  format.is_simple_format_ = true;
  format.is_line_term_by_counting_field_ = false;


  void *temp_buf = (ob_malloc(OB_MALLOC_BIG_BLOCK_SIZE));
  ASSERT_TRUE(temp_buf != NULL);
  ObLoadFileBuffer *buffer = new(temp_buf)ObLoadFileBuffer(OB_MALLOC_BIG_BLOCK_SIZE
                                                           - sizeof(ObLoadFileBuffer));


  for (int i = 0; i < TPCH_TABLE_CNT; i++) {
    FileMeta &f_meta = tpch_file_metas[i];
    ObCSVParser parser;
    ObBitSet<> empty_set;
    ASSERT_EQ(OB_SUCCESS, parser.init(f_meta.column_num, format, empty_set));


    std::string file_name;
    if (file_path != NULL) {
      file_name.append(file_path).append("/").append(f_meta.file_name);
    } else {
      file_name = f_meta.file_name;
    }

    ObFileReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.open(file_name.c_str(), false));
    int64_t file_size = get_file_size(file_name.c_str());
    int64_t total_read_bytes = 0;
    int64_t rows = 0;
    int64_t temp_idx = 1;
    fprintf(stdout, "## Start parsing table:%s\n", f_meta.file_name);
    int64_t start_time = ObTimeUtility::current_time();
    for (int64_t read_bytes = INT64_MAX; read_bytes != 0;) {
      //fprintf(stdout, "==> DEBUG parsing %ld, %ld\n", buffer->get_remain_len(), total_read_bytes);
      ASSERT_EQ(OB_SUCCESS, reader.pread(buffer->current_ptr(),
                                         buffer->get_remain_len(),
                                         total_read_bytes,
                                         read_bytes));
      buffer->update_pos(read_bytes);
      parser.reuse();
      parser.next_buf(buffer->begin_ptr(), buffer->get_data_len(), 0 == read_bytes);
      for (bool has_next_line = true; has_next_line;) {
        ASSERT_EQ(OB_SUCCESS, parser.next_line(has_next_line));
        if (has_next_line) {
          rows++;
        }
      }

      int64_t unfinish_len = buffer->get_data_len() - parser.get_complete_lines_len();
      char *unfinish_ptr = buffer->begin_ptr() + parser.get_complete_lines_len();
      MEMMOVE(buffer->begin_ptr(), unfinish_ptr, unfinish_len);
      buffer->reset();
      buffer->update_pos(unfinish_len);
      total_read_bytes += read_bytes;

      if (rows > temp_idx * 100000) {
        int64_t time_dur = ObTimeUtility::current_time() - start_time;
        fprintf(stdout, "==> parsing progress %ld%%\t rows:%ldK\t speed:%ldM/s\n",
                total_read_bytes * 100 /file_size, rows/1000,
                (total_read_bytes >> 20) * USECS_PER_SEC/time_dur);
        temp_idx++;
      }
    }

    fprintf(stdout, "## done parsing table:%s\trows:%ld\n", f_meta.file_name, rows);
  }
}
*/

TEST_F(TestParser, general_parser)
{

  ObDataInFileStruct file_struct;
  file_struct.field_term_str_ = "|";

  void *temp_buf = (ob_malloc(OB_MALLOC_BIG_BLOCK_SIZE, ObNewModIds::TEST));
  ASSERT_TRUE(temp_buf != NULL);
  ObLoadFileBuffer *buffer = new(temp_buf)ObLoadFileBuffer(OB_MALLOC_BIG_BLOCK_SIZE
                                                           - sizeof(ObLoadFileBuffer) - 1024);


  for (int i = 0; i < TPCH_TABLE_CNT; i++) {
    FileMeta &f_meta = tpch_file_metas[i];

    ObCSVGeneralParser parser;
    ASSERT_EQ(OB_SUCCESS,
              parser.init(file_struct, f_meta.column_num, CS_TYPE_UTF8MB4_BIN));


    std::string file_name;
    if (file_path != NULL) {
      file_name.append(file_path).append("/").append(f_meta.file_name);
    } else {
      file_name = f_meta.file_name;
    }

    ObFileReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.open(file_name.c_str(), false));
    int64_t file_size = get_file_size(file_name.c_str());
    int64_t total_read_bytes = 0;
    int64_t rows = 0;
    int64_t temp_idx = 1;
    fprintf(stdout, "## Start parsing table:%s\n", f_meta.file_name);
    int64_t start_time = ObTimeUtility::current_time();
    auto counting_lines = [](ObIArray<ObCSVGeneralParser::FieldValue> &arr) -> int {
      UNUSED(arr);
      return OB_SUCCESS;
    };
    ObSEArray<ObCSVGeneralParser::LineErrRec, 256> error_msgs;
    for (int64_t read_bytes = INT64_MAX; read_bytes != 0;) {
      //fprintf(stdout, "==> DEBUG parsing %ld, %ld\n", buffer->get_remain_len(), total_read_bytes);
      ASSERT_EQ(OB_SUCCESS, reader.pread(buffer->current_ptr(),
                                         buffer->get_remain_len(),
                                         total_read_bytes,
                                         read_bytes));
      buffer->update_pos(read_bytes);
      const int64_t max_read_rows = 1;
      int64_t cur_rows = max_read_rows;
      const char *ptr = buffer->begin_ptr();
      const char *end = buffer->begin_ptr() + buffer->get_data_len();
      while (cur_rows == max_read_rows) {
        ASSERT_EQ(OB_SUCCESS, parser.scan(ptr, end, cur_rows, NULL, NULL, counting_lines, error_msgs, false));
        ASSERT_EQ(0, error_msgs.count());
        ASSERT_EQ(parser.get_fields_per_line().count(), f_meta.column_num);
        rows += cur_rows;
      }

      int64_t unfinish_len = end - ptr;
      const char *unfinish_ptr = ptr;
      MEMMOVE(buffer->begin_ptr(), unfinish_ptr, unfinish_len);
      buffer->reset();
      buffer->update_pos(unfinish_len);
      total_read_bytes += read_bytes;

      if (rows > temp_idx * 100000) {
        int64_t time_dur = ObTimeUtility::current_time() - start_time;
        fprintf(stdout, "==> parsing progress %ld%%\t rows:%ldK\t speed:%ldM/s\n",
                total_read_bytes * 100 /file_size, rows/1000,
                (total_read_bytes >> 20) * USECS_PER_SEC/time_dur);
        temp_idx++;
      }
    }
    fprintf(stdout, "## done parsing table:%s\trows:%ld\n", f_meta.file_name, rows);
  }
}


TEST_F(TestParser, general_parser_escape)
{
  ObDataInFileStruct file_struct;
  file_struct.field_term_str_ = ",";
  file_struct.field_enclosed_str_ = "\"";
  file_struct.field_enclosed_char_ = '"';

  void *temp_buf =  NULL;
  temp_buf = (ob_malloc(OB_MALLOC_BIG_BLOCK_SIZE, ObNewModIds::TEST));
  ASSERT_TRUE(temp_buf != NULL);
  ObLoadFileBuffer *buffer = new(temp_buf)ObLoadFileBuffer(OB_MALLOC_BIG_BLOCK_SIZE
                                                           - sizeof(ObLoadFileBuffer) - 1024);
  temp_buf = (ob_malloc(OB_MALLOC_BIG_BLOCK_SIZE, ObNewModIds::TEST));
  ASSERT_TRUE(temp_buf != NULL);
  ObLoadFileBuffer *escape = new(temp_buf)ObLoadFileBuffer(OB_MALLOC_BIG_BLOCK_SIZE
                                                           - sizeof(ObLoadFileBuffer) - 1024);
  FileMeta f_meta;
  f_meta.column_num = 3;
  f_meta.file_name = "general_parser_escape_test_file";

  ObCSVGeneralParser parser;
  ASSERT_EQ(OB_SUCCESS,
            parser.init(file_struct, f_meta.column_num, CS_TYPE_UTF8MB4_BIN));


  std::string file_name;
  if (file_path != NULL) {
    file_name.append(file_path).append("/").append(f_meta.file_name);
  } else {
    file_name = f_meta.file_name;
  }

  ObFileReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(file_name.c_str(), false));
  int64_t file_size = get_file_size(file_name.c_str());
  int64_t total_read_bytes = 0;
  int64_t rows = 0;
  int64_t temp_idx = 1;
  fprintf(stdout, "## Start parsing table:%s\n", f_meta.file_name);
  int64_t start_time = ObTimeUtility::current_time();
  auto counting_lines = [](ObIArray<ObCSVGeneralParser::FieldValue> &arr) -> int {
    UNUSED(arr);
    LOG_INFO("parse result", K(arr));
    return OB_SUCCESS;
  };
  ObSEArray<ObCSVGeneralParser::LineErrRec, 256> error_msgs;
  for (int64_t read_bytes = INT64_MAX; read_bytes != 0;) {
    //fprintf(stdout, "==> DEBUG parsing %ld, %ld\n", buffer->get_remain_len(), total_read_bytes);
    ASSERT_EQ(OB_SUCCESS, reader.pread(buffer->current_ptr(),
                                       buffer->get_remain_len(),
                                       total_read_bytes,
                                       read_bytes));
    buffer->update_pos(read_bytes);
    const int64_t max_read_rows = 1;
    int64_t cur_rows = max_read_rows;
    const char *ptr = buffer->begin_ptr();
    const char *end = buffer->begin_ptr() + buffer->get_data_len();
    while (cur_rows == max_read_rows) {
      ASSERT_EQ(OB_SUCCESS, (parser.scan<decltype(counting_lines), true>(ptr, end, cur_rows,
                                        escape->begin_ptr(), escape->begin_ptr() + escape->get_buffer_size(),
                                        counting_lines, error_msgs, false)));
      //ASSERT_EQ(0, error_msgs.count());
      ASSERT_EQ(parser.get_fields_per_line().count(), f_meta.column_num);
      rows += cur_rows;
    }

    int64_t unfinish_len = end - ptr;
    const char *unfinish_ptr = ptr;
    MEMMOVE(buffer->begin_ptr(), unfinish_ptr, unfinish_len);
    buffer->reset();
    buffer->update_pos(unfinish_len);
    total_read_bytes += read_bytes;

    if (rows > temp_idx * 100000) {
      int64_t time_dur = ObTimeUtility::current_time() - start_time;
      fprintf(stdout, "==> parsing progress %ld%%\t rows:%ldK\t speed:%ldM/s\n",
              total_read_bytes * 100 /file_size, rows/1000,
              (total_read_bytes >> 20) * USECS_PER_SEC/time_dur);
      temp_idx++;
    }
  }
  fprintf(stdout, "## done parsing table:%s\trows:%ld\n", f_meta.file_name, rows);

}

int main(int argc, char **argv)
{
  init_sql_factories();
  OB_LOGGER.set_log_level("INFO");
  if (argc > 1 && argv[1] != NULL) {
    file_path = argv[1];
  }
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
