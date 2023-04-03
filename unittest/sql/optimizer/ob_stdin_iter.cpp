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

#include "ob_stdin_iter.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "string.h"
#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_info.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share::schema;



char ObStdinIter::rand_char()
{
  char char_max = '~';
  char char_min = ' ';
  char char_ret = (char)((double)(char_max - char_min) * ((double)rand() / (double)RAND_MAX) + char_min);
  return char_ret;
}

ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(NULLT)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  UNUSED(seed);
  ObObj rand_obj;
  rand_obj.set_null();
  return rand_obj;
}

ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(BIGINT)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  ObObj rand_obj;
  int64_t val = 0;
  if (need_random) {
//val = (int64_t)((double)(max_.get_int() - min_.get_int()) * ((double)rand() / (double)RAND_MAX)) + min_.get_int();
    val = rand() % max_.get_int();
  } else {
    val = seed;
  }
  rand_obj.set_int(val);
  return rand_obj;
}

ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(VARCHAR)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  ObObj rand_obj;
  //char *str_buf = static_cast<char *>(buf.alloc(length + 1));
  char *str_buf = my_buf;
  if (NULL == str_buf) {
    _OB_LOG(WARN, "failed alloc memory");
  } else {
    //memset(str_buf, 0, length + 1);
    if (need_random) {
      for (int64_t i = 0; i < length; ++i) {
        if (length > common_prefix_len && i < common_prefix_len) {
          str_buf[i] = 'Z';
        } else {
          str_buf[i] = rand_char();
        }
      }
      str_buf[length] = '\0';
    } else {
      snprintf(str_buf, length + 1, "%0*ld", static_cast<int>(length), seed);
    }
    rand_obj.set_varchar(str_buf);
  }
  return rand_obj;
}

ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(TS)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  ObObj rand_obj;
  int64_t val = 0;
  if (need_random) {
    val = (int64_t)((double)(max_.get_int() - min_.get_int()) * ((double)rand() / (double)RAND_MAX)) + min_.get_int();
  } else {
    val = seed;
  }
  rand_obj.set_timestamp(val);
  return rand_obj;
}


ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(NUMBER)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  ObObj rand_obj;
  int64_t val = 0;
  if (need_random) {
    val = (int64_t)((double)(max_.get_int() - min_.get_int()) * ((double)rand() / (double)RAND_MAX)) + min_.get_int();
  } else {
    val = seed;
  }
  rand_obj.set_int(val);
  ObObj res_obj;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStdinIter::cast_to_type(rand_obj, res_obj, type_, buf, acc_))) {
    OB_LOG(WARN, "failed to cast", K(ret));
  }
  return res_obj;
}

ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(DOUBLE)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  ObObj rand_obj;
  int64_t val = 0;
  if (need_random) {
    val = (int64_t)((double)(max_.get_int() - min_.get_int()) * ((double)rand() / (double)RAND_MAX)) + min_.get_int();
  } else {
    val = seed;
  }
  rand_obj.set_int(val);
  ObObj res_obj;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStdinIter::cast_to_type(rand_obj, res_obj, type_, buf))) {
    OB_LOG(WARN, "failed to cast", K(ret));
  }
  return res_obj;
}

ObObj ObStdinIter::RANDOM_CELL_GEN_CLASS_NAME(FLOAT)::gen(ObIAllocator &buf, int64_t seed)
{
  UNUSED(buf);
  ObObj rand_obj;
  int64_t val = 0;
  if (need_random) {
    val = (int64_t)((double)(max_.get_int() - min_.get_int()) * ((double)rand() / (double)RAND_MAX)) + min_.get_int();
  } else {
    val = seed;
  }
  rand_obj.set_int(val);
  ObObj res_obj;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStdinIter::cast_to_type(rand_obj, res_obj, type_, buf))) {
    OB_LOG(WARN, "failed to cast", K(ret));
  }
  return res_obj;
}


int ObStdinIter::init_type_map()
{
  int ret = OB_SUCCESS;
  if (!map_inited) {
    if (OB_SUCCESS != (ret = str_to_type_map_mysql_.create(10, ObModIds::OB_HASH_BUCKET))) {
      _OB_LOG(WARN, "failed to allocate memory ret = %d", ret);
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("tinyint"), ObTinyIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("smallint"), ObSmallIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("mediumint"), ObMediumIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("int"), ObInt32Type))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("bigint"), ObIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("tinyint unsigned"),
            ObUTinyIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("smallint unsigned"),
            ObUSmallIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("mediumint unsigned"),
            ObUMediumIntType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("int unsigned"), ObUInt32Type))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("bigint unsigned"), ObUInt64Type))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("float"), ObFloatType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("double"), ObDoubleType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("varchar"), ObVarcharType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("char"), ObCharType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("number"), ObNumberType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("decimal"), ObNumberType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("timestamp"), ObTimestampType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    } else if (OB_SUCCESS != (ret =
        str_to_type_map_mysql_.set_refactored(ObString::make_string("decimal unsigned"), ObUNumberType))) {
      _OB_LOG(WARN, "failed to insert type hash");
    }
  }
  if (OB_SUCCESS == ret) {
    map_inited = true;
    ret = OB_SUCCESS;
  } else {
    ret = OB_ERROR;
  }
  return ret;
}

int ObStdinIter::lookup_type_from_str(const char *str,
                                               int64_t len,
                                               ObObjType &type)
{
  int ret = OB_SUCCESS;
  if (false == map_inited) {
    ret = OB_NOT_INIT;
  } else {
    ObString key(0, static_cast<int32_t>(len), str);
    if (OB_HASH_NOT_EXIST == (ret = str_to_type_map_mysql_.get_refactored(key, type))) {
      type = ObMaxType;
      ret = OB_OBJ_TYPE_ERROR;
    } else { }
  }
  return ret;
}

int ObStdinIter::extract_column_info(const string &line, ObIArray<ObObjType> &column_info)
{
  int ret = OB_SUCCESS;
  const static uint64_t MAX_LINE_LENGTH = 1024;
  char line_buf[MAX_LINE_LENGTH + 1] = {0};
  char *saved_strtok_pos = NULL;
  char *token_start = NULL;
  if (line.length() > MAX_LINE_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "line too long");
  } else {
    memcpy(line_buf, line.c_str(), line.length());
    if (NULL == (token_start = strtok_r(line_buf, "|", &saved_strtok_pos))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      bool line_valid = true;
      bool line_finish = false;
      ObObjType column_type = ObMaxType;
      do {
        if (OB_FAIL(lookup_type_from_str(token_start, strlen(token_start), column_type))) {
          line_valid = false;
          _OB_LOG(WARN, "Invalid column type");
        } else if (OB_FAIL(column_info.push_back(column_type))) {
          _OB_LOG(WARN, "push to column_info failed");
        } else if (NULL == (token_start = strtok_r(NULL, "|", &saved_strtok_pos))) {
          line_finish = true;
        }
      } while (OB_SUCCESS == ret && !line_finish && line_valid);
    }
  }
  return ret;
}

void ObStdinIter::set_common_prefix_len(int64_t len)
{
  for (int64_t i = 0; i < random_cell_generators.count(); ++i) {
    static_cast<RandomCellGen *>(random_cell_generators.at(i))->common_prefix_len = len;
  }
}

int ObStdinIter::init_random_cell_gen(const ObTableSchema &schema, const ObIArray<ObColDesc> &column_types, ObIAllocator &buf)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_types.count(); ++i) {
    const ObColDesc &col_desc = column_types.at(i);
    const ObObjType column_type = col_desc.col_type_.get_type();
    const ObColumnSchemaV2 *col_schema = schema.get_column_schema(col_desc.col_id_);
    if (NULL == col_schema) {
      _OB_LOG(WARN, "fail to get col schema");
    } else {
      RandomCellGenCtr ctr = random_cell_gen_ctrs[column_type];
      if (NULL == ctr) {
        _OB_LOG(WARN, "Not supported random cell type");
        ret = OB_INVALID_ARGUMENT;
      } else {
        RandomCellGen *gen = ctr(buf);
        gen->need_random = need_random[i];
        gen->acc_ = &col_schema->get_accuracy();
        gen->type_ = column_type;
        if (NULL == gen) {
          _OB_LOG(WARN, "Alloc memory failed");
        } else {
          //TODO:specify max min value in meta instead of hard-coding
          ObObj max;
          ObObj min;
          switch (column_type) {
          case ObDoubleType:
          case ObFloatType:
          case ObIntType: {
            max.set_int(seed_max);
            min.set_int(seed_min);
            gen->set_max(max);
            gen->set_min(min);
            break;
          }
          case ObNumberType: {
            max.set_int(INT_MAX);
            min.set_int(INT_MIN);
            gen->set_max(max);
            gen->set_min(min);

            break;
          }
          case ObVarcharType: {
            ObString max_str = ObString::make_string("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            ObString min_str = ObString::make_string("");
            max.set_varchar(max_str);
            min.set_varchar(min_str);
            gen->set_max(max);
            gen->set_min(min);
            gen->length = col_schema->get_data_length();
            int64_t occupy_buf_size = gen->length + 1;
            gen->my_buf = str_main_buf_ + str_main_buf_ptr;
            str_main_buf_ptr += occupy_buf_size;
            break;
          }
          case ObTimestampType: {
            max.set_int(INT_MAX);
            min.set_int(INT_MIN);
            gen->set_max(max);
            gen->set_min(min);
            break;
          }
          default:
            OB_LOG(WARN, "Not supported random cell type", K(column_type));
            ret = OB_INVALID_ARGUMENT;
          }
          random_cell_generators.push_back(gen);
        }
      }
    }
  }


  return ret;
}

/*
int ObStdinIter::init_schema(string &line, ObIAllocator &buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extract_column_info(line, column_types))) {
    _OB_LOG(WARN, "Invalid column type");
  } else if (OB_FAIL(init_random_cell_gen(column_types, buf))) {
    _OB_LOG(WARN, "Invalid column type");
  }
  return ret;
}*/

int ObStdinIter::init_schema(const ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  schema_ = &schema;
  if (OB_FAIL(schema.get_column_ids(col_descs))) {
    _OB_LOG(WARN, "fail to get col desc");
  } else if (OB_FAIL(init_random_cell_gen(schema, col_descs, buf_))) {
    _OB_LOG(WARN, "Invalid column type");
  } else if (NULL == (new_row_.cells_ = static_cast<ObObj *>(buf_.alloc(col_descs.count() * sizeof(ObObj))))) {
    _OB_LOG(WARN, "fail to alloc row");
  } else {
    new_row_.count_ = col_descs.count();
    new_row_.projector_ = NULL;
    new_row_.projector_size_ = 0;
  }
  seed = seed_min;
  return ret;
}

int ObStdinIter::cast_to_type(const ObObj &src, ObObj& res, ObObjType type, ObIAllocator &buf, const ObAccuracy *acc)
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx(NULL, NULL, NULL, &buf);
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NO_CAST_INT_UINT);
  ObAccuracy a;
  if (NULL != acc) {
    a = *acc;
    cast_ctx.res_accuracy_ = &a;
  }
  if (OB_SUCC(ret) && OB_FAIL(ObObjCaster::to_type(type, cast_ctx, src, res))) {
    SQL_LOG(WARN, "failed to cast object to ", K(type), K(ret), K(src));
  }
  return ret;
}

int ObStdinIter::build_obj_from_str(const char *str,
                                             int64_t len,
                                             ObObjType type,
                                             ObObj &obj,
                                             ObIAllocator& buf)
{
  int ret = OB_SUCCESS;
  if (0 == strncmp("null", str, len)) {
    obj.set_null();
  } else {
    ObString str_val(0, static_cast<int32_t>(len), str);
    ObObj str_obj;
    str_obj.set_varchar(str_val);
    ObCastCtx cast_ctx(&buf,
                       NULL,
                       0,
                       CM_NONE,
                       CS_TYPE_INVALID,
                       NULL);
    if (OB_FAIL(ObObjCaster::to_type(type, cast_ctx, str_obj, obj))) {
      _OB_LOG(WARN, "failed to cast obj, str = %s",str);
    }
  }
  return ret;
}

int ObStdinIter::fill_new_row_with_random(ObNewRow &row, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    if(OB_FAIL(gen_random_cell_for_column(i, row.cells_[i], allocator, seed))) {
      _OB_LOG(WARN, "Gen random cell failed");
    }
  }

  ++seed_step_current;
  if (seed_step_current == seed_step_length) {
    seed_step_current = 0;
    seed += seed_step;
//    if (seed > seed_max) {
//      seed = seed_min;
//    }
  }

  return ret;

}

int ObStdinIter::gen_random_cell_for_column(const int64_t column_id, ObObj& obj, ObIAllocator &buf, int64_t seed) const
{
  int ret = OB_SUCCESS;
  if (0 > column_id || column_id >= random_cell_generators.count()) {
    _OB_LOG(WARN, "Random cell column range error");
    ret = OB_INVALID_ARGUMENT;
  } else {
    IRandomCellGen *gen = random_cell_generators.at(column_id);
    obj = gen->gen(buf, seed);
  }
  return ret;
}

int ObStdinIter::fill_new_row_from_line(const std::string &line, ObNewRow &row, oceanbase::common::ObIArray<ObColDesc> &col_desc, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const static uint64_t MAX_LINE_LENGTH = 1024;
  char line_buf[MAX_LINE_LENGTH + 1] = {0};
  char *saved_strtok_pos = NULL;
  char *token_start = NULL;
  if (line.length() > MAX_LINE_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "line too long");
  } else {
    memcpy(line_buf, line.c_str(), line.length());
    if (NULL == (token_start = strtok_r(line_buf, ",", &saved_strtok_pos))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      int64_t column_index = 0;
      ObObj obj;
      bool line_valid = true;
      bool line_finish = false;
      do {
        if (column_index >= col_desc.count()) {
          line_valid = false;
          _OB_LOG(WARN, "Column count larger than meta");
        } else if (OB_FAIL(build_obj_from_str(token_start, strlen(token_start), col_desc.at(column_index).col_type_.get_type(), obj, allocator))) {
          _OB_LOG(WARN, "Failed to cast obj");
        } else {
          row.cells_[column_index] = obj;
          ++column_index;
        }
        if (OB_SUCCESS == ret && NULL == (token_start = strtok_r(NULL, ",", &saved_strtok_pos))) {
          line_finish = true;
        }
      } while (OB_SUCCESS == ret && !line_finish && line_valid);
      if (OB_SUCCESS == ret && line_finish && column_index != col_desc.count()) {
        ret = OB_INVALID_ARGUMENT;
        _OB_LOG(WARN, "Column count smaller than meta");
      }
    }
  }
  return ret;
}

int ObStdinIter::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  std::string line;
  bool got_non_comment_line = false;

  if (!is_iter_end()) {
    while (!pure_random_ && OB_SUCCESS == ret && !got_non_comment_line && std::getline(cin, line)) {
      if (line.size() <= 0 //skip invalid/comment lines
          || '#' == line.at(0)) {
        continue;
      } else {
        got_non_comment_line = true;
        if (OB_FAIL(fill_new_row_from_line(line, *row, col_descs, buf_))) {
          _OB_LOG(WARN, "Failed to parse line");
        } else {
          advance_iter();
        }
      }
    }
    if (pure_random_ || cin.eof()) {
      if (TERMINATE == on_eof_) {
        ret = OB_ITER_END;
      } else if (RANDOM == on_eof_) {
        if (is_iter_end()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(fill_new_row_with_random(*row, buf_))) {
          _OB_LOG(WARN, "Failed gen random line");
        } else {
          advance_iter();
        }
      } else if (REWIND == on_eof_) {
        ret = OB_ITER_END; //todo
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}



