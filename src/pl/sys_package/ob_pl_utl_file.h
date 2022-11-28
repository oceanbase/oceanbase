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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_UTL_FILE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_UTL_FILE_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/utl_file/ob_utl_file_constants.h"

namespace oceanbase
{
namespace pl
{
class ObPLAssocArray;

class ObPLUtlFile
{
private:
  enum OpenMode
  {
    READ,
    WRITE,
    APPEND
  };

  struct FdParam
  {
    FdParam();
    FdParam(const int64_t session_id, const int64_t sz, OpenMode mode);

    int64_t session_id_;
    int64_t max_line_size_;
    OpenMode open_mode_;

    TO_STRING_KV(K_(session_id), K_(max_line_size), K_(open_mode));
  };
public:
  static int fopen(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fclose(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_line(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int get_line_raw(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_buffer(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fflush(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fopen_nchar(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int put_raw(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fseek(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fremove(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fcopy(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fgetattr(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fgetpos(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int frename(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int fis_open(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
public:
  static int close_all(const int64_t session_id);
private:
  static int get_dir_path(sql::ObExecContext &ctx, const ObString &dir_name, ObString &dir_path);
  static int deep_copy(const ObString &str, char *buffer, int size);
  static int parse_open_mode(const char *open_mode, OpenMode &mode);
  static int get_fd_param(const int64_t fd, FdParam &fd_param);
private:
  static constexpr const char* const ERRMSG_TOO_MANY_FILES_OPEN = "too many files open";
  static common::hash::ObHashMap<int64_t, FdParam> fd_map_;
  static constexpr int STRING_SIZE = 256;
};
} // namespace pl
} // namespace oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_UTL_FILE_H_ */
