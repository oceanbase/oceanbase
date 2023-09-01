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

#ifndef SHARE_OB_TABLE_ACCESS_HELPER_H
#define SHARE_OB_TABLE_ACCESS_HELPER_H
#include "share/ob_occam_time_guard.h"
#include "common/ob_range.h"
#include "lib/function/ob_function.h"
#include "lib/list/ob_dlist.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_server_struct.h"
#include <cstdio>
#include <cstring>
#include <type_traits>
#include <utility>
#include "lib/container/ob_tuple.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_occam_thread_pool.h"
#include "lib/string/ob_string_holder.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{
class LsElectionReferenceInfoRow;
}
}
namespace common
{

constexpr int STACK_BUFFER_SIZE = 512;
#define OB_LOG_(args...) OB_LOG(args, PRINT_WRAPPER)
class ObTableAccessHelper
{
  friend class logservice::coordinator::LsElectionReferenceInfoRow;
public:
  static int split_string_by_char(const ObStringHolder &arg_str, const char character, ObIArray<ObStringHolder> &result)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    ObString str = arg_str.get_ob_string();
    if (OB_UNLIKELY(str.empty())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "invalid argument", KR(ret), K(MTL_ID()), K(str), K(character));
    } else {
      const char *find_pos = nullptr;
      do {
        find_pos = str.find(character);
        if (OB_NOT_NULL(find_pos)) {
          ObString splited_str = str.split_on(find_pos);
          if (splited_str.empty()) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(ERROR, "split str failed, not expected", KR(ret), K(MTL_ID()), K(str), K(character));
          } else {
            if (CLICK_FAIL(result.push_back(ObStringHolder()))) {
              OB_LOG(WARN, "push back string to array failed", KR(ret), K(MTL_ID()), K(str), K(character));
            } else if (CLICK_FAIL(result.at(result.count() - 1).assign(splited_str))) {
              OB_LOG(WARN, "create string holder failed", KR(ret), K(MTL_ID()), K(str), K(character));
            }
          }
        }
      } while(OB_SUCC(ret) && OB_NOT_NULL(find_pos));
      if (OB_SUCC(ret) && OB_ISNULL(find_pos)) {
        if (CLICK_FAIL(result.push_back(ObStringHolder()))) {
          OB_LOG(WARN, "push back final str to array failed", KR(ret), K(MTL_ID()), K(str), K(character));
        } else if (CLICK_FAIL(result.at(result.count() - 1).assign(str))) {
          OB_LOG(WARN, "create string holder failed", KR(ret), K(MTL_ID()), K(str), K(character));
        }
      }
    }
    return ret;
  }
  template <typename ...Rest>
  static int join_string(char *buffer, const int64_t len, const ObString &connect_str, Rest &&...strs)
  {
    int64_t pos = 0;
    return join_string_(buffer, len, pos, connect_str, std::forward<Rest>(strs)...);
  }
  template <typename T>
  static int join_string(char *buffer, const int64_t len, const ObString &connect_str, const ObIArray<T> &array)
  {
    int64_t pos = 0;
    return join_string_(buffer, len, pos, connect_str, array);
  }
  template <typename ...T>
  static int join_string_with_begin_end(char *buffer,
                                        const int64_t len,
                                        ObString begin,
                                        ObString end,
                                        ObString connect_str,
                                        T &&...strs)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(begin), K(end), K(connect_str)
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(begin)))) {
      OB_LOG_(WARN, "databuff_printf begin failed");
    } else if (CLICK_FAIL(join_string_(buffer, len, pos, connect_str, std::forward<T>(strs)...))) {
      OB_LOG_(WARN, "databuff_printf others failed");
    } else if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(end)))) {
      OB_LOG_(WARN, "databuff_printf end failed");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <typename T>
  static int join_string_array_with_begin_end(char *buffer,
                                              const int64_t len,
                                              ObString begin,
                                              ObString end,
                                              ObString connect_str,
                                              const ObIArray<T> &array)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(begin), K(end), K(connect_str)
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(begin)))) {
      OB_LOG_(WARN, "databuff_printf begin failed");
    } else if (CLICK_FAIL(join_string_(buffer, len, pos, connect_str, array))) {
      OB_LOG_(WARN, "databuff_printf others failed");
    } else if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(end)))) {
      OB_LOG_(WARN, "databuff_printf end failed");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  // 获取单行信息
  template <int N, typename ...T>
  static int read_single_row(const uint64_t tenant_id,
                             const char* (&columns)[N],
                             const ObString &table,
                             const ObString &where_condition,
                             T &...values)
  {
    static_assert(N > 0, "columns size must greater than 0");
    static_assert(sizeof...(T) == N, "number of value size must equal than N");
    return read_and_convert_to_values_(tenant_id,
                                       columns,
                                       N,
                                       table,
                                       where_condition,
                                       values...);
  }
  template <typename ...T>
  static int read_single_row(const uint64_t tenant_id,
                             const std::initializer_list<const char *> &columns,
                             const ObString &table,
                             const ObString &where_condition,
                             T &...values)
  {
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(table), K(where_condition)
    int ret = OB_SUCCESS;
    if (columns.size() != sizeof...(T)) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG_(WARN, "sizeof... values should equal to columns size");
    } else {
      const char * columns_array[columns.size()];
      auto iter = std::begin(columns);
      for (int64_t idx = 0; iter != std::end(columns); ++idx && ++iter) {
        columns_array[idx] = *iter;
      }
      if (OB_FAIL(read_and_convert_to_values_(tenant_id,
                                              columns_array,
                                              columns.size(),
                                              table,
                                              where_condition,
                                              values...))) {
        OB_LOG_(WARN, "fail to read and convert to values");
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  // 获取多行的信息
  template <int N, typename ...T>
  static int read_multi_row(const uint64_t tenant_id,
                            const char* (&columns)[N],
                            const ObString &table,
                            const ObString &condition,
                            common::ObIArray<ObTuple<T...>> &output_array)
  {
    static_assert(N > 0, "columns size must greater than 0");
    static_assert(sizeof...(T) == N, "number of value size must equal than N");
    return read_and_convert_to_tuples_(tenant_id,
                                       columns,
                                       N,
                                       table,
                                       condition,
                                       output_array);
  }
  template <typename ...T>
  static int read_multi_row(const uint64_t tenant_id,
                            const std::initializer_list<const char *> &columns,
                            const ObString &table,
                            const ObString &condition,
                            common::ObIArray<ObTuple<T...>> &output_array)
  {
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(table), K(condition)
    int ret = OB_SUCCESS;
    if (columns.size() != sizeof...(T)) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG_(WARN, "sizeof... values should equal to columns size");
    } else {
      const char * columns_array[columns.size()];
      auto iter = std::begin(columns);
      for (int64_t idx = 0; iter != std::end(columns); ++idx && ++iter) {
        columns_array[idx] = *iter;
      }
      if (OB_FAIL(read_and_convert_to_tuples_(tenant_id,
                                              columns_array,
                                              columns.size(),
                                              table,
                                              condition,
                                              output_array))) {
        OB_LOG_(WARN, "fail to read and convert to tuples");
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  static int insert_row(const uint64_t tenant_id,
                        const ObString &table,
                        const ObString &value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(table), K(value), K(sql)
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (CLICK_FAIL(sql.append_fmt("INSERT INTO %s VALUES %s", to_cstring(table), to_cstring(value)))) {
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG_(WARN, "GCTX.sql_proxy_ is nullptr");
    } else if (CLICK_FAIL(GCTX.sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
      OB_LOG_(WARN, "GCTX.sql_proxy_ insert row failed");
    } else {
      OB_LOG_(INFO, "GCTX.sql_proxy_ insert row success");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <int N, typename ...T>
  static int insert_row(const uint64_t write_tenant,
                        const ObString &table,
                        const char* (&columns)[N],
                        T &&...value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(table), K(sql)
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    char column_str[STACK_BUFFER_SIZE] = {0};
    char value_str[STACK_BUFFER_SIZE] = {0};
    if (CLICK_FAIL(join_string(column_str, STACK_BUFFER_SIZE, ",", columns))) {
      OB_LOG_(WARN, "join column failed");
    } else if (CLICK_FAIL(join_string_with_begin_end(value_str, STACK_BUFFER_SIZE, "'", "'", "','", std::forward<T>(value)...))) {
      OB_LOG_(WARN, "join values failed");
    } else if (CLICK_FAIL(sql.append_fmt("INSERT INTO %s(%s) VALUES (%s)", to_cstring(table), column_str, value_str))) {
      OB_LOG_(WARN, "format sql failed");
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG_(WARN, "GCTX.sql_proxy_ is nullptr");
    } else if (CLICK_FAIL(GCTX.sql_proxy_->write(write_tenant, sql.ptr(), affected_rows))) {
      OB_LOG_(WARN, "GCTX.sql_proxy_ insert row failed");
    } else {
      OB_LOG_(INFO, "GCTX.sql_proxy_ insert row success");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  static int set_parameter(const ObString &value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(value), K(sql)
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (CLICK_FAIL(sql.append_fmt("ALTER SYSTEM %s", to_cstring(value)))) {
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG_(WARN, "GCTX.sql_proxy_ is nullptr");
    } else if (CLICK_FAIL(GCTX.sql_proxy_->write(MTL_ID(), sql.ptr(), affected_rows))) {
      OB_LOG_(WARN, "GCTX.sql_proxy_ execute alter system failed");
    } else {
      OB_LOG_(INFO, "GCTX.sql_proxy_ execute alter system success");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
private:
  template <typename ...T>
  static int read_and_convert_to_values_(const uint64_t tenant_id,
                                         const char **columns,
                                         const int64_t culumn_size,
                                         const ObString &table,
                                         const ObString &condition,
                                         T &...values)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(table), K(condition)
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      OB_LOG_(WARN, "GCTX.sql_proxy_ is null");
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(get_my_sql_result_(columns, culumn_size, table, condition, *GCTX.sql_proxy_, tenant_id, res, result))) {
          OB_LOG_(WARN, "fail to get ObMySQLResult");
        } else if (OB_NOT_NULL(result)) {
          int64_t iter_times = 0;
          while (OB_SUCC(ret) && OB_SUCC(result->next())) {
            if (++iter_times > 1) {
              ret = OB_ERR_MORE_THAN_ONE_ROW;
              OB_LOG_(WARN, "there are more than one row been selected");
              break;
            } else if (CLICK_FAIL(get_values_from_row_<0>(result, columns, values...))) {
              OB_LOG_(WARN, "failed to get column from row");
            }
          }
          if (OB_ITER_END == ret) {
            if (1 == iter_times) {
              ret = OB_SUCCESS;
            } else if (0 == iter_times) {
              ret = OB_EMPTY_RESULT;
            }
          } else {
            OB_LOG_(WARN, "iter failed", K(iter_times));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG_(WARN, "get mysql result failed");
        }
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <typename ...T>
  static int read_and_convert_to_tuples_(const uint64_t tenant_id,
                                         const char **columns,
                                         const int64_t culumn_size,
                                         const ObString &table,
                                         const ObString &condition,
                                         common::ObIArray<ObTuple<T...>> &output_array)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(table), K(condition)
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      OB_LOG_(WARN, "GCTX.sql_proxy_ is null");
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(get_my_sql_result_(columns, culumn_size, table, condition, *GCTX.sql_proxy_, tenant_id, res, result))) {
          OB_LOG_(WARN, "fail to get ObMySQLResult");
        } else if (OB_NOT_NULL(result)) {
          int64_t iter_times = 0;
          while (OB_SUCC(ret) && OB_SUCC(result->next())) {
            if (CLICK_FAIL(output_array.push_back(ObTuple<T...>()))) {
              OB_LOG_(WARN, "push new tuple to array failed", K(iter_times));
            } else if (OB_SUCCESS != (ret = AccessHelper<sizeof...(T) - 1, T...>::
                        get_values_to_tuple_from_row(result, columns, output_array.at(iter_times)))) {
              OB_LOG_(WARN, "failed to get values from row", K(iter_times));
            }
            iter_times++;
          }
          if (OB_ITER_END == ret && iter_times > 0) {
            ret = OB_SUCCESS;
          } else {
            OB_LOG_(WARN, "iter failed", K(iter_times));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG_(WARN, "get mysql result failed");
        }
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  static int get_my_sql_result_(const char **columns,
                                const int64_t column_size,
                                const ObString &table,
                                const ObString &condition,
                                ObISQLClient &proxy,
                                const uint64_t tenant_id,
                                ObMySQLProxy::MySQLResult &res,
                                common::sqlclient::ObMySQLResult *&result)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(tenant_id), K(columns), K(table), K(condition), K(sql), K(columns_str)
    int ret = OB_SUCCESS;
    ObSqlString sql;
    char columns_str[STACK_BUFFER_SIZE] = {0};
    int64_t pos = 0;
    for (int i = 0; i < column_size; ++i) {
      if (i != column_size - 1) {
        if (CLICK_FAIL(databuff_printf(&columns_str[0], STACK_BUFFER_SIZE, pos, "%s,", columns[i]))) {
          OB_LOG_(WARN, "failed to format column string");
        }
      } else {
        if (CLICK_FAIL(databuff_printf(&columns_str[0], STACK_BUFFER_SIZE, pos, "%s", columns[i]))) {
          OB_LOG_(WARN, "failed to format column string");
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (CLICK_FAIL(sql.append_fmt("SELECT %s FROM %s %s", columns_str, to_cstring(table), to_cstring(condition)))) {
        OB_LOG_(WARN, "failed to append sql");
      } else if (CLICK_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        OB_LOG_(WARN, "GCTX.sql_proxy_ read failed");
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG_(WARN, "failed to get result");
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <typename T>
  static int join_string_(char *buffer,
                          const int64_t len,
                          int64_t &pos,
                          const ObString &connect_str,
                          T &&str)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(connect_str), K(str)
    int ret = OB_SUCCESS;
    if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(str)))) {
      OB_LOG_(WARN, "databuff_printf string failed");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <int N>
  static int join_string_(char *buffer, const int64_t len, int64_t &pos, const ObString &connect_str, const char *(&array)[N])
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = OB_SUCCESS;
    // int64_t pos = 0;
    for (int i = 0; i < N && OB_SUCC(ret); ++i) {
      if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", array[i]))) {
        OB_LOG(WARN, "print array failed", KR(ret), K(i), K(connect_str));
      } else if (i != N - 1) {
        if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(connect_str)))) {
          OB_LOG(WARN, "print connect str failed", KR(ret), K(i), K(connect_str));
        }
      }
    }
    return ret;
  }
  template <typename T, typename ...Rest>
  static int join_string_(char *buffer,
                          const int64_t len,
                          int64_t &pos,
                          const ObString &connect_str,
                          T &&str,
                          Rest &&...strs)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(connect_str), K(str)
    int ret = OB_SUCCESS;
    if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(str)))) {
      OB_LOG_(WARN, "databuff_printf string failed");
    } else if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(connect_str)))) {
      OB_LOG_(WARN, "databuff_printf connect_str failed");
    } else if (CLICK_FAIL(join_string_(buffer, len, pos, connect_str, std::forward<Rest>(strs)...))) {
      OB_LOG_(WARN, "databuff_printf rest strs failed");
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  template <typename T>
  static int join_string_(char *buffer,
                          const int64_t len,
                          int64_t &pos,
                          const ObString &connect_str,
                          const ObIArray<T> &array)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    #define PRINT_WRAPPER KR(ret), K(MTL_ID()), K(connect_str), K(array), K(i)
    int ret = OB_SUCCESS;
    for (int i = 0; i < array.count() && OB_SUCC(ret); ++i) {
      if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(array.at(i))))) {
        OB_LOG_(WARN, "databuff_printf string failed");
      } else if (i != array.count() - 1) {
        if (CLICK_FAIL(databuff_printf(buffer, len, pos, "%s", to_cstring(connect_str)))) {
          OB_LOG_(WARN, "databuff_printf string failed");
        }
      }
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  static int get_signle_column_from_signle_row_(common::sqlclient::ObMySQLResult *row,
                                                const char *column,
                                                share::ObLSID &ls_id)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    int64_t value = 0;
    if (CLICK_FAIL(row->get_int(column, value))) {
      OB_LOG(WARN, "get_column_from_signle_row failed", KR(ret), K(MTL_ID()), K(column));
    } else {
      ls_id = share::ObLSID(value);
      OB_LOG(TRACE, "get_column_from_signle_row success", KR(ret), K(MTL_ID()), K(column), K(value));
    }
    return ret;
  }
  static int get_signle_column_from_signle_row_(common::sqlclient::ObMySQLResult *row,
                                                const char *column,
                                                common::ObTabletID &tablet_id)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    int64_t value = 0;
    if (CLICK_FAIL(row->get_int(column, value))) {
      OB_LOG(WARN, "get_column_from_signle_row failed", KR(ret), K(MTL_ID()), K(column));
    } else {
      tablet_id = common::ObTabletID(value);
      OB_LOG(TRACE, "get_column_from_signle_row success", KR(ret), K(MTL_ID()), K(column), K(value));
    }
    return ret;
  }
  static int get_signle_column_from_signle_row_(common::sqlclient::ObMySQLResult *row,
                                                const char *column,
                                                int64_t &value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    if (CLICK_FAIL(row->get_int(column, value))) {
      OB_LOG(WARN, "get_column_from_signle_row failed", KR(ret), K(MTL_ID()), K(column));
    } else {
      OB_LOG(TRACE, "get_column_from_signle_row success", KR(ret), K(MTL_ID()), K(column), K(value));
    }
    return ret;
  }
  static int get_signle_column_from_signle_row_(common::sqlclient::ObMySQLResult *row,
                                                const char *column,
                                                uint64_t &value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    if (CLICK_FAIL(row->get_uint(column, value))) {
      OB_LOG(WARN, "get_column_from_signle_row failed", KR(ret), K(MTL_ID()), K(column));
    } else {
      OB_LOG(TRACE, "get_column_from_signle_row success", KR(ret), K(MTL_ID()), K(column), K(value));
    }
    return ret;
  }
  static int get_signle_column_from_signle_row_(common::sqlclient::ObMySQLResult *row,
                                                const char *column,
                                                ObStringHolder &value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    ObString temp_str;
    if (CLICK_FAIL(row->get_varchar(column, temp_str))) {
      OB_LOG(WARN, "get_column_from_signle_row failed", KR(ret), K(MTL_ID()), K(column));
    } else if (CLICK_FAIL(value.assign(temp_str))) {
      OB_LOG(WARN, "create ObStringHolder success", KR(ret), K(MTL_ID()), K(column));
    }
    return ret;
  }
  static int get_signle_column_from_signle_row_(common::sqlclient::ObMySQLResult *row,
                                                const char *column,
                                                bool &value)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    if (CLICK_FAIL(row->get_bool(column, value))) {
      OB_LOG(WARN, "get_column_from_signle_row failed", KR(ret), K(MTL_ID()), K(column));
    } else {
      OB_LOG(TRACE, "get_column_from_signle_row success", KR(ret), K(MTL_ID()), K(column), K(value));
    }
    return ret;
  }
  // 可变参数模版展开的递归基
  template <int FLOOR>
  static int get_values_from_row_(common::sqlclient::ObMySQLResult *row, const char **columns)
  {
    UNUSED(row);
    UNUSED(columns);
    return OB_SUCCESS;
  }
  // 可变参数模版展开，从行中获取每一个入参
  template <int FLOOR, typename V, typename ...T>
  static int get_values_from_row_(common::sqlclient::ObMySQLResult *row,
                                  const char **columns,
                                  V &value,
                                  T &...others)
  {
    TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
    int ret = common::OB_SUCCESS;
    if (CLICK_FAIL(get_signle_column_from_signle_row_(row, columns[FLOOR], value))) {
      OB_LOG(WARN, "get value failed", KR(ret), K(MTL_ID()), K(FLOOR), K(columns[FLOOR]));
    } else if (CLICK_FAIL(get_values_from_row_<FLOOR + 1>(row, columns, others...))) {
      OB_LOG(WARN, "get others value failed", KR(ret), K(MTL_ID()), K(FLOOR), K(columns[FLOOR]));
    } else {
      OB_LOG(TRACE, "get value success", KR(ret), K(MTL_ID()), K(FLOOR), K(columns[FLOOR]), K(value));// DUBUG
    }
    return ret;
  }
  template <int FLOOR, typename ...T>
  friend class AccessHelper;
  // 便特化必须靠类定义来协助
  template <int FLOOR, typename ...T>
  struct AccessHelper
  {
    // 从行中获取元组中的每一个元素
    static int get_values_to_tuple_from_row(common::sqlclient::ObMySQLResult *row,
                                            const char **columns,
                                            ObTuple<T...> &tuple)
    {
      TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
      static_assert(FLOOR > 0 && FLOOR <= sizeof...(T), "unexpected compile error");
      int ret = common::OB_SUCCESS;
      if (CLICK_FAIL(get_signle_column_from_signle_row_(row, columns[FLOOR], std::get<FLOOR>(tuple.tuple())))) {
        OB_LOG(WARN, "get value failed", KR(ret), K(MTL_ID()), K(columns[FLOOR]));
      } else {
        ret = AccessHelper<FLOOR - 1, T...>::get_values_to_tuple_from_row(row, columns, tuple);
      }
      return ret;
    }
  };
  // 模版偏特化递归基
  template <typename ...T>
  struct AccessHelper<0, T...>
  {
    // 从行中获取元组中的每一个元素的递归基
    static int get_values_to_tuple_from_row(common::sqlclient::ObMySQLResult *row,
                                            const char **columns,
                                            ObTuple<T...> &tuple)
    {
      TIMEGUARD_INIT(OCCAM, 1_s, 60_s);
      int ret = common::OB_SUCCESS;
      if (CLICK_FAIL(get_signle_column_from_signle_row_(row, columns[0], std::get<0>(tuple.tuple())))) {
        OB_LOG(WARN, "get value failed", KR(ret), K(MTL_ID()), K(columns[0]));
      }
      return ret;
    }
  };
};
#undef OB_LOG_

}
}
#endif
