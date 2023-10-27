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

#include "ob_lua_api.h"
#include "ob_lua_handler.h"

#include <algorithm>
#include <bitset>
#include <functional>
#include <vector>

#include "lib/alloc/memory_dump.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_di_cache.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "observer/virtual_table/ob_all_virtual_sys_stat.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_tenant_mgr.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_tx_stat.h"
#include "observer/ob_server.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"

#include <lua.hpp>

using namespace oceanbase;
using namespace common;
using namespace diagnose;
using namespace oceanbase::observer;
using namespace oceanbase::rpc::frame;
using namespace obrpc;
using namespace rpc;
using namespace sql;
using namespace transaction;

namespace oceanbase
{
namespace diagnose
{
class LuaAllocator : public ObIAllocator
{
public:
  static LuaAllocator& get_instance()
  {
    static LuaAllocator allocator;
    return allocator;
  }
  void *alloc(const int64_t size) override
  {
    void *ret = nullptr;
    if (0 != size && ObLuaHandler::get_instance().memory_usage() + size + 8 < ObLuaHandler::LUA_MEMORY_LIMIT) {
#if defined(OB_USE_ASAN) || defined(ENABLE_SANITY)
      if (OB_NOT_NULL(ret = ::malloc(size + 8))) {
#else
      if (MAP_FAILED == (ret = ::mmap(nullptr, size + 8, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
        ret = nullptr;
      } else {
#endif
        *static_cast<uint64_t *>(ret) = size;
        ret = (char*)ret + 8;
        ObLuaHandler::get_instance().memory_update(size + 8);
      }
    }
    return ret;
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override
  {
    if (OB_NOT_NULL(ptr)) {
      const uint64_t size = *(uint64_t *)((char *)ptr - 8);
      ObLuaHandler::get_instance().memory_update(- 8 - size);
#if defined(OB_USE_ASAN) || defined(ENABLE_SANITY)
      ::free((void *)((char *)ptr - 8));
#else
      ::munmap((void *)((char *)ptr - 8), size + 8);
#endif
    }
  }
};
}

static ObFIFOAllocator &get_global_allocator()
{
  static ObFIFOAllocator allocator;
  if (OB_UNLIKELY(!allocator.is_inited())) {
    IGNORE_RETURN allocator.init(&LuaAllocator::get_instance(), (1 << 13) - 8, lib::ObMemAttr(OB_SERVER_TENANT_ID, "LuaAlloc"), 0, 0, INT64_MAX);
  }
  return allocator;
}
}

static constexpr const char *usage_str =
"API List:\n\n"
"string = usage()\n"
"print_to_client(arg1, arg2...)\n"
"int = now()\n"
"{int, int, ...} = get_tenant_id_list()\n"
"int = get_tenant_mem_limit(int)\n"
"int = get_tenant_sysstat_by_id(int, int)\n"
"int = get_tenant_sysstat_by_name(int, string)\n"
"string = get_easy_diagnose_info()\n"
"string = get_tenant_pcode_statistics(int, int)\n"
"{{row1}, {row2}, ...} = select_processlist()\n"
"{{row1}, {row2}, ...} = select_sysstat()\n"
"{{row1}, {row2}, ...} = select_memory_info()\n"
"{{row1}, {row2}, ...} = select_tenant_ctx_memory_info()\n"
"{{row1}, {row2}, ...} = select_trans_stat()\n"
"{{row1}, {row2}, ...} = select_sql_workarea_active()\n"
"{{row1}, {row2}, ...} = select_sys_task_status()\n"
"{{row1}, {row2}, ...} = select_dump_tenant_info()\n"
"{{row1}, {row2}, ...} = select_disk_stat()\n"
"{{row1}, {row2}, ...} = select_tenant_memory_info()\n"
"string = show_log_probe()\n"
"int = set_log_probe(string)\n"
"{{row1}, {row2}, ...} = select_mem_leak_checker_info()\n"
"{{row1}, {row2}, ...} = select_compaction_diagnose_info()\n"
"{{row1}, {row2}, ...} = select_server_schema_info()\n"
"{{row1}, {row2}, ...} = select_schema_slot()\n"
"{{row1}, {row2}, ...} = dump_thread_info()\n"
"{{row1}, {row2}, ...} = select_malloc_sample_info()\n"
"int = set_system_tenant_limit_mode(int)\n"
;

class LuaVtableGenerator
{
public:
  LuaVtableGenerator(lua_State* L, std::vector<const char*>& columns)
    : stack_(L),
      row_id_(0),
      column_id_(-1),
      offset_(0), // dump from first row by default
      length_(INT_MAX), // dump all rows by default
      dump_(false),
      select_(),
      columns_(columns)
  {
    select_.set(); // enable all column by default
    if (1 == lua_gettop(stack_)) {
      filter();
    }
    if (dump_) {
      lua_pushnil(stack_);
    } else {
      lua_newtable(stack_);
    }
  }
  void next_row()
  {
    if (row_id_ >= offset_ && row_id_ - offset_ < length_) {
      if (!dump_) {
        lua_pushinteger(stack_, row_id_ + 1);
        lua_newtable(stack_);
      }
    }
  }
  void row_end()
  {
    if (row_id_ >= offset_ && row_id_ - offset_ < length_) {
      if (!dump_) {
        lua_settable(stack_, -3);
      } else {
        int ret = OB_SUCCESS;
        if (OB_FAIL(APIRegister::get_instance().append("\n"))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    }
    ++row_id_;
    column_id_ = -1;
  }
  void next_column()
  {
    if (in_range()) {
      if (!dump_) {
        lua_pushstring(stack_, columns_[column_id_]);
        lua_pushnil(stack_);
        lua_settable(stack_, -3);
      } else {
        int ret = OB_SUCCESS;
        if (OB_FAIL(APIRegister::get_instance().append("NULL "))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    }
  }
  void next_column(const char* str)
  {
    if (in_range()) {
      if (!dump_) {
        lua_pushstring(stack_, columns_[column_id_]);
        lua_pushstring(stack_, str);
        lua_settable(stack_, -3);
      } else {
        int ret = OB_SUCCESS;
        if (OB_FAIL(APIRegister::get_instance().append(str))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
        if (OB_FAIL(APIRegister::get_instance().append(" "))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    }
  }
  void next_column(bool value)
  {
    if (in_range()) {
      if (!dump_) {
        lua_pushstring(stack_, columns_[column_id_]);
        lua_pushboolean(stack_, value);
        lua_settable(stack_, -3);
      } else {
        int ret = OB_SUCCESS;
        if (OB_FAIL(APIRegister::get_instance().append(value ? "1 " : "0 "))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    }
  }
  void next_column(int64_t value)
  {
    if (in_range()) {
      if (!dump_) {
        lua_pushstring(stack_, columns_[column_id_]);
        lua_pushinteger(stack_, value);
        lua_settable(stack_, -3);
      } else {
        int ret = OB_SUCCESS;
        char buf[32];
        snprintf(buf, 32, "%ld ", value);
        if (OB_FAIL(APIRegister::get_instance().append(buf))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    }
  }
  void next_column(double value)
  {
    if (in_range()) {
      if (!dump_) {
        lua_pushstring(stack_, columns_[column_id_]);
        lua_pushnumber(stack_, value);
        lua_settable(stack_, -3);
      } else {
        int ret = OB_SUCCESS;
        char buf[32];
        snprintf(buf, 32, "%lf ", value);
        if (OB_FAIL(APIRegister::get_instance().append(buf))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    }
  }
  void next_column(uint64_t value)
  {
    next_column((int64_t)value);
  }
  void next_column(int32_t value)
  {
    next_column((int64_t)value);
  }
  void next_column(uint32_t value)
  {
    next_column((int64_t)value);
  }
  bool is_end()
  {
    return row_id_ - offset_ >= length_;
  }
private:
  void filter()
  {
    luaL_checktype(stack_, -1, LUA_TTABLE);
    lua_pushnil(stack_);
    while (lua_next(stack_, -2) != 0) {
      luaL_checktype(stack_, -2, LUA_TSTRING);
      const char *key = lua_tostring(stack_, -2);
      if (0 == strcmp(key, "select")) {
        luaL_checktype(stack_, -1, LUA_TTABLE);
        lua_pushnil(stack_);
        select_.reset();
        while (lua_next(stack_, -2) != 0) {
          luaL_checktype(stack_, -1, LUA_TSTRING);
          const char *column_name = lua_tostring(stack_, -1);
          for (auto i = 0; i < columns_.size(); ++i) {
            if (0 == strcmp(columns_[i], column_name)) {
              select_.set(i);
            }
          }
          lua_pop(stack_, 1);
        }
      } else if (0 == strcmp(key, "limit")) {
        luaL_checktype(stack_, -1, LUA_TTABLE);
        lua_pushnil(stack_);
        if (lua_next(stack_, -2) != 0) {
          offset_ = lua_tointeger(stack_, -1);
          lua_pop(stack_, 1);
          if (lua_next(stack_, -2) != 0) {
            length_ = lua_tointeger(stack_, -1);
            lua_pop(stack_, 2);
          } else {
            length_ = offset_;
            offset_ = 0;
          }
        }
      } else if (0 == strcmp(key, "dump")) {
        luaL_checktype(stack_, -1, LUA_TBOOLEAN);
        if (0 != lua_toboolean(stack_, -1)) {
          dump_ = true;
        }
      } else {
        OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid arguments", K(key));
      }
      lua_pop(stack_, 1);
    }
    lua_pop(stack_, 1);
  }
  bool in_range()
  {
    return row_id_ >= offset_ && row_id_ - offset_ < length_ && select_[++column_id_];
  }
private:
  lua_State* stack_;
  int row_id_;
  int column_id_;
  int offset_;
  int length_;
  bool dump_;
  std::bitset<256> select_;
  std::vector<const char*>& columns_;
};

/**
 * All API need to use the following format.
 * int xxx(lua_State* L), which return the count of return values.
 *
 * lua API for reference: http://www.lua.org/manual/5.4/
 **/

// string = usage()
int usage(lua_State* L)
{
  int argc = lua_gettop(L);
  if (0 != argc) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "call usage() failed, bad arguments count, should be 0.");
    lua_pushnil(L);
  } else {
    lua_pushstring(L, usage_str);
  }
  return 1;
}

// print_to_client(arg1, arg2...)
int print_to_client(lua_State* L)
{
  int nargs = lua_gettop(L);
  for (int i = 1; i <= nargs; i++) {
    int ret = OB_SUCCESS;
    if (lua_isinteger(L, i) || lua_isstring(L, i) || lua_isnumber(L, i)) {
      size_t len = 0;
      const char *arg = lua_tolstring(L, i, &len);
      if (OB_FAIL(APIRegister::get_instance().append(arg, len))) {
        OB_LOG(ERROR, "append failed", K(ret), K(len));
      }
    } else if (lua_isboolean(L, i)) {
      if (0 == lua_toboolean(L, i)) {
        if (OB_FAIL(APIRegister::get_instance().append("0"))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      } else {
        if (OB_FAIL(APIRegister::get_instance().append("1"))) {
          OB_LOG(ERROR, "append failed", K(ret));
        }
      }
    } else if (lua_islightuserdata(L, i)) {
      void *ptr = lua_touserdata(L, i);
      char buf[65];
      snprintf(buf, sizeof(buf), "%p", ptr);
      if (OB_FAIL(APIRegister::get_instance().append(buf))) {
        OB_LOG(ERROR, "append failed", K(ret));
      }
    } else if (lua_isnil(L, i)) {
      ret = APIRegister::get_instance().append("NULL");
    } else {
      OB_LOG(ERROR, "arg was not convertible", K(i));
    }
    APIRegister::get_instance().append(" ");
  }
  APIRegister::get_instance().append("\n");
  return 0;
}

// int = now()
int now(lua_State* L)
{
  int argc = lua_gettop(L);
  if (0 != argc) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "call get_tenant_id_list() failed, bad arguments count, should be 0.");
    lua_pushinteger(L, 0);
  } else {
    lua_pushinteger(L, common::ObTimeUtility::fast_current_time());
  }
  return 1;
}

// list = get_tenant_id_list()
int get_tenant_id_list(lua_State* L)
{
  int argc = lua_gettop(L);
  if (0 != argc) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "call get_tenant_id_list() failed, bad arguments count, should be 0.");
  } else {
    lua_newtable(L);
    uint64_t *tenant_ids = nullptr;
    int count = 0;
    if (OB_NOT_NULL(tenant_ids = (uint64_t *)diagnose::alloc(OB_MAX_SERVER_TENANT_CNT * sizeof(uint64_t)))) {
      get_tenant_ids(tenant_ids, OB_MAX_SERVER_TENANT_CNT, count);
    }
    for (int64_t i = 0; i < count; ++i) {
      lua_pushinteger(L, i + 1);
      lua_pushinteger(L, tenant_ids[i]);
      lua_settable(L, -3);
    }
    diagnose::free(tenant_ids);
  }
  return 1;
}

// int = get_tenant_mem_limit(int)
int get_tenant_mem_limit(lua_State* L)
{
  int argc = lua_gettop(L);
  if (1 != argc) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "call get_tenant_mem_limit() failed, bad arguments count, should be 1.");
  } else {
    luaL_checktype(L, 1, LUA_TNUMBER);
    lua_pushinteger(L, ObMallocAllocator::get_instance()->get_tenant_limit(lua_tointeger(L, 1)));
  }
  return 1;
}

int get_tenant_parameter(lua_State* L)
{
  // TODO
  lua_pushnil(L);
  return 1;
}

// forward declaration
int get_tenant_sysstat(int64_t tenant_id, int64_t stat_id, int64_t &value);

// int = get_tenant_sysstat_by_id(int, int)
int get_tenant_sysstat_by_id(lua_State* L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (2 != argc) {
    OB_LOG(ERROR, "call get_tenant_sysstat_by_id() failed, bad arguments count, should be 2.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    luaL_checktype(L, 1, LUA_TNUMBER);
    luaL_checktype(L, 2, LUA_TNUMBER);
    int tenant_id = lua_tointeger(L, 1);
    int stat_id = lua_tointeger(L, 2);
    int64_t value = 0;
    for (int i = 0; i < ObStatEventIds::STAT_EVENT_SET_END; ++i) {
      if (OB_STAT_EVENTS[i].stat_id_ == stat_id) {
        if (OB_FAIL(get_tenant_sysstat(tenant_id, i, value))) {
          OB_LOG(ERROR, "failed to get tenant diag info", K(ret), K(tenant_id), K(i), K(stat_id));
        } else {
          lua_pushinteger(L, value);
        }
        break;
      }
    }
  }
  if (OB_FAIL(ret)) {
    lua_pushnil(L);
  }
  return 1;
}

// int = get_tenant_sysstat_by_name(int, string)
int get_tenant_sysstat_by_name(lua_State* L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (2 != argc) {
    OB_LOG(ERROR, "call get_tenant_sysstat_by_name() failed, bad arguments count, should be 2.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    luaL_checktype(L, 1, LUA_TNUMBER);
    luaL_checktype(L, 2, LUA_TSTRING);
    int tenant_id = lua_tointeger(L, 1);
    const char *name = lua_tostring(L, 2);
    int64_t value = 0;
    ret = OB_ENTRY_NOT_EXIST;
    for (int i = 0; i < ObStatEventIds::STAT_EVENT_SET_END; ++i) {
      if (0 == strcmp(OB_STAT_EVENTS[i].name_, name)) {
        if (OB_FAIL(get_tenant_sysstat(tenant_id, i, value))) {
          OB_LOG(ERROR, "failed to get tenant diag info", K(ret), K(tenant_id), K(i), K(name));
        } else {
          lua_pushinteger(L, value);
        }
        break;
      }
    }
  }
  if (OB_FAIL(ret)) {
    lua_pushnil(L);
  }
  return 1;
}

int summary_each_eio_info(char *buf, int &pos, int buf_len, easy_io_t *eio, const char *rpc_des);

int get_easy_diagnose_info(lua_State* L)
{
  int ret = OB_SUCCESS;
  easy_io_t *eio = NULL;
  ObNetEasy *net_easy = ObServer::get_instance().get_net_frame().get_net_easy();
  if (NULL == net_easy) {
    lua_pushnil(L);
  } else {
    int pos = 0;
    const int summary_len = 48 * 1024;
    HEAP_VAR(ByteBuf<summary_len>, summary) {
      memset(summary, 0, sizeof(summary));

      eio = net_easy->get_rpc_eio();
      ret = summary_each_eio_info(summary, pos, sizeof(summary), eio, "rpc_eio:\n");

      eio = net_easy->get_batch_rpc_eio();
      if (OB_SUCC(ret)) {
        ret = summary_each_eio_info(summary, pos, sizeof(summary), eio, "batch_rpc_eio:\n");
      }

      eio = net_easy->get_high_prio_eio();
      if (OB_SUCC(ret)) {
        ret = summary_each_eio_info(summary, pos, sizeof(summary), eio, "high_prio_rpc_eio:\n");
      }

      eio = net_easy->get_mysql_eio();
      if (OB_SUCC(ret)) {
        ret = summary_each_eio_info(summary, pos, sizeof(summary), eio, "my_sql_rpc_eio:\n");
      }

      lua_pushstring(L, (const char*) summary);
    }
  }

  return 1;
}

int get_tenant_pcode_statistics(lua_State* L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (1 != argc) {
    OB_LOG(ERROR, "call get_tenant_pcode_statistics() failed, bad arguments count, should be 1.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    luaL_checktype(L, 1, LUA_TNUMBER);
    int tenant_id = lua_tointeger(L, 1);
    ObRpcPacketSet &set = ObRpcPacketSet::instance();
    RpcStatItem item;
    char *pcode_name = nullptr;
    int64_t dcount = 0;
    HEAP_VAR(ByteBuf<64 * ObRpcPacketSet::THE_PCODE_COUNT>, output) {
      char str[64] = {0};
      int pos = 0;
      for (int64_t pcode_idx = 0; pcode_idx < ObRpcPacketSet::THE_PCODE_COUNT; pcode_idx++) {
        if (OB_FAIL(RPC_STAT_GET(pcode_idx, tenant_id, item))) {
          // continue
        } else if (item.dcount_ != 0) {
          pcode_name = const_cast<char*>(set.name_of_idx(pcode_idx));
          snprintf(str, sizeof(str), "pcode_name:%s, count:%ld\n", pcode_name, item.dcount_);
          strncpy(output + pos, str, strlen(str));
          pos += strlen(str);
        }
      }
      lua_pushstring(L, (const char*)output);
    }
  }
  if (OB_FAIL(ret)) {
    lua_pushnil(L);
  }
  return 1;
}

// int = set_log_probe(string)
int set_log_probe(lua_State* L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (1 != argc) {
    OB_LOG(ERROR, "call set_log_probe() failed, bad arguments count, should be 1.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    luaL_checktype(L, 1, LUA_TSTRING);
    size_t len = 0;
    const char *arg = lua_tolstring(L, 1, &len);
    char *ptr = (char*)diagnose::alloc(len + 1);
    DEFER(if (ptr) diagnose::free(ptr););
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "alloc memory failed", K(len));
    } else {
      memcpy(ptr, arg, len);
      ptr[len] = '\0';
      int probe_cnt = OB_LOGGER.set_probe(ptr);
      lua_pushinteger(L, probe_cnt);
    }
  }
  if (OB_FAIL(ret)) {
    lua_pushinteger(L, -1);
  }
  return 1;
}

// string = show_log_probe()
int show_log_probe(lua_State* L)
{
  int argc = lua_gettop(L);
  if (0 != argc) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call show_log_probe() failed, bad arguments count, should be 0.");
  } else {
    lua_pushstring(L, OB_LOGGER.show_probe());
  }
  return 1;
}

// list{list, list...} = select_processlist()
int select_processlist(lua_State* L)
{
  class GetProcess
  {
  public:
    GetProcess(LuaVtableGenerator& gen) : gen_(gen) {}
    bool operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
    {
      constexpr int64_t BUF_LEN = (1 << 10);
      char buf[BUF_LEN];
      gen_.next_row();
      // id
      gen_.next_column(static_cast<uint64_t>(key.sessid_));
      // user
      if (sess_info->get_is_deserialized()) {
        gen_.next_column();
      } else {
        UNUSED(sess_info->get_user_name().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      }
      // tenant
      UNUSED(sess_info->get_tenant_name().to_string(buf, BUF_LEN));
      gen_.next_column(buf);
      // host
      UNUSED(sess_info->get_peer_addr().ip_port_to_string(buf, BUF_LEN));
      gen_.next_column(buf);
      // db
      if (0 == sess_info->get_database_name().length()) {
        gen_.next_column();
      } else {
        UNUSED(sess_info->get_database_name().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      }
      // command
      gen_.next_column(sess_info->get_mysql_cmd_str());
      // sql_id
      {
        char sql_id[common::OB_MAX_SQL_ID_LENGTH + 1];
        if (obmysql::COM_QUERY == sess_info->get_mysql_cmd() ||
            obmysql::COM_STMT_EXECUTE == sess_info->get_mysql_cmd() ||
            obmysql::COM_STMT_PREPARE == sess_info->get_mysql_cmd() ||
            obmysql::COM_STMT_PREXECUTE == sess_info->get_mysql_cmd()) {
          sess_info->get_cur_sql_id(sql_id, OB_MAX_SQL_ID_LENGTH + 1);
        } else {
          sql_id[0] = '\0';
        }
        gen_.next_column(sql_id);
      }
      // time
      gen_.next_column((::oceanbase::common::ObTimeUtility::current_time() - sess_info->get_cur_state_start_time()) / 1000000);
      // state
      gen_.next_column(sess_info->get_session_state_str());
      // info
      if (obmysql::COM_QUERY == sess_info->get_mysql_cmd() ||
          obmysql::COM_STMT_EXECUTE == sess_info->get_mysql_cmd() ||
          obmysql::COM_STMT_PREPARE == sess_info->get_mysql_cmd() ||
          obmysql::COM_STMT_PREXECUTE == sess_info->get_mysql_cmd()) {
        UNUSED(sess_info->get_current_query_string().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      } else {
        gen_.next_column();
      }
      // sql_port
      gen_.next_column(GCONF.mysql_port);
      // proxy_sessid
      if (ObBasicSessionInfo::VALID_PROXY_SESSID == sess_info->get_proxy_sessid()) {
        gen_.next_column();
      } else {
        gen_.next_column(sess_info->get_proxy_sessid());
      }
      // master_sessid
      if (ObBasicSessionInfo::INVALID_SESSID == sess_info->get_master_sessid()) {
        gen_.next_column();
      } else {
        gen_.next_column(sess_info->get_master_sessid());
      }
      // user_client_ip
      if (sess_info->get_client_ip().empty()) {
        gen_.next_column();
      } else {
        UNUSED(sess_info->get_client_ip().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      }
      // user_host
      if (sess_info->get_host_name().empty()) {
        gen_.next_column();
      } else {
        UNUSED(sess_info->get_host_name().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      }
      // trans_id
      uint64_t tx_id = sess_info->get_tx_id();
      gen_.next_column(tx_id);
      // thread_id
      gen_.next_column(static_cast<uint64_t>(sess_info->get_thread_id()));
      // ssl_cipher
      if (sess_info->get_ssl_cipher().empty()) {
        gen_.next_column();
      } else {
        UNUSED(sess_info->get_ssl_cipher().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      }
      // trace_id
      if (obmysql::COM_QUERY == sess_info->get_mysql_cmd() ||
          obmysql::COM_STMT_EXECUTE == sess_info->get_mysql_cmd() ||
          obmysql::COM_STMT_PREPARE == sess_info->get_mysql_cmd() ||
          obmysql::COM_STMT_PREXECUTE == sess_info->get_mysql_cmd()) {
        UNUSED(sess_info->get_current_trace_id().to_string(buf, BUF_LEN));
        gen_.next_column(buf);
      } else {
        gen_.next_column();
      }
      // trans_state
      if (sess_info->is_in_transaction()) {
        gen_.next_column(sess_info->get_tx_desc()->get_tx_state_str());
      } else {
        gen_.next_column();
      }
      // total_time
      if (ObSQLSessionState::QUERY_ACTIVE == sess_info->get_session_state()) {
        // time_sec = current time - sql packet received from easy time
        int64_t time_sec = (::oceanbase::common::ObTimeUtility::current_time() - sess_info->get_query_start_time()) / 1000000;
        gen_.next_column(time_sec);
      } else {
        int64_t time_sec = (::oceanbase::common::ObTimeUtility::current_time() - sess_info->get_cur_state_start_time()) / 1000000;
        gen_.next_column(time_sec);
      }
      // retry_cnt
      gen_.next_column(sess_info->get_retry_info().get_retry_cnt());
      // retry_info
      gen_.next_column(sess_info->get_retry_info().get_last_query_retry_err());
      // action
      gen_.next_column(sess_info->get_action_name());
      // module
      gen_.next_column(sess_info->get_module_name());
      // client_info
      gen_.next_column(sess_info->get_client_info());

      gen_.row_end();
      return !gen_.is_end();
    }
  private:
    LuaVtableGenerator& gen_;
  };

  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG(ERROR, "call select_processlist() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    std::vector<const char*> columns = {
      "id",
      "user",
      "tenant",
      "host",
      "db",
      "command",
      "sql_id",
      "time",
      "state",
      "info",
      "sql_port",
      "proxy_sessid",
      "master_sessid",
      "user_client_ip",
      "user_host",
      "trans_id",
      "thread_id",
      "ssl_cipher",
      "trace_id",
      "trans_state",
      "total_time",
      "retry_cnt",
      "retry_info",
      "action",
      "module",
      "client_info"
    };
    LuaVtableGenerator gen(L, columns);
    GetProcess iter(gen);
    if (OB_FAIL(GCTX.session_mgr_->for_each_session(iter))) {
      OB_LOG(ERROR, "failed to select_processlist", K(ret));
    }
  }
  return 1;
}

// list{list, list...} = select_sysstat()
int select_sysstat(lua_State* L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG(ERROR, "call select_sysstat() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    common::ObVector<uint64_t> ids;
    std::vector<const char*> columns = {
      "tenant_id",
      "statistic",
      "value",
      "value_type",
      "stat_id",
      "name",
      "class",
      "can_visible"
    };
    GCTX.omt_->get_tenant_ids(ids);
    LuaVtableGenerator gen(L, columns);
    for (int64_t i = 0; i < ids.size() && !gen.is_end(); ++i) {
      ObArenaAllocator diag_allocator;
      HEAP_VAR(ObDiagnoseTenantInfo, diag_info, &diag_allocator) {
        if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_the_diag_info(ids.at(i), diag_info))) {
          OB_LOG(ERROR, "failed to get_the_diag_info", K(ids.at(i)), K(ret));
        } else if (OB_FAIL(observer::ObAllVirtualSysStat::update_all_stats(ids.at(i), diag_info.get_set_stat_stats()))) {
          OB_LOG(ERROR, "failed to update_all_stats", K(ids.at(i)), K(ret));
        } else {
          for (int64_t stat_idx = 0;
               stat_idx < ObStatEventIds::STAT_EVENT_SET_END && !gen.is_end();
               ++stat_idx) {
            if (ObStatEventIds::STAT_EVENT_ADD_END == stat_idx) {
              // do nothing
            } else {
              gen.next_row();
              // tenant_id
              gen.next_column(ids.at(i));
              // statistic
              {
                if (stat_idx < ObStatEventIds::STAT_EVENT_ADD_END) {
                  gen.next_column(stat_idx);
                } else {
                  gen.next_column(stat_idx - 1);
                }
              }
              // value
              {
                if (stat_idx < ObStatEventIds::STAT_EVENT_ADD_END) {
                  if (OB_ISNULL(diag_info.get_add_stat_stats().get(stat_idx))) {
                    gen.next_column();
                    OB_LOG(ERROR, "invalid argument", K(stat_idx), K(ids.at(i)));
                  } else {
                    gen.next_column(diag_info.get_add_stat_stats().get(stat_idx)->stat_value_);
                  }
                } else {
                  if (OB_ISNULL(diag_info.get_set_stat_stats().get(stat_idx - ObStatEventIds::STAT_EVENT_ADD_END - 1))) {
                    gen.next_column();
                    OB_LOG(ERROR, "invalid argument", K(stat_idx), K(ids.at(i)));
                  } else {
                    gen.next_column(diag_info.get_set_stat_stats().get(stat_idx - ObStatEventIds::STAT_EVENT_ADD_END - 1)->stat_value_);
                  }
                }
              }
              // value_type
              {
                if (stat_idx < ObStatEventIds::STAT_EVENT_ADD_END) {
                  gen.next_column("ADD_VALUE");
                } else {
                  gen.next_column("SET_VALUE");
                }
              }
              // stat_id
              gen.next_column(OB_STAT_EVENTS[stat_idx].stat_id_);
              // name
              gen.next_column(OB_STAT_EVENTS[stat_idx].name_);
              // class
              gen.next_column(OB_STAT_EVENTS[stat_idx].stat_class_);
              // can_visible
              gen.next_column(OB_STAT_EVENTS[stat_idx].can_visible_);

              gen.row_end();
            }
          }
        }
      }
    }
  }
  return 1;
}

// list{list, list...} = select_memory_info()
int select_memory_info(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG(ERROR, "call select_memory_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    uint64_t *tenant_ids = nullptr;
    std::vector<const char*> columns = {
      "tenant_id",
      "ctx_id",
      "ctx_name",
      "label",
      "hold",
      "used",
      "count"
    };
    LuaVtableGenerator gen(L, columns);
    int tenant_cnt = 0;
    if (OB_NOT_NULL(tenant_ids = (uint64_t *)diagnose::alloc(OB_MAX_SERVER_TENANT_CNT * sizeof(uint64_t)))) {
      get_tenant_ids(tenant_ids, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);
    }
    for (int64_t i = 0; i < tenant_cnt && !gen.is_end(); ++i) {
      auto tenant_id = tenant_ids[i];
      for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ++ctx_id) {
        auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
        if (nullptr == ta) {
          ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                                      ctx_id);
        }
        if (nullptr == ta) {
          // do nothing
        } else {
          IGNORE_RETURN ta->iter_label([&](lib::ObLabel &label, LabelItem *l_item)
          {
            const char *mod_name = label.str_;
            int64_t hold = l_item->hold_;
            int64_t used = l_item->used_;
            int64_t count = l_item->count_;

            gen.next_row();
            // tenant_id
            gen.next_column(tenant_id);
            // ctx_id
            gen.next_column(ctx_id);
            // ctx_name
            gen.next_column(get_global_ctx_info().get_ctx_name(ctx_id));
            // label
            gen.next_column(mod_name);
            // hold
            gen.next_column(hold);
            // used
            gen.next_column(used);
            // count
            gen.next_column(count);

            gen.row_end();
            return OB_SUCCESS;
          });
        }
      }
    }

    diagnose::free(tenant_ids);
  }
  return 1;
}

// list{list, list...} = select_tenant_ctx_memory_info()
int select_tenant_ctx_memory_info(lua_State *L)
{
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "call select_tenant_ctx_memory_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    uint64_t *tenant_ids = nullptr;
    int tenant_cnt = 0;
    std::vector<const char*> columns = {
      "tenant_id",
      "ctx_id",
      "ctx_name",
      "hold",
      "used",
      "limit"
    };
    LuaVtableGenerator gen(L, columns);
    if (OB_NOT_NULL(tenant_ids = (uint64_t *)diagnose::alloc(OB_MAX_SERVER_TENANT_CNT * sizeof(uint64_t)))) {
      get_tenant_ids(tenant_ids, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);
    }

    for (int64_t i = 0; i < tenant_cnt && !gen.is_end(); ++i) {
      auto tenant_id = tenant_ids[i];
      for (int64_t ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ++ctx_id) {
        auto ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
        if (nullptr == ta) {
          ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id,
                                                                                      ctx_id);
        }
        if (nullptr == ta) {
          // do nothing
        } else {
          gen.next_row();

          // tenant_id
          gen.next_column(tenant_id);
          // ctx_id
          gen.next_column(ctx_id);
          // ctx_name
          gen.next_column(get_global_ctx_info().get_ctx_name(ctx_id));
          // hold
          gen.next_column(ta->get_hold());
          // used
          gen.next_column(ta->get_used());
          // limit
          gen.next_column(ta->get_limit());

          gen.row_end();
        }
      }
    }

    diagnose::free(tenant_ids);
  }
  return 1;
}

// list{list, list...} = select_trans_stat()
int select_trans_stat(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  ObTransService *trans_service = nullptr;
  ObArray<uint64_t> tenant_ids;
  ObTxStat tx_stat;
  if (argc > 1) {
    OB_LOG(ERROR, "call select_trans_stat() failed, bad arguments count, should be 0.");
    lua_pushnil(L);
  } else if (OB_ISNULL(GCTX.omt_)) {
    OB_LOG(ERROR, "omt is nullptr");
    lua_pushnil(L);
  } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
    OB_LOG(ERROR, "failed to get tenant_ids", K(ret));
    lua_pushnil(L);
  } else {
    HEAP_VAR(ObTxStatIterator, iter) {
      iter.reset();
      for (int i = 0; i < tenant_ids.count() && OB_SUCC(ret); ++i) {
        uint64_t tenant_id = tenant_ids.at(i);
        MTL_SWITCH(tenant_id) {
          auto* txs = MTL(transaction::ObTransService*);
          if (OB_FAIL(txs->iterate_all_observer_tx_stat(iter))) {
            OB_LOG(ERROR, "iterate transaction stat failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        lua_pushnil(L);
      } else if (OB_FAIL(iter.set_ready())) {
        OB_LOG(ERROR, "ObTxStatIterator set ready error", K(ret));
        lua_pushnil(L);
      } else {
        ret = iter.get_next(tx_stat);
      }
      static constexpr int64_t OB_MAX_BUFFER_SIZE = 512;
      static constexpr int64_t OB_MIN_BUFFER_SIZE = 128;
      std::vector<const char*> columns = {
        "tenant_id",
        "tx_type",
        "tx_id",
        "session_id",
        "scheduler_addr",
        "is_decided",
        "ls_id",
        "participants",
        "tx_ctx_create_time",
        "expired_time",
        "ref_cnt",
        "last_op_sn",
        "pending_write",
        "state",
        "part_tx_action",
        "tx_ctx_addr",
        "mem_ctx_id",
        "pending_log_size",
        "flushed_log_size",
        "role_state",
        "is_exiting",
        "coord",
        "last_request_ts",
        "gtrid",
        "bqual",
        "format_id"
      };
      LuaVtableGenerator gen(L, columns);
      while (OB_SUCC(ret) && !gen.is_end()) {
        gen.next_row();
        // tenant_id
        gen.next_column(tx_stat.tenant_id_);
        // tx_type
        gen.next_column(tx_stat.tx_type_);
        // tx_id
        gen.next_column(tx_stat.tx_id_.get_id());
        // session_id
        gen.next_column(tx_stat.session_id_);
        // scheduler_addr
        char addr_buf[MAX_IP_PORT_LENGTH + 8];
        tx_stat.scheduler_addr_.to_string(addr_buf, MAX_IP_PORT_LENGTH + 8);
        gen.next_column(addr_buf);
        // is_decided
        gen.next_column(tx_stat.has_decided_);
        // ls_id
        gen.next_column(tx_stat.ls_id_.id());
        // participants
        if (0 < tx_stat.participants_.count()) {
          char participants_buffer[OB_MAX_BUFFER_SIZE];
          tx_stat.participants_.to_string(participants_buffer, OB_MAX_BUFFER_SIZE);
          gen.next_column(participants_buffer);
        } else {
          ret = iter.get_next(tx_stat);
        }
        // tx_ctx_create_time
        gen.next_column(tx_stat.tx_ctx_create_time_);
        // expired_time
        gen.next_column(tx_stat.tx_expired_time_);
        // ref_cnt
        gen.next_column(tx_stat.ref_cnt_);
        // last_op_sn
        gen.next_column(tx_stat.last_op_sn_);
        // pending_write
        gen.next_column(tx_stat.pending_write_);
        // state
        gen.next_column(tx_stat.state_);
        // part_tx_action
        gen.next_column(tx_stat.part_tx_action_);
        // tx_ctx_addr
        gen.next_column((uint64_t)tx_stat.tx_ctx_addr_);
        // mem_ctx_id
        gen.next_column(-1);
        // pending_log_size
        gen.next_column(tx_stat.pending_log_size_);
        // flushed_log_size
        gen.next_column(tx_stat.flushed_log_size_);
        // role_state
        gen.next_column(tx_stat.role_state_);
        // is_exiting
        gen.next_column(tx_stat.is_exiting_);
        // coord
        gen.next_column(tx_stat.coord_.id());
        // last_request_ts
        gen.next_column(tx_stat.last_request_ts_);
        // gtrid
        gen.next_column(tx_stat.xid_.get_gtrid_str());
        // bqual
        gen.next_column(tx_stat.xid_.get_bqual_str());
        // format_id
        gen.next_column(tx_stat.xid_.get_format_id());

        gen.row_end();
        ret = iter.get_next(tx_stat);
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      OB_LOG(ERROR, "iter failed", K(ret));
    }
  }
  return 1;
}

// list{list, list...} = select_sql_workarea_active()
int select_sql_workarea_active(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG(ERROR, "call select_sql_workarea_active() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else if (OB_ISNULL(GCTX.omt_)) {
    OB_LOG(ERROR, "GCTX.omt_ is NULL");
    lua_pushnil(L);
  } else {
    ObArray<uint64_t> ids;
    common::ObSEArray<sql::ObSqlWorkareaProfileInfo, 32> wa_actives;
    std::vector<const char*> columns = {
      "plan_id",
      "sql_id",
      "sql_exec_id",
      "operation_type",
      "operation_id",
      "sid",
      "active_time",
      "work_area_size",
      "expect_size",
      "actual_mem_used",
      "max_mem_used",
      "number_passes",
      "tempseg_size",
      "tenant_id",
      "policy"
    };
    LuaVtableGenerator gen(L, columns);
    GCTX.omt_->get_mtl_tenant_ids(ids);
    for (int64_t i = 0; i < ids.size() && !gen.is_end(); ++i) {
      uint64_t tenant_id = ids[i];
      wa_actives.reset();
      MTL_SWITCH(tenant_id) {
        auto *sql_mem_mgr = MTL(ObTenantSqlMemoryManager*);
        if (OB_NOT_NULL(sql_mem_mgr) && OB_FAIL(sql_mem_mgr->get_all_active_workarea(wa_actives))) {
          OB_LOG(ERROR, "failed to get workarea stat", K(ret));
        }
      }
      for (int64_t j = 0; j < wa_actives.count() && !gen.is_end(); ++j) {
        auto &wa_active = wa_actives[j];
        gen.next_row();
        // plan_id
        gen.next_column(wa_active.plan_id_);
        // sql_id
        gen.next_column(wa_active.sql_id_);
        // sql_exec_id
        gen.next_column(wa_active.sql_exec_id_);
        // operation_type
        gen.next_column(get_phy_op_name(wa_active.profile_.get_operator_type()));
        // operation_id
        gen.next_column(wa_active.profile_.get_operator_id());
        // sid
        gen.next_column(wa_active.session_id_);
        // active_time
        gen.next_column(ObTimeUtility::current_time() - wa_active.profile_.get_active_time());
        // work_area_size
        gen.next_column(wa_active.profile_.get_cache_size());
        // expected_size
        gen.next_column(wa_active.profile_.get_expect_size());
        // actual_mem_used
        gen.next_column(wa_active.profile_.get_mem_used());
        // max_mem_used
        gen.next_column(wa_active.profile_.get_max_mem_used());
        // number_passes
        gen.next_column(wa_active.profile_.get_number_pass());
        // tempseg_size
        gen.next_column(wa_active.profile_.get_dumped_size());
        // tenant_id
        gen.next_column(tenant_id);
        // policy
        if (wa_active.profile_.get_auto_policy()) {
          gen.next_column(EXECUTION_AUTO_POLICY);
        } else {
          gen.next_column(EXECUTION_MANUAL_POLICY);
        }

        gen.row_end();
      }
    }
  }
  return 1;
}

// list{list, list...} = select_sys_task_status()
int select_sys_task_status(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  HEAP_VAR(share::ObSysStatMgrIter, iter) {
    if (argc > 1) {
      OB_LOG(ERROR, "call select_sys_task_status() failed, bad arguments count, should be less than 2.");
      lua_pushnil(L);
    } else if (OB_FAIL(SYS_TASK_STATUS_MGR.get_iter(iter))) {
      OB_LOG(ERROR, "failed to get iter", K(ret));
      lua_pushnil(L);
    } else if (!iter.is_ready()) {
      OB_LOG(ERROR, "iter not ready");
    } else {
      share::ObSysTaskStat status;
      std::vector<const char*> columns = {
        "start_time",
        "task_type",
        "task_id",
        "tenant_id",
        "comment",
        "is_cancel"
      };
      LuaVtableGenerator gen(L, columns);
      while (OB_SUCC(iter.get_next(status)) && !gen.is_end()) {
        gen.next_row();
        // start_time
        gen.next_column(status.start_time_);
        // task_type
        gen.next_column(sys_task_type_to_str(status.task_type_));
        // task_id
        {
          char task_id[common::OB_TRACE_STAT_BUFFER_SIZE];
          int64_t n = status.task_id_.to_string(task_id, sizeof(task_id));
          if (n < 0 || n >= sizeof(task_id)) {
            OB_LOG(ERROR, "buffer not enough");
            gen.next_column();
          } else {
            gen.next_column(task_id);
          }
        }
        // tenant_id
        gen.next_column(status.tenant_id_);
        // comment
        gen.next_column(status.comment_);
        // is_cancel
        gen.next_column(status.is_cancel_);

        gen.row_end();
      }
    }
  }
  return 1;
}

// list{list, list...} = select_dump_tenant_info()
int select_dump_tenant_info(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  auto *omt = GCTX.omt_;
  if (argc > 1) {
    OB_LOG(ERROR, "call select_dump_tenant_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else if (OB_ISNULL(omt)) {
    OB_LOG(ERROR, "omt is nullptr");
    lua_pushnil(L);
  } else {
    auto *omt = GCTX.omt_;
    std::vector<const char*> columns = {
      "tenant_id",
      "compat_mode",
      "unit_min_cpu",
      "unit_max_cpu",
      "slice",
      "remain_slice",
      "token_cnt",
      "ass_token_cnt",
      "lq_tokens",
      "used_lq_tokens",
      "stopped",
      "idle_us",
      "recv_hp_rpc_cnt",
      "recv_np_rpc_cnt",
      "recv_lp_rpc_cnt",
      "recv_mysql_cnt",
      "recv_task_cnt",
      "recv_large_req_cnt",
      "recv_large_queries",
      "actives",
      "workers",
      "lq_warting_workers",
      "req_queue_total_size",
      "queue_0",
      "queue_1",
      "queue_2",
      "queue_3",
      "queue_4",
      "queue_5",
      "large_queued"
    };
    LuaVtableGenerator gen(L, columns);
    auto func = [&] (omt::ObTenant &t) {
      gen.next_row();
      // tenant_id
      gen.next_column(t.id_);
      // compat_mode
      gen.next_column(static_cast<int64_t>(t.get_compat_mode()));
      // unit_min_cpu
      gen.next_column(t.unit_min_cpu_);
      // unit_max_cpu
      gen.next_column(t.unit_max_cpu_);
      // slice
      gen.next_column(0);
      // remain_slice
      gen.next_column(0);
      // token_cnt
      gen.next_column(t.worker_count());
      // ass_token_cnt
      gen.next_column(t.worker_count());
      // lq_tokens
      gen.next_column(0);
      // used_lq_tokens
      gen.next_column(0);
      // stopped
      gen.next_column(t.stopped_);
      // idle_us
      gen.next_column(0);
      // recv_hp_rpc_cnt
      gen.next_column(t.recv_hp_rpc_cnt_);
      // recv_np_rpc_cnt
      gen.next_column(t.recv_np_rpc_cnt_);
      // recv_lp_rpc_cnt
      gen.next_column(t.recv_lp_rpc_cnt_);
      // recv_mysql_cnt
      gen.next_column(t.recv_mysql_cnt_);
      // recv_task_cnt
      gen.next_column(t.recv_task_cnt_);
      // recv_large_req_cnt
      gen.next_column(t.recv_large_req_cnt_);
      // recv_large_queries
      gen.next_column(t.tt_large_quries_);
      // actives
      gen.next_column(t.workers_.get_size());
      // workers
      gen.next_column(t.workers_.get_size());
      // lq_warting_workers
      gen.next_column(0);
      // req_queue_total_size
      gen.next_column(t.req_queue_.size());
      // queue_0
      gen.next_column(t.req_queue_.queue_size(0));
      // queue_1
      gen.next_column(t.req_queue_.queue_size(1));
      // queue_2
      gen.next_column(t.req_queue_.queue_size(2));
      // queue_3
      gen.next_column(t.req_queue_.queue_size(3));
      // queue_4
      gen.next_column(t.req_queue_.queue_size(4));
      // queue_5
      gen.next_column(t.req_queue_.queue_size(5));
      // large_queued
      gen.next_column(t.lq_retry_queue_size());
      gen.row_end();
      return OB_SUCCESS;
    };
    if (OB_FAIL(omt->for_each(func))) {
      OB_LOG(ERROR, "omt for each failed", K(ret));
    }
  }
  return 1;
}

// list{list, list...} = select_disk_stat()
int select_disk_stat(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
  int64_t data_disk_abnormal_time = 0;
  if (argc > 1) {
    OB_LOG(ERROR, "call select_disk_stat() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else if (OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs,
                     data_disk_abnormal_time))) {
    OB_LOG(WARN, "get device health status failed", K(ret));
    lua_pushnil(L);
  } else {
    std::vector<const char*> columns = {
      "total_size",
      "used_size",
      "free_size",
      "is_disk_valid",
      "disk_error_begin_ts"
    };
    LuaVtableGenerator gen(L, columns);
    gen.next_row();

    // total_size
    gen.next_column(OB_SERVER_BLOCK_MGR.get_total_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size());
    // used_size
    gen.next_column(OB_SERVER_BLOCK_MGR.get_used_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size());
    // free_size
    gen.next_column(OB_SERVER_BLOCK_MGR.get_free_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size());
    // is_disk_valid
    gen.next_column(DEVICE_HEALTH_NORMAL != dhs ? 0 : 1);
    // disk_error_begin_ts
    gen.next_column(DEVICE_HEALTH_NORMAL != dhs ? data_disk_abnormal_time : 0);

    gen.row_end();
  }
  return 1;
}

// list{list, list...} = select_tenant_memory_info()
int select_tenant_memory_info(lua_State *L)
{
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "call select_tenant_memory_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    uint64_t *tenant_ids = nullptr;
    int tenant_cnt = 0;
    std::vector<const char*> columns = {
      "tenant_id",
      "hold",
      "limit"
    };
    LuaVtableGenerator gen(L, columns);
    if (OB_NOT_NULL(tenant_ids = (uint64_t *)diagnose::alloc(OB_MAX_SERVER_TENANT_CNT * sizeof(uint64_t)))) {
      get_tenant_ids(tenant_ids, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);
    }
    for (int64_t i = 0; i < tenant_cnt && !gen.is_end(); ++i) {
      auto tenant_id = tenant_ids[i];
      gen.next_row();
      // tenant_id
      gen.next_column(tenant_id);
      // hold
      gen.next_column(ObMallocAllocator::get_instance()->get_tenant_hold(tenant_id));
      // limit
      gen.next_column(ObMallocAllocator::get_instance()->get_tenant_limit(tenant_id));

      gen.row_end();
    }
    diagnose::free(tenant_ids);
  }
  return 1;
}

// list{list, list...} = select_mem_leak_checker_info()
int select_mem_leak_checker_info(lua_State *L)
{
  int argc = lua_gettop(L);
  ObMemLeakChecker* leak_checker = &get_mem_leak_checker();
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call select_mem_leak_checker_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else if (OB_ISNULL(leak_checker)) {
    OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "leak checker is null");
    lua_pushnil(L);
  } else {
    int ret = OB_SUCCESS;
    ObMemLeakChecker::mod_info_map_t info_map;
    if (OB_FAIL(info_map.create(10000))) {
      OB_LOG(ERROR, "failed to create hashmap", K(ret));
    } else if (OB_FAIL(leak_checker->load_leak_info_map(info_map))) {
      OB_LOG(ERROR, "failed to collection leak info", K(ret));
    } else {
      std::vector<const char*> columns = {
        "mod_name",
        "mod_type",
        "alloc_count",
        "alloc_size",
        "back_trace"
      };
      LuaVtableGenerator gen(L, columns);
      for (auto it = info_map->begin(); it != info_map->end() && !gen.is_end(); ++it) {
        gen.next_row();
        // mod_name
        gen.next_column(leak_checker->get_str());
        // mod_type
        gen.next_column("user");
        // alloc_count
        gen.next_column(it->second.first);
        // alloc_size
        gen.next_column(it->second.second);
        // back_trace
        gen.next_column(it->first.bt_);

        gen.row_end();
      }
    }
  }
  return 1;
}

// list{list, list...} = select_compaction_diagnose_info()
int select_compaction_diagnose_info(lua_State *L)
{
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call select_compaction_diagnose_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    int ret = OB_SUCCESS;
    compaction::ObCompactionDiagnoseIterator diagnose_info_iter;
    // OB_SYS_TENANT_ID means dump all
    if (OB_FAIL(diagnose_info_iter.open(OB_SYS_TENANT_ID))) {
      OB_LOG(ERROR, "Fail to open suggestion iter", K(ret));
      lua_pushnil(L);
    } else {
      std::vector<const char*> columns = {
        "tenant_id",
        "merge_type",
        "ls_id",
        "tablet_id",
        "status",
        "create_time",
        "diagnose_info"
      };
      LuaVtableGenerator gen(L, columns);
      compaction::ObCompactionDiagnoseInfo diagnose_info;
      while (OB_SUCC(diagnose_info_iter.get_next_info(diagnose_info)) && !gen.is_end()) {
        gen.next_row();
        // tenant_id
        gen.next_column(diagnose_info.tenant_id_);
        // merge_type
        gen.next_column(merge_type_to_str(diagnose_info.merge_type_));
        // ls_id
        gen.next_column(diagnose_info.ls_id_);
        // tablet_id
        gen.next_column(diagnose_info.tablet_id_);
        // status
        gen.next_column(diagnose_info.get_diagnose_status_str(diagnose_info.status_));
        // create_time
        gen.next_column(diagnose_info.timestamp_);
        // diagnose_info
        gen.next_column(diagnose_info.diagnose_info_);

        gen.row_end();
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        OB_LOG(ERROR, "Fail to get next suggestion info", K(ret));
      }
    }
  }
  return 1;
}

// list{list, list...} = select_server_schema_info()
int select_server_schema_info(lua_State *L)
{
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call select_server_schema_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    int ret = OB_SUCCESS;
    const static int64_t DEFAULT_TENANT_NUM = 10;
    ObSEArray<uint64_t, DEFAULT_TENANT_NUM> tenant_ids;
    share::schema::ObSchemaGetterGuard guard;
    auto& schema_service = OBSERVER.get_root_service().get_schema_service();
    if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      OB_LOG(ERROR, "fail to get schema guard", K(ret));
      lua_pushnil(L);
    } else if (OB_FAIL(guard.get_tenant_ids(tenant_ids))) {
      OB_LOG(ERROR, "fail to get tenant_ids", K(ret));
      lua_pushnil(L);
    } else {
      std::vector<const char*> columns = {
        "tenant_id",
        "refreshed_schema_version",
        "received_schema_version",
        "schema_count",
        "schema_size",
        "min_sstable_schema_version"
      };
      LuaVtableGenerator gen(L, columns);
      for (uint64_t idx = 0; idx < tenant_ids.count() && !gen.is_end(); ++idx) {
        const uint64_t tenant_id = tenant_ids[idx];
        int64_t refreshed_schema_version = OB_INVALID_VERSION;
        int64_t received_schema_version = OB_INVALID_VERSION;
        int64_t schema_count = OB_INVALID_ID;
        int64_t schema_size = OB_INVALID_ID;
        if (OB_FAIL(schema_service.get_tenant_refreshed_schema_version(tenant_id, refreshed_schema_version))) {
          OB_LOG(ERROR, "fail to get tenant refreshed schema version", K(ret), K(tenant_id), K(refreshed_schema_version));
        } else if (OB_FAIL(schema_service.get_tenant_received_broadcast_version(tenant_id, received_schema_version))) {
          OB_LOG(ERROR, "fail to get tenant receieved schema version", K(ret), K(tenant_id), K(received_schema_version));
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = schema_service.get_tenant_schema_guard(tenant_id, guard))) {
            OB_LOG(ERROR, "fail to get schema guard", K(tmp_ret), K(tenant_id));
          } else if (OB_SUCCESS != (tmp_ret = guard.get_schema_count(tenant_id, schema_count))) {
            OB_LOG(ERROR, "fail to get schema count", K(tmp_ret), K(tenant_id));
          } else if (OB_SUCCESS != (tmp_ret = guard.get_schema_size(tenant_id, schema_size))) {
            OB_LOG(ERROR, "fail to get schema size", K(tmp_ret), K(tenant_id));
          }
          gen.next_row();
          // tenant_id
          gen.next_column(tenant_id);
          // refreshed_schema_version
          gen.next_column(refreshed_schema_version);
          // received_schema_version
          gen.next_column(received_schema_version);
          // schema_count
          gen.next_column(schema_count);
          // schema_size
          gen.next_column(schema_size);
          // min_sstable_schema_version
          gen.next_column(OB_INVALID_VERSION);

          gen.row_end();
        }
      }
    }
  }
  return 1;
}

// list{list, list...} = select_schema_slot()
int select_schema_slot(lua_State *L)
{
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call select_schema_slot() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else {
    int ret = OB_SUCCESS;
    auto& schema_service = OBSERVER.get_root_service().get_schema_service();
    const static int64_t DEFAULT_TENANT_NUM = 10;
    ObSEArray<uint64_t, DEFAULT_TENANT_NUM> tenant_ids;
    if (OB_FAIL(schema_service.get_schema_store_tenants(tenant_ids))) {
      OB_LOG(ERROR, "fail to get schema store tenants", K(ret));
      lua_pushnil(L);
    } else {
      std::vector<const char*> columns = {
        "tenant_id",
        "slot_id",
        "schema_version",
        "schema_count",
        "total_ref_cnt",
        "ref_info"
      };
      LuaVtableGenerator gen(L, columns);
      for (int64_t idx = 0; idx < tenant_ids.count() && !gen.is_end(); ++idx) {
        const static int64_t DEFAULT_SLOT_NUM = 32;
        ObSEArray<ObSchemaSlot, DEFAULT_SLOT_NUM> schema_slot_infos;
        uint64_t tenant_id = tenant_ids[idx];
        if (OB_FAIL(schema_service.get_tenant_slot_info(get_global_allocator(), tenant_id, schema_slot_infos))) {
          OB_LOG(ERROR, "fail to get tenant slot info", K(ret), K(tenant_id));
        } else {
          for (int64_t slot_idx = 0; slot_idx < schema_slot_infos.count() && !gen.is_end(); ++slot_idx) {
            auto& schema_slot = schema_slot_infos.at(slot_idx);
            gen.next_row();
            // tenant_id
            gen.next_column(schema_slot.get_tenant_id());
            // slot_id
            gen.next_column(schema_slot.get_slot_id());
            // schema_version
            gen.next_column(schema_slot.get_schema_version());
            // schema_count
            gen.next_column(schema_slot.get_schema_count());
            // total_ref_cnt
            gen.next_column(schema_slot.get_ref_cnt());
            // ref_info
            if (OB_NOT_NULL(schema_slot.get_mod_ref_infos().ptr())) {
              gen.next_column(schema_slot.get_mod_ref_infos());
            } else {
              gen.next_column("");
            }

            gen.row_end();
          }
        }
        for (int64_t slot_idx = 0; slot_idx < schema_slot_infos.count(); ++slot_idx) {
          auto* ptr = schema_slot_infos.at(slot_idx).get_mod_ref_infos().ptr();
          if (OB_NOT_NULL(ptr)) {
            get_global_allocator().free((void*)ptr);
          }
          schema_slot_infos.at(slot_idx).reset();
        }
        schema_slot_infos.reset();
      }
    }
  }
  return 1;
}

#define GET_OTHER_TSI_ADDR(var_name, addr) \
const int64_t var_name##_offset = ((int64_t)addr - (int64_t)pthread_self()); \
decltype(*addr) var_name = *(decltype(addr))(thread_base + var_name##_offset);

// list{list, list...} = dump_threads_info()
int dump_thread_info(lua_State *L)
{
  int argc = lua_gettop(L);
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call dump_thread_info() failed, bad arguments count, should be less than 2.");
  } else {
    std::vector<const char*> columns = {
      "tname",
      "tid",
      "thread_base",
      "loop_ts",
      "latch_hold",
      "latch_wait",
      "trace_id",
      "status",
      "wait_event"
    };
    LuaVtableGenerator gen(L, columns);
    pid_t pid = getpid();
    StackMgr::Guard guard(g_stack_mgr);
    for(auto* header = *guard; OB_NOT_NULL(header) && !gen.is_end(); header = guard.next()) {
      auto* thread_base = (char*)(header->pth_);
      if (OB_NOT_NULL(thread_base)) {
        // avoid SMART_CALL stack
        gen.next_row();
        // tname
        GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
        // PAY ATTENTION HERE
        gen.next_column(thread_base + tname_offset);
        // tid
        GET_OTHER_TSI_ADDR(tid, &get_tid_cache());
        gen.next_column(tid);
        // thread_base
        {
          char addr[32];
          snprintf(addr, 32, "%p", thread_base);
          gen.next_column(addr);
        }
        // loop_ts
        GET_OTHER_TSI_ADDR(loop_ts, &oceanbase::lib::Thread::loop_ts_);
        gen.next_column(loop_ts);
        // latch_hold
        {
          char addrs[256];
          GET_OTHER_TSI_ADDR(locks_addr, &(ObLatch::current_locks[0]));
          GET_OTHER_TSI_ADDR(slot_cnt, &ObLatch::max_lock_slot_idx)
          const int64_t cnt = std::min(ARRAYSIZEOF(ObLatch::current_locks), (int64_t)slot_cnt);
          decltype(&locks_addr) locks = (decltype(&locks_addr))(thread_base + locks_addr_offset);
          addrs[0] = 0;
          for (int64_t i = 0, j = 0; i < cnt; ++i) {
            int64_t idx = (slot_cnt + i) % ARRAYSIZEOF(ObLatch::current_locks);
            if (OB_NOT_NULL(locks[idx]) && j < 256) {
              bool has_segv = false;
              uint32_t val = 0;
              struct iovec local_iov = {&val, sizeof(val)};
              struct iovec remote_iov = {locks[idx], sizeof(val)};
              ssize_t n = process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0);
              if (n != sizeof(val)) {
              } else if (0 != val) {
                j += snprintf(addrs + j, 256 - j, "%p ", locks[idx]);
              }
            }
          }
          if (0 == addrs[0]) {
            gen.next_column("NULL");
          } else {
            gen.next_column(addrs);
          }
        }
        // latch_wait
        GET_OTHER_TSI_ADDR(wait_addr, &ObLatch::current_wait);
        if (OB_NOT_NULL(wait_addr)) {
          char addr[32];
          snprintf(addr, 32, "%p", wait_addr);
          gen.next_column(addr);
        } else {
          gen.next_column("NULL");
        }
        // trace_id
        {
          GET_OTHER_TSI_ADDR(trace_id, ObCurTraceId::get_trace_id());
          char trace_id_buf[40];
          IGNORE_RETURN trace_id.to_string(trace_id_buf, 40);
          gen.next_column(trace_id_buf);
        }
        // status
        GET_OTHER_TSI_ADDR(blocking_ts, &Thread::blocking_ts_);
        {
          GET_OTHER_TSI_ADDR(join_addr, &Thread::thread_joined_);
          GET_OTHER_TSI_ADDR(sleep_us, &Thread::sleep_us_);
          const char* status_str = nullptr;
          if (0 != join_addr) {
            status_str = "Join";
          } else if (0 != sleep_us) {
            status_str = "Sleep";
          } else if (0 != blocking_ts) {
            status_str = "Wait";
          } else {
            status_str = "Run";
          }
          gen.next_column(status_str);
        }
        // wait_event
        {
          GET_OTHER_TSI_ADDR(wait_addr, &ObLatch::current_wait);
          GET_OTHER_TSI_ADDR(join_addr, &Thread::thread_joined_);
          GET_OTHER_TSI_ADDR(sleep_us, &Thread::sleep_us_);
          GET_OTHER_TSI_ADDR(rpc_dest_addr, &Thread::rpc_dest_addr_);
          GET_OTHER_TSI_ADDR(event, &Thread::wait_event_);
          constexpr int64_t BUF_LEN = 64;
          char wait_event[BUF_LEN];
          ObAddr addr;
          struct iovec local_iov = {&addr, sizeof(ObAddr)};
          struct iovec remote_iov = {thread_base + rpc_dest_addr_offset, sizeof(ObAddr)};
          wait_event[0] = '\0';
          if (0 != join_addr) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "thread %u %ld", *(uint32_t*)(join_addr + tid_offset), tid_offset);
          } else if (OB_NOT_NULL(wait_addr)) {
            uint32_t val = 0;
            struct iovec local_iov = {&val, sizeof(val)};
            struct iovec remote_iov = {wait_addr, sizeof(val)};
            ssize_t n = process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0);
            if (n != sizeof(val)) {
            } else if (0 != (val & (1<<30))) {
              IGNORE_RETURN snprintf(wait_event, BUF_LEN, "wrlock on %u", val & 0x3fffffff);
            } else {
              IGNORE_RETURN snprintf(wait_event, BUF_LEN, "%u rdlocks", val & 0x3fffffff);
            }
          } else if (sizeof(ObAddr) == process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0)
                     && addr.is_valid()) {
            GET_OTHER_TSI_ADDR(pcode, &Thread::pcode_);
            int64_t pos1 = 0;
            int64_t pos2 = 0;
            if (((pos1 = snprintf(wait_event, 37, "rpc 0x%X(%s", pcode, obrpc::ObRpcPacketSet::instance().name_of_idx(obrpc::ObRpcPacketSet::instance().idx_of_pcode(pcode)) + 3)) > 0)
                && ((pos2 = snprintf(wait_event + std::min(36L, pos1), 6, ") to ")) > 0)) {
              int64_t pos = std::min(36L, pos1) + std::min(5L, pos2);
              pos += addr.to_string(wait_event + pos, BUF_LEN - pos);
            }
          } else if (0 != blocking_ts && (0 != (Thread::WAIT_IN_TENANT_QUEUE & event))) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "tenant worker request");
          } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_IO_EVENT & event))) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "IO events");
          } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_LOCAL_RETRY & event))) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "local retry");
          } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_PX_MSG & event))) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "px message");
          } else if (0 != sleep_us) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "%ld us", sleep_us);
          } else if (0 != blocking_ts) {
            IGNORE_RETURN snprintf(wait_event, BUF_LEN, "%ld us", common::ObTimeUtility::fast_current_time() - blocking_ts);
          }
          gen.next_column(wait_event);
        }
        gen.row_end();
      }
    }
  }
  return 1;
}

// list{list, list...} = select_malloc_sample_info()
int select_malloc_sample_info(lua_State *L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  ObMallocSampleMap malloc_sample_map;
  if (argc > 1) {
    OB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "call select_malloc_sample_info() failed, bad arguments count, should be less than 2.");
    lua_pushnil(L);
  } else if (OB_FAIL(malloc_sample_map.create(1000, "MallocInfoMap", "MallocInfoMap"))) {
    OB_LOG(ERROR, "failed to create hashmap", K(ret));
  } else if (OB_FAIL(ObMemoryDump::get_instance().load_malloc_sample_map(malloc_sample_map))) {
    OB_LOG(ERROR, "failed to create memory info map", K(ret));
  } else {
    std::vector<const char*> columns = {
      "tenant_id",
      "ctx_id",
      "mod_name",
      "back_trace",
      "ctx_name",
      "alloc_count",
      "alloc_bytes"
    };
    LuaVtableGenerator gen(L, columns);
    for (auto it = malloc_sample_map.begin(); it != malloc_sample_map.end() && !gen.is_end(); ++it) {
      gen.next_row();
      // tenant_id
      gen.next_column(it->first.tenant_id_);
      // ctx_id
      gen.next_column(it->first.ctx_id_);
      // mod_name
      gen.next_column(it->first.label_);
      // back_trace
      {
        char bt[MAX_BACKTRACE_LENGTH];
        IGNORE_RETURN parray(bt, sizeof(bt), (int64_t*)it->first.bt_, AOBJECT_BACKTRACE_COUNT);
        gen.next_column(bt);
      }
      // ctx_name
      gen.next_column(get_global_ctx_info().get_ctx_name(it->first.ctx_id_));
      // alloc_count
      gen.next_column(it->second.alloc_count_);
      // alloc_bytes
      gen.next_column(it->second.alloc_bytes_);

      gen.row_end();
    }
  }
  return 1;
}

// int = set_system_tenant_limit_mode(int)
int set_system_tenant_limit_mode(lua_State* L)
{
  int ret = OB_SUCCESS;
  int argc = lua_gettop(L);
  if (1 != argc) {
    OB_LOG(ERROR, "call set_system_tenant_limit_mode() failed, bad arguments count, should be 1.");
    ret = OB_INVALID_ARGUMENT;
  } else {
    luaL_checktype(L, 1, LUA_TNUMBER);
    const int64_t limit_mode = lua_tointeger(L, 1);
#ifdef ENABLE_500_MEMORY_LIMIT
    GMEMCONF.set_500_tenant_limit(limit_mode);
#endif
    lua_pushinteger(L, 1);
  }
  if (OB_FAIL(ret)) {
    lua_pushinteger(L, -1);
  }
  return 1;
}

// API end

int get_tenant_sysstat(int64_t tenant_id, int64_t statistic, int64_t &value)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator diag_allocator;
  HEAP_VAR(ObDiagnoseTenantInfo, diag_info, &diag_allocator) {
    if (statistic < 0
        || statistic >= ObStatEventIds::STAT_EVENT_SET_END
        || ObStatEventIds::STAT_EVENT_ADD_END == statistic) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_the_diag_info(tenant_id, diag_info))) {
      // do nothing
    } else if (OB_FAIL(observer::ObAllVirtualSysStat::update_all_stats(tenant_id, diag_info.get_set_stat_stats()))) {
      // do nothing
    } else if (statistic < ObStatEventIds::STAT_EVENT_ADD_END) {
      ObStatEventAddStat* stat = diag_info.get_add_stat_stats().get(statistic);
      if (OB_ISNULL(stat)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        value = stat->stat_value_;
      }
    } else {
      ObStatEventSetStat* stat = diag_info.get_set_stat_stats().get(statistic - ObStatEventIds::STAT_EVENT_ADD_END - 1);
      if (OB_ISNULL(stat)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        value = stat->stat_value_;
      }
    }
  }
  return ret;
}

int summary_each_eio_info(char *buf, int &pos, int buf_len, easy_io_t *eio, const char *rpc_des)
{
  int ret = OB_SUCCESS;
  int io_th_count = 0;
  easy_io_thread_t *ioth = NULL;
  easy_connection_t *c = NULL;
  int len = 0;
  char output[512];

  if (OB_ISNULL(buf) || OB_ISNULL(eio) || OB_ISNULL(rpc_des)) {
    ret = OB_SUCCESS;
  } else if (pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
      len = strlen(rpc_des);
      if (pos + len > buf_len) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        strncpy(buf + pos, rpc_des, len);
        pos += len;
        io_th_count = eio->io_thread_count;

        for (int i = 0; i < io_th_count && (!ret); i++) {
          ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, i);
          snprintf(output, sizeof(output), "  ioth%d, rx_doing_request_count:%d, rx_done_request_count:%lu, "
		  	"tx_doing_request_count:%d, tx_done_request_count:%lu\n",
		    i,ioth->rx_doing_request_count, ioth->rx_done_request_count,
			ioth->tx_doing_request_count, ioth->tx_done_request_count);
          len = strlen(output);
          if (pos + len > buf_len) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            strncpy(buf + pos, output, len);
            pos += len;
            easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node) {
              snprintf(output, sizeof(output), "    connection:%s, doing:%d, done:%lu\n", easy_connection_str(c),
                      c->con_summary->doing_request_count, c->con_summary->done_request_count);
              len = strlen(output);
              if (len + pos > buf_len) {
                ret = OB_INVALID_ARGUMENT;
                break;
              }
              strncpy(buf + pos, output, len);
              pos += len;
            }
          }
        }
      }
    }

  return ret;
}

void *diagnose::alloc(const int size)
{
  return get_global_allocator().alloc(size);
}

void diagnose::free(void *ptr)
{
  if (OB_NOT_NULL(ptr)) {
    get_global_allocator().free(ptr);
  }
}

void APIRegister::register_api(lua_State* L)
{
  lua_register(L, "usage", usage);
  lua_register(L, "print_to_client", print_to_client);
  lua_register(L, "now", now);
  lua_register(L, "get_tenant_id_list", get_tenant_id_list);
  lua_register(L, "get_tenant_mem_limit", get_tenant_mem_limit);
  lua_register(L, "get_tenant_sysstat_by_id", get_tenant_sysstat_by_id);
  lua_register(L, "get_tenant_sysstat_by_name", get_tenant_sysstat_by_name);
  lua_register(L, "get_easy_diagnose_info", get_easy_diagnose_info);
  lua_register(L, "get_tenant_pcode_statistics", get_tenant_pcode_statistics);
  lua_register(L, "select_processlist", select_processlist);
  lua_register(L, "select_sysstat", select_sysstat);
  lua_register(L, "select_memory_info", select_memory_info);
  lua_register(L, "select_tenant_ctx_memory_info", select_tenant_ctx_memory_info);
  lua_register(L, "select_trans_stat", select_trans_stat);
  lua_register(L, "select_sql_workarea_active", select_sql_workarea_active);
  lua_register(L, "select_sys_task_status", select_sys_task_status);
  lua_register(L, "select_dump_tenant_info", select_dump_tenant_info);
  lua_register(L, "select_disk_stat", select_disk_stat);
  lua_register(L, "select_tenant_memory_info", select_tenant_memory_info);
  lua_register(L, "set_log_probe", set_log_probe);
  lua_register(L, "show_log_probe", show_log_probe);
  lua_register(L, "select_mem_leak_checker_info", select_mem_leak_checker_info);
  lua_register(L, "select_compaction_diagnose_info", select_compaction_diagnose_info);
  lua_register(L, "select_server_schema_info", select_server_schema_info);
  lua_register(L, "select_schema_slot", select_schema_slot);
  lua_register(L, "dump_thread_info", dump_thread_info);
  lua_register(L, "select_malloc_sample_info", select_malloc_sample_info);
  lua_register(L, "set_system_tenant_limit_mode", set_system_tenant_limit_mode);
}

int APIRegister::flush()
{
  int ret = OB_SUCCESS;
  if (0 == print_offset_) {
    // do nothing
  } else if (conn_fd_ < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    print_buffer_[print_offset_++] = '\0';
    if (send(conn_fd_, print_buffer_, print_offset_, 0) < 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "failed to send", K(errno));
    }
    print_offset_ = 0;
    memset(print_buffer_, 0, print_capacity_);
  }
  return ret;
}

int APIRegister::append(const char *buffer, const int len)
{
  int ret = OB_SUCCESS;
  if (print_offset_ + len + 1 < print_capacity_) {
    strncpy(print_buffer_ + print_offset_, buffer, len);
    print_offset_ += len;
  } else if (OB_FAIL(flush())) {
    OB_LOG(ERROR, "failed to flush", K(ret));
  } else if (print_offset_ + len + 1 >= print_capacity_) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    strncpy(print_buffer_ + print_offset_, buffer, len);
    print_offset_ += len;
  }
  return ret;
}
