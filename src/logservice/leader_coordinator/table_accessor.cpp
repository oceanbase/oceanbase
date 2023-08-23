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


#include "share/ob_errno.h"
#include "share/ob_occam_time_guard.h"
#include "share/rc/ob_tenant_base.h"
#include "table_accessor.h"
#include "lib/hash/ob_hash.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "observer/ob_server_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_ls_id.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{
TableAccessor::ServerZoneNameCache TableAccessor::SERVER_ZONE_NAME_CACHE;
using namespace common;

int get_tenant_server_list(common::ObIArray<common::ObAddr> &tenant_server_list)
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  tenant_server_list.reset();

  return ret;
}

LsElectionReferenceInfoRow::LsElectionReferenceInfoRow(const uint64_t tenant_id, const share::ObLSID &ls_id)
: tenant_id_(tenant_id), ls_id_(ls_id), exec_tenant_id_(get_private_table_exec_tenant_id(tenant_id_)) {}

LsElectionReferenceInfoRow::~LsElectionReferenceInfoRow()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  if (trans_.is_started()) {
    COORDINATOR_LOG_(WARN, "LsElectionReferenceInfoRow destroyed with begined transaction");
    if (CLICK_FAIL(trans_.end(false))) {
      COORDINATOR_LOG_(WARN, "LsElectionReferenceInfoRow roll back transaction failed");
    }
  }
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::begin_()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(trans_.start(GCTX.sql_proxy_, exec_tenant_id_))) {
    COORDINATOR_LOG(WARN, "fail to start trans", KR(ret), K(*this));
  } else {
    COORDINATOR_LOG(INFO, "success to start trans", KR(ret), K(*this));
  }
  return ret;
}

int LsElectionReferenceInfoRow::end_(const bool true_to_commit)
{
  LC_TIME_GUARD(1_s);
  return trans_.end(true_to_commit);
}

int LsElectionReferenceInfoRow::change_zone_priority(const ObArray<ObArray<ObStringHolder>> &zone_list_list)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret), K(zone_list_list)
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(start_and_read_())) {
    COORDINATOR_LOG_(WARN, "failed when start trans, read row, convert info");
  } else if (CLICK_FAIL(row_for_user_.element<2>().assign(zone_list_list))) {
    COORDINATOR_LOG_(WARN, "fail to assign zone list list");
  } else if (CLICK_FAIL(write_and_commit_())) {
    COORDINATOR_LOG_(WARN, "failed when convert info, write row, end trans");
  } else {
    COORDINATOR_LOG_(INFO, "change_zone_priority");
  }
  if (trans_.is_started()) {
    COORDINATOR_LOG_(WARN, "transaction execute failed");
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = end_(false))) {
      COORDINATOR_LOG_(WARN, "fail to roll back transaction");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(schedule_refresh_priority_task_())) {
    COORDINATOR_LOG_(WARN, "failed to schedule refresh priority task", KR(ret));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::change_manual_leader(const common::ObAddr &manual_leader_server)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret), K(manual_leader_server)
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(start_and_read_())) {
    COORDINATOR_LOG_(WARN, "failed when start trans, read row, convert info");
  } else if (FALSE_IT(row_for_user_.element<3>() = manual_leader_server)) {
  } else if (CLICK_FAIL(write_and_commit_())) {
    COORDINATOR_LOG_(WARN, "failed when convert info, write row, end trans");
  } else {
    COORDINATOR_LOG_(INFO, "change_manual_leader");
  }
  if (trans_.is_started()) {
    COORDINATOR_LOG_(WARN, "transaction execute failed");
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = end_(false))) {
      COORDINATOR_LOG_(WARN, "fail to roll back transaction");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(schedule_refresh_priority_task_())) {
    COORDINATOR_LOG_(WARN, "failed to schedule refresh priority task", KR(ret));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::add_server_to_blacklist(const common::ObAddr &server, InsertElectionBlacklistReason reason)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret), K(server), K(reason)
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COORDINATOR_LOG_(WARN, "server is invalid or reason is empty");
  } else {
    if (CLICK_FAIL(start_and_read_())) {
      COORDINATOR_LOG_(WARN, "failed when start trans, read row, convert info");
    } else {
      for (int64_t idx = 0; idx < row_for_user_.element<4>().count() && OB_SUCC(ret); ++idx) {
        if (row_for_user_.element<4>().at(idx).element<0>() == server) {
          ret = OB_ENTRY_EXIST;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (CLICK_FAIL(row_for_user_.element<4>().push_back(ObTuple<ObAddr, ObStringHolder>()))) {
          COORDINATOR_LOG_(WARN, "failed to create new tuple for reason");
        } else if (FALSE_IT(row_for_user_.element<4>().at(row_for_user_.element<4>().count() - 1).element<0>() = server)) {
        } else if (CLICK_FAIL(row_for_user_.element<4>().at(row_for_user_.element<4>().count() - 1).element<1>().assign(to_cstring(reason)))) {
          COORDINATOR_LOG_(WARN, "copy reason failed");
        } else if (CLICK_FAIL(write_and_commit_())) {
          COORDINATOR_LOG_(WARN, "failed when convert info, write row, end trans");
        } else {
          COORDINATOR_LOG_(INFO, "add_server_to_blacklist");
        }
      }
    }
  }
  if (trans_.is_started()) {
    COORDINATOR_LOG_(WARN, "transaction execute failed");
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = end_(false))) {
      COORDINATOR_LOG_(WARN, "fail to roll back transaction");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(schedule_refresh_priority_task_())) {
    COORDINATOR_LOG_(WARN, "failed to schedule refresh priority task", KR(ret));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::delete_server_from_blacklist(const common::ObAddr &server)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret), K(server)
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COORDINATOR_LOG_(WARN, "server is invalid");
  } else {
    if (CLICK_FAIL(start_and_read_())) {
      COORDINATOR_LOG_(WARN, "failed when start trans, read row, convert info");
    } else {
      ObArray<ObTuple<ObAddr, ObStringHolder>> &old_array = row_for_user_.element<4>();
      ObArray<ObTuple<ObAddr, ObStringHolder>> new_array;
      for (int64_t idx = 0; idx < old_array.count() && OB_SUCC(ret); ++idx) {
        if (old_array.at(idx).element<0>() != server) {
          if (CLICK_FAIL(new_array.push_back(old_array.at(idx)))) {
            COORDINATOR_LOG_(WARN, "push tuple to new array failed");
          }
        }
      }
      if (old_array.count() == new_array.count()) {
        ret = OB_ENTRY_NOT_EXIST;
        COORDINATOR_LOG_(WARN, "this server not in blacklist column");
      } else if (CLICK_FAIL(old_array.assign(new_array))) {
        COORDINATOR_LOG_(WARN, "replace old array with new array failed");
      } else if (CLICK_FAIL(write_and_commit_())) {
        COORDINATOR_LOG_(WARN, "failed when convert info, write row, end trans");
      } else {
        COORDINATOR_LOG_(INFO, "delete_server_from_blacklist");
      }
    }
  }
  if (trans_.is_started()) {
    COORDINATOR_LOG_(WARN, "transaction execute failed");
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = end_(false))) {
      COORDINATOR_LOG_(WARN, "fail to roll back transaction");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(schedule_refresh_priority_task_())) {
    COORDINATOR_LOG_(WARN, "failed to schedule refresh priority task", KR(ret));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::set_or_replace_server_in_blacklist(
    const common::ObAddr &server,
    InsertElectionBlacklistReason reason)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret), K(server), K(reason)
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COORDINATOR_LOG_(WARN, "server is invalid or reason is empty");
  } else if (CLICK_FAIL(start_and_read_())) {
    COORDINATOR_LOG_(WARN, "failed when start trans, read row, convert info");
  } else if (CLICK_FAIL(set_user_row_for_specific_reason_(server, reason))) {
    COORDINATOR_LOG_(WARN, "set_user_row_for_specific_reason_ failed");
  } else if (CLICK_FAIL(write_and_commit_())) {
    COORDINATOR_LOG_(WARN, "failed when convert info, write row, end trans");
  } else {
    COORDINATOR_LOG_(INFO, "set_or_replace_server_in_blacklist", K(server), "reason", to_cstring(reason));
  }
  if (trans_.is_started()) {
    COORDINATOR_LOG_(WARN, "transaction execute failed");
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = end_(false))) {
      COORDINATOR_LOG_(WARN, "fail to roll back transaction");
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(schedule_refresh_priority_task_())) {
    COORDINATOR_LOG_(WARN, "failed to schedule refresh priority task", KR(ret));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::set_user_row_for_specific_reason_(
    const common::ObAddr &server,
    InsertElectionBlacklistReason reason)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret), K(server), K(reason)
  int ret = OB_SUCCESS;
  const char *reason_str = to_cstring(reason);
  int64_t server_idx = -1;
  int64_t number_of_the_reason = 0;
  for (int64_t idx = 0; idx < row_for_user_.element<4>().count(); ++idx) {
    if (row_for_user_.element<4>().at(idx).element<0>() == server) {
      server_idx = idx;
    }
    if (row_for_user_.element<4>().at(idx).element<1>() == reason_str) {
      number_of_the_reason += 1;
    }
  }
  if (-1 != server_idx && 1 == number_of_the_reason) {
    ret = OB_ENTRY_EXIST;
  } else {
    ObArray<ObTuple<ObAddr, ObStringHolder>> &old_array = row_for_user_.element<4>();
    ObArray<ObTuple<ObAddr, ObStringHolder>> new_array;
    for (int64_t idx = 0; idx < old_array.count() && OB_SUCC(ret); ++idx) {
      if (old_array.at(idx).element<1>() != reason_str) {
        if (CLICK_FAIL(new_array.push_back(old_array.at(idx)))) {
          COORDINATOR_LOG_(WARN, "push tuple to new array failed");
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(old_array.assign(new_array))) {
      COORDINATOR_LOG_(WARN, "replace old array with new array failed");
    } else if (CLICK_FAIL(row_for_user_.element<4>().push_back(ObTuple<ObAddr, ObStringHolder>()))) {
      COORDINATOR_LOG_(WARN, "failed to create new tuple for reason");
    } else if (FALSE_IT(row_for_user_.element<4>().at(row_for_user_.element<4>().count() - 1).element<0>() = server)) {
    } else if (CLICK_FAIL(row_for_user_.element<4>().at(row_for_user_.element<4>().count() - 1).element<1>().assign(to_cstring(reason)))) {
      COORDINATOR_LOG_(WARN, "copy reason failed");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::start_and_read_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret)
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(begin_())) {
    COORDINATOR_LOG_(WARN, "start transaction failed");
  } else if (CLICK_FAIL(get_row_from_table_())) {
    COORDINATOR_LOG_(WARN, "read row from table failed");
  } else if (CLICK_FAIL(convert_table_info_to_user_info_())) {
    COORDINATOR_LOG_(WARN, "convert table info to user info failed");
  } else {
    COORDINATOR_LOG_(INFO, "read __all_ls_election_reference_info");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::write_and_commit_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret)
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(convert_user_info_to_table_info_())) {
    COORDINATOR_LOG_(WARN, "convert user info to table info failed");
  } else if (CLICK_FAIL(update_row_to_table_())) {
    COORDINATOR_LOG_(WARN, "update column failed");
  } else if (CLICK_FAIL(end_(true))) {
    COORDINATOR_LOG_(WARN, "commit change failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::schedule_refresh_priority_task_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER K(*this), KR(ret)
  int ret = OB_SUCCESS;
  ObLeaderCoordinator* coordinator = MTL(ObLeaderCoordinator*);
  if (OB_ISNULL(coordinator)) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG_(ERROR, "unexpected null of leader coordinator", KR(ret));
  } else if (OB_FAIL(coordinator->schedule_refresh_priority_task())) {
    COORDINATOR_LOG_(WARN, "failed to schedule refresh priority task", KR(ret));
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::convert_user_info_to_table_info_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  row_for_user_.element<0>() = tenant_id_;
  row_for_user_.element<1>() = ls_id_;
  row_for_table_.element<0>() = tenant_id_;
  row_for_table_.element<1>() = ls_id_.id();
  ObArray<ObStringHolder> temp_holders;
  for (int64_t idx = 0; idx < row_for_user_.element<2>().count() && OB_SUCC(ret); ++idx) {
    char buffer[STACK_BUFFER_SIZE] = {0};
    if (CLICK_FAIL(ObTableAccessHelper::join_string_array_with_begin_end(buffer, STACK_BUFFER_SIZE, "", "", ",", row_for_user_.element<2>().at(idx)))) {
      COORDINATOR_LOG_(WARN, "join string failed");
    } else if (CLICK_FAIL(temp_holders.push_back(ObStringHolder()))) {
      COORDINATOR_LOG_(WARN, "get where condition string failed");
    } else if (CLICK_FAIL(temp_holders[temp_holders.count() - 1].assign(ObString(strlen(buffer), buffer)))) {
      COORDINATOR_LOG_(WARN, "get where condition string failed");
    }
  }
  if (OB_SUCC(ret)) {
    char buffer[STACK_BUFFER_SIZE] = {0};
    if (CLICK_FAIL(ObTableAccessHelper::join_string_array_with_begin_end(buffer, STACK_BUFFER_SIZE, "", "", ";", temp_holders))) {
      COORDINATOR_LOG_(WARN, "join string failed");
    } else if (CLICK_FAIL(row_for_table_.element<2>().assign(ObString(ObString(strlen(buffer), buffer))))) {
      COORDINATOR_LOG_(WARN, "create StringHolder failed");
    } else if (FALSE_IT(memset(buffer, 0, STACK_BUFFER_SIZE))) {
    } else if (CLICK_FAIL(row_for_user_.element<3>().ip_port_to_string(buffer, STACK_BUFFER_SIZE))) {
      COORDINATOR_LOG_(WARN, "ip port to string failed");
    } else if (CLICK_FAIL(row_for_table_.element<3>().assign(ObString(strlen(buffer), buffer)))) {
      COORDINATOR_LOG_(WARN, "create ObStingHolder failed");
    } else {
      ObArray<ObStringHolder> remove_addr_and_reason_list;
      for (int64_t idx = 0; idx < row_for_user_.element<4>().count() && OB_SUCC(ret); ++idx) {
        ObStringHolder ip_port_holder;
        if (FALSE_IT(memset(buffer, 0, STACK_BUFFER_SIZE))) {
        } else if (CLICK_FAIL(row_for_user_.element<4>().at(idx).element<0>().ip_port_to_string(buffer, STACK_BUFFER_SIZE))) {
          COORDINATOR_LOG_(WARN, "ip port to string failed");
        } else if (CLICK_FAIL(ip_port_holder.assign(ObString(strlen(buffer), buffer)))) {
          COORDINATOR_LOG_(WARN, "create ObStringHolder failed");
        } else if (FALSE_IT(memset(buffer, 0, STACK_BUFFER_SIZE))) {
        } else if (CLICK_FAIL(ObTableAccessHelper::join_string(buffer, STACK_BUFFER_SIZE, "", ip_port_holder.get_ob_string(), ObString("("), row_for_user_.element<4>().at(idx).element<1>(), ObString(")")))) {
          COORDINATOR_LOG_(WARN, "join string failed");
        } else if (CLICK_FAIL(remove_addr_and_reason_list.push_back(ObStringHolder()))) {
          COORDINATOR_LOG_(WARN, "push back new string holder failed");
        } else if (CLICK_FAIL(remove_addr_and_reason_list.at(remove_addr_and_reason_list.count() - 1).assign(ObString(strlen(buffer), buffer)))) {
          COORDINATOR_LOG_(WARN, "create remove_addr_and_reason string holder failed");
        }
      }
      if (OB_SUCC(ret)) {
        if (FALSE_IT(memset(buffer, 0, STACK_BUFFER_SIZE))) {
        } else if (CLICK_FAIL(ObTableAccessHelper::join_string_array_with_begin_end(buffer, STACK_BUFFER_SIZE, "", "", ";", remove_addr_and_reason_list))) {
        } else if (CLICK_FAIL(row_for_table_.element<4>().assign(ObString(strlen(buffer), buffer)))) {
          COORDINATOR_LOG_(WARN, "create remove_addr_and_reason string holder in row failed");
        } else {
          COORDINATOR_LOG_(INFO, "LsElectionReferenceInfoRow setted success");
        }
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::convert_table_info_to_user_info_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  row_for_table_.element<0>() = tenant_id_;
  row_for_table_.element<1>() = ls_id_.id();
  row_for_user_.element<0>() = tenant_id_;
  row_for_user_.element<1>() = ls_id_;
  row_for_user_.element<2>().reset();
  ObArray<ObStringHolder> split_by_semicolon;
  if (!row_for_table_.element<2>().empty() && CLICK_FAIL(ObTableAccessHelper::split_string_by_char(row_for_table_.element<2>(), ';', split_by_semicolon))) {
    COORDINATOR_LOG_(WARN, "split zone priority by semicolon failed");
  } else {
    for (int64_t idx = 0; idx < split_by_semicolon.count() && OB_SUCC(ret); ++idx) {
      if (CLICK_FAIL(row_for_user_.element<2>().push_back(ObArray<ObStringHolder>()))) {
        COORDINATOR_LOG_(WARN, "push new array to array of array failed");
      } else if (CLICK_FAIL(ObTableAccessHelper::split_string_by_char(split_by_semicolon.at(idx), ',', row_for_user_.element<2>().at(row_for_user_.element<2>().count() - 1)))) {
        COORDINATOR_LOG_(WARN, "split string by ',' failed");
      }
    }
    if (OB_SUCC(ret)) {
      if (!row_for_table_.element<3>().empty() && CLICK_FAIL(row_for_user_.element<3>().parse_from_string(row_for_table_.element<3>().get_ob_string()))) {
        COORDINATOR_LOG_(WARN, "get ObAddr from ObString failed");
      } else {
        ObArray<ObStringHolder> split_by_comma;
        if (!row_for_table_.element<4>().empty() && CLICK_FAIL(ObTableAccessHelper::split_string_by_char(row_for_table_.element<4>(), ';', split_by_comma))) {
          COORDINATOR_LOG_(WARN, "split blacklist by ',' failed");
        } else {
          for (int64_t idx = 0; idx < split_by_comma.count() && OB_SUCC(ret); ++idx) {
            ObString reason = split_by_comma.at(idx).get_ob_string();
            ObString server = reason.split_on('(');
            reason = reason.split_on(')');
            if (server.empty() || reason.empty()) {
              ret = OB_ERR_UNEXPECTED;
              COORDINATOR_LOG_(ERROR, "should not get empty reason or server", K(server), K(reason));
            } else if (CLICK_FAIL(row_for_user_.element<4>().push_back(ObTuple<ObAddr, ObStringHolder>()))) {
              COORDINATOR_LOG_(WARN, "create new addr and reason tuple failed", K(server), K(reason));
            } else if (CLICK_FAIL(row_for_user_.element<4>().at(row_for_user_.element<4>().count() - 1).element<0>().parse_from_string(server))) {
              COORDINATOR_LOG_(WARN, "fail to resolve server ip port", K(server), K(reason));
            } else if (CLICK_FAIL(row_for_user_.element<4>().at(row_for_user_.element<4>().count() - 1).element<1>().assign(reason))) {
              COORDINATOR_LOG_(WARN, "fail to copy removed reason string holder", K(server), K(reason));
            }
          }
        }
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::update_row_to_table_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(*this), K(sql), K(affected_rows)
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (CLICK_FAIL(sql.append_fmt(
    "UPDATE %s SET zone_priority='%s', manual_leader_server='%s', blacklist='%s' WHERE tenant_id=%ld and ls_id=%ld",
    share::OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME,
    to_cstring(row_for_table_.element<2>()), to_cstring(row_for_table_.element<3>()), to_cstring(row_for_table_.element<4>()),
    row_for_table_.element<0>(), row_for_table_.element<1>()))) {
    COORDINATOR_LOG_(WARN, "format insert or update sql failed");
  } else if (!trans_.is_started()) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG_(WARN, "transaction is not started yet");
  } else if (CLICK_FAIL(trans_.write(exec_tenant_id_, sql.ptr(), affected_rows))) {
    COORDINATOR_LOG_(WARN, "fail to do insert or update sql");
  } else {
    COORDINATOR_LOG_(INFO, "success to do insert or update sql");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LsElectionReferenceInfoRow::get_row_from_table_()
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  constexpr int64_t culumn_size = 5;
  const char *columns[culumn_size] = { "tenant_id", "ls_id", "zone_priority", "manual_leader_server", "blacklist" };
  char buffer[STACK_BUFFER_SIZE] = {0};
  int64_t pos = 0;
  if (CLICK_FAIL(databuff_printf(buffer, STACK_BUFFER_SIZE, pos, "where tenant_id=%ld and ls_id=%ld for update", tenant_id_, ls_id_.id()))) {
    COORDINATOR_LOG_(WARN, "data buf printf where condition");
  } else if (!trans_.is_started()) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG_(WARN, "transaction is not started yet");
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(ObTableAccessHelper::get_my_sql_result_(columns, culumn_size, share::OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME, buffer, trans_, exec_tenant_id_, res, result))) {
        COORDINATOR_LOG_(WARN, "fail to get mysql result");
      } else if (OB_NOT_NULL(result)) {
        int64_t iter_times = 0;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (++iter_times > 1) {
            ret = OB_ERR_MORE_THAN_ONE_ROW;
            COORDINATOR_LOG_(WARN, "there are more than one row been selected");
            break;
          } else if (CLICK_FAIL(ObTableAccessHelper::get_values_from_row_<0>(result, columns, row_for_table_.element<0>(), row_for_table_.element<1>(), row_for_table_.element<2>(), row_for_table_.element<3>(), row_for_table_.element<4>()))) {
            COORDINATOR_LOG_(WARN, "failed to get column from row");
          }
        }
        if (OB_ITER_END == ret && 1 == iter_times) {
          ret = OB_SUCCESS;
        } else if (OB_ITER_END == ret && 0 == iter_times) {
          ret = OB_ENTRY_NOT_EXIST;
          COORDINATOR_LOG_(WARN, "iter failed", K(iter_times));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        COORDINATOR_LOG_(WARN, "get mysql result failed");
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::get_all_ls_election_reference_info(common::ObIArray<LsElectionReferenceInfo> &all_ls_election_reference_info)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(all_ls_election_reference_info)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObStringHolder zone_name_holder;
  ObStringHolder region_name_holder;
  ObArray<ObTuple<int64_t, ObStringHolder, ObStringHolder, ObStringHolder>> lines;
  bool is_zone_stopped = false;
  bool is_server_stopped =false;
  bool self_in_primary_region = false;
  const char *columns[4] = {"ls_id", "zone_priority", "manual_leader_server", "blacklist"};
  char where_condition[STACK_BUFFER_SIZE] = {0};
  if (OB_FAIL(SERVER_ZONE_NAME_CACHE.get_zone_name_from_global_cache(zone_name_holder))) {
    if (OB_CACHE_INVALID == ret) {
      if (CLICK_FAIL(get_self_zone_name(zone_name_holder))) {
        COORDINATOR_LOG_(WARN, "get self zone name failed");
      } else {
        SERVER_ZONE_NAME_CACHE.set_zone_name_to_global_cache(zone_name_holder);
      }
    } else {
      COORDINATOR_LOG_(WARN, "fail to get zone name from cache");
    }
  }
  if (zone_name_holder.empty()) {
    COORDINATOR_LOG_(WARN, "zone name is empty");
  } else if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where tenant_id=%ld", MTL_ID()))) {
    COORDINATOR_LOG_(WARN, "get where condition string failed");
  } else if (CLICK_FAIL(ObTableAccessHelper::read_multi_row(get_private_table_exec_tenant_id(MTL_ID()), columns, share::OB_ALL_LS_ELECTION_REFERENCE_INFO_TNAME, where_condition, lines))) {
    COORDINATOR_LOG_(WARN, "read multi row failed");
  } else if (CLICK_FAIL(get_zone_stop_status(zone_name_holder, is_zone_stopped))) {
    COORDINATOR_LOG_(WARN, "get zone stop status failed");
  } else if (CLICK_FAIL(get_server_stop_status(is_server_stopped))) {
    COORDINATOR_LOG_(WARN, "get server stop status and server status failed");
  } else if (CLICK_FAIL(get_self_zone_region(zone_name_holder, region_name_holder))) {
    COORDINATOR_LOG_(WARN, "get self zone region failed");
  } else if (CLICK_FAIL(is_primary_region(region_name_holder, self_in_primary_region))) {
    COORDINATOR_LOG_(WARN, "check self region is primary region failed");
  } else {
    all_ls_election_reference_info.reset();
    for (int64_t idx = 0; idx < lines.count() && OB_SUCC(ret); ++idx) {
      if (CLICK_FAIL(all_ls_election_reference_info.push_back(LsElectionReferenceInfo()))) {
        COORDINATOR_LOG_(WARN, "push back new election reference info failed");
      } else {
        LsElectionReferenceInfo &back = all_ls_election_reference_info.at(all_ls_election_reference_info.count() - 1);
        char ip_port_string[32] = {0};
        back.element<0>() = lines[idx].element<0>();// ls_id
        if (CLICK_FAIL(calculate_zone_priority_score(lines[idx].element<1>()/*zone_priority*/, zone_name_holder, back.element<1>()))) {// self server score
          COORDINATOR_LOG_(WARN, "get self zone score failed");
        } else if (CLICK_FAIL(GCTX.self_addr().ip_port_to_string(ip_port_string, 32))) {
          COORDINATOR_LOG_(WARN, "ip port to string failed");
        } else {
          back.element<2>() = (lines[idx].element<2>().get_ob_string().case_compare(ip_port_string) == 0);// is_manual_leader
          if (CLICK_FAIL(get_removed_status_and_reason(lines[idx].element<3>(), back.element<3>().element<0>(), back.element<3>().element<1>()))) {
            COORDINATOR_LOG_(WARN, "get removed status failed");
          } else {
            back.element<4>() = is_zone_stopped;
            back.element<5>() = is_server_stopped;
            back.element<6>() = self_in_primary_region;
          }
        }
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::get_self_zone_name(ObStringHolder &zone_name_holder)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(columns), K(where_condition), K(zone_name_holder)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *columns[1] = {"zone"};
  char where_condition[STACK_BUFFER_SIZE] = {0};
  char ip_string[INET6_ADDRSTRLEN] = {0};
  if (!GCTX.self_addr().ip_to_string(ip_string, sizeof(ip_string))) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG_(WARN, "ip to string failed");
  } else if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where svr_ip='%s' and svr_port=%d", ip_string, GCTX.self_addr().get_port()))) {
    COORDINATOR_LOG_(WARN, "where condition to string failed");
  } else if (CLICK_FAIL(ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, share::OB_ALL_SERVER_TNAME, where_condition, zone_name_holder))) {
    COORDINATOR_LOG_(WARN, "get zone from __all_server failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::get_self_zone_region(const ObStringHolder &zone_name_holder,
                                        ObStringHolder &region_name_holder)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(columns), K(where_condition), K(zone_name_holder), K(region_name_holder)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *columns[1] = {"info"};
  char where_condition[STACK_BUFFER_SIZE] = {0};
  if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where zone = '%s' and name = 'region'", to_cstring(zone_name_holder)))) {
    COORDINATOR_LOG_(WARN, "where condition to string failed");
  } else if (CLICK_FAIL(ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, share::OB_ALL_ZONE_TNAME, where_condition, region_name_holder))) {
    COORDINATOR_LOG_(WARN, "get zone region from __all_zone failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::is_primary_region(const ObStringHolder &region_name_holder, bool &is_primary_region)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(columns), K(where_condition), K(region_name_holder)
  is_primary_region = false;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *columns[1] = {"primary_zone"};
  char where_condition[STACK_BUFFER_SIZE] = {0};
  ObStringHolder primary_zone_list;
  if (region_name_holder.empty()) {
    ret = OB_INVALID_ARGUMENT;
    COORDINATOR_LOG_(ERROR, "invalid region name");
  } else if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where tenant_id = '%ld'", MTL_ID()))) {
    COORDINATOR_LOG_(WARN, "where condition to string failed");
  } else if (CLICK_FAIL(ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, share::OB_ALL_TENANT_TNAME, where_condition, primary_zone_list))) {
    COORDINATOR_LOG_(WARN, "get primary_zone from __all_tenant failed");
  } else if (primary_zone_list.get_ob_string().case_compare("RANDOM") == 0) {
    is_primary_region = true;
  } else {
    ObArray<ObStringHolder> primary_zone_list_split_by_semicolon;
    ObArray<ObStringHolder> primary_zone_list_split_by_semicolon_comma;
    if (CLICK_FAIL(ObTableAccessHelper::split_string_by_char(primary_zone_list, ';', primary_zone_list_split_by_semicolon))) {
      COORDINATOR_LOG_(WARN, "split primary_zone_list failed");
    } else if (primary_zone_list_split_by_semicolon.empty()) {
      ret = OB_ERR_UNEXPECTED;
      COORDINATOR_LOG_(ERROR, "fail to split primary_zone");
    } else if (CLICK_FAIL(ObTableAccessHelper::split_string_by_char(primary_zone_list_split_by_semicolon[0], ',', primary_zone_list_split_by_semicolon_comma))) {
      COORDINATOR_LOG_(WARN, "split primary_zone_list_split_by_semicolon[0] failed");
    } else if (primary_zone_list_split_by_semicolon_comma.empty()) {
      ret = OB_ERR_UNEXPECTED;
      COORDINATOR_LOG_(ERROR, "fail to split primary_zone_list_split_by_semicolon[0]");
    } else {
      const char *columns[1] = {"info"};
      ObStringHolder primary_region;
      ObStringHolder &one_primary_zone = primary_zone_list_split_by_semicolon_comma[0];
      pos = 0;
      if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where zone = '%s' and name = 'region'", to_cstring(one_primary_zone)))) {
        COORDINATOR_LOG_(WARN, "where condition to string failed");
      } else if (CLICK_FAIL(ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, share::OB_ALL_ZONE_TNAME, where_condition, primary_region))) {
        COORDINATOR_LOG_(WARN, "get primary_region from __all_zone failed");
      } else if (primary_region.empty()) {
        ret = OB_ERR_UNEXPECTED;
        COORDINATOR_LOG_(WARN, "get empty primary zone");
      } else if (primary_region.get_ob_string().case_compare(region_name_holder.get_ob_string()) == 0) {
        is_primary_region = true;
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::calculate_zone_priority_score(ObStringHolder &zone_priority, ObStringHolder &self_zone_name, int64_t &score)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(zone_priority), K(score), K(self_zone_name)
  int ret = OB_SUCCESS;
  ObArray<ObStringHolder> split_by_semicolon;
  score = INT64_MAX;
  if (zone_priority.empty()) {
    COORDINATOR_LOG_(WARN, "zone_priority is empty");
  } else if (CLICK_FAIL(ObTableAccessHelper::split_string_by_char(zone_priority, ';', split_by_semicolon))) {
    COORDINATOR_LOG_(WARN, "split zone priority by semicolon failed");
  } else {
    for (int64_t idx = 0; idx < split_by_semicolon.count() && OB_SUCC(ret) && score == INT64_MAX; ++idx)
    {
      ObArray<ObStringHolder> split_by_comma;
      if (CLICK_FAIL(ObTableAccessHelper::split_string_by_char(split_by_semicolon[idx], ',', split_by_comma))) {
        COORDINATOR_LOG_(WARN, "split string by comma failed", K(split_by_semicolon));
      } else {
        for (int64_t idx2 = 0; idx2 < split_by_comma.count() && score == INT64_MAX; ++idx2) {
          if (split_by_comma[idx2].get_ob_string().case_compare(self_zone_name.get_ob_string()) == 0) {
            score = idx;
            COORDINATOR_LOG_(TRACE, "get zone priority score success");
          }
        }
      }
    }
    if (score == INT64_MAX) {
      COORDINATOR_LOG_(WARN, "zone name not found in zone priority column");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::get_removed_status_and_reason(ObStringHolder &blacklist, bool &status, ObStringHolder &reason)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(blacklist), K(status), K(reason)
  int ret = OB_SUCCESS;
  ObArray<ObStringHolder> arr;
  if (!blacklist.empty() && CLICK_FAIL(ObTableAccessHelper::split_string_by_char(blacklist, ';', arr))) {
    COORDINATOR_LOG_(WARN, "split_string_by_char(;) failed");
  } else {
    char ip_port_string[64] = {0};
    if (CLICK_FAIL(GCTX.self_addr().ip_port_to_string(ip_port_string, 64))) {
      COORDINATOR_LOG_(WARN, "self addr to string failed");
    } else {
      int64_t len = strlen(ip_port_string);
      status = false;
      for (int64_t idx = 0; idx < arr.count() && OB_SUCC(ret) && !status; ++idx) {
        if (memcmp(arr[idx].get_ob_string().ptr(), ip_port_string, len) == 0) {
          ObString str = arr[idx].get_ob_string();
          const char *pos1 = str.find('(');
          const char *pos2 = str.reverse_find(')');
          if (pos1 == nullptr || pos2 == nullptr || pos2 - pos1 < 1) {
            ret = OB_ERR_UNEXPECTED;
            COORDINATOR_LOG_(WARN, "find '(' and ')' failed", K(str));
          } else {
            ObString temp_reason(pos2 - pos1, pos1 + 1);
            if (CLICK_FAIL(reason.assign(temp_reason))) {
              COORDINATOR_LOG_(WARN, "create reason ObStringHolder failed", K(str));
            } else {
              status = true;
            }
          }
        }
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::get_zone_stop_status(ObStringHolder &zone_name, bool &is_zone_stopped)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(zone_name)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *columns[1] = {"value"};
  char where_condition[STACK_BUFFER_SIZE] = {0};
  int64_t value;
  if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where zone='%s' and name='status'", to_cstring(zone_name)))) {
    COORDINATOR_LOG_(WARN, "create where condition failed");
  } else if (CLICK_FAIL(ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, share::OB_ALL_ZONE_TNAME, where_condition, value))) {
    COORDINATOR_LOG_(WARN, "read row failed");
  } else if (value == share::ObZoneStatus::INACTIVE) {
    is_zone_stopped = true;
  } else {
    is_zone_stopped = false;
  }
  return ret;
  #undef PRINT_WRAPPER
}

int TableAccessor::get_server_stop_status(bool &is_server_stopped)
{
  LC_TIME_GUARD(1_s);
  #define PRINT_WRAPPER KR(ret), K(is_server_stopped)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *columns[1] = {"stop_time"};
  char where_condition[STACK_BUFFER_SIZE] = {0};
  char svr_ip_string[16] = {0};
  int64_t stop_time = 0;
  if (!GCTX.self_addr().ip_to_string(svr_ip_string, 16)) {
    ret = OB_ERR_UNEXPECTED;
    COORDINATOR_LOG_(INFO, "self ip to string failed");
  } else if (CLICK_FAIL(databuff_printf(where_condition, STACK_BUFFER_SIZE, pos, "where svr_ip='%s' and svr_port=%d", svr_ip_string, GCTX.self_addr().get_port()))) {
    COORDINATOR_LOG_(INFO, "create where condition failed");
  } else if (ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, columns, share::OB_ALL_SERVER_TNAME, where_condition, stop_time)) {
    COORDINATOR_LOG_(INFO, "read row failed");
  } else {
    is_server_stopped = (stop_time != 0);
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}