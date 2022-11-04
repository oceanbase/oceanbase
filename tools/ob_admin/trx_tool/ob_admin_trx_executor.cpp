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

#include "storage/transaction/ob_trans_define.h"
#include "share/ob_rpc_struct.h"
#include "ob_admin_trx_executor.h"

#define HELP_FMT "\t%-30s%-12s\n"
namespace oceanbase
{
using namespace oceanbase::transaction;
namespace tools
{

ObAdminTransExecutor::ObAdminTransExecutor()
  : inited_(false),
    status_(-1),
    trans_version_(-1),
    end_log_ts_(-1),
    cmd_(-1),
    timeout_(3 * 1000 * 1000)
{
}

ObAdminTransExecutor::~ObAdminTransExecutor()
{
}


int ObAdminTransExecutor::parse_options(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  static const int MAX_IP_LEN = 15;
  static const int MAX_PORT_LEN = 5;
  int opt = 0;
  const char* opt_string = "hc:i:p:t:k:s:v:e:";
  uint64_t table_id;
  int64_t partition_id;
  int64_t partition_cnt;
  int64_t trans_inc;
  int64_t trans_timestamp;
  char ipport[MAX_IP_LEN + MAX_PORT_LEN + 2] = "\0";
  common::ObString dst_host;
  int32_t dst_port = 0;

  struct option longopts[] = {
    // commands
    { "help", 0, NULL, 'h' },
    { "command", 1, NULL, 'c' },
    // options
    { "ip", 1, NULL, 'i' },
    { "port", 1, NULL, 'p' },
    { "table-key", 1, NULL, 't' },
    { "trans-key", 1, NULL, 'k' },
    { "trans-status", 1, NULL, 's' },
    { "trans-version", 1, NULL, 'v'},
    { "end-log-ts", 1, NULL, 'e'},
  };

  int index = -1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, &index)) != -1) {
    switch (opt) {
    case 'h': {
      print_usage();
      exit(1);
    }
    case 'c': {
      if (0 == strcmp(optarg, "modify")) {
        cmd_ = MODIFY;
      } else if (0 == strcmp(optarg, "dump")) {
        cmd_ = DUMP;
      } else if (0 == strcmp(optarg, "kill")) {
        cmd_ = KILL;
      } else {
        print_usage();
        exit(1);
      }
      break;
    }  // case
    case 's': {
      if (0 == strcmp(optarg, "RUNNING")) {
        status_ = int(ObTransTableStatusType::RUNNING);
      } else if (0 == strcmp(optarg, "COMMIT")) {
        status_ = int(ObTransTableStatusType::COMMIT);
      } else if (0 == strcmp(optarg, "ABORT")) {
        status_ = int(ObTransTableStatusType::ABORT);
      }
      break;
    }
    case 'i': {
      dst_host.assign_ptr(optarg, strlen(optarg));
      break;
    }
    case 'p': {
      dst_port = static_cast<int32_t>(strtol(optarg, NULL, 10));
      if (dst_port <= 0 || dst_port >= 65536) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "port not valid:", K(dst_port));
      }
      break;
    }
    case 't': {
      if (3 != sscanf(optarg, "%ld:%lu:%ld", &table_id, &partition_id, &partition_cnt)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument", K(ret), K(partition_id), K(partition_cnt), K(table_id));
      } else if (OB_FAIL(pkey_.init(table_id, partition_id, partition_cnt))) {
        COMMON_LOG(WARN, "init partition key failed.", K(table_id), K(partition_id), K(partition_cnt));
      }
      break;
    }
    case 'k': {
      common::ObAddr server;
      if (3 != sscanf(optarg, "%ld:%ld:%s", &trans_inc, &trans_timestamp, ipport)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument", K(ret), K(trans_inc), K(trans_timestamp), K(ipport));
      } else if (OB_FAIL(server.parse_from_cstring(ipport))) {
        COMMON_LOG(WARN, "get server failed.", K(ret), K(ipport));
      } else {
        transaction::ObTransID trans_id(server, trans_inc, trans_timestamp);
        trans_id_ = trans_id;
      }
      break;
    }
    case 'v': {
      if (1 != sscanf(optarg, "%ld", &trans_version_)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument", K(ret), K(trans_version_));
      }
      break;
    }
    case 'e': {
      if (1 != sscanf(optarg, "%ld", &end_log_ts_)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument", K(ret), K(end_log_ts_));
      }
      break;
    }
    default:
    {
      print_usage();
      exit(1);
    }
    }  // switch
    if (OB_FAIL(ret)) {
      print_usage();
      exit(1);
    }
  }
  dst_server_.set_ip_addr(dst_host, dst_port);
  return ret;
}

void ObAdminTransExecutor::print_usage()
{
  printf("\n");
  printf("Usage: trx_tool command [command args]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");
  printf(HELP_FMT, "-c,--command", "modify,modify part trans ctx; dump, dump part trans ctx; kill, kill the trans ctx.");

  printf("options:\n");
  printf(HELP_FMT, "-i,--ip", "the sever ip of the part trans ctx");
  printf(HELP_FMT, "-p,--port", "server port of the part trans ctx");
  printf(HELP_FMT, "-t,--table-key", "table key: table_id:partition_id:partition_cnt");
  printf(HELP_FMT, "-k,--trans-key", "trans key: trans_inc:timestamp:ip:port");
  printf(HELP_FMT, "-s,--trans-status", "trans status: RUNNING/COMMIT/ABORT");
  printf(HELP_FMT, "-v,--trans-version", "the commit version or abort version");
  printf(HELP_FMT, "-e,--end-log-ts", "the timestamp of the last log");
  printf("samples:\n");
  printf("  modify part trans ctx: \n");
  printf("\tob_admin trx_tool -c modify -i 127.0.0.1 -p 1201 -t 1100611139453780:0:0 -k 70935:1626946927718124:1.1.1.1:1201 -s ABORT -v 1626946927718124 -e 1626946927718125\n");
  printf("  dump part trans ctx: \n");
  printf("\tob_admin trx_tool -c dump -i 127.0.0.1 -p 1201 -t 1100611139453780:0:0 -k 70935:1626946927718124:1.1.1.1:1201\n");
}

int ObAdminTransExecutor::modify_trans()
{
  int ret = OB_SUCCESS;
  obrpc::ObTrxToolArg arg;
  obrpc::ObTrxToolRes res;
  arg.trans_id_ = trans_id_;
  arg.status_ = status_;
  arg.trans_version_ = trans_version_;
  arg.end_log_ts_ = end_log_ts_;
  arg.cmd_ = cmd_;
  if (OB_FAIL(srv_proxy_.handle_part_trans_ctx(arg, res))) {
    COMMON_LOG(ERROR, "send req fail", K(ret), K(arg));
  }
  COMMON_LOG(INFO, "modify_trans finish", K(ret), K(arg), K(res));
  return ret;
}

int ObAdminTransExecutor::dump_trans()
{
  int ret = OB_SUCCESS;
  obrpc::ObTrxToolArg arg;
  obrpc::ObTrxToolRes res;
  arg.trans_id_ = trans_id_;
  arg.cmd_ = cmd_;
  if (OB_FAIL(srv_proxy_.handle_part_trans_ctx(arg, res))) {
    COMMON_LOG(ERROR, "send req fail", K(ret), K(arg));
  } else {
    fprintf(stdout, "trans_info: %s\n", res.trans_info_.ptr());
  }
  COMMON_LOG(INFO, "dump_trans finish", K(ret), K(arg), K(res));
  return ret;
}

int ObAdminTransExecutor::kill_trans()
{
  int ret = OB_NOT_SUPPORTED;
  COMMON_LOG(ERROR, "not support", K(ret));
  return ret;
}

int ObAdminTransExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(parse_options(argc, argv))) {
    COMMON_LOG(INFO, "cmd is", K(cmd_));
    if (OB_FAIL(client_.init())) {
      COMMON_LOG(WARN, "client init failed", K(ret));
    } else if (OB_FAIL(client_.get_proxy(srv_proxy_))) {
      COMMON_LOG(WARN, "get_proxy failed", K(ret));
    } else {
      srv_proxy_.set_server(dst_server_);
      srv_proxy_.set_timeout(timeout_);
      uint64_t tenant_id = pkey_.get_tenant_id();
      srv_proxy_.set_tenant(tenant_id);
      inited_ = true;
      switch(cmd_) {
      case MODIFY:
        ret = modify_trans();
        break;
      case DUMP:
        ret = dump_trans();
        break;
      case KILL:
        ret = kill_trans();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      COMMON_LOG(INFO, "cmd ret", K(ret), K(cmd_));
    }
  }
  return ret;
}

}
}
