/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_client_info_mgr.h"

using namespace oceanbase;
using namespace oceanbase::table;
using namespace oceanbase::common;

int ObTableClientInfoUpdateOp::operator()(MapKV &entry)
{
  int ret = OB_SUCCESS;
  ObTableClientInfo *cli_info = nullptr;
  if (OB_ISNULL(cli_info = entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client info is NULL", K(ret), K(entry.first));
  } else {
    cli_info->last_login_ts_ = ObClockGenerator::getClock();
    char *ptr = cli_info->client_info_str_.ptr();
    if (OB_NOT_NULL(ptr)) {
      allocator_.free(ptr);
    }
    if (OB_FAIL(ob_write_string(allocator_, cli_info_str_, cli_info->client_info_str_))) {
      LOG_WARN("fail to copy client info str ", K(ret), K(cli_info_str_));
    }
  }
  return ret;
}

int ObTableClientInfoScanRetireOp::operator()(MapKV &entry)
{
  int ret = OB_SUCCESS;
  ObTableClientInfo *cli_info = nullptr;
  if (OB_ISNULL(cli_info = entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client info is NULL", K(ret), K(entry.first));
  } else if (OB_FAIL(cli_info_arr_.push_back(std::make_pair(entry.first, cli_info->last_login_ts_)))) {
    LOG_WARN("fail to push client info", K(ret), K(entry.first), KPC(cli_info));
  }
  return ret;
}

int ObTableClientInfoScanRetireOp::get_retired_keys(ObIArray<uint64_t> &retired_arr)
{
  int ret = OB_SUCCESS;
  int64_t cli_info_count = cli_info_arr_.count();
  if (cli_info_count == 0) {
    // do nothing
  } else {
    lib::ob_sort(cli_info_arr_.begin(), cli_info_arr_.end(), ObTableClientInfoScanRetireOp::CompareBySecond());
    bool is_stop = false;
    int64_t cur_ts = ObClockGenerator::getClock();
    int64_t stop_idx = -1;
    for (int64_t i = 0; i < cli_info_count && OB_SUCC(ret) && !is_stop; i++) {
      uint64_t cli_key_hash = cli_info_arr_.at(i).first;
      int64_t last_login_ts = cli_info_arr_.at(i).second;
      if (cur_ts - last_login_ts >= ObTableClientInfoMgr::CLI_INFO_MAX_ALIVE_TS) {
        if (OB_FAIL(retired_arr.push_back(cli_key_hash))) {
          LOG_WARN("fail to push back client id", K(ret), K(cli_key_hash), K(last_login_ts), K(cur_ts));
        }
      } else {
        is_stop = true;
        stop_idx = i;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (cli_info_count - retired_arr.count() > ObTableClientInfoMgr::MAX_CLIENT_INFO_SIZE) {
      int64_t extra_retired_count = cli_info_count - retired_arr.count() - ObTableClientInfoMgr::MAX_CLIENT_INFO_SIZE;
      for (int64_t i = stop_idx; i < extra_retired_count && OB_SUCC(ret); i++) {
        if (OB_FAIL(retired_arr.push_back(cli_info_arr_.at(i).first))) {
          LOG_WARN("fail to push back client id", K(ret), K(cli_info_arr_.at(i).first));
        }
      }
    }

  }
  return ret;
}


int ObTableClientInfoMgr::init()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    const ObMemAttr attr(MTL_ID(), "TbCliInfoMgr");
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      LOG_WARN("fail to init allocator", K(ret));
    } else if (OB_FAIL(client_infos_.create(MAX_CLIENT_INFO_SIZE,
                                            "HashBucCliInFo",
                                            "HashNodCliInfo",
                                            MTL_ID()))) {
      LOG_WARN("fail to init client info map", K(ret), K(MTL_ID()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableClientInfoMgr::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table client info mgr is not inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 retire_task_,
                                 RETIRE_TASK_INTERVAL,
                                 true))) {
    LOG_WARN("fail to schedule table client info retire task", K(ret));
  } else {
    retire_task_.is_inited_ = true;
  }
  return ret;
}

int ObTableClientInfoMgr::record(const ObTableLoginRequest &login_req, const common::ObAddr &cli_addr)
{
  int ret = OB_SUCCESS;
  uint64_t cli_id = 0;
  ObTableClientInfoUpdateOp cli_update_op(allocator_, login_req.client_info_);
  if (OB_FAIL(parse_cli_info_json(login_req.client_info_, cli_id))) {
    LOG_WARN("fail parse cli info to get client id", K(ret), K(login_req.client_info_));
  } else {
    ObTableClientInfoKey cli_key(cli_id, cli_addr);
    if (OB_FAIL(client_infos_.atomic_refactored(cli_key.hash_val_, cli_update_op))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("fail to get client info", K(ret), K(cli_key));
      } else {
        ObTableClientInfo *cli_info = nullptr;
        if (OB_FAIL(init_client_info(login_req, cli_id, cli_addr, cli_info))) {
          LOG_WARN("fail to init client info", K(ret), K(login_req), K(cli_id), K(cli_addr));
        } else if (OB_ISNULL(cli_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("client info is NULL", K(ret));
        } else if (OB_FAIL(client_infos_.set_refactored(cli_key.hash_val_, cli_info))) {
          if (OB_HASH_EXIST != ret) {
            LOG_WARN("fail to set client info to hash map", K(ret), K(cli_key));
          } else {
            ret = OB_SUCCESS; // be set by ohther thread, and no need to add set again
          }
          // client_info_str_ should free when cli_info is be refresh each time and it should alloc separately
          char *str_ptr = cli_info->client_info_str_.ptr();
          if (OB_NOT_NULL(str_ptr)) {
            allocator_.free(str_ptr);
          }
          allocator_.free(cli_info);
          cli_info = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObTableClientInfoMgr::init_client_info(const ObTableLoginRequest &login_req,
                                           uint64_t cli_id,
                                           const common::ObAddr &cli_addr,
                                           ObTableClientInfo *&cli_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cli_info = OB_NEWx(ObTableClientInfo, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableClientInfo", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, login_req.client_info_, cli_info->client_info_str_))) {
    LOG_WARN("fail to copy client info str", K(ret), K(login_req.client_info_));
  } else {
    int64_t cur_ts = ObClockGenerator::getClock();
    cli_info->client_id_ = cli_id;
    cli_info->first_login_ts_ = cur_ts;
    cli_info->last_login_ts_ = cur_ts;
    cli_info->client_addr_ = cli_addr;
    MEMCPY(cli_info->user_name_, login_req.user_name_.ptr(), login_req.user_name_.length());
    cli_info->user_name_length_ = login_req.user_name_.length();
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(cli_info)) {
    if (OB_NOT_NULL(cli_info->client_info_str_.ptr())) {
      allocator_.free(cli_info->client_info_str_.ptr());
    }
    allocator_.free(cli_info);
    cli_info = nullptr;
  }
  return ret;
}

int ObTableClientInfoMgr::parse_cli_info_json(const ObString &cli_info_str, uint64_t &cli_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("KvCliInfonAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  json::Parser json_parser;
  json::Value *root = nullptr;
  if (cli_info_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("client info is empty", K(ret));
  } else if (OB_FAIL(json_parser.init(&allocator))) {
    LOG_WARN("failed to init json parser", K(ret));
  } else if (OB_FAIL(json_parser.parse(cli_info_str.ptr(), cli_info_str.length(), root))) {
    LOG_WARN("failed to parse kv attributes", K(ret), K(cli_info_str));
  } else if (NULL == root) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != root->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret), K(cli_info_str));
  } else {
    bool is_find = false;
    DLIST_FOREACH_X(it, root->get_object(), OB_SUCC(ret) && !is_find) {
      if (it->name_.case_compare("client_id") == 0) {
        json::Value *cli_id_val = it->value_;
        if (NULL != cli_id_val && cli_id_val->get_type() == json::JT_NUMBER) {
          // cast to uint64_t and it is a long type value in client
          cli_id = static_cast<uint64_t>(cli_id_val->get_number());
          is_find = true;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid client id", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !is_find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot find client id", K(ret));
    }
  }

  return ret;
}

void ObTableClientInfoMgr::stop()
{
  if (OB_LIKELY(retire_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), retire_task_);
  }
}

void ObTableClientInfoMgr::wait()
{
  if (OB_LIKELY(retire_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), retire_task_);
  }
}

void ObTableClientInfoMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // 1. cancel the timer task
    if (retire_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), retire_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), retire_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), retire_task_);
        }
      }
      is_inited_ = false;
    }
    // 2. free the ObTableClientInfo
    ObTableClientInfoMap::iterator iter = client_infos_.begin();
    for (;iter != client_infos_.end(); iter++) {
      ObTableClientInfo *cli_info = iter->second;
      allocator_.free(cli_info->client_info_str_.ptr());
      cli_info->~ObTableClientInfo();
      allocator_.free(cli_info);
      cli_info = nullptr;
    }
  }
}

void ObTableClientInfoMgr::ObTableClientInfoRetireTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObTableClientInfoScanRetireOp scan_retired_op;
  ObSEArray<uint64_t, 128> retired_cli_keys;
  if (OB_FAIL(mgr_.client_infos_.foreach_refactored(scan_retired_op))) {
    LOG_WARN("fail to scan client info map", K(ret));
  } else if (OB_FAIL(scan_retired_op.get_retired_keys(retired_cli_keys))) {
    LOG_WARN("fail to get retired client ids", K(ret));
  } else {
    ObTableClientInfoEraseOp erase_op(ObTableClientInfoMgr::CLI_INFO_MAX_ALIVE_TS);
    for (int64_t i = 0; i < retired_cli_keys.count() && OB_SUCC(ret); i++) {
      ObTableClientInfo *cli_info = nullptr;
      bool is_erased = false;
      if (OB_FAIL(mgr_.client_infos_.erase_if(retired_cli_keys.at(i), erase_op, is_erased, &cli_info))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to erase client info from map", K(ret), K(retired_cli_keys.at(i)));
        }
      } else if (is_erased && OB_NOT_NULL(cli_info)) {
        char *str_ptr = cli_info->client_info_str_.ptr();
        if (OB_NOT_NULL(str_ptr)) {
          mgr_.allocator_.free(str_ptr);
        }
        mgr_.allocator_.free(cli_info);
      }
    }
  }
}
