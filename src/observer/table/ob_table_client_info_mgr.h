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

#ifndef OB_TABLE_CLIENT_INFO_MGR_H_
#define OB_TABLE_CLIENT_INFO_MGR_H_
#include "lib/hash/ob_hashmap.h"
#include "lib/task/ob_timer.h"
#include "lib/container/ob_se_array.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/table/ob_table_rpc_struct.h"
#include "lib/net/ob_addr.h"


namespace oceanbase
{
namespace table
{

struct ObTableClientInfoKey
{
  explicit ObTableClientInfoKey(uint64_t client_id, const common::ObAddr client_addr)
    : client_id_(client_id),
      client_addr_(client_addr)
  {
    uint64_t seed = 0;
    hash_val_ = murmurhash(&client_id_, sizeof(client_id_), seed);
    if (client_addr_.using_ipv4()) {
      uint32_t ipv4 = client_addr_.get_ipv4();
      hash_val_ = murmurhash(&ipv4, sizeof(ipv4), hash_val_);
    } else if (client_addr_.using_ipv6()) {
      uint64_t ipv6_high = client_addr_.get_ipv6_high();
      uint64_t ipv6_low = client_addr_.get_ipv6_low();
      hash_val_ = murmurhash(&ipv6_high, sizeof(ipv6_high), hash_val_);
      hash_val_ = murmurhash(&ipv6_low, sizeof(ipv6_low), hash_val_);
    }
  }

  uint64_t client_id_;
  common::ObAddr client_addr_;
  uint64_t hash_val_;
  TO_STRING_KV(K_(client_id),
               K_(client_addr),
               K_(hash_val));
};

struct ObTableClientInfo
{
  ObTableClientInfo()
    : client_id_(0),
      client_addr_(),
      user_name_(),
      user_name_length_(0),
      first_login_ts_(0),
      last_login_ts_(0),
      client_info_str_()
  {}

  uint64_t client_id_;
  common::ObAddr client_addr_;
  char user_name_[OB_MAX_USER_NAME_LENGTH];
  int64_t user_name_length_;
  int64_t first_login_ts_;
  int64_t last_login_ts_;
  ObString client_info_str_;

  TO_STRING_KV(K_(client_id),
               K_(client_addr),
               K_(user_name),
               K_(user_name_length),
               K_(first_login_ts),
               K_(last_login_ts),
               K_(client_info_str));
};

class ObTableClientInfoUpdateOp final
{
public:
  typedef common::hash::HashMapPair<uint64_t, ObTableClientInfo*> MapKV;
  explicit ObTableClientInfoUpdateOp(ObIAllocator &allocator, const ObString &cli_info_str) 
    : allocator_(allocator),
      cli_info_str_(cli_info_str)
  {}
  int operator()(MapKV &entry);
  ~ObTableClientInfoUpdateOp() {}
private:
  common::ObIAllocator &allocator_;
  const ObString &cli_info_str_;
};

struct ObTableClientInfoEraseOp final
{  
  typedef common::hash::HashMapPair<uint64_t, ObTableClientInfo*> MapKV;
  explicit ObTableClientInfoEraseOp(int64_t max_alive_ts)
    : max_alive_ts_(max_alive_ts)
  {
    cur_ts_ = common::ObTimeUtility::fast_current_time();
  }

  bool operator()(MapKV &entry)
  {
    return (entry.second == nullptr) || (cur_ts_ - entry.second->last_login_ts_) > max_alive_ts_;
  }

  int64_t cur_ts_;
  int64_t max_alive_ts_;
};

class ObTableClientInfoScanRetireOp final
{
public:
  typedef common::hash::HashMapPair<uint64_t, ObTableClientInfo*> MapKV;
  ObTableClientInfoScanRetireOp() {}
  ~ObTableClientInfoScanRetireOp() {}
  int operator()(MapKV &entry);
  int get_retired_keys(ObIArray<uint64_t> &retired_arr);
  struct CompareBySecond {
    bool operator()(const std::pair<uint64_t, int64_t>& a, const std::pair<uint64_t, int64_t>& b) const {
        return a.second < b.second;
    }
  };
private:
  common::ObSEArray<std::pair<uint64_t, int64_t>, 128> cli_info_arr_;
};

class ObTableClientInfoMgr final
{
public:
  using ObTableClientInfoMap = common::hash::ObHashMap<uint64_t, ObTableClientInfo*>;
  static const int64_t MAX_CLIENT_INFO_SIZE = 1000;
  static const int64_t RETIRE_TASK_INTERVAL = 1 * 60 * 1000 * 1000; // 1 min
  static const int64_t CLI_INFO_MAX_ALIVE_TS = 10 * 60 * 1000 * 1000; // 10 min
  friend class ObTableClientInfoRetireTask;
  ObTableClientInfoMgr()
    : is_inited_(false),
      allocator_(MTL_ID()),
      retire_task_(*this)
  {}
public:
  class ObTableClientInfoRetireTask : public common::ObTimerTask
  {
  public:
    ObTableClientInfoRetireTask(ObTableClientInfoMgr &mgr) 
      : is_inited_(false),
        mgr_(mgr)
    {}
    void runTimerTask(void);
  public:
    bool is_inited_;
    ObTableClientInfoMgr &mgr_;
  };
public:
  static int mtl_init(ObTableClientInfoMgr *&mgr) { return mgr->init(); }
  int start();
  void stop();
  void wait();
  void destroy();
  int init();
  int record(const ObTableLoginRequest &login_req, const common::ObAddr &cli_addr);
  ObTableClientInfoMap& get_cli_info_map() { return client_infos_; }
private:
  int parse_cli_info_json(const ObString &cli_info_str, uint64_t &cli_id);
  int init_client_info(const ObTableLoginRequest &login_req, 
                       uint64_t cli_id, 
                       const common::ObAddr &cli_addr, 
                       ObTableClientInfo *&cli_info);
private:
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  ObTableClientInfoMap client_infos_; // key is client_id
  ObTableClientInfoRetireTask retire_task_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableClientInfoMgr);
};

#define TABLEAPI_CLI_INFO_MGR (MTL(ObTableClientInfoMgr*))

} // end namespace observer
} // end namespace oceanbase

#endif /* OB_TABLE_CILENT_INFO_MGR_H_ */
