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

#ifndef OCEANBASE_OBMYSQL_OB_RDMA_SERVER_H_
#define OCEANBASE_OBMYSQL_OB_RDMA_SERVER_H_
#include <hash_map>
#include <string>
#include "ob_mysql_rdma_handler.h"
#include "reasy_header_obp.h"
#include "lib/lock/ob_drw_lock.h"


using namespace oceanbase;

namespace oceanbase
{
namespace obmysql
{

#define OB_MYSQL_SESSION_GROUP_COUNT 128
#define OB_MYSQL_MCONN_HASHMAP_COUNT 8

#define OB_MYSQL_HASH_CODE(x, y)                                          \
  ({                                                                      \
    int res;                                                              \
    res = ((x >> 5) + (x >> 10) + y) & (OB_MYSQL_SESSION_GROUP_COUNT - 1); \
    res;                                                                  \
  })

extern bool checkLogLevel(enum ObRdmaDebugLogLevelReq req);

typedef common::ObLinkHashMap<ObMysqlSessionID, ObMysqlRdmaConnection, AllocHandle<ObMysqlSessionID, ObMysqlRdmaConnection>, common::RefHandle, 128> ObRdmaConnMap;


struct hash_A{
        size_t operator()(const class ObMysqlSessionID & A)const{
                return A.sessid_;
        }
};

struct equal_A{
        bool operator()(const class ObMysqlSessionID & a1, const class ObMysqlSessionID & a2)const{
                return  a1.sessid_ == a2.sessid_;
        }
};

class ObRdmaServer
{
public:
  ObRdmaServer();
  virtual ~ObRdmaServer();
  int init(ObMySQLRdmaHandler& handler, const common::ObAddr &addr,
      uint32_t port, int io_thread_count);
  int start();
  void wait();
  int stop();
  void destroy();

  int create_session(struct reasy_rdma_buffer *req);
  int destroy_session(struct reasy_rdma_buffer *req);
  int process(struct reasy_rdma_buffer *req);
  int update_credit(struct reasy_rdma_buffer *req);
  int send_keepalive(struct reasy_rdma_buffer *req);
  int reg_mconn(ObMysqlRdmaConnection& mconn);
  int unreg_mconn(ObMysqlRdmaConnection& mconn);
  int get_mconn(struct ObRdmaMysqlHeader *rheader, struct reasy_connection_desc *rdesc, ObMysqlRdmaConnection *&mconn);
  void put_mconn(ObMysqlRdmaConnection *mconn);
  static int send_response(ObMysqlRdmaConnection& mconn, char *buffer, size_t length);
  static int encode_reasy_header(ObMysqlRdmaConnection& mconn, reasy_rdma_buffer *rsp,
      uint8 type, uint32_t len);
  static int send_response(ObMysqlRdmaConnection& mconn, reasy_rdma_buffer *rsp,
      uint8 type, uint32_t len);

  inline int get_map_pos(int64_t ver_id)
  {
    return io_thread_count_ == 0 ? 0 : ver_id % io_thread_count_;
  }

private:
  ObMySQLRdmaHandler *handler_;
  common::ObAddr listen_addr_;
  uint32_t listen_port_;
  struct reasy_io_t *rio_;
  bool is_inited_;
  bool started_;
  int io_thread_count_;
  __gnu_cxx::hash_map<ObMysqlSessionID, ObMysqlRdmaConnection *, hash_A, equal_A> mconn_hmap_[OB_MYSQL_MCONN_HASHMAP_COUNT];
  common::DRWLock sess_grp_locks_[OB_MYSQL_SESSION_GROUP_COUNT];
};
extern ObRdmaServer* global_rdma_server;

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* #define OCEANBASE_OBMYSQL_OB_RDMA_SERVER_H_ */
