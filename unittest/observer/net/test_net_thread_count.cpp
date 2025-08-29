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

#define USING_LOG_PREFIX RPC_TEST
#include <gtest/gtest.h>
#define private public
#include "lib/net/ob_net_util.h"
#include "rpc/obrpc/ob_poc_rpc_proxy.h"
#include "share/ob_srv_rpc_proxy.h"
#include "test_rpc_frame_util.h"
#include <pthread.h>

#define rk_log_macro(level, ret, format, ...) _OB_LOG_RET(level, ret, "PNIO " format, ##__VA_ARGS__)
extern "C" {
int tranlate_to_ob_error(int err);
#include "rpc/pnio/interface/group.h"
#include "ussl-hook.h"
#include "rpc/pnio/pkt-nio.c"
};
#define WOKER_CNT 30
using namespace oceanbase::obrpc;
using namespace oceanbase::observer;
namespace oceanbase
{
namespace observer
{
OB_DEFINE_PROCESSOR_S(Test, OB_TEST2_PCODE, ObRpcTestP);
int ObRpcTestP::process()
{
  int ret = OB_SUCCESS;
  result_ = OB_EAGAIN;
  ob_usleep(6 * 1000 * 1000);
  return ret;
}
}; // end of namespace observer

namespace unittest {
void *post_rpc(void *data);

class SimpleRpcServer {
public:
  SimpleRpcServer(int port): pid_(-1), port_(port), is_client_(false) {}
  SimpleRpcServer(int port, const ObAddr &dst): pid_(-1), port_(port),
    is_client_(true), dst_(dst) {}
  const int server_set_thread_count_delay = 2 * 1000 * 1000;
  const int normal_delay = 4 * 1000 * 1000;
  int server_run()
  {
    set_thread_name(__FUNCTION__);
    int ret = OB_SUCCESS;
    int net_thread_count = 1;
    ObPocRpcServer &server = global_poc_server;
    uint32_t local_ip_value = 0;
    ObAddr local_addr;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(obsys::ObNetUtil::get_local_addr_ipv4("eth0", local_ip_value))
      && OB_TMP_FAIL(obsys::ObNetUtil::get_local_addr_ipv4("bond0", local_ip_value))) {
      local_addr.set_ip_addr("127.0.0.1", port_);
    } else {
      local_ip_value = ntohl(local_ip_value);
      local_addr.set_ipv4_addr(local_ip_value, port_);
      LOG_INFO("get local ip", K(local_addr));
    }
    ObTestRpcServerCtx rpc_ctx;
    ObSrvRpcXlator *xlator = &rpc_ctx.translator_.rpc_xlator_;
    RPC_PROCESSOR(ObRpcTestP, gctx_);
    if (OB_FAIL(rpc_ctx.init())) {
      LOG_WARN("rpc_ctx init failed");
    } else if (OB_FAIL(server.start(port_, net_thread_count, &rpc_ctx.deliver_))) {
    } else if (OB_FAIL(rpc_ctx.deliver_.qth_.set_thread_count(WOKER_CNT))) {
      LOG_ERROR("set_thread_count failed");
    } else if (OB_FAIL(server.update_tcp_keepalive_params(3 * 1000 * 1000))) {
      LOG_WARN("update_tcp_keepalive_params failed");
    } else {
      for (int i = 0; i < 3 && OB_SUCC(ret); i++) {
        ob_usleep(server_set_thread_count_delay);
        net_thread_count = net_thread_count * 2;
        if (OB_FAIL(server.update_thread_count(net_thread_count))) {
          LOG_WARN("update_thread_count failed");
        } else {
          ob_usleep(normal_delay - server_set_thread_count_delay);
        }
      }
      ob_usleep(10 * 1000 * 1000);
      server.destroy();
      ussl_stop();
      ussl_wait();
    }
    LOG_INFO("server process exit",K(ret));
    return ret;
  }
  int client_run()
  {
    set_thread_name(__FUNCTION__);
    int ret = OB_SUCCESS;
    int net_thread_count = 1;
    ObPocRpcServer &server = global_poc_server;

    // create thread to send long rpc
    pthread_t thread;
    int err = 0;
    if (OB_FAIL(server.start_net_client(net_thread_count))) {
    } else if((err = pthread_create(&thread, nullptr, post_rpc, this)) != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pthread_create failed", K(err));
    } else {
      // sleep and set net_thread_count
      for (int i = 0; i < 3 && OB_SUCC(ret); i++) {
        net_thread_count = net_thread_count * 2;
        if (OB_FAIL(server.update_thread_count(net_thread_count))) {
          LOG_WARN("update_thread_count failed");
        } else {
          ob_usleep(normal_delay);
        }
      }
      if (OB_FAIL(server.update_thread_count(1))) {
        LOG_WARN("update_thread_count to 1 failed");
      }
      // finish and wait post_rpc thread
      void* thread_ret;
      pthread_join(thread, &thread_ret);
      if (thread_ret == NULL) {
        LOG_INFO("post_rpc success");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("post_rpc failed");
      }
      server.destroy();
    }
    return ret;
  }
  int start()
  {
    int ret = OB_SUCCESS;
    pid_ = fork();
    if (pid_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fork first child failed");
    } else if (pid_ > 0) {
      LOG_INFO("create child process success", K_(pid));
    } else { // in child process
      ObClockGenerator::init();
      if (!is_client_) {
        ret = server_run();
      } else {
        ret = client_run();
      }
      LOG_INFO("child process exit", K_(pid), K(ret));
      exit(ret);
    }
    return ret;
  };
  int destroy()
  {
    int ret = OB_SUCCESS;
    int stat = 0;
    if (pid_ > 0) {
      waitpid(pid_, &stat, 0);
    }
    if (stat != 0) {
      LOG_ERROR("child process exit with error, you need to check the exit status or process coredump",
                  K_(pid), K(stat));
      ret = OB_ERROR;
    }
    return ret;
  };
  pid_t pid_;
  int port_;
  bool is_client_;
  ObAddr dst_;
};

void *post_rpc(void *data)
{
  set_thread_name(__FUNCTION__);
  ob_usleep(200000);
  SimpleRpcServer *s = reinterpret_cast<SimpleRpcServer*>(data);
  ObTestRpcProxy rpc_proxy;
  ObReqTransport dummy_transport(NULL, NULL);
  rpc_proxy.init(&dummy_transport);
  ObAddr dst = s->dst_;
  int ret = OB_SUCCESS;
  void *retv = NULL;
  TestRpcRequest arg;
  const int sleep_seconds = 10;
  ObTestRpcCb cb;
  int post_count = 0;
  for (int i = 0; i < sleep_seconds && OB_SUCC(ret); i++) {
    for (int j = 0; j < 5; j++) {
      if (OB_FAIL(rpc_proxy.to(dst).by(OB_SYS_TENANT_ID).timeout(100000000).async_rpc_test(arg, &cb))) {
        retv = (void*)-1;
        LOG_ERROR("post rpc failed", K(dst), K(arg));
      } else {
        post_count ++;
      }
    }
    ob_usleep(1000000);
  }
  LOG_INFO("post rpc finished", K(post_count));
  int wait_seconds = 0;
  if (OB_SUCC(ret)) {
    while (wait_seconds++ < 10 && ATOMIC_LOAD(&cb.success_count_) != post_count) {
      ob_usleep(1000000);
    }
    if (ATOMIC_LOAD(&cb.success_count_) != post_count) {
      LOG_ERROR("wait rpc failed", K(post_count), K(cb.success_count_), K(cb.timeout_count_), K(wait_seconds));
      retv = (void*)-2;
    } else {
      LOG_INFO("wait rpc success", K(post_count), K(cb.success_count_), K(cb.timeout_count_), K(wait_seconds));
    }
  }
  return retv;
}

class TestNetThreadCount : public testing::Test
{
public:
  TestNetThreadCount()
  {}
  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    int thread_count = 1;
    int count = pn_provision(-1, TEST_PNIO_GROUP, thread_count);
    if (count != thread_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("create test group failed");
    }
  }
  virtual void TearDown() override
  {
    pn_stop(TEST_PNIO_GROUP);
    pn_wait(TEST_PNIO_GROUP);
  }
  virtual ~TestNetThreadCount()
  {}
  const static int TEST_PNIO_GROUP = 10;
};

TEST_F(TestNetThreadCount, test_pn_interface)
{
  // increase pn thread
  ASSERT_EQ(pn_update_thread_count(TEST_PNIO_GROUP, 2), 0);
  ASSERT_EQ(pn_update_thread_count(TEST_PNIO_GROUP, 8), 0);
  pn_grp_t* pn_grp = locate_grp(TEST_PNIO_GROUP);
  int config_count = LOAD(&pn_grp->config_count);
  ASSERT_NE(config_count, pn_grp->count);
  ASSERT_EQ(pn_set_thread_count(-1, TEST_PNIO_GROUP, config_count), 0);
  ASSERT_EQ(pn_update_thread_count(TEST_PNIO_GROUP, 129), -EINVAL);

  // decrease pn thread
  ASSERT_EQ(pn_update_thread_count(TEST_PNIO_GROUP, 0), -EINVAL);
  ASSERT_EQ(pn_update_thread_count(TEST_PNIO_GROUP, 1), 0);
  config_count = LOAD(&pn_grp->config_count);
  ASSERT_EQ(pn_set_thread_count(-1, TEST_PNIO_GROUP, config_count), 0);
}

TEST_F(TestNetThreadCount, test_pn_write_queue_move)
{

  const int insert_count = 100;
  dlink_t dlink_array[insert_count];

  // move a empty wq
  {
    write_queue_t wq, wq_new;
    wq_init(&wq);
    wq_init(&wq_new);
    wq_move(&wq_new, &wq);
    ASSERT_EQ(dqueue_empty(&wq_new.queue), true);
    ASSERT_EQ(dqueue_top(&wq_new.queue), &wq_new.queue.head);
  }

  // move a non-empty wq
  {
    write_queue_t wq, wq_new;
    wq_init(&wq);
    wq_init(&wq_new);
    for (int i = 0; i < insert_count; i++) {
      dlink_t* l = &dlink_array[i];
      dlink_init(l);
      wq_push(&wq, l);
    }
    wq_move(&wq_new, &wq);
    int wq_new_cnt = 0;
    dlink_for(&wq_new.queue.head, p) {
      ASSERT_EQ(p, &dlink_array[wq_new_cnt]);
      wq_new_cnt++;
    }
    ASSERT_EQ(wq_new_cnt, insert_count);
    ASSERT_EQ(dqueue_empty(&wq_new.queue), false);

    // check reverse
    for(dlink_t* p = wq_new.queue.head.prev; p != &wq_new.queue.head; p = p->prev) {
      wq_new_cnt--;
      ASSERT_EQ(p, &dlink_array[wq_new_cnt]);
    }
    ASSERT_EQ(wq_new_cnt, 0);
  }

  // move a wq with one element
  {
    write_queue_t wq, wq_new;
    wq_init(&wq);
    wq_init(&wq_new);
    dlink_t* l = &dlink_array[0];
    dlink_init(l);
    wq_push(&wq, l);
    wq_move(&wq_new, &wq);
    ASSERT_EQ(dqueue_empty(&wq_new.queue), false);
    ASSERT_EQ(dqueue_top(&wq_new.queue), l);
    ASSERT_EQ(l->next, &wq_new.queue.head);
    ASSERT_EQ(l->prev, &wq_new.queue.head);
  }
}

TEST_F(TestNetThreadCount, update_rpc_thread_count)
{
  // init
  int ret = OB_SUCCESS;
  ObAddr dst;
  int port = find_port();
  uint32_t local_ip_value = 0;
  dst.set_ip_addr("127.0.0.1", port);

  // We cannot create two RPC servers in one process because of the dependency of some global variables
  SimpleRpcServer server_end(port);
  SimpleRpcServer client_end(port, dst);
  ASSERT_EQ(server_end.start(), OB_SUCCESS);
  ASSERT_EQ(client_end.start(), OB_SUCCESS);

  ASSERT_EQ(server_end.destroy(), OB_SUCCESS);
  ASSERT_EQ(client_end.destroy(), OB_SUCCESS);
}

}; // end of namespace unittest

} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_net_thread_count.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_net_thread_count.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
