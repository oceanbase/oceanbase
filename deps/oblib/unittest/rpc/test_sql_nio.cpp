/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <atomic>
#include <thread>
#include "rpc/obmysql/ob_sql_nio.h"
#include "rpc/obmysql/ob_sql_sock_session.h"
#include "rpc/obmysql/ob_i_sql_sock_handler.h"
#include "rpc/obmysql/ob_mysql_handler.h"
#include "rpc/obmysql/ob_sql_sock_handler.h"
#include "rpc/obmysql/ob_sql_sock_processor.h"
#include "lib/ob_running_mode.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/net/ob_net_util.h"
#include "lib/ash/ob_active_session_guard.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

// Compile the actual implementation with distinct names: oblib's unity object
// also contains ObSqlSockSession, which is used by the connection callbacks.
#define ObSqlNio TestSqlNio
#define ObSqlNioImpl TestSqlNioImpl
#define ObSqlSock TestSqlSock
#define ObSqlSockSession TestSqlSockSession
#define ObSqlSessionMemPool TestSqlSessionMemPool
#define ObISMConnectionCallback TestSqlConnectionCallback
#define ObSqlSockHandler TestSqlSockHandler
#define ObSqlSockProcessor TestSqlSockProcessor
#define ObDList TestSqlDList
#define ReadyFlag TestSqlReadyFlag
#define SingleWaitCond TestSqlSingleWaitCond
#define SocketReader TestSqlSocketReader
#define ReadBuffer TestSqlReadBuffer
#define PendingWriteTask TestSqlPendingWriteTask
#define Evfd TestSqlEvfd
#define get_fd_from_sess test_get_fd_from_sess
#define get_sql_sess_from_sess test_get_sql_sess_from_sess
#define private public
#undef OCEANBASE_OBMYSQL_OB_SQL_NIO_H_
#undef OCEANBASE_OBMYSQL_OB_SQL_SOCK_SESSION_H_
#undef OCEANBASE_OBMYSQL_OB_I_SM_CONN_CALLBACK_H_
#undef OCEANBASE_OBMYSQL_OB_SQL_SOCK_HANDLER_H_
#undef OCEANBASE_OBMYSQL_OB_SQL_SOCK_PROCESSOR_H_
#include "rpc/obmysql/ob_sql_nio.cpp"
#include "rpc/obmysql/ob_sql_sock_session.cpp"
#include "rpc/obmysql/ob_sql_sock_processor.cpp"
#include "rpc/obmysql/ob_sql_sock_handler.cpp"
#undef private
#undef get_sql_sess_from_sess
#undef get_fd_from_sess
#undef Evfd
#undef PendingWriteTask
#undef ReadBuffer
#undef SocketReader
#undef SingleWaitCond
#undef ReadyFlag
#undef ObDList
#undef ObSqlSock
#undef ObISMConnectionCallback
#undef ObSqlSessionMemPool
#undef ObSqlSockSession
#undef ObSqlSockHandler
#undef ObSqlSockProcessor
#undef ObSqlNioImpl
#undef ObSqlNio

namespace oceanbase
{
namespace obmysql
{

TEST(TestSqlNioReadyFlag, disconnect_probe_has_one_owner)
{
  TestSqlReadyFlag flag;
  std::atomic<int> owners(0);
  std::thread contenders[8];
  for (int i = 0; i < 8; ++i) {
    contenders[i] = std::thread([&flag, &owners]() {
      if (flag.try_acquire_idle()) {
        ++owners;
      }
    });
  }
  for (auto &contender : contenders) {
    contender.join();
  }
  EXPECT_EQ(1, owners.load());
  EXPECT_EQ(1, flag.get_pending_flag());
  flag.cancel_handle();
  EXPECT_EQ(0, flag.get_pending_flag());
}

class NioDisconnectCallback : public TestSqlConnectionCallback
{
public:
  NioDisconnectCallback() : disconnect_count_(0), destroy_count_(0) {}
  int init(TestSqlSockSession &, observer::ObSMConnection &) override { return OB_SUCCESS; }
  void destroy(observer::ObSMConnection &conn) override
  {
    ++destroy_count_;
    conn.~ObSMConnection();
  }
  int on_disconnect(observer::ObSMConnection &) override
  {
    ++disconnect_count_;
    return OB_SUCCESS;
  }
  int disconnect_count_;
  int destroy_count_;
};

class NioTestHandler : public ObISqlSockHandler
{
public:
  explicit NioTestHandler(NioDisconnectCallback &callback)
      : callback_(callback), readable_count_(0), probe_count_(0), close_count_(0),
        probe_result_(OB_SUCCESS), deliver_quit_(false), finish_in_probe_(false), delegate_(nullptr) {}
  int on_connect(void *udata, int) override
  {
    new (udata) TestSqlSockSession(callback_, nullptr);
    return OB_SUCCESS;
  }
  int on_readable(void *) override
  {
    ++readable_count_;
    return OB_SUCCESS;
  }
  int on_disconnect_readable(void *udata, bool &quit_delivered) override
  {
    int ret = probe_result_;
    ++probe_count_;
    quit_delivered = deliver_quit_;
    if (nullptr != delegate_) {
      ret = delegate_->on_disconnect_readable(udata, quit_delivered);
    } else if (deliver_quit_ && finish_in_probe_) {
      TestSqlSock *sock = sess2sock(udata);
      sock->set_shutdown();
      sock->get_nio_impl()->revert_sock(sock);
    }
    return ret;
  }
  void on_close(void *udata, int) override
  {
    ++close_count_;
    static_cast<TestSqlSockSession *>(udata)->destroy();
  }
  void on_flushed(void *) override {}
  NioDisconnectCallback &callback_;
  int readable_count_;
  int probe_count_;
  int close_count_;
  int probe_result_;
  bool deliver_quit_;
  bool finish_in_probe_;
  ObISqlSockHandler *delegate_;
};

class TestSqlNioDisconnect : public ::testing::Test
{
public:
  TestSqlNioDisconnect() : handler_(callback_), impl_(handler_), sock_(nullptr), peer_fd_(-1) {}
  void SetUp() override
  {
    int fd[2] = {-1, -1};
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fd));
    peer_fd_ = fd[1];
    sock_ = impl_.alloc_sql_sock(fd[0]);
    ASSERT_NE(nullptr, sock_);
    ASSERT_EQ(OB_SUCCESS, handler_.on_connect(sock_->sess_, fd[0]));
    impl_.epfd_ = epoll_create1(EPOLL_CLOEXEC);
    ASSERT_GE(impl_.epfd_, 0);
    ASSERT_EQ(0, epoll_regist(impl_.epfd_, fd[0], EPOLLIN | EPOLLRDHUP | EPOLLET, sock_));
  }
  void TearDown() override
  {
    if (nullptr != sock_) {
      if (!sock_->handler_close_been_called()) {
        handler_.on_close(sock_->sess_, 0);
      }
      sock_->do_close();
      impl_.free_sql_sock(sock_);
      sock_ = nullptr;
    }
    if (peer_fd_ >= 0) {
      close(peer_fd_);
    }
    if (impl_.epfd_ >= 0) {
      close(impl_.epfd_);
      impl_.epfd_ = -1;
    }
  }
  void expect_destroyed()
  {
    const int fd = sock_->get_fd();
    impl_.handle_pending_destroy_list();
    ASSERT_EQ(1, handler_.close_count_);
    EXPECT_EQ(1, callback_.destroy_count_);
    impl_.handle_pending_destroy_list();
    sock_ = nullptr;
    EXPECT_EQ(impl_.all_list_.head(), impl_.all_list_.head()->next_);
    EXPECT_EQ(impl_.pending_destroy_list_.head(), impl_.pending_destroy_list_.head()->next_);
    EXPECT_EQ(-1, fcntl(fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);
  }
  NioDisconnectCallback callback_;
  NioTestHandler handler_;
  TestSqlNioImpl impl_;
  TestSqlSock *sock_;
  int peer_fd_;
};

TEST_F(TestSqlNioDisconnect, later_hangup_waits_for_delivered_quit_before_destroy)
{
  handler_.deliver_quit_ = true;
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(1, handler_.probe_count_);
  EXPECT_EQ(0, handler_.readable_count_);
  EXPECT_EQ(1, sock_->get_pending_flag());
  EXPECT_EQ(0, callback_.disconnect_count_);
  EXPECT_FALSE(sock_->has_error());

  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  impl_.handle_sock_event(sock_, EPOLLERR | EPOLLHUP);
  EXPECT_EQ(1, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  EXPECT_TRUE(sock_->has_error());
  impl_.handle_pending_destroy_list();
  EXPECT_EQ(0, handler_.close_count_);

  sock_->set_shutdown();
  impl_.revert_sock(sock_);
  impl_.handle_close_req_queue();
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, worker_may_finish_before_probe_returns)
{
  handler_.deliver_quit_ = true;
  handler_.finish_in_probe_ = true;
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(1, handler_.probe_count_);
  EXPECT_EQ(0, callback_.disconnect_count_);
  impl_.handle_sock_event(sock_, EPOLLHUP);
  impl_.handle_close_req_queue();
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, rejected_probe_balances_ownership_and_closes_once)
{
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(1, handler_.probe_count_);
  EXPECT_EQ(0, handler_.readable_count_);
  EXPECT_EQ(0, sock_->get_pending_flag());
  EXPECT_EQ(1, callback_.disconnect_count_);
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  impl_.handle_sock_event(sock_, EPOLLHUP);
  EXPECT_EQ(1, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, failed_probe_balances_ownership_and_does_not_retry_normal_decode)
{
  handler_.probe_result_ = OB_ALLOCATE_MEMORY_FAILED;
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(1, handler_.probe_count_);
  EXPECT_EQ(0, handler_.readable_count_);
  EXPECT_EQ(0, sock_->get_pending_flag());
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, busy_request_is_disconnected_before_worker_finishes)
{
  ASSERT_TRUE(sock_->set_readable());
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  EXPECT_EQ(1, sock_->get_pending_flag());
  impl_.handle_pending_destroy_list();
  EXPECT_EQ(0, handler_.close_count_);
  impl_.revert_sock(sock_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, queued_readiness_remains_busy)
{
  ASSERT_TRUE(sock_->set_readable());
  ASSERT_FALSE(sock_->set_readable());
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  EXPECT_EQ(2, sock_->get_pending_flag());
  impl_.revert_sock(sock_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, rdhup_without_readable_is_closed_without_probe)
{
  impl_.handle_sock_event(sock_, EPOLLRDHUP);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, full_hangup_takes_precedence_over_probe)
{
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP | EPOLLHUP);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, socket_error_takes_precedence_over_probe)
{
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP | EPOLLERR);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, shutdown_connection_cannot_start_probe)
{
  sock_->set_shutdown();
  impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_F(TestSqlNioDisconnect, normal_readiness_keeps_normal_dispatch)
{
  impl_.handle_sock_event(sock_, EPOLLIN);
  EXPECT_EQ(1, handler_.readable_count_);
  EXPECT_EQ(0, handler_.probe_count_);
  EXPECT_EQ(0, callback_.disconnect_count_);
  EXPECT_EQ(1, sock_->get_pending_flag());
  sock_->set_shutdown();
  impl_.revert_sock(sock_);
  impl_.handle_close_req_queue();
  expect_destroyed();
}

class NioCountingDeliver : public rpc::frame::ObReqDeliver
{
public:
  NioCountingDeliver() : count_(0), ret_(OB_SUCCESS), command_(-1), content_length_(0) {}
  int init() override { return OB_SUCCESS; }
  void stop() override {}
  int deliver(rpc::ObRequest &req) override
  {
    ++count_;
    const ObMySQLRawPacket *packet = static_cast<const ObMySQLRawPacket *>(&req.get_packet());
    command_ = packet->get_cmd();
    content_length_ = packet->get_clen();
    return ret_;
  }
  int count_;
  int ret_;
  int command_;
  int64_t content_length_;
};

// The socket processor's existing constructor takes an easy handler, although
// decoding through ObSqlSockSession does not call its easy connection methods.
class NioUnusedEasyHandler : public ObMySQLHandler
{
public:
  explicit NioUnusedEasyHandler(rpc::frame::ObReqDeliver &deliver) : ObMySQLHandler(deliver) {}
  bool is_in_connected_phase(easy_connection_t *) const override { return false; }
  bool is_in_ssl_connect_phase(easy_connection_t *) const override { return false; }
  bool is_in_authed_phase(easy_connection_t *) const override { return true; }
  bool is_compressed(easy_connection_t *) const override { return false; }
  void set_ssl_connect_phase(easy_connection_t *) override {}
  void set_connect_phase(easy_connection_t *) override {}
  rpc::ConnectionPhaseEnum get_connection_phase(easy_connection_t *) const override
  {
    return rpc::ConnectionPhaseEnum::CPE_AUTHED;
  }
  uint32_t get_sessid(easy_connection_t *) const override { return 0; }
  ObMysqlPktContext *get_mysql_pkt_context(easy_connection_t *) override { return nullptr; }
  ObCompressedPktContext *get_compressed_pkt_context(easy_connection_t *) override { return nullptr; }
  ObProto20PktContext *get_proto20_pkt_context(easy_connection_t *) override { return nullptr; }
  common::ObCSProtocolType get_cs_protocol_type(easy_connection_t *) const override
  {
    return common::OB_MYSQL_CS_TYPE;
  }
};

class TestSqlNioQuitProtocol : public TestSqlNioDisconnect,
                               public ::testing::WithParamInterface<bool>
{
public:
  TestSqlNioQuitProtocol()
      : easy_handler_(deliver_), processor_(easy_handler_),
        protocol_handler_(callback_, processor_, nio_), sess_(nullptr) {}
  void SetUp() override
  {
    TestSqlNioDisconnect::SetUp();
    ASSERT_NE(nullptr, sock_);
    sess_ = static_cast<TestSqlSockSession *>(static_cast<void *>(sock_->sess_));
    sess_->nio_ = &nio_;
    sess_->conn_.is_proxy_ = GetParam();
    sess_->conn_.is_sess_alloc_ = true;
    sess_->conn_.set_auth_phase();
    ASSERT_EQ(OB_SUCCESS, protocol_handler_.init(&deliver_));
    handler_.delegate_ = &protocol_handler_;
  }
  void send_and_fin(const char *data, int64_t length)
  {
    ASSERT_EQ(length, send(peer_fd_, data, length, MSG_NOSIGNAL));
    ASSERT_EQ(0, ::shutdown(peer_fd_, SHUT_WR));
    impl_.handle_sock_event(sock_, EPOLLIN | EPOLLRDHUP);
  }
  void expect_quit_delivered()
  {
    EXPECT_EQ(1, deliver_.count_);
    EXPECT_EQ(COM_QUIT, deliver_.command_);
    EXPECT_EQ(1, deliver_.content_length_);
    EXPECT_EQ(0, callback_.disconnect_count_);
    EXPECT_EQ(0, handler_.readable_count_);
    EXPECT_FALSE(sock_->has_error());
    sock_->set_shutdown();
    sess_->revert_sock();
    impl_.handle_close_req_queue();
    EXPECT_EQ(1, callback_.disconnect_count_);
    expect_destroyed();
  }
  void expect_packet_rejected()
  {
    // EOF during decoding can already have enqueued the one winning close request.
    impl_.handle_close_req_queue();
    EXPECT_EQ(0, deliver_.count_);
    EXPECT_EQ(0, handler_.readable_count_);
    EXPECT_EQ(0, sock_->get_pending_flag());
    EXPECT_EQ(1, callback_.disconnect_count_);
    expect_destroyed();
  }
  NioCountingDeliver deliver_;
  NioUnusedEasyHandler easy_handler_;
  TestSqlSockProcessor processor_;
  TestSqlNio nio_;
  TestSqlSockHandler protocol_handler_;
  TestSqlSockSession *sess_;
};

TEST_P(TestSqlNioQuitProtocol, complete_mysql_quit_is_delivered)
{
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  expect_quit_delivered();
}

TEST_P(TestSqlNioQuitProtocol, later_hangup_preserves_delivered_quit_packet)
{
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  ASSERT_EQ(1, deliver_.count_);
  impl_.handle_sock_event(sock_, EPOLLHUP);
  EXPECT_EQ(1, callback_.disconnect_count_);
  impl_.handle_pending_destroy_list();
  ASSERT_EQ(0, handler_.close_count_);
  const ObMySQLRawPacket &packet = static_cast<const ObMySQLRawPacket &>(sess_->sql_req_.get_packet());
  EXPECT_EQ(COM_QUIT, packet.get_cmd());
  EXPECT_EQ(1, packet.get_clen());
  sock_->set_shutdown();
  sess_->revert_sock();
  expect_destroyed();
}

TEST_P(TestSqlNioQuitProtocol, normal_sql_before_fin_is_not_delivered)
{
  const char data[] = {3, 0, 0, 0, COM_QUERY, 's', 'q'};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, sql_followed_by_quit_is_not_skipped_to_quit)
{
  const char data[] = {3, 0, 0, 0, COM_QUERY, 's', 'q', 1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, partial_quit_followed_by_eof_is_not_delivered)
{
  const char data[] = {1, 0, 0, 0};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, incomplete_large_packet_followed_by_eof_is_not_delivered)
{
  const char data[] = {0, 0, 2, 0, COM_QUERY};
  send_and_fin(data, sizeof(data));
  EXPECT_EQ(0, deliver_.count_);
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, quit_without_fin_keeps_normal_dispatch)
{
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  ASSERT_EQ(sizeof(data), send(peer_fd_, data, sizeof(data), MSG_NOSIGNAL));
  ASSERT_TRUE(sock_->set_readable());
  ASSERT_EQ(OB_SUCCESS, protocol_handler_.on_readable(sock_->sess_));
  EXPECT_EQ(1, deliver_.count_);
  EXPECT_EQ(COM_QUIT, deliver_.command_);
  EXPECT_EQ(0, callback_.disconnect_count_);
  sock_->set_shutdown();
  sess_->revert_sock();
  impl_.handle_close_req_queue();
  expect_destroyed();
}

TEST_P(TestSqlNioQuitProtocol, eof_without_packet_is_not_delivered)
{
  send_and_fin(nullptr, 0);
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, quit_with_extra_payload_is_rejected)
{
  const char data[] = {2, 0, 0, 0, COM_QUIT, 'x'};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, unallocated_session_cannot_deliver_quit)
{
  sess_->conn_.is_sess_alloc_ = false;
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, unauthenticated_connection_cannot_deliver_quit)
{
  sess_->conn_.set_connect_phase();
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, freed_session_cannot_deliver_quit)
{
  sess_->conn_.is_sess_free_ = true;
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  expect_packet_rejected();
}

TEST_P(TestSqlNioQuitProtocol, failed_delivery_closes_and_releases_ownership)
{
  deliver_.ret_ = OB_SIZE_OVERFLOW;
  const char data[] = {1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  EXPECT_EQ(1, deliver_.count_);
  EXPECT_EQ(COM_QUIT, deliver_.command_);
  EXPECT_EQ(0, handler_.readable_count_);
  EXPECT_EQ(0, sock_->get_pending_flag());
  EXPECT_EQ(1, callback_.disconnect_count_);
  expect_destroyed();
}

TEST_P(TestSqlNioQuitProtocol, compressed_protocol_quit_uses_existing_decoder)
{
  sess_->conn_.cap_flags_.cap_flags_.OB_CLIENT_COMPRESS = 1;
  // A compressed-protocol frame whose payload was too small to compress.
  const char data[] = {5, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, COM_QUIT};
  send_and_fin(data, sizeof(data));
  expect_quit_delivered();
}

TEST_P(TestSqlNioQuitProtocol, ob20_quit_uses_existing_decoder)
{
  sess_->conn_.proxy_cap_flags_.cap_flags_.OB_CAP_OB_PROTOCOL_V2 = 1;
  const char data[] = {
      33, 0, 0, 0, 0, 0, 0,                // Compression header.
      static_cast<char>(0xab), 0x20, 20, 0, // Magic and version.
      0, 0, 0, 0,                          // Connection id.
      1, 0, 0, 0,                          // Request id and sequence.
      5, 0, 0, 0,                          // MySQL payload length.
      2, 0, 0, 0,                          // Last-packet flag.
      0, 0, 0, 0,                          // Reserved and header checksum.
      1, 0, 0, 0, COM_QUIT,
      0, 0, 0, 0};                         // Optional payload checksum.
  send_and_fin(data, sizeof(data));
  expect_quit_delivered();
}

// Parameter 0 is a direct connection; parameter 1 is a Proxy connection.
INSTANTIATE_TEST_CASE_P(DirectAndProxy, TestSqlNioQuitProtocol, ::testing::Values(false, true));

} // namespace obmysql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}
