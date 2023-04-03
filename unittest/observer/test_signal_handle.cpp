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

#include <gtest/gtest.h>
#include <signal.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include "share/ob_define.h"
/*
//signal function should not call I/O function and nother non-reentrant functions
//this test would have a dead lock. Because localtime_r have a lock. When signal interrupt
//occurs, if the main function get the lock and had not released it, the dead lock produced.
void handler(int signum)
{
  char result[100];
  time_t now;
  struct tm time1;
  now = time(NULL);
  localtime_r(&now, &time1);
  strftime(result, 100, "%T", &time1);
  printf("At %s, user pressed Ctrl-C\n", result);
}

int main (void)
{
  time_t now;
  struct tm ltime;

  if (signal(SIGINT, handler) == SIG_IGN)
    signal(SIGINT, SIG_IGN);

  now = time(NULL);
  while(1) {
    localtime_r(&now, &ltime);
  }
  return 0;
}
*/

namespace test {

bool g_run_test = false;
void signal_handler(int signum)
{
  printf("singal num: %d", signum);
  char result[100];
  time_t now;
  struct tm time1;
  now = time(NULL);
  localtime_r(&now, &time1);
  strftime(result, 100, "%T", &time1);
  printf("At %s, user pressed Ctrl-C\n", result);
}

class ObSignalDealThread: public share::ObThreadPool
{
public:
  virtual void run1();
};

class ObSignalTest: public ::testing::Test
{
public:
  void run_test();
protected:
  ObSignalDealThread signal_deal_thread_;
};

void ObSignalDealThread::run(obsys::CThread *thread, void *arg)
{

  UNUSED(arg);
  sigset_t   waitset;
  int        signum;
  sigemptyset(&waitset);
  sigaddset(&waitset, SIGINT);
  struct timespec timeout = {1, 0};
  while (!has_set_stop()) {
    if ( -1 == (signum = sigtimedwait(&waitset, NULL, &timeout))) {
      //do not log error, because timeout will also return -1.
      printf("time out or error, errno=%d, errmsg=%m\n", errno);
    } else {
      printf("sigwaitinfo() fetch the signal: %d\n", signum);
      signal_handler(signum);
    }
  }
}

void ObSignalTest::run_test()
{
  if (g_run_test) {
    //first sigmask in main thread
    sigset_t bset, oset;
    sigemptyset(&bset);
    sigaddset(&bset, SIGINT);
    if (pthread_sigmask(SIG_BLOCK, &bset, &oset) != 0)
      printf("!! Set pthread mask failed\n");

    //second start signal deal thread
    signal_deal_thread_.start();

    //loop call localtime_r
    time_t now;
    struct tm ltime;
    now = time(NULL);
    while(1) {
      localtime_r(&now, &ltime);
    }
    signal_deal_thread_.wait();
  }
}

TEST_F(ObSignalTest, signal_test)
{
  run_test();
}
}

//use ./test_signal_handle run
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  if (argc >= 2) {
    if (strcmp("run", argv[1]) ==0) {
      ::test::g_run_test = true;
    }
  }
  return RUN_ALL_TESTS();
}
