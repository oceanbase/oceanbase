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
#include "lib/ob_errno.h"
#include "lib/queue/ob_lighty_queue.h" // ObLightyQueue
#include "logservice/archive/ob_archive_task.h"
#include "logservice/archive/ob_archive_task_queue.h"
#include "logservice/archive/ob_archive_worker.h"
#include "logservice/archive/ob_ls_task.h"
#include "logservice/archive/ob_archive_allocator.h"
#include  <pthread.h>

namespace oceanbase
{
const uint64_t MAX_CONSUMER_THREAD_NUM = 5;
const uint64_t MAX_LS_COUNT = 10;
using namespace archive;
using namespace testing::internal;
namespace unittest
{

typedef common::ObLinkHashMap<ObLSID, ObLSArchiveTask> LSArchiveMap;
class FakeArchiveWorker : public ObArchiveWorker
{
public:
  int push_task_status(ObArchiveTaskStatus *task_status)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(task_status)) {
      ret = OB_INVALID_ARGUMENT;
      ARCHIVE_LOG(ERROR, "invalid argument", K(ret), K(task_status));
    } else if (OB_FAIL(task_queue_.push(task_status))) {
      ARCHIVE_LOG(WARN, "push fail", K(ret), KPC(task_status));
    }
    return OB_SUCCESS;
  }

  int handle_task_list(void *data)
  {
    int ret = OB_SUCCESS;
    bool exist = false;
    ObLink *link = NULL;
    ObArchiveSendTask *task = NULL;
    ObArchiveTaskStatus *task_status = static_cast<ObArchiveTaskStatus *>(data);

    for (int64_t i = 0; OB_SUCC(ret) && i < 100 ; i++) {
      exist = false;
      task = NULL;
      if (OB_FAIL(task_status->top(link, exist))) {
        ARCHIVE_LOG(WARN, "top failed", K(ret));
      } else if (! exist) {
        break;
      } else if (OB_ISNULL(link)) {
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "link is NULL", K(ret));
      } else if (FALSE_IT(task = static_cast<ObArchiveSendTask *>(link))) {

      }

      // handle task
      if (OB_SUCC(ret) && NULL != task) {
        if (OB_FAIL(task_status->pop_front(1))) {
          ARCHIVE_LOG(ERROR, "pop front failed", K(ret), K(task_status));
        } else {
          //release_send_task(task);
        }
      }
    }

    return ret;
  }
  common::ObSpLinkQueue task_queue_;
};

class Works
{
public:
  Works() : flag_(false), map_(), queue_(), worker_(), allocator_() {}
  ~Works() { flag_ = false; }
public:
  bool flag_;
  LSArchiveMap map_;
  ObSpLinkQueue queue_;
  FakeArchiveWorker worker_;
  ObArchiveAllocator allocator_;
};

void *do_produce(void *data)
{
  int err = 0;
  Works *work = static_cast<Works *>(data);
  FakeArchiveWorker worker;
  ARCHIVE_LOG(INFO, "COME ");
  for (int i = 1; i < 20; i++) {
    for (int j = 0; j < MAX_LS_COUNT; j++) {
      ObLSID key(j + 1);
      ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
      void *buf = ob_malloc(sizeof(ObArchiveSendTask), attr);
      ObArchiveSendTask *send_task = new (buf) ObArchiveSendTask();
      ObLSArchiveTask *ls_task = NULL;
      if (0 != (err = work->map_.get(key, ls_task))) {
        ARCHIVE_LOG(WARN, "map get fail", K(err));
      } else {
        ls_task->push_send_task(*send_task, worker);
      }
      usleep(10);
    }
  }

  for (int i = 0; i < MAX_LS_COUNT; i++) {
    ObLSID key(i + 1);
    ObLSArchiveTask *ls_task = NULL;
    if (0 != (err = work->map_.get(key, ls_task))) {
      ARCHIVE_LOG(WARN, "map get fail", K(err));
    } else {
//      ls_task->mock_free_task_status();
    }
  }

  usleep(1000);
  work->flag_ = false;
  return NULL;
}

void *do_consume(void *data)
{
  int err = 0;
  Works *work = static_cast<Works *>(data);
  ObSpLinkQueue &queue = work->worker_.task_queue_;
  while (work->flag_) {
    if (! queue.is_empty()) {
      ObLink *link = NULL;
      err = queue.pop(link);
      if (NULL == link) {
        ARCHIVE_LOG(WARN, "link is NULL", K(err));
      } else {
        ObArchiveTaskStatus *status = static_cast<ObArchiveTaskStatus *>(link);

        bool jump = false;
        int num = 0;
        while (! jump) {
          ObArchiveSendTask *task = NULL;
          bool exist = false;
          ObLink *t_link = NULL;
          err = status->pop(t_link, exist);
          EXPECT_EQ(0, err);

          if (! exist) {
            jump = true;
          } else {
            task = static_cast<ObArchiveSendTask *>(t_link);
            ob_free(task);
            jump = (++num) >= 2;
          }
        }

        {
          bool is_queue_empty = false;
          bool is_discarded = false;
          err = status->retire(is_queue_empty, is_discarded);
          EXPECT_EQ(0, err);
          usleep(50);
        }
      }
    } else {
      usleep(10);
    }
  }
  return NULL;
}

TEST(TestObArchiveTask, smoke_test)
{
  int err = OB_SUCCESS;

  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
  void *buf = ob_malloc(sizeof(Works), attr);
  Works *work = new (buf) Works();
  work->flag_ = true;
  work->allocator_.init();
  for (int i = 0; i < MAX_LS_COUNT; i++)
  {
    ObLSID key(i + 1);
    ObLSArchiveTask *ls_archive_task = NULL;
    if (0 != (err = work->map_.alloc_value(ls_archive_task))) {
      ARCHIVE_LOG(WARN, "alloc_value fail");
    } else {
      ls_archive_task->mock_init(key, &work->allocator_);
      if (0 != (err = work->map_.insert_and_get(key, ls_archive_task))) {
        ARCHIVE_LOG(WARN, "insert_and_get fail");
      } else {
        work->map_.revert(ls_archive_task);
      }
    }
  }

  {
    ARCHIVE_LOG(INFO, "Archive Task Consumer start");
    fprintf(stdout, "Archive Task Consumer start");
    pthread_t threads[MAX_CONSUMER_THREAD_NUM];
    for (int i = 0; i < MAX_CONSUMER_THREAD_NUM; i++) {
      err = pthread_create(threads + i, NULL, do_consume, work);
      ASSERT_EQ(0, err);
    }
  }

  {
    ARCHIVE_LOG(INFO, "Archive Task Producer start");
    fprintf(stdout, "Archive Task Producer start");
    pthread_t threads;
    err = pthread_create(&threads, NULL, do_produce, work);
  }
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_archive_task.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
