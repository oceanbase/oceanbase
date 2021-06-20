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

#include "archive/ob_pg_archive_task.h"
#include <gtest/gtest.h>
#include "archive/ob_archive_allocator.h"
#include <pthread.h>

namespace oceanbase {
const uint64_t MAX_CONSUMER_THREAD_NUM = 5;
const uint64_t MAX_PG_COUNT = 10;
using namespace archive;
using namespace testing::internal;
namespace unittest {

typedef common::ObLinkHashMap<ObPGKey, ObPGArchiveTask> PGArchiveMap;
class Works {
public:
  Works() : flag_(false), map_(), queue_()
  {}
  ~Works()
  {
    flag_ = false;
  }

public:
  bool flag_;
  PGArchiveMap map_;
  ObSpLinkQueue queue_;
  ObArchiveAllocator allocator_;
};

void* do_produce(void* data)
{
  int err = 0;
  Works* work = static_cast<Works*>(data);
  ObSpLinkQueue& queue = work->queue_;
  ARCHIVE_LOG(INFO, "COME ");
  for (int i = 1; i < 20; i++) {
    for (int j = 0; j < MAX_PG_COUNT; j++) {
      ObPGKey key(j + 1, 0, 0);
      ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
      void* buf = ob_malloc(sizeof(ObArchiveSendTask), attr);
      ObArchiveSendTask* send_task = new (buf) ObArchiveSendTask();
      send_task->start_log_id_ = i;
      ObPGArchiveTask* pg_task = NULL;
      if (0 != (err = work->map_.get(key, pg_task))) {
        ARCHIVE_LOG(WARN, "map get fail", K(err));
      } else {
        pg_task->mock_push_task(*send_task, queue);
      }
      usleep(10);
    }
  }

  for (int i = 0; i < MAX_PG_COUNT; i++) {
    ObPGKey key(i + 1, 0, 0);
    ObPGArchiveTask* pg_task = NULL;
    if (0 != (err = work->map_.get(key, pg_task))) {
      ARCHIVE_LOG(WARN, "map get fail", K(err));
    } else {
      ARCHIVE_LOG(INFO, "COme ", KPC(pg_task));
      pg_task->mock_free_task_status();
    }
  }

  usleep(1000);
  work->flag_ = false;
  return NULL;
}

void* do_consume(void* data)
{
  int err = 0;
  Works* work = static_cast<Works*>(data);
  ObSpLinkQueue& queue = work->queue_;
  while (work->flag_) {
    if (!queue.is_empty()) {
      ObLink* link = NULL;
      err = queue.pop(link);
      if (NULL == link) {
        ARCHIVE_LOG(WARN, "link is NULL", K(err));
      } else {
        ObArchiveSendTaskStatus* status = static_cast<ObArchiveSendTaskStatus*>(link);

        bool jump = false;
        int num = 0;
        while (!jump) {
          ObArchiveSendTask* task = NULL;
          bool exist = false;
          ObLink* t_link = NULL;
          err = status->pop(t_link, exist);
          EXPECT_EQ(0, err);

          if (!exist) {
            jump = true;
          } else {
            task = static_cast<ObArchiveSendTask*>(t_link);
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
  void* buf = ob_malloc(sizeof(Works), attr);
  Works* work = new (buf) Works();
  work->flag_ = true;
  work->allocator_.init();
  for (int i = 0; i < MAX_PG_COUNT; i++) {
    ObPGKey key(i + 1, 0, 0);
    ObPGArchiveTask* pg_archive_task = NULL;
    if (0 != (err = work->map_.alloc_value(pg_archive_task))) {
      ARCHIVE_LOG(WARN, "alloc_value fail");
    } else {
      pg_archive_task->mock_init(key, &work->allocator_);
      if (0 != (err = work->map_.insert_and_get(key, pg_archive_task))) {
        ARCHIVE_LOG(WARN, "insert_and_get fail");
      } else {
        work->map_.revert(pg_archive_task);
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

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_archive_task.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
