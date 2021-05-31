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

#ifdef TG_DEF
// (tg_def_id, name, desc, scope, type, args...)
// for test
TG_DEF(TEST1, test1, "", TG_STATIC, TIMER)
TG_DEF(TEST2, test2, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(1, 1), 10)
TG_DEF(TEST3, test3, "", TG_STATIC, DEDUP_QUEUE, ThreadCountPair(1, 1), 8, 8, 16L << 20, 16L << 20, 8192, "test")
TG_DEF(TEST4, test4, "", TG_STATIC, THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(TEST5, test5, "", TG_STATIC, ASYNC_TASK_QUEUE, ThreadCountPair(1, 1), 16)
TG_DEF(TEST6, test6, "", TG_STATIC, TIMER_GROUP, ThreadCountPair(2, 2))
// other
TG_DEF(MEMORY_DUMP, memDump, "", TG_STATIC, THREAD_POOL, ThreadCountPair(1, 1))
TG_DEF(IOFaultDetector, IOFDetector, "", TG_STATIC, QUEUE_THREAD, ThreadCountPair(1, 1), 100)
TG_DEF(SchemaRefTask, SchemaRefTask, "", TG_STATIC, DEDUP_QUEUE, ThreadCountPair(1, 1), 1024, 1024, 1L << 30,
    512L << 20, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObModIds::OB_SCHEMA_DEDUP_QUEUE)
TG_DEF(CONFIG_MGR, ConfigMgr, "", TG_STATIC, TIMER)
TG_DEF(ReqMemEvict, ReqMemEvict, "", TG_DYNAMIC, TIMER)
TG_DEF(IOMGR, IOMGR, "", TG_STATIC, THREAD_POOL,
    ThreadCountPair(ObIOManager::MAX_CALLBACK_THREAD_CNT + 1, ObIOManager::MINI_MODE_MAX_CALLBACK_THREAD_CNT + 1))
TG_DEF(IODISK, IODISK, "", TG_DYNAMIC, THREAD_POOL,
    ThreadCountPair(ObDisk::MAX_DISK_CHANNEL_CNT * 2, ObDisk::MINI_MODE_DISK_CHANNEL_CNT * 2))
TG_DEF(TIMEZONE_MGR, TimezoneMgr, "", TG_STATIC, TIMER)
#endif
