/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef TG_DEF
// (tg_def_id, name, type, args...)
// for test
TG_DEF(TEST1, test1, TIMER)
TG_DEF(TEST2, test2, QUEUE_THREAD, 1, 10)
TG_DEF(TEST3, test3, DEDUP_QUEUE, 1, 8, 8, 16L << 20, 16L << 20,8192, "test")
TG_DEF(TEST4, test4, THREAD_POOL, 1)
TG_DEF(TEST5, test5, ASYNC_TASK_QUEUE, 1, 16)
TG_DEF(TEST6, test6, MAP_QUEUE_THREAD, 2)
// TG_DEF(TEST7, test7, QUEUE_THREAD, 10, 10)
TG_DEF(TEST8, test8, REENTRANT_THREAD_POOL, 1)
// other
TG_DEF(MEMORY_DUMP, memDump, THREAD_POOL, 1)
TG_DEF(SchemaRefTask, SchemaRefTask, DEDUP_QUEUE, 1, 1024, 1024, 1L << 30, 512L << 20, common::OB_MALLOC_BIG_BLOCK_SIZE, "SchemaDedupQueu")
TG_DEF(replica_control, replica_control, THREAD_POOL, 1)
TG_DEF(QUEUE_THREAD_MANAGER, qth_mgr, THREAD_POOL, 1)
TG_DEF(SYSLOG_COMPRESS, SyslogCompress, THREAD_POOL, 1)
#endif
