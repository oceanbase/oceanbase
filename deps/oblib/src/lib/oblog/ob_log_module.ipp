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

/*
 * Currently it is divided by directory. Each module can be adjusted according to the
 * specific situation during the use process
 */

//statement of parent modules
LOG_PAR_MOD_BEGIN                            // src directory
DEFINE_LOG_PAR_MOD(CLIENT)                   // client
DEFINE_LOG_PAR_MOD(CLOG)                     // clog
DEFINE_LOG_PAR_MOD(COMMON)                   // observer
DEFINE_LOG_PAR_MOD(ELECT)                    // election
DEFINE_LOG_PAR_MOD(OCCAM)                    // occam tools
DEFINE_LOG_PAR_MOD(LIB)                      // lib
DEFINE_LOG_PAR_MOD(OFS)                      // OFS
DEFINE_LOG_PAR_MOD(RPC)                      // rpc
DEFINE_LOG_PAR_MOD(RS)                       // rootserver
DEFINE_LOG_PAR_MOD(BOOTSTRAP)                // rootserver
DEFINE_LOG_PAR_MOD(SERVER)                   // rpc, common/server_framework
DEFINE_LOG_PAR_MOD(SHARE)                    // share
DEFINE_LOG_PAR_MOD(SQL)                      // sql
DEFINE_LOG_PAR_MOD(PL)                       // pl
DEFINE_LOG_PAR_MOD(JIT)                      // jit
DEFINE_LOG_PAR_MOD(STORAGE)                  // storage, blocksstable
DEFINE_LOG_PAR_MOD(TLOG)                     // liboblog
DEFINE_LOG_PAR_MOD(STORAGETEST)              // storagetest
DEFINE_LOG_PAR_MOD(LOGTOOL)                  // logtool
DEFINE_LOG_PAR_MOD(WRS)                      // weak read service
DEFINE_LOG_PAR_MOD(ARCHIVE)                  // archive log
DEFINE_LOG_PAR_MOD(PHYSICAL_RESTORE_ARCHIVE) // physical restore log
DEFINE_LOG_PAR_MOD(EASY)                     // libeasy
DEFINE_LOG_PAR_MOD(DETECT)                   // dead lock
DEFINE_LOG_PAR_MOD(PALF)                     // palf
DEFINE_LOG_PAR_MOD(STANDBY)                  // primary and standby cluster
DEFINE_LOG_PAR_MOD(COORDINATOR)              // leader coordinator
DEFINE_LOG_PAR_MOD(FLT)                      // trace
DEFINE_LOG_PAR_MOD(OBTRACE)                  // trace
DEFINE_LOG_PAR_MOD(BALANCE)                  // balance module
DEFINE_LOG_PAR_MOD(MDS)                      // multi data source
DEFINE_LOG_PAR_MOD(DATA_DICT)                // data_dictionary module
DEFINE_LOG_PAR_MOD(MVCC)                     // concurrency_control
DEFINE_LOG_PAR_MOD(WR)                       // workload repository
DEFINE_LOG_PAR_MOD(LOGMINER)                 // logminer
LOG_PAR_MOD_END

//statement of WRS's sub_modules
LOG_SUB_MOD_BEGIN(WRS)
DEFINE_LOG_SUB_MOD(CLUSTER)
DEFINE_LOG_SUB_MOD(SERVER)
LOG_SUB_MOD_END(WRS)

//statement of LIB's sub-modules
LOG_SUB_MOD_BEGIN(LIB)                   // lib directory
DEFINE_LOG_SUB_MOD(ALLOC)                // allocator
DEFINE_LOG_SUB_MOD(CONT)                 // all container except hash: list, queue, array, hash
DEFINE_LOG_SUB_MOD(FILE)                 // file
DEFINE_LOG_SUB_MOD(HASH)                 // hash
DEFINE_LOG_SUB_MOD(LOCK)                 // lock
DEFINE_LOG_SUB_MOD(MYSQLC)               // internal mysqlclient
DEFINE_LOG_SUB_MOD(STRING)               // string
DEFINE_LOG_SUB_MOD(TIME)                 // time
DEFINE_LOG_SUB_MOD(UTIL)                 // utility
DEFINE_LOG_SUB_MOD(CHARSET)              // charset
DEFINE_LOG_SUB_MOD(WS)                   // word segment
DEFINE_LOG_SUB_MOD(OCI)                  // oci log
LOG_SUB_MOD_END(LIB)

//statement of OFS's sub-modules
LOG_SUB_MOD_BEGIN(OFS)                   // OFS directory
DEFINE_LOG_SUB_MOD(BLOCK)                // block
DEFINE_LOG_SUB_MOD(BLOCKSERVER)          // blockserver
DEFINE_LOG_SUB_MOD(CLIENT)               // client
DEFINE_LOG_SUB_MOD(CLUSTER)              // cluster
DEFINE_LOG_SUB_MOD(COMMON)               // common
DEFINE_LOG_SUB_MOD(FILE)                 // file
DEFINE_LOG_SUB_MOD(MASTER)               // master
DEFINE_LOG_SUB_MOD(STREAM)               // stream
DEFINE_LOG_SUB_MOD(RPC)                  // rpc
DEFINE_LOG_SUB_MOD(UTIL)                 // util
DEFINE_LOG_SUB_MOD(FS)                   // fs
DEFINE_LOG_SUB_MOD(REGRESSION)           // regression
DEFINE_LOG_SUB_MOD(LIBS)                 // libs
DEFINE_LOG_SUB_MOD(SHARE)                // share
LOG_SUB_MOD_END(OFS)

// statements of RPC's sub-modules
LOG_SUB_MOD_BEGIN(RPC)
DEFINE_LOG_SUB_MOD(FRAME)
DEFINE_LOG_SUB_MOD(OBRPC)
DEFINE_LOG_SUB_MOD(OBMYSQL)
DEFINE_LOG_SUB_MOD(TEST)
LOG_SUB_MOD_END(RPC)

//statement of COMMON's sub-modules
LOG_SUB_MOD_BEGIN(COMMON)                // common directory
DEFINE_LOG_SUB_MOD(CACHE)                // cache
DEFINE_LOG_SUB_MOD(EXPR)                 // expression
DEFINE_LOG_SUB_MOD(LEASE)                // lease
DEFINE_LOG_SUB_MOD(MYSQLP)               // mysql_proxy
DEFINE_LOG_SUB_MOD(PRI)                  // privilege
DEFINE_LOG_SUB_MOD(STAT)                 // stat
DEFINE_LOG_SUB_MOD(UPSR)                 // ups_reclaim
LOG_SUB_MOD_END(COMMON)

//statement of SHARE's sub-modules
LOG_SUB_MOD_BEGIN(SHARE)                 // share directory
DEFINE_LOG_SUB_MOD(CONFIG)               // config
DEFINE_LOG_SUB_MOD(FILE)                 // file
DEFINE_LOG_SUB_MOD(INNERT)               // inner_table
DEFINE_LOG_SUB_MOD(INTERFACE)            // interface
DEFINE_LOG_SUB_MOD(LOG)                  // log
DEFINE_LOG_SUB_MOD(PT)                   // partition_table
DEFINE_LOG_SUB_MOD(SCHEMA)               // schema
DEFINE_LOG_SUB_MOD(TRIGGER)              // trigger
DEFINE_LOG_SUB_MOD(LOCATION)             // location_cache
LOG_SUB_MOD_END(SHARE)

//statement of storage's sub-modules
LOG_SUB_MOD_BEGIN(STORAGE)               // storage, blocksstable directory
DEFINE_LOG_SUB_MOD(REDO)                 // replay
DEFINE_LOG_SUB_MOD(COMPACTION)           // compaction
DEFINE_LOG_SUB_MOD(BSST)                 // blocksstable
DEFINE_LOG_SUB_MOD(MEMT)                 // memtable
DEFINE_LOG_SUB_MOD(TRANS)                // transaction
DEFINE_LOG_SUB_MOD(RU)                   // transaction
DEFINE_LOG_SUB_MOD(REPLAY)               // replay engine
DEFINE_LOG_SUB_MOD(IMC)                  // imc
DEFINE_LOG_SUB_MOD(DUP_TABLE)            // dup_table
DEFINE_LOG_SUB_MOD(TABLELOCK)            // tablelock
DEFINE_LOG_SUB_MOD(BLKMGR)               // block manager
LOG_SUB_MOD_END(STORAGE)

// statement of clog's sub-modules
LOG_SUB_MOD_BEGIN(CLOG)                  // clog directory
DEFINE_LOG_SUB_MOD(EXTLOG)               // external fetch log
DEFINE_LOG_SUB_MOD(CSR)                  // cursor cache
DEFINE_LOG_SUB_MOD(ARCHIVE)              //archive
LOG_SUB_MOD_END(CLOG)

//statement of SQL's sub-modules
LOG_SUB_MOD_BEGIN(SQL)                   // sql directory
DEFINE_LOG_SUB_MOD(ENG)                  // engine
DEFINE_LOG_SUB_MOD(EXE)                  // execute
DEFINE_LOG_SUB_MOD(OPT)                  // optimizer
DEFINE_LOG_SUB_MOD(JO)                   // join order
DEFINE_LOG_SUB_MOD(PARSER)               // parser
DEFINE_LOG_SUB_MOD(PC)                   // plan_cache
DEFINE_LOG_SUB_MOD(RESV)                 // resolver
DEFINE_LOG_SUB_MOD(REWRITE)              // rewrite
DEFINE_LOG_SUB_MOD(SESSION)              // session
DEFINE_LOG_SUB_MOD(CG)                   // code_generator
DEFINE_LOG_SUB_MOD(MONITOR)              // monitor
DEFINE_LOG_SUB_MOD(DTL)                  // data transfer layer
DEFINE_LOG_SUB_MOD(DAS)                  // data access service
DEFINE_LOG_SUB_MOD(SPM)                  // sql plan baseline
DEFINE_LOG_SUB_MOD(QRR)                  // query rewrite rule
LOG_SUB_MOD_END(SQL)

// observer submodules
LOG_SUB_MOD_BEGIN(SERVER)
DEFINE_LOG_SUB_MOD(OMT)                  // user mode scheduler
LOG_SUB_MOD_END(SERVER)

// RS submodules
LOG_SUB_MOD_BEGIN(RS)
DEFINE_LOG_SUB_MOD(LB)                  // load balance
DEFINE_LOG_SUB_MOD(RESTORE)             // restore related
LOG_SUB_MOD_END(RS)

// Balance submodules
LOG_SUB_MOD_BEGIN(BALANCE)
DEFINE_LOG_SUB_MOD(TRANSFER)            // transfer service
LOG_SUB_MOD_END(BALANCE)

// liboblog submodules
LOG_SUB_MOD_BEGIN(TLOG)
DEFINE_LOG_SUB_MOD(FETCHER)                 // fetcher
DEFINE_LOG_SUB_MOD(PARSER)                  // Parser
DEFINE_LOG_SUB_MOD(SEQUENCER)               // Sequencer
DEFINE_LOG_SUB_MOD(FORMATTER)               // Formatter
DEFINE_LOG_SUB_MOD(COMMITTER)               // committer
DEFINE_LOG_SUB_MOD(TAILF)                   // oblog_tailf
DEFINE_LOG_SUB_MOD(SCHEMA)                  // schema
DEFINE_LOG_SUB_MOD(STORAGER)                // storager
DEFINE_LOG_SUB_MOD(READER)                  // reader
DEFINE_LOG_SUB_MOD(DISPATCHER)              // redo_dispatcher
DEFINE_LOG_SUB_MOD(SORTER)                  // br_sorter
LOG_SUB_MOD_END(TLOG)

// easy submodules
LOG_SUB_MOD_BEGIN(EASY)
DEFINE_LOG_SUB_MOD(IO)                  // EasyIO
LOG_SUB_MOD_END(EASY)

// storagetest submodules
LOG_SUB_MOD_BEGIN(STORAGETEST)
DEFINE_LOG_SUB_MOD(TEST)                  // Parser
LOG_SUB_MOD_END(STORAGETEST)

// PL submodules
LOG_SUB_MOD_BEGIN(PL)
DEFINE_LOG_SUB_MOD(RESV)               // resolver
DEFINE_LOG_SUB_MOD(CG)                 // code generator
DEFINE_LOG_SUB_MOD(SPI)                // service program interface
DEFINE_LOG_SUB_MOD(PACK)               // package
DEFINE_LOG_SUB_MOD(TYPE)               // type
DEFINE_LOG_SUB_MOD(DEBUG)              // debug
DEFINE_LOG_SUB_MOD(CACHE)              // cache
DEFINE_LOG_SUB_MOD(STORAGEROUTINE)     // storage routine
LOG_SUB_MOD_END(PL)
