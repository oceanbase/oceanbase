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

namespace oceanbase
{
namespace common
{
#define REG_LOG_PAR_MOD(ParMod)                                 \
  struct Reg_Log_Par_Mod_##ParMod                               \
  {                                                             \
    Reg_Log_Par_Mod_##ParMod()                                  \
    {                                                           \
      OB_LOGGER.register_mod(OB_LOG_ROOT::M_##ParMod, #ParMod); \
    }                                                           \
  } reg_log_par_mod_##ParMod;

#define REG_LOG_SUB_MOD(ParMod, SubMod)                                                      \
  struct Reg_Log_Sub_Mod_##ParMod##_##SubMod                                                 \
  {                                                                                          \
    Reg_Log_Sub_Mod_##ParMod##_##SubMod()                                                    \
    {                                                                                        \
      OB_LOGGER.register_mod(OB_LOG_ROOT::M_##ParMod, OB_LOG_##ParMod::M_##SubMod, #SubMod); \
    }                                                                                        \
  } reg_log_sub_mod_##ParMod##_##SubMod;

//regist par modules
REG_LOG_PAR_MOD(CLIENT)
REG_LOG_PAR_MOD(CLOG)
REG_LOG_PAR_MOD(COMMON)
REG_LOG_PAR_MOD(ELECT)
REG_LOG_PAR_MOD(LIB)
REG_LOG_PAR_MOD(OFS)
REG_LOG_PAR_MOD(RPC)
REG_LOG_PAR_MOD(RS)
REG_LOG_PAR_MOD(SERVER)
REG_LOG_PAR_MOD(SHARE)
REG_LOG_PAR_MOD(SQL)
REG_LOG_PAR_MOD(STORAGE)
REG_LOG_PAR_MOD(TLOG)
REG_LOG_PAR_MOD(STORAGETEST)
REG_LOG_PAR_MOD(WRS)
REG_LOG_PAR_MOD(PALF)
REG_LOG_PAR_MOD(ARCHIVE)
REG_LOG_PAR_MOD(LOGTOOL)
REG_LOG_PAR_MOD(DATA_DICT)
REG_LOG_PAR_MOD(BALANCE)
REG_LOG_PAR_MOD(LOGMINER)

//regist WRS's sub-modules
REG_LOG_SUB_MOD(WRS, CLUSTER)
REG_LOG_SUB_MOD(WRS,SERVER)

//regist LIB's sub-modules
REG_LOG_SUB_MOD(LIB, ALLOC)
REG_LOG_SUB_MOD(LIB, CONT)
REG_LOG_SUB_MOD(LIB, FILE)
REG_LOG_SUB_MOD(LIB, HASH)
REG_LOG_SUB_MOD(LIB, LOCK)
REG_LOG_SUB_MOD(LIB, MYSQLC)
REG_LOG_SUB_MOD(LIB, STRING)
REG_LOG_SUB_MOD(LIB, TIME)
REG_LOG_SUB_MOD(LIB, UTIL)
REG_LOG_SUB_MOD(LIB, CHARSET)

//regist OFS's sub-modules
REG_LOG_SUB_MOD(OFS, BLOCK)
REG_LOG_SUB_MOD(OFS, BLOCKSERVER)
REG_LOG_SUB_MOD(OFS, CLIENT)
REG_LOG_SUB_MOD(OFS, CLUSTER)
REG_LOG_SUB_MOD(OFS, COMMON)
REG_LOG_SUB_MOD(OFS, FILE)
REG_LOG_SUB_MOD(OFS, FS)
REG_LOG_SUB_MOD(OFS, MASTER)
REG_LOG_SUB_MOD(OFS, REGRESSION)
REG_LOG_SUB_MOD(OFS, RPC)
REG_LOG_SUB_MOD(OFS, STREAM)
REG_LOG_SUB_MOD(OFS, UTIL)
REG_LOG_SUB_MOD(OFS, LIBS)
REG_LOG_SUB_MOD(OFS, SHARE)

// register RPC's sub-modules
REG_LOG_SUB_MOD(RPC, FRAME)
REG_LOG_SUB_MOD(RPC, OBRPC)
REG_LOG_SUB_MOD(RPC, OBMYSQL)
REG_LOG_SUB_MOD(RPC, TEST)

//regist COMMON's sub-modules
REG_LOG_SUB_MOD(COMMON, CACHE)
REG_LOG_SUB_MOD(COMMON, EXPR)
REG_LOG_SUB_MOD(COMMON, LEASE)
REG_LOG_SUB_MOD(COMMON, MYSQLP)
REG_LOG_SUB_MOD(COMMON, PRI)
REG_LOG_SUB_MOD(COMMON, STAT)
REG_LOG_SUB_MOD(COMMON, UPSR)

//regist SHARE's sub-modules
REG_LOG_SUB_MOD(SHARE, CONFIG)
REG_LOG_SUB_MOD(SHARE, FILE)
REG_LOG_SUB_MOD(SHARE, INNERT)
REG_LOG_SUB_MOD(SHARE, INTERFACE)
REG_LOG_SUB_MOD(SHARE, LOG)
REG_LOG_SUB_MOD(SHARE, PT)
REG_LOG_SUB_MOD(SHARE, SCHEMA)
REG_LOG_SUB_MOD(SHARE, TRIGGER)
REG_LOG_SUB_MOD(SHARE, LOCATION)

//regist STORAGE's sub-modules
REG_LOG_SUB_MOD(STORAGE, REDO)
REG_LOG_SUB_MOD(STORAGE, COMPACTION)
REG_LOG_SUB_MOD(STORAGE, BSST)
REG_LOG_SUB_MOD(STORAGE, MEMT)
REG_LOG_SUB_MOD(STORAGE, TRANS)
REG_LOG_SUB_MOD(STORAGE, DUP_TABLE)
REG_LOG_SUB_MOD(STORAGE, REPLAY)
REG_LOG_SUB_MOD(STORAGE, IMC)
REG_LOG_SUB_MOD(STORAGE, TABLELOCK)
REG_LOG_SUB_MOD(STORAGE, BLKMGR)
REG_LOG_SUB_MOD(STORAGE, FTS)

// reigst CLOG's sub-modules
REG_LOG_SUB_MOD(CLOG, EXTLOG)
REG_LOG_SUB_MOD(CLOG, CSR)
REG_LOG_SUB_MOD(CLOG, ARCHIVE)

//regist SQL's sub-modules
REG_LOG_SUB_MOD(SQL, ENG)
REG_LOG_SUB_MOD(SQL, EXE)
REG_LOG_SUB_MOD(SQL, OPT)
REG_LOG_SUB_MOD(SQL, JO)
REG_LOG_SUB_MOD(SQL, PARSER)
REG_LOG_SUB_MOD(SQL, PC)
REG_LOG_SUB_MOD(SQL, RESV)
REG_LOG_SUB_MOD(SQL, REWRITE)
REG_LOG_SUB_MOD(SQL, SESSION)
REG_LOG_SUB_MOD(SQL, CG)
REG_LOG_SUB_MOD(SQL, MONITOR)

//observer
REG_LOG_SUB_MOD(SERVER, OMT)

// rootserver
REG_LOG_SUB_MOD(RS, LB)
REG_LOG_SUB_MOD(RS, RESTORE)

//regist storagetest
REG_LOG_SUB_MOD(STORAGETEST, TEST)

// register liboblog's sub-modules
REG_LOG_SUB_MOD(TLOG, FETCHER)
REG_LOG_SUB_MOD(TLOG, PARSER)
REG_LOG_SUB_MOD(TLOG, SEQUENCER)
REG_LOG_SUB_MOD(TLOG, FORMATTER)
REG_LOG_SUB_MOD(TLOG, COMMITTER)
REG_LOG_SUB_MOD(TLOG, TAILF)
REG_LOG_SUB_MOD(TLOG, SCHEMA)
REG_LOG_SUB_MOD(TLOG, STORAGER)
REG_LOG_SUB_MOD(TLOG, READER)
REG_LOG_SUB_MOD(TLOG, DISPATCHER)
REG_LOG_SUB_MOD(TLOG, SORTER)

// regist balance.transfer
REG_LOG_SUB_MOD(BALANCE, TRANSFER)

}
}
