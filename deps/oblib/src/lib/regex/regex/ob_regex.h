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

#ifndef OCEANBASE_LIB_REGEX_REGEX_OB_REGEX_
#define OCEANBASE_LIB_REGEX_REGEX_OB_REGEX_ /* never again */
/*
 * regular expressions
 *
 * Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
 *
 * Development of this software was funded, in part, by Cray Research Inc.,
 * UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
 * Corporation, none of whom are responsible for the results. The author
 * thanks all of them.
 *
 * Redistribution and use in source and binary forms -- with or without
 * modification -- are permitted for any purpose, provided that
 * redistributions in source form retain this entire copyright notice and
 * indicate the origin and nature of any modifications.
 *
 * I'd appreciate being given credit for this package in the documentation of
 * software which uses it, but that is not a requirement.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * HENRY SPENCER BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *
 * Prototypes etc. marked with "^" within comments get gathered up (and
 * possibly edited) by the regfwd program and inserted near the bottom of this
 * file.
 *
 * We offer the option of declaring one wide-character version of the RE
 * functions as well as the char versions. To do that, define __REG_WIDE_T to
 * the type of wide characters (unfortunately, there is no consensus that
 * wchar_t is suitable) and __REG_WIDE_COMPILE and __REG_WIDE_EXEC to the
 * names to be used for the compile and execute functions (suggestion:
 * re_Xcomp and re_Xexec, where X is a letter suggestive of the wide type,
 * e.g. re_ucomp and re_uexec for Unicode). For cranky old compilers, it may
 * be necessary to do something like:
 * #define	__REG_WIDE_COMPILE(a,b,c,d)	re_Xcomp(a,b,c,d)
 * #define	__REG_WIDE_EXEC(a,b,c,d,e,f,g)	re_Xexec(a,b,c,d,e,f,g)
 * rather than just #defining the names as parameterless macros.
 *
 * For some specialized purposes, it may be desirable to suppress the
 * declarations of the "front end" functions, ob_regcomp() and ob_regexec(), or of
 * the char versions of the compile and execute functions. To suppress the
 * front-end functions, define __REG_NOFRONT. To suppress the char versions,
 * define __REG_NOCHAR.
 *
 * The right place to do those defines (and some others you may want, see
 * below) would be <sys/types.h>. If you don't have control of that file, the
 * right place to add your own defines to this file is marked below. This is
 * normally done automatically, by the makefile and regmkhdr, based on the
 * contents of regcustom.h.
 */

#include "common/ob_common_utility.h"

/*
 * voodoo for C++
 */
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Add your own defines, if needed, here.
 */

/*
 * Location where a chunk of regcustom.h is automatically spliced into this
 * file (working from its prototype, regproto.h).
 */

/* --- begin --- */
/* ensure certain things don't sneak in from system headers */
#ifdef __REG_WIDE_T
#undef __REG_WIDE_T
#endif
#ifdef __REG_WIDE_COMPILE
#undef __REG_WIDE_COMPILE
#endif
#ifdef __REG_WIDE_EXEC
#undef __REG_WIDE_EXEC
#endif
#ifdef __REG_REGOFF_T
#undef __REG_REGOFF_T
#endif
#ifdef __REG_VOID_T
#undef __REG_VOID_T
#endif
#ifdef __REG_CONST
#undef __REG_CONST
#endif
#ifdef __REG_NOFRONT
#undef __REG_NOFRONT
#endif
#ifdef __REG_NOCHAR
#undef __REG_NOCHAR
#endif
/* interface types */
#define __REG_WIDE_T Ob_UniChar
#define __REG_REGOFF_T long /* not really right, but good enough... */
#define __REG_VOID_T void
#define __REG_CONST const
/* names and declarations */
#define __REG_WIDE_COMPILE ObReComp
#define __REG_WIDE_EXEC ObReExec
#define __REG_NOFRONT /* don't want ob_regcomp() and ob_regexec() */
#define __REG_NOCHAR  /* or the char versions */
#define ob_regfree ObReFree
#define ob_regerror ObReError
/* --- end --- */
#ifdef REGEX_STANDALONE
#undef ob_regfree
#undef ob_regerror
#define ob_regfree ob_re_free
#define ob_regerror ob_re_error
#undef __REG_WIDE_T
#define __REG_WIDE_T wchar_t
#undef __REG_WIDE_COMPILE
#define __REG_WIDE_COMPILE ob_re_wcomp
#undef __REG_WIDE_EXEC
#define __REG_WIDE_EXEC ob_re_wexec
#ifndef REGEX_WCHAR
#undef __REG_NOCHAR
#endif
#endif

/*
 * interface types etc.
 */

/*
 * ob_regoff_t has to be large enough to hold either off_t or ssize_t, and must
 * be signed; it's only a guess that long is suitable, so we offer
 * <sys/types.h> an override.
 */
#ifdef __REG_REGOFF_T
typedef __REG_REGOFF_T ob_regoff_t;
#else
typedef long ob_regoff_t;
#endif

/*
 * For benefit of old compilers, we offer <sys/types.h> the option of
 * overriding the `void' type used to declare nonexistent return types.
 */
#ifdef __REG_VOID_T
typedef __REG_VOID_T re_void;
#else
typedef void re_void;
#endif

/*
 * Also for benefit of old compilers, <sys/types.h> can supply a macro which
 * expands to a substitute for `const'.
 */
#ifndef __REG_CONST
#define __REG_CONST const
#endif

/*
 * other interface types
 */

/* the biggie, a compiled RE (or rather, a front end to same) */
typedef struct {
  int re_magic;   /* magic number */
  size_t re_nsub; /* number of subexpressions */
  long re_info;   /* information about RE */
#define OB_REG_UBACKREF 000001
#define OB_REG_ULOOKAHEAD 000002
#define OB_REG_UBOUNDS 000004
#define OB_REG_UBRACES 000010
#define OB_REG_UBSALNUM 000020
#define OB_REG_UPBOTCH 000040
#define OB_REG_UBBS 000100
#define OB_REG_UNONPOSIX 000200
#define OB_REG_UUNSPEC 000400
#define OB_REG_UUNPORT 001000
#define OB_REG_ULOCALE 002000
#define OB_REG_UEMPTYMATCH 004000
#define OB_REG_UIMPOSSIBLE 010000
#define OB_REG_USHORTEST 020000
  int re_csize;           /* sizeof(character) */
  const wchar_t* re_endp; /* backward compatibility kludge */
  /* the rest is opaque pointers to hidden innards */
  char* re_guts; /* `char *' is more portable than `void *' */
  char* re_fns;
} ob_regex_t;

/* result reporting (may acquire more fields later) */
typedef struct {
  ob_regoff_t rm_so; /* start of substring */
  ob_regoff_t rm_eo; /* end of substring */
} ob_regmatch_t;

/* supplementary control and reporting */
typedef struct {
  ob_regmatch_t rm_extend; /* see REG_EXPECT */
} ob_rm_detail_t;

/*
 * compilation
 ^ #ifndef __REG_NOCHAR
 ^ int ob_re_comp(ob_regex_t *, __REG_CONST char *, size_t, int);
 ^ #endif
 ^ #ifndef __REG_NOFRONT
 ^ int ob_regcomp(ob_regex_t *, __REG_CONST char *, int);
 ^ #endif
 ^ #ifdef __REG_WIDE_T
 ^ int __REG_WIDE_COMPILE(ob_regex_t *, __REG_CONST __REG_WIDE_T *, size_t, int);
 ^ #endif
 */
#define OB_REG_BASIC 000000       /* BREs (convenience) */
#define OB_REG_EXTENDED 000001    /* EREs */
#define OB_REG_ADVF 000002        /* advanced features in EREs */
#define OB_REG_ADVANCED 000003    /* AREs (which are also EREs) */
#define OB_REG_QUOTE 000004       /* no special characters, none */
#define OB_REG_NOSPEC REG_QUOTE   /* historical synonym */
#define OB_REG_ICASE 000010       /* ignore case */
#define OB_REG_NOSUB 000020       /* don't care about subexpressions */
#define OB_REG_EXPANDED 000040    /* expanded format, white space & comments */
#define OB_REG_NLSTOP 000100      /* \n doesn't match . or [^ ] */
#define OB_REG_NLANCH 000200      /* ^ matches after \n, $ before */
#define OB_REG_NEWLINE 000300     /* newlines are line terminators */
#define OB_REG_PEND 000400        /* ugh -- backward-compatibility hack */
#define OB_REG_EXPECT 001000      /* report details on partial/limited matches */
#define OB_REG_BOSONLY 002000     /* temporary kludge for BOS-only matches */
#define OB_REG_DUMP 004000        /* none of your business :-) */
#define OB_REG_FAKE 010000        /* none of your business :-) */
#define OB_REG_PROGRESS 020000    /* none of your business :-) */
#define OB_REG_ORACLE_MODE 040000 /*distinguish is oracle or mysql call regexp*/

/*
 * execution
 ^ #ifndef __REG_NOCHAR
 ^ int ob_re_exec(ob_regex_t *, __REG_CONST char *, size_t,
 ^				ob_rm_detail_t *, size_t, ob_regmatch_t [], int);
 ^ #endif
 ^ #ifndef __REG_NOFRONT
 ^ int ob_regexec(ob_regex_t *, __REG_CONST char *, size_t, ob_regmatch_t [], int);
 ^ #endif
 ^ #ifdef __REG_WIDE_T
 ^ int __REG_WIDE_EXEC(ob_regex_t *, __REG_CONST __REG_WIDE_T *, size_t,
 ^				ob_rm_detail_t *, size_t, ob_regmatch_t [], int);
 ^ #endif
 */
#define OB_REG_NOTBOL 0001   /* BOS is not BOL */
#define OB_REG_NOTEOL 0002   /* EOS is not EOL */
#define OB_REG_STARTEND 0004 /* backward compatibility kludge */
#define OB_REG_FTRACE 0010   /* none of your business */
#define OB_REG_MTRACE 0020   /* none of your business */
#define OB_REG_SMALL 0040    /* none of your business */

/*
 * misc generics (may be more functions here eventually)
 ^ re_void ob_regfree(ob_regex_t *);
 */

/*
 * error reporting
 * Be careful if modifying the list of error codes -- the table used by
 * ob_regerror() is generated automatically from this file!
 *
 * Note that there is no wide-char variant of ob_regerror at this time; what kind
 * of character is used for error reports is independent of what kind is used
 * in matching.
 *
 ^ extern size_t ob_regerror(int, __REG_CONST ob_regex_t *, char *, size_t);
 */
#define OB_REG_OKAY 0     /* no errors detected */
#define OB_REG_NOMATCH 1  /* failed to match */
#define OB_REG_BADPAT 2   /* invalid regexp */
#define OB_REG_ECOLLATE 3 /* invalid collating element */
#define OB_REG_ECTYPE 4   /* invalid character class */
#define OB_REG_EESCAPE 5  /* invalid escape \ sequence */
#define OB_REG_ESUBREG 6  /* invalid backreference number */
#define OB_REG_EBRACK 7   /* brackets [] not balanced */
#define OB_REG_EPAREN 8   /* parentheses () not balanced */
#define OB_REG_EBRACE 9   /* braces {} not balanced */
#define OB_REG_BADBR 10   /* invalid repetition count(s) */
#define OB_REG_ERANGE 11  /* invalid character range */
#define OB_REG_ESPACE 12  /* out of memory */
#define OB_REG_BADRPT 13  /* quantifier operand invalid */
#define OB_REG_ASSERT 15  /* "can't happen" -- you found a bug */
#define OB_REG_INVARG 16  /* invalid argument to regex function */
#define OB_REG_MIXED 17   /* character widths of regex and string differ */
#define OB_REG_BADOPT 18  /* invalid embedded option */
#define OB_REG_ETOOBIG 19 /* nfa has too many states */
/* two specials for debugging and testing */
#define OB_REG_ATOI 101 /* convert error-code name to number */
#define OB_REG_ITOA 102 /* convert error-code number to name */

/*
 * the prototypes, as possibly munched by regfwd
 */
/* =====^!^===== begin forwards =====^!^===== */
/* automatically gathered by fwd; do not hand-edit */
/* === regproto.h === */
#ifndef __REG_NOCHAR
int ob_re_comp(ob_regex_t*, __REG_CONST unsigned char*, size_t, int);
#endif
#ifndef __REG_NOFRONT
int ob_regcomp(ob_regex_t*, __REG_CONST char*, int);
#endif
#ifdef __REG_WIDE_T
MODULE_SCOPE int __REG_WIDE_COMPILE(ob_regex_t*, __REG_CONST __REG_WIDE_T*, size_t, int);
#endif
#ifndef __REG_NOCHAR
int ob_re_exec(ob_regex_t*, __REG_CONST unsigned char*, size_t, ob_rm_detail_t*, size_t, ob_regmatch_t[], int);
#endif
#ifndef __REG_NOFRONT
int ob_regexec(ob_regex_t*, __REG_CONST char*, size_t, ob_regmatch_t[], int);
#endif
#ifdef __REG_WIDE_T
MODULE_SCOPE int __REG_WIDE_EXEC(
    ob_regex_t*, __REG_CONST __REG_WIDE_T*, size_t, ob_rm_detail_t*, size_t, ob_regmatch_t[], int);
#endif
MODULE_SCOPE re_void ob_regfree(ob_regex_t*);
MODULE_SCOPE size_t ob_regerror(int, __REG_CONST ob_regex_t*, char*, size_t);
/* automatically gathered by fwd; do not hand-edit */
/* =====^!^===== end forwards =====^!^===== */

/*
 * more C++ voodoo
 */
#ifdef __cplusplus
}
#endif

#endif
