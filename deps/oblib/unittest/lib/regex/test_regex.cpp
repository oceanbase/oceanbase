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
#include "lib/ob_define.h"

#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "lib/regex/regex/ob_regex.h"
#include <assert.h>

#include "test_regex.ih"

#include "lib/utility/ob_macro_utils.h"

using namespace oceanbase::common;

int debug = 0;
int line = 0;
int status = 0;

int copts = OB_REG_EXTENDED;
int eopts = 0;
ob_regoff_t startoff = 0;
ob_regoff_t endoff = 0;

// extern void regprint();

/*
 - split - divide a string into fields, like awk split()
 = int split(char *string, char *fields[], int nfields, char *sep);
 */
int                                     /* number of fields, including overflow */
    split(char* string, char* fields[], /* list is not NULL-terminated */
        int nfields,                    /* number of entries available in fields[] */
        char* sep                       /* "" white, "c" single char, "ab" [ab]+ */
    )
{
  register char* p = string;
  register char c; /* latest character */
  register char sepc = sep[0];
  register char sepc2;
  register int fn;
  register char** fp = fields;
  register char* sepp;
  register int trimtrail;

  /* white space */
  if (sepc == '\0') {
    while ((c = *p++) == ' ' || c == '\t')
      continue;
    p--;
    trimtrail = 1;
    static char static_sep[5] = " \t";
    sep = static_sep; /* note, code below knows this is 2 long */
    sepc = ' ';
  } else
    trimtrail = 0;
  sepc2 = sep[1]; /* now we can safely pick this up */

  /* catch empties */
  if (*p == '\0')
    return (0);

  /* single separator */
  if (sepc2 == '\0') {
    fn = nfields;
    for (;;) {
      *fp++ = p;
      fn--;
      if (fn == 0)
        break;
      while ((c = *p++) != sepc)
        if (c == '\0')
          return (nfields - fn);
      *(p - 1) = '\0';
    }
    /* we have overflowed the fields vector -- just count them */
    fn = nfields;
    for (;;) {
      while ((c = *p++) != sepc)
        if (c == '\0')
          return (fn);
      fn++;
    }
    /* not reached */
  }

  /* two separators */
  if (sep[2] == '\0') {
    fn = nfields;
    for (;;) {
      *fp++ = p;
      fn--;
      while ((c = *p++) != sepc && c != sepc2)
        if (c == '\0') {
          if (trimtrail && **(fp - 1) == '\0')
            fn++;
          return (nfields - fn);
        }
      if (fn == 0)
        break;
      *(p - 1) = '\0';
      while ((c = *p++) == sepc || c == sepc2)
        continue;
      p--;
    }
    /* we have overflowed the fields vector -- just count them */
    fn = nfields;
    while (c != '\0') {
      while ((c = *p++) == sepc || c == sepc2)
        continue;
      p--;
      fn++;
      while ((c = *p++) != '\0' && c != sepc && c != sepc2)
        continue;
    }
    /* might have to trim trailing white space */
    if (trimtrail) {
      p--;
      while ((c = *--p) == sepc || c == sepc2)
        continue;
      p++;
      if (*p != '\0') {
        if (fn == nfields + 1)
          *p = '\0';
        fn--;
      }
    }
    return (fn);
  }

  /* n separators */
  fn = 0;
  for (;;) {
    if (fn < nfields)
      *fp++ = p;
    fn++;
    for (;;) {
      c = *p++;
      if (c == '\0')
        return (fn);
      sepp = sep;
      while ((sepc = *sepp++) != '\0' && sepc != c)
        continue;
      if (sepc != '\0') /* it was a separator */
        break;
    }
    if (fn < nfields)
      *(p - 1) = '\0';
    for (;;) {
      c = *p++;
      sepp = sep;
      while ((sepc = *sepp++) != '\0' && sepc != c)
        continue;
      if (sepc == '\0') /* it wasn't a separator */
        break;
    }
    p--;
  }

  /* not reached */
}

/*
 - regress - main loop of regression test
 == bool regress(FILE *in);
 */
bool regress(FILE* in)
{
  char inbuf[1000];
#define MAXF 10
  char* f[MAXF];
  int nf;
  int i;
  char erbuf[100];
  size_t ne;
  const char* badpat = "invalid regular expression";
#define SHORT 10
  const char* bpname = "OB_REG_BADPAT";
  ob_regex_t re;
  char sep[5] = "\t\t";

  while (fgets(inbuf, sizeof(inbuf), in) != NULL) {
    line++;
    if (inbuf[0] == '#' || inbuf[0] == '\n')
      continue;                      /* NOTE CONTINUE */
    inbuf[strlen(inbuf) - 1] = '\0'; /* get rid of stupid \n */
    if (debug)
      fprintf(stdout, "%d:\n", line);
    nf = split(inbuf, f, MAXF, sep);
    if (nf < 3) {
      fprintf(stderr, "bad input, line %d\n", line);
      return false;
    }
    for (i = 0; i < nf; i++)
      if (strcmp(f[i], "\"\"") == 0)
        f[i][0] = '\0';
    // f[i] = "";
    if (nf <= 3)
      f[3] = NULL;
    if (nf <= 4)
      f[4] = NULL;
    try_case(f[0], f[1], f[2], f[3], f[4], options('c', f[1]));
    if (opt('&', f[1])) /* try with either type of RE */
      try_case(f[0], f[1], f[2], f[3], f[4], options('c', f[1]) & ~OB_REG_EXTENDED);
  }

  ne = ob_regerror(OB_REG_BADPAT, (ob_regex_t*)NULL, erbuf, sizeof(erbuf));
  if (strcmp(erbuf, badpat) != 0 || ne != strlen(badpat) + 1) {
    fprintf(stderr, "end: regerror() test gave `%s' not `%s'\n", erbuf, badpat);
    status = 1;
  }
  ne = ob_regerror(OB_REG_BADPAT, (ob_regex_t*)NULL, erbuf, (size_t)SHORT);
  if (strncmp(erbuf, badpat, SHORT - 1) != 0 || erbuf[SHORT - 1] != '\0' || ne != strlen(badpat) + 1) {
    fprintf(stderr, "end: regerror() short test gave `%s' not `%.*s'\n", erbuf, SHORT - 1, badpat);
    status = 1;
  }
  ne = ob_regerror(OB_REG_ITOA | OB_REG_BADPAT, (ob_regex_t*)NULL, erbuf, sizeof(erbuf));
  if (strcmp(erbuf, bpname) != 0 || ne != strlen(bpname) + 1) {
    fprintf(stderr, "end: regerror() ITOA test gave `%s' not `%s'\n", erbuf, bpname);
    status = 1;
  }
  re.re_endp = bpname;
  ne = ob_regerror(OB_REG_ATOI, &re, erbuf, sizeof(erbuf));
  if (atoi(erbuf) != (int)OB_REG_BADPAT) {
    fprintf(stderr, "end: regerror() ATOI test gave `%s' not `%ld'\n", erbuf, (long)OB_REG_BADPAT);
    status = 1;
  } else if (ne != strlen(erbuf) + 1) {
    fprintf(stderr, "end: regerror() ATOI test len(`%s') = %ld\n", erbuf, (long)OB_REG_BADPAT);
    status = 1;
  }
  return 0 == status;
}

/*
 - try_case - try it, and report on problems
 == void try_case(char *f0, char *f1, char *f2, char *f3, char *f4, int opts);
 */
void try_case(char* f0, char* f1, char* f2, char* f3, char* f4, int opts /* may not match f1 */
)
{
  ob_regex_t re;
#define NSUBS 10
  ob_regmatch_t subs[NSUBS];
#define NSHOULD 15
  char* should[NSHOULD];
  int nshould;
  char erbuf[100];
  int err;
  int len;
  const char* type = (opts & OB_REG_EXTENDED) ? "ERE" : "BRE";
  register int i;
  char* grump;
  char f0copy[1000];
  char f2copy[1000];
  char sep[5] = ",";

  strcpy(f0copy, f0);
  re.re_endp = (opts & OB_REG_PEND) ? f0copy + strlen(f0copy) : NULL;
  fixstr(f0copy);
  err = ob_regcomp(&re, f0copy, opts, &ob_charset_utf8mb4_general_ci);
  if (err != 0 && (!opt('C', f1) || err != efind(f2))) {
    /* unexpected error or wrong error */
    len = (int)ob_regerror(err, &re, erbuf, sizeof(erbuf));
    fprintf(stderr, "%d: %s error %s, %d/%d `%s'\n", line, type, eprint(err), len, (int)sizeof(erbuf), erbuf);
    status = 1;
  } else if (err == 0 && opt('C', f1)) {
    /* unexpected success */
    fprintf(stderr, "%d: %s should have given OB_REG_%s\n", line, type, f2);
    status = 1;
    err = 1; /* so we won't try regexec */
  }

  if (err != 0) {
    ob_regfree(&re);
    return;
  }

  strcpy(f2copy, f2);
  fixstr(f2copy);

  if (options('e', f1) & OB_REG_STARTEND) {
    if (strchr(f2, '(') == NULL || strchr(f2, ')') == NULL)
      fprintf(stderr, "%d: bad STARTEND syntax\n", line);
    subs[0].rm_so = strchr(f2, '(') - f2 + 1;
    subs[0].rm_eo = strchr(f2, ')') - f2;
  }
  err = ob_regexec(&re, f2copy, NSUBS, subs, options('e', f1));

  if (err != 0 && (f3 != NULL || err != OB_REG_NOMATCH)) {
    /* unexpected error or wrong error */
    len = (int)ob_regerror(err, &re, erbuf, sizeof(erbuf));
    fprintf(stderr, "%d: %s exec error %s, %d/%d `%s'\n", line, type, eprint(err), len, (int)sizeof(erbuf), erbuf);
    status = 1;
  } else if (err != 0) {
    /* nothing more to check */
  } else if (f3 == NULL) {
    /* unexpected success */
    fprintf(stderr, "%d: %s exec should have failed\n", line, type);
    status = 1;
    err = 1; /* just on principle */
  } else if (opts & OB_REG_NOSUB) {
    /* nothing more to check */
  } else if ((grump = check(f2, subs[0], f3)) != NULL) {
    fprintf(stderr, "%d: %s %s\n", line, type, grump);
    status = 1;
    err = 1;
  }

  if (err != 0 || f4 == NULL) {
    ob_regfree(&re);
    return;
  }

  for (i = 1; i < NSHOULD; i++)
    should[i] = NULL;
  nshould = split(f4, should + 1, NSHOULD - 1, sep);
  if (nshould == 0) {
    nshould = 1;
    should[1][0] = '\0';
    // should[1] = "";
  }
  for (i = 1; i < NSUBS; i++) {
    grump = check(f2, subs[i], should[i]);
    if (grump != NULL) {
      fprintf(stderr, "%d: %s $%d %s\n", line, type, i, grump);
      status = 1;
      err = 1;
    }
  }

  ob_regfree(&re);
}

/*
 - options - pick options out of a regression-test string
 == int options(int type, char *s);
 */
int options(int type, /* 'c' compile, 'e' exec */
    char* s)
{
  register char* p;
  register int o = (type == 'c') ? copts : eopts;
  register const char* legal = (type == 'c') ? "bisnmp" : "^$#tl";

  for (p = s; *p != '\0'; p++)
    if (strchr(legal, *p) != NULL)
      switch (*p) {
        case 'b':
          o &= ~OB_REG_EXTENDED;
          break;
        case 'i':
          o |= OB_REG_ICASE;
          break;
        case 's':
          o |= OB_REG_NOSUB;
          break;
        case 'n':
          o |= OB_REG_NEWLINE;
          break;
        case 'm':
          o &= ~OB_REG_EXTENDED;
          o |= OB_REG_NOSPEC;
          break;
        case 'p':
          o |= OB_REG_PEND;
          break;
        case '^':
          o |= OB_REG_NOTBOL;
          break;
        case '$':
          o |= OB_REG_NOTEOL;
          break;
        case '#':
          o |= OB_REG_STARTEND;
          break;
        case 't': /* trace */
          o |= OB_REG_TRACE;
          break;
        case 'l': /* force long representation */
          o |= OB_REG_LARGE;
          break;
        case 'r': /* force backref use */
          o |= OB_REG_BACKR;
          break;
      }
  return (o);
}

/*
 - opt - is a particular option in a regression string?
 == int opt(int c, char *s);
 */
int /* predicate */
    opt(int c, char* s)
{
  return (strchr(s, c) != NULL);
}

/*
 - fixstr - transform magic characters in strings
 == void fixstr(register char *p);
 */
void fixstr(register char* p)
{
  if (p == NULL)
    return;

  for (; *p != '\0'; p++)
    if (*p == 'N')
      *p = '\n';
    else if (*p == 'T')
      *p = '\t';
    else if (*p == 'S')
      *p = ' ';
    else if (*p == 'Z')
      *p = '\0';
}

/*
 - check - check a substring match
 == char *check(char *str, ob_regmatch_t sub, char *should);
 */
char* /* NULL or complaint */
    check(char* str, ob_regmatch_t sub, char* should)
{
  register int len;
  register int shlen;
  register char* p;
  static char grump[500];
  register char* at = NULL;

  if (should != NULL && strcmp(should, "-") == 0)
    should = NULL;
  if (should != NULL && should[0] == '@') {
    at = should + 1;
    should[0] = '\0';
  }

  /* check rm_so and rm_eo for consistency */
  if (sub.rm_so > sub.rm_eo || (sub.rm_so == -1 && sub.rm_eo != -1) || (sub.rm_so != -1 && sub.rm_eo == -1) ||
      (sub.rm_so != -1 && sub.rm_so < 0) || (sub.rm_eo != -1 && sub.rm_eo < 0)) {
    sprintf(grump, "start %ld end %ld", (long)sub.rm_so, (long)sub.rm_eo);
    return (grump);
  }

  /* check for no match */
  if (sub.rm_so == -1 && should == NULL)
    return (NULL);
  if (sub.rm_so == -1) {
    static char ret_err_buf[50] = "did not match";
    return (ret_err_buf);
  }

  /* check for in range */
  if (sub.rm_eo > strlen(str)) {
    sprintf(grump, "start %ld end %ld, past end of string", (long)sub.rm_so, (long)sub.rm_eo);
    return (grump);
  }

  len = (int)(sub.rm_eo - sub.rm_so);
  shlen = (int)strlen(should);
  p = str + sub.rm_so;

  /* check for not supposed to match */
  if (should == NULL) {
    sprintf(grump, "matched `%.*s'", len, p);
    return (grump);
  }

  /* check for wrong match */
  if (len != shlen || strncmp(p, should, (size_t)shlen) != 0) {
    sprintf(grump, "matched `%.*s' instead", len, p);
    return (grump);
  }
  if (shlen > 0)
    return (NULL);

  /* check null match in right place */
  if (at == NULL)
    return (NULL);
  shlen = (int)strlen(at);
  if (shlen == 0)
    shlen = 1; /* force check for end-of-string */
  if (strncmp(p, at, shlen) != 0) {
    sprintf(grump, "matched null at `%.20s'", p);
    return (grump);
  }
  return (NULL);
}

/*
 - eprint - convert error number to name
 == static char *eprint(int err);
 */
static char* eprint(int err)
{
  static char epbuf[100];
  size_t len;

  len = ob_regerror(OB_REG_ITOA | err, (ob_regex_t*)NULL, epbuf, sizeof(epbuf));
  assert(len <= sizeof(epbuf));
  UNUSED(len);
  return (epbuf);
}

/*
 - efind - convert error name to number
 == static int efind(char *name);
 */
static int efind(char* name)
{
  static char efbuf[100];
  // size_t n;
  ob_regex_t re;

  sprintf(efbuf, "OB_REG_%s", name);
  assert(strlen(efbuf) < sizeof(efbuf));
  re.re_endp = efbuf;
  (void)ob_regerror(OB_REG_ATOI, &re, efbuf, sizeof(efbuf));
  return (atoi(efbuf));
}

class ObRegexTest : public ::testing::Test {
public:
  ObRegexTest();
  virtual ~ObRegexTest();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  ObRegexTest(const ObRegexTest& other);
  ObRegexTest& operator=(const ObRegexTest& other);

private:
  // data members
};
ObRegexTest::ObRegexTest()
{}

ObRegexTest::~ObRegexTest()
{}

void ObRegexTest::SetUp()
{}

void ObRegexTest::TearDown()
{}

TEST_F(ObRegexTest, basic_test)
{
  /*
     - main - do the simple case, hand off to regress() for regression
     */
  char test_filename[] = "regex/tests";
  FILE* test_file = fopen(test_filename, "r");
  if (NULL == test_file) {
    fprintf(stderr, "fail to open file '%s'\n", test_filename);
  } else {
    ASSERT_TRUE(regress(test_file));
    fclose(test_file);
  }
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  // ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
