/*
 * re_*exec and friends - match REs
 *
 * Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
 *
 * Development of this software was funded, in part, by Cray Research Inc.,
 * UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
 * Corporation, none of whom are responsible for the results.  The author
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
 */

#include "regguts.h"
/*
 * Lazy-DFA representation.
 */

struct arcp { /* "pointer" to an outarc */
  struct sset* ss;
  color co;
};

struct sset {       /* state set */
  unsigned* states; /* pointer to bitvector */
  unsigned hash;    /* hash of bitvector */
#define HASH(bv, nw) (((nw) == 1) ? *(bv) : hash(bv, nw))
#define HIT(h, bv, ss, nw) \
  ((ss)->hash == (h) && ((nw) == 1 || memcmp(VS(bv), VS((ss)->states), (nw) * sizeof(unsigned)) == 0))
  int flags;
#define STARTER 01      /* the initial state set */
#define POSTSTATE 02    /* includes the goal state */
#define LOCKED 04       /* locked in cache */
#define NOPROGRESS 010  /* zero-progress state set */
  struct arcp ins;      /* chain of inarcs pointing here */
  chr* lastseen;        /* last entered on arrival here */
  struct sset** outs;   /* outarc vector indexed by color */
  struct arcp* inchain; /* chain-pointer vector for outarcs */
};

struct dfa {
  int nssets;             /* size of cache */
  int nssused;            /* how many entries occupied yet */
  int nstates;            /* number of states */
  int ncolors;            /* length of outarc and inchain vectors */
  int wordsper;           /* length of state-set bitvectors */
  struct sset* ssets;     /* state-set cache */
  unsigned* statesarea;   /* bitvector storage */
  unsigned* work;         /* pointer to work area within statesarea */
  struct sset** outsarea; /* outarc-vector storage */
  struct arcp* incarea;   /* inchain storage */
  struct cnfa* cnfa;
  struct colormap* cm;
  chr* lastpost;       /* location of last cache-flushed success */
  chr* lastnopr;       /* location of last cache-flushed NOPROGRESS */
  struct sset* search; /* replacement-search-pointer memory */
  int cptsmalloced;    /* were the areas individually malloced? */
  char* mallocarea;    /* self, or master malloced area, or NULL */
};

#define WORK 1 /* number of work bitvectors needed */

/*
 * Setup for non-malloc allocation for small cases.
 */

#define FEWSTATES 20 /* must be less than UBITS */
#define FEWCOLORS 15
struct smalldfa {
  struct dfa dfa;
  struct sset ssets[FEWSTATES * 2];
  unsigned statesarea[FEWSTATES * 2 + WORK];
  struct sset* outsarea[FEWSTATES * 2 * FEWCOLORS];
  struct arcp incarea[FEWSTATES * 2 * FEWCOLORS];
};
#define DOMALLOC ((struct smalldfa*)NULL) /* force malloc */

/*
 * Internal variables, bundled for easy passing around.
 */

struct vars {
  ob_regex_t* re;
  struct guts* g;
  int eflags; /* copies of arguments */
  size_t nmatch;
  ob_regmatch_t* pmatch;
  ob_rm_detail_t* details;
  chr* start;       /* start of string */
  chr* stop;        /* just past end of string */
  int err;          /* error code if any (0 none) */
  ob_regoff_t* mem; /* memory vector for backtracking */
  struct smalldfa dfa1;
  struct smalldfa dfa2;
};
#define VISERR(vv) ((vv)->err != 0) /* have we seen an error yet? */
#define ISERR() VISERR(v)
#define VERR(vv, e) (((vv)->err) ? (vv)->err : ((vv)->err = (e)))
#define ERR(e) VERR(v, e) /* record an error */
#define NOERR()      \
  {                  \
    if (ISERR())     \
      return v->err; \
  } /* if error seen, return it */
#define OFF(p) ((p)-v->start)
#define LOFF(p) ((long)OFF(p))

/*
 * forward declarations
 */
/* =====^!^===== begin forwards =====^!^===== */
/* automatically gathered by fwd; do not hand-edit */
/* === regexec.c === */
int exec(ob_regex_t*, const chr*, size_t, ob_rm_detail_t*, size_t, ob_regmatch_t[], int);
static int find(struct vars*, struct cnfa*, struct colormap*);
static int cfind(struct vars*, struct cnfa*, struct colormap*);
static int cfindloop(struct vars*, struct cnfa*, struct colormap*, struct dfa*, struct dfa*, chr**);
static void zapsubs(ob_regmatch_t*, size_t);
static void zapmem(struct vars*, struct subre*);
static void subset(struct vars*, struct subre*, chr*, chr*);
static int dissect(struct vars*, struct subre*, chr*, chr*);
static int condissect(struct vars*, struct subre*, chr*, chr*);
static int altdissect(struct vars*, struct subre*, chr*, chr*);
static int cdissect(struct vars*, struct subre*, chr*, chr*);
static int ccondissect(struct vars*, struct subre*, chr*, chr*);
static int crevdissect(struct vars*, struct subre*, chr*, chr*);
static int cbrdissect(struct vars*, struct subre*, chr*, chr*);
static int caltdissect(struct vars*, struct subre*, chr*, chr*);
/* === rege_dfa.c === */
static chr* longest(struct vars*, struct dfa*, chr*, chr*, int*);
static chr* shortest(struct vars*, struct dfa*, chr*, chr*, chr*, chr**, int*);
static chr* lastcold(struct vars*, struct dfa*);
static struct dfa* newdfa(struct vars*, struct cnfa*, struct colormap*, struct smalldfa*);
static void freedfa(struct dfa*);
static unsigned hash(unsigned*, int);
static struct sset* initialize(struct vars*, struct dfa*, chr*);
static struct sset* miss(struct vars*, struct dfa*, struct sset*, pcolor, chr*, chr*);
static int lacon(struct vars*, struct cnfa*, chr*, pcolor);
static struct sset* getvacant(struct vars*, struct dfa*, chr*, chr*);
static struct sset* pickss(struct vars*, struct dfa*, chr*, chr*);
/* automatically gathered by fwd; do not hand-edit */
/* =====^!^===== end forwards =====^!^===== */

/*
 - exec - match regular expression
 ^ int exec(ob_regex_t *, const chr *, size_t, ob_rm_detail_t *,
 ^					size_t, ob_regmatch_t [], int);
 */
int exec(ob_regex_t* re, const chr* string, size_t len, ob_rm_detail_t* details, size_t nmatch, ob_regmatch_t pmatch[],
    int flags)
{
  AllocVars(v);
  int st;
  size_t n;
  int backref;
#define LOCALMAT 20
  ob_regmatch_t mat[LOCALMAT];
#define LOCALMEM 40
  ob_regoff_t mem[LOCALMEM];

  /*
   * Sanity checks.
   */
  (void)(len);
  if (re == NULL || string == NULL || re->re_magic != REMAGIC) {
    FreeVars(v);
    return OB_REG_INVARG;
  }
  if (re->re_csize != sizeof(chr)) {
    FreeVars(v);
    return OB_REG_MIXED;
  }

  /*
   * Setup.
   */

  v->re = re;
  v->g = (struct guts*)re->re_guts;
  if ((v->g->cflags & OB_REG_EXPECT) && details == NULL) {
    FreeVars(v);
    return OB_REG_INVARG;
  }
  if (v->g->info & OB_REG_UIMPOSSIBLE) {
    FreeVars(v);
    return OB_REG_NOMATCH;
  }
  backref = (v->g->info & OB_REG_UBACKREF) ? 1 : 0;
  v->eflags = flags;
  if (v->g->cflags & OB_REG_NOSUB) {
    nmatch = 0; /* override client */
  }
  v->nmatch = nmatch;
  if (backref) {
    /*
     * Need work area.
     */

    if (v->g->nsub + 1 <= LOCALMAT) {
      v->pmatch = mat;
    } else {
      v->pmatch = (ob_regmatch_t*)MALLOC((v->g->nsub + 1) * sizeof(ob_regmatch_t));
    }
    if (v->pmatch == NULL) {
      FreeVars(v);
      return OB_REG_ESPACE;
    }
    v->nmatch = v->g->nsub + 1;
  } else {
    v->pmatch = pmatch;
  }
  v->details = details;
  v->start = (chr*)string + pmatch[0].rm_so;
  v->stop = (chr*)string + pmatch[0].rm_eo;
  v->err = 0;
  if (backref) {
    /*
     * Need retry memory.
     */

    assert(v->g->ntree >= 0);
    n = (size_t)v->g->ntree;
    if (n <= LOCALMEM) {
      v->mem = mem;
    } else {
      v->mem = (ob_regoff_t*)MALLOC(n * sizeof(ob_regoff_t));
    }
    if (v->mem == NULL) {
      if (v->pmatch != pmatch && v->pmatch != mat) {
        FREE(v->pmatch);
      }
      FreeVars(v);
      return OB_REG_ESPACE;
    }
  } else {
    v->mem = NULL;
  }

  /*
   * Do it.
   */

  assert(v->g->tree != NULL);
  if (backref) {
    st = cfind(v, &v->g->tree->cnfa, &v->g->cmap);
  } else {
    st = find(v, &v->g->tree->cnfa, &v->g->cmap);
  }

  /*
   * Copy (portion of) match vector over if necessary.
   */

  if (st == OB_REG_OKAY && v->pmatch != pmatch && nmatch > 0) {
    zapsubs(pmatch, nmatch);
    n = (nmatch < v->nmatch) ? nmatch : v->nmatch;
    memcpy(VS(pmatch), VS(v->pmatch), n * sizeof(ob_regmatch_t));
  }

  /*
   * Clean up.
   */

  if (v->pmatch != pmatch && v->pmatch != mat) {
    FREE(v->pmatch);
  }
  if (v->mem != NULL && v->mem != mem) {
    FREE(v->mem);
  }
  FreeVars(v);
  return st;
}

/*
 - find - find a match for the main NFA (no-complications case)
 ^ static int find(struct vars *, struct cnfa *, struct colormap *);
 */
static int find(struct vars* v, struct cnfa* cnfa, struct colormap* cm)
{
  struct dfa* s = NULL;
  struct dfa* d = NULL;
  chr* begin;
  chr* end = NULL;
  chr* cold;
  chr* open; /* Open and close of range of possible
              * starts */
  chr* close;
  int hitend;
  int shorter = (v->g->tree->flags & SHORTER) ? 1 : 0;

  /*
   * First, a shot with the search RE.
   */

  s = newdfa(v, &v->g->search, cm, &v->dfa1);
  assert(!(ISERR() && s != NULL));
  if (ISERR()) {
    freedfa(s);
    return v->err;
  }
  MDEBUG(("\nsearch at %ld\n", LOFF(v->start)));
  cold = NULL;
  close = shortest(v, s, v->start, v->start, v->stop, &cold, NULL);
  freedfa(s);
  NOERR();
  if (v->g->cflags & OB_REG_EXPECT) {
    assert(v->details != NULL);
    if (cold != NULL) {
      v->details->rm_extend.rm_so = OFF(cold);
    } else {
      v->details->rm_extend.rm_so = OFF(v->stop);
    }
    v->details->rm_extend.rm_eo = OFF(v->stop); /* unknown */
  }
  if (close == NULL) { /* not found */
    return OB_REG_NOMATCH;
  }
  if (v->nmatch == 0) { /* found, don't need exact location */
    return OB_REG_OKAY;
  }

  /*
   * Find starting point and match.
   */

  assert(cold != NULL);
  open = cold;
  cold = NULL;
  MDEBUG(("between %ld and %ld\n", LOFF(open), LOFF(close)));
  d = newdfa(v, cnfa, cm, &v->dfa1);
  assert(!(ISERR() && d != NULL));
  if (ISERR()) {
    freedfa(d);
    return v->err;
  }
  for (begin = open; begin <= close; begin++) {
    MDEBUG(("\nfind trying at %ld\n", LOFF(begin)));
    if (shorter) {
      end = shortest(v, d, begin, begin, v->stop, NULL, &hitend);
    } else {
      end = longest(v, d, begin, v->stop, &hitend);
    }
    NOERR();
    if (hitend && cold == NULL) {
      cold = begin;
    }
    if (end != NULL) {
      break; /* NOTE BREAK OUT */
    }
  }
  assert(end != NULL); /* search RE succeeded so loop should */
  freedfa(d);

  /*
   * And pin down details.
   */

  assert(v->nmatch > 0);
  v->pmatch[0].rm_so = OFF(begin);
  v->pmatch[0].rm_eo = OFF(end);
  if (v->g->cflags & OB_REG_EXPECT) {
    if (cold != NULL) {
      v->details->rm_extend.rm_so = OFF(cold);
    } else {
      v->details->rm_extend.rm_so = OFF(v->stop);
    }
    v->details->rm_extend.rm_eo = OFF(v->stop); /* unknown */
  }
  if (v->nmatch == 1) { /* no need for submatches */

    return OB_REG_OKAY;
  }

  /*
   * Submatches.
   */

  zapsubs(v->pmatch, v->nmatch);
  return dissect(v, v->g->tree, begin, end);
}

/*
 - cfind - find a match for the main NFA (with complications)
 ^ static int cfind(struct vars *, struct cnfa *, struct colormap *);
 */
static int cfind(struct vars* v, struct cnfa* cnfa, struct colormap* cm)
{
  struct dfa* s = NULL;
  struct dfa* d = NULL;
  chr* cold = NULL; /* silence gcc 4 warning */
  int ret;

  s = newdfa(v, &v->g->search, cm, &v->dfa1);
  if (ISERR()) {
    freedfa(s);
    return v->err;
  }
  d = newdfa(v, cnfa, cm, &v->dfa2);
  if (ISERR()) {
    freedfa(d);
    freedfa(s);
    return v->err;
  }

  ret = cfindloop(v, cnfa, cm, d, s, &cold);

  freedfa(d);
  freedfa(s);
  NOERR();
  if (v->g->cflags & OB_REG_EXPECT) {
    assert(v->details != NULL);
    if (cold != NULL) {
      v->details->rm_extend.rm_so = OFF(cold);
    } else {
      v->details->rm_extend.rm_so = OFF(v->stop);
    }
    v->details->rm_extend.rm_eo = OFF(v->stop); /* unknown */
  }
  return ret;
}

/*
 - cfindloop - the heart of cfind
 ^ static int cfindloop(struct vars *, struct cnfa *, struct colormap *,
 ^	struct dfa *, struct dfa *, chr **);
 */
static int cfindloop(struct vars* v, struct cnfa* cnfa __attribute__((unused)),
    struct colormap* cm __attribute__((unused)), struct dfa* d, struct dfa* s,
    chr** coldp) /* where to put coldstart pointer */
{
  chr* begin;
  chr* end;
  chr* cold;
  chr* open; /* Open and close of range of possible
              * starts */
  chr* close;
  chr* estart;
  chr* estop;
  int er;
  int shorter = v->g->tree->flags & SHORTER;
  int hitend;

  assert(v != NULL && cnfa != NULL && cm != NULL && d != NULL && s != NULL);
  cold = NULL;
  close = v->start;
  do {
    MDEBUG(("\ncsearch at %ld\n", LOFF(close)));
    close = shortest(v, s, close, close, v->stop, &cold, NULL);
    if (close == NULL) {
      break; /* NOTE BREAK */
    }
    assert(cold != NULL);
    open = cold;
    cold = NULL;
    MDEBUG(("cbetween %ld and %ld\n", LOFF(open), LOFF(close)));
    for (begin = open; begin <= close; begin++) {
      MDEBUG(("\ncfind trying at %ld\n", LOFF(begin)));
      estart = begin;
      estop = v->stop;
      for (;;) {
        if (shorter) {
          end = shortest(v, d, begin, estart, estop, NULL, &hitend);
        } else {
          end = longest(v, d, begin, estop, &hitend);
        }
        if (hitend && cold == NULL) {
          cold = begin;
        }
        if (end == NULL) {
          break; /* NOTE BREAK OUT */
        }

        MDEBUG(("tentative end %ld\n", LOFF(end)));
        zapsubs(v->pmatch, v->nmatch);
        zapmem(v, v->g->tree);
        er = cdissect(v, v->g->tree, begin, end);
        if (er == OB_REG_OKAY) {
          if (v->nmatch > 0) {
            v->pmatch[0].rm_so = OFF(begin);
            v->pmatch[0].rm_eo = OFF(end);
          }
          *coldp = cold;
          return OB_REG_OKAY;
        }
        if (er != OB_REG_NOMATCH) {
          ERR(er);
          return er;
        }
        if ((shorter) ? end == estop : end == begin) {
          /*
           * No point in trying again.
           */

          *coldp = cold;
          return OB_REG_NOMATCH;
        }

        /*
         * Go around and try again
         */

        if (shorter) {
          estart = end + 1;
        } else {
          estop = end - 1;
        }
      }
    }
  } while (close < v->stop);

  *coldp = cold;
  return OB_REG_NOMATCH;
}

/*
 - zapsubs - initialize the subexpression matches to "no match"
 ^ static void zapsubs(ob_regmatch_t *, size_t);
 */
static void zapsubs(ob_regmatch_t* p, size_t n)
{
  size_t i;

  for (i = n - 1; i > 0; i--) {
    p[i].rm_so = -1;
    p[i].rm_eo = -1;
  }
}

/*
 - zapmem - initialize the retry memory of a subtree to zeros
 ^ static void zapmem(struct vars *, struct subre *);
 */
static void zapmem(struct vars* v, struct subre* t)
{
  if (t == NULL) {
    return;
  }

  assert(v->mem != NULL);
  v->mem[t->retry] = 0;
  if (t->op == '(') {
    assert(t->subno > 0);
    v->pmatch[t->subno].rm_so = -1;
    v->pmatch[t->subno].rm_eo = -1;
  }

  if (t->left != NULL) {
    zapmem(v, t->left);
  }
  if (t->right != NULL) {
    zapmem(v, t->right);
  }
}

/*
 - subset - set any subexpression relevant to a successful subre
 ^ static void subset(struct vars *, struct subre *, chr *, chr *);
 */
static void subset(struct vars* v, struct subre* sub, chr* begin, chr* end)
{
  int n = sub->subno;

  assert(n > 0);
  if ((size_t)n >= v->nmatch) {
    return;
  }

  MDEBUG(("setting %d\n", n));
  v->pmatch[n].rm_so = OFF(begin);
  v->pmatch[n].rm_eo = OFF(end);
}

/*
 - dissect - determine subexpression matches (uncomplicated case)
 ^ static int dissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                               /* ob_regexec return code */
    dissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                        /* end of same */
{
  assert(t != NULL);
  MDEBUG(("dissect %ld-%ld\n", LOFF(begin), LOFF(end)));

  switch (t->op) {
    case '=': /* terminal node */
      assert(t->left == NULL && t->right == NULL);
      return OB_REG_OKAY; /* no action, parent did the work */
      break;
    case '|': /* alternation */
      assert(t->left != NULL);
      return altdissect(v, t, begin, end);
      break;
    case 'b': /* back ref -- shouldn't be calling us! */
      return OB_REG_ASSERT;
      break;
    case '.': /* concatenation */
      assert(t->left != NULL && t->right != NULL);
      return condissect(v, t, begin, end);
      break;
    case '(': /* capturing */
      assert(t->left != NULL && t->right == NULL);
      assert(t->subno > 0);
      subset(v, t, begin, end);
      return dissect(v, t->left, begin, end);
      break;
    default:
      return OB_REG_ASSERT;
      break;
  }
}

/*
 - condissect - determine concatenation subexpression matches (uncomplicated)
 ^ static int condissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                  /* ob_regexec return code */
    condissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                           /* end of same */
{
  struct dfa* d = NULL;
  struct dfa* d2 = NULL;
  chr* mid;
  int i;
  int shorter = (t->left->flags & SHORTER) ? 1 : 0;
  chr* stop = (shorter) ? end : begin;

  assert(t->op == '.');
  assert(t->left != NULL && t->left->cnfa.nstates > 0);
  assert(t->right != NULL && t->right->cnfa.nstates > 0);

  d = newdfa(v, &t->left->cnfa, &v->g->cmap, &v->dfa1);
  if (ISERR()) {
    freedfa(d);
    return v->err;
  }
  d2 = newdfa(v, &t->right->cnfa, &v->g->cmap, &v->dfa2);
  if (ISERR()) {
    freedfa(d);
    freedfa(d2);
    return v->err;
  }

  /*
   * Pick a tentative midpoint.
   */

  if (shorter) {
    mid = shortest(v, d, begin, begin, end, NULL, NULL);
  } else {
    mid = longest(v, d, begin, end, NULL);
  }
  if (mid == NULL) {
    freedfa(d);
    freedfa(d2);
    return OB_REG_ASSERT;
  }
  MDEBUG(("tentative midpoint %ld\n", LOFF(mid)));

  /*
   * Iterate until satisfaction or failure.
   */

  while (longest(v, d2, mid, end, NULL) != end) {
    /*
     * That midpoint didn't work, find a new one.
     */

    if (mid == stop) {
      /*
       * All possibilities exhausted!
       */

      MDEBUG(("no midpoint!\n"));
      freedfa(d);
      freedfa(d2);
      return OB_REG_ASSERT;
    }
    if (shorter) {
      mid = shortest(v, d, begin, mid + 1, end, NULL, NULL);
    } else {
      mid = longest(v, d, begin, mid - 1, NULL);
    }
    if (mid == NULL) {
      /*
       * Failed to find a new one!
       */

      MDEBUG(("failed midpoint!\n"));
      freedfa(d);
      freedfa(d2);
      return OB_REG_ASSERT;
    }
    MDEBUG(("new midpoint %ld\n", LOFF(mid)));
  }

  /*
   * Satisfaction.
   */

  MDEBUG(("successful\n"));
  freedfa(d);
  freedfa(d2);
  i = dissect(v, t->left, begin, mid);
  if (i != OB_REG_OKAY) {
    return i;
  }
  return dissect(v, t->right, mid, end);
}

/*
 - altdissect - determine alternative subexpression matches (uncomplicated)
 ^ static int altdissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                  /* ob_regexec return code */
    altdissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                           /* end of same */
{
  struct dfa* d = NULL;
  int i;

  assert(t != NULL);
  assert(t->op == '|');

  for (i = 0; t != NULL; t = t->right, i++) {
    MDEBUG(("trying %dth\n", i));
    assert(t->left != NULL && t->left->cnfa.nstates > 0);
    d = newdfa(v, &t->left->cnfa, &v->g->cmap, &v->dfa1);
    if (ISERR()) {
      freedfa(d);
      return v->err;
    }
    if (longest(v, d, begin, end, NULL) == end) {
      MDEBUG(("success\n"));
      freedfa(d);
      return dissect(v, t->left, begin, end);
    }
    freedfa(d);
  }
  return OB_REG_ASSERT; /* none of them matched?!? */
}

/*
 - cdissect - determine subexpression matches (with complications)
 * The retry memory stores the offset of the trial midpoint from begin, plus 1
 * so that 0 uniquely means "clean slate".
 ^ static int cdissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                /* ob_regexec return code */
    cdissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                         /* end of same */
{
  int er;

  assert(t != NULL);
  MDEBUG(("cdissect %ld-%ld %c\n", LOFF(begin), LOFF(end), t->op));

  switch (t->op) {
    case '=': /* terminal node */
      assert(t->left == NULL && t->right == NULL);
      return OB_REG_OKAY; /* no action, parent did the work */
      break;
    case '|': /* alternation */
      assert(t->left != NULL);
      return caltdissect(v, t, begin, end);
      break;
    case 'b': /* back ref -- shouldn't be calling us! */
      assert(t->left == NULL && t->right == NULL);
      return cbrdissect(v, t, begin, end);
      break;
    case '.': /* concatenation */
      assert(t->left != NULL && t->right != NULL);
      return ccondissect(v, t, begin, end);
      break;
    case '(': /* capturing */
      assert(t->left != NULL && t->right == NULL);
      assert(t->subno > 0);
      er = cdissect(v, t->left, begin, end);
      if (er == OB_REG_OKAY) {
        subset(v, t, begin, end);
      }
      return er;
      break;
    default:
      return OB_REG_ASSERT;
      break;
  }
}

/*
 - ccondissect - concatenation subexpression matches (with complications)
 * The retry memory stores the offset of the trial midpoint from begin, plus 1
 * so that 0 uniquely means "clean slate".
 ^ static int ccondissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                   /* ob_regexec return code */
    ccondissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                            /* end of same */
{
  struct dfa* d = NULL;
  struct dfa* d2 = NULL;
  chr* mid;
  int er;

  assert(t->op == '.');
  assert(t->left != NULL && t->left->cnfa.nstates > 0);
  assert(t->right != NULL && t->right->cnfa.nstates > 0);

  if (t->left->flags & SHORTER) { /* reverse scan */
    return crevdissect(v, t, begin, end);
  }

  d = newdfa(v, &t->left->cnfa, &v->g->cmap, DOMALLOC);
  if (ISERR()) {
    freedfa(d);
    return v->err;
  }
  d2 = newdfa(v, &t->right->cnfa, &v->g->cmap, DOMALLOC);
  if (ISERR()) {
    freedfa(d);
    freedfa(d2);
    return v->err;
  }
  MDEBUG(("cconcat %d\n", t->retry));

  /*
   * Pick a tentative midpoint.
   */

  if (v->mem[t->retry] == 0) {
    mid = longest(v, d, begin, end, NULL);
    if (mid == NULL) {
      freedfa(d);
      freedfa(d2);
      return OB_REG_NOMATCH;
    }
    MDEBUG(("tentative midpoint %ld\n", LOFF(mid)));
    v->mem[t->retry] = (mid - begin) + 1;
  } else {
    mid = begin + (v->mem[t->retry] - 1);
    MDEBUG(("working midpoint %ld\n", LOFF(mid)));
  }

  /*
   * Iterate until satisfaction or failure.
   */

  for (;;) {
    /*
     * Try this midpoint on for size.
     */

    er = cdissect(v, t->left, begin, mid);
    if ((er == OB_REG_OKAY) && (longest(v, d2, mid, end, NULL) == end) &&
        (er = cdissect(v, t->right, mid, end)) == OB_REG_OKAY) {
      break; /* NOTE BREAK OUT */
    }
    if ((er != OB_REG_OKAY) && (er != OB_REG_NOMATCH)) {
      freedfa(d);
      freedfa(d2);
      return er;
    }

    /*
     * That midpoint didn't work, find a new one.
     */

    if (mid == begin) {
      /*
       * All possibilities exhausted.
       */

      MDEBUG(("%d no midpoint\n", t->retry));
      freedfa(d);
      freedfa(d2);
      return OB_REG_NOMATCH;
    }
    mid = longest(v, d, begin, mid - 1, NULL);
    if (mid == NULL) {
      /*
       * Failed to find a new one.
       */

      MDEBUG(("%d failed midpoint\n", t->retry));
      freedfa(d);
      freedfa(d2);
      return OB_REG_NOMATCH;
    }
    MDEBUG(("%d: new midpoint %ld\n", t->retry, LOFF(mid)));
    v->mem[t->retry] = (mid - begin) + 1;
    zapmem(v, t->left);
    zapmem(v, t->right);
  }

  /*
   * Satisfaction.
   */

  MDEBUG(("successful\n"));
  freedfa(d);
  freedfa(d2);
  return OB_REG_OKAY;
}

/*
 - crevdissect - determine backref shortest-first subexpression matches
 * The retry memory stores the offset of the trial midpoint from begin, plus 1
 * so that 0 uniquely means "clean slate".
 ^ static int crevdissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                   /* ob_regexec return code */
    crevdissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                            /* end of same */
{
  struct dfa* d = NULL;
  struct dfa* d2 = NULL;
  chr* mid;
  int er;

  assert(t->op == '.');
  assert(t->left != NULL && t->left->cnfa.nstates > 0);
  assert(t->right != NULL && t->right->cnfa.nstates > 0);
  assert(t->left->flags & SHORTER);

  /*
   * Concatenation -- need to split the substring between parts.
   */

  d = newdfa(v, &t->left->cnfa, &v->g->cmap, DOMALLOC);
  if (ISERR()) {
    freedfa(d);
    return v->err;
  }
  d2 = newdfa(v, &t->right->cnfa, &v->g->cmap, DOMALLOC);
  if (ISERR()) {
    freedfa(d);
    freedfa(d2);
    return v->err;
  }
  MDEBUG(("crev %d\n", t->retry));

  /*
   * Pick a tentative midpoint.
   */

  if (v->mem[t->retry] == 0) {
    mid = shortest(v, d, begin, begin, end, NULL, NULL);
    if (mid == NULL) {
      freedfa(d);
      freedfa(d2);
      return OB_REG_NOMATCH;
    }
    MDEBUG(("tentative midpoint %ld\n", LOFF(mid)));
    v->mem[t->retry] = (mid - begin) + 1;
  } else {
    mid = begin + (v->mem[t->retry] - 1);
    MDEBUG(("working midpoint %ld\n", LOFF(mid)));
  }

  /*
   * Iterate until satisfaction or failure.
   */

  for (;;) {
    /*
     * Try this midpoint on for size.
     */

    er = cdissect(v, t->left, begin, mid);
    if ((er == OB_REG_OKAY) && (longest(v, d2, mid, end, NULL) == end) &&
        (er = cdissect(v, t->right, mid, end)) == OB_REG_OKAY) {
      break; /* NOTE BREAK OUT */
    }
    if (er != OB_REG_OKAY && er != OB_REG_NOMATCH) {
      freedfa(d);
      freedfa(d2);
      return er;
    }

    /*
     * That midpoint didn't work, find a new one.
     */

    if (mid == end) {
      /*
       * All possibilities exhausted.
       */

      MDEBUG(("%d no midpoint\n", t->retry));
      freedfa(d);
      freedfa(d2);
      return OB_REG_NOMATCH;
    }
    mid = shortest(v, d, begin, mid + 1, end, NULL, NULL);
    if (mid == NULL) {
      /*
       * Failed to find a new one.
       */

      MDEBUG(("%d failed midpoint\n", t->retry));
      freedfa(d);
      freedfa(d2);
      return OB_REG_NOMATCH;
    }
    MDEBUG(("%d: new midpoint %ld\n", t->retry, LOFF(mid)));
    v->mem[t->retry] = (mid - begin) + 1;
    zapmem(v, t->left);
    zapmem(v, t->right);
  }

  /*
   * Satisfaction.
   */

  MDEBUG(("successful\n"));
  freedfa(d);
  freedfa(d2);
  return OB_REG_OKAY;
}

/*
 - cbrdissect - determine backref subexpression matches
 ^ static int cbrdissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                  /* ob_regexec return code */
    cbrdissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                           /* end of same */
{
  int i;
  int n = t->subno;
  size_t len;
  chr* paren;
  chr* p;
  chr* stop;
  int min = t->min;
  int max = t->max;

  assert(t != NULL);
  assert(t->op == 'b');
  assert(n >= 0);
  assert((size_t)n < v->nmatch);

  MDEBUG(("cbackref n%d %d{%d-%d}\n", t->retry, n, min, max));

  if (v->pmatch[n].rm_so == -1) {
    return OB_REG_NOMATCH;
  }
  paren = v->start + v->pmatch[n].rm_so;
  len = v->pmatch[n].rm_eo - v->pmatch[n].rm_so;

  /*
   * No room to maneuver -- retries are pointless.
   */

  if (v->mem[t->retry]) {
    return OB_REG_NOMATCH;
  }
  v->mem[t->retry] = 1;

  /*
   * Special-case zero-length string.
   */

  if (len == 0) {
    if (begin == end) {
      return OB_REG_OKAY;
    }
    return OB_REG_NOMATCH;
  }

  /*
   * And too-short string.
   */

  assert(end >= begin);
  if ((size_t)(end - begin) < len) {
    return OB_REG_NOMATCH;
  }
  stop = end - len;

  /*
   * Count occurrences.
   */

  i = 0;
  for (p = begin; p <= stop && (i < max || max == INFINITY_1); p += len) {
    if ((*v->g->compare)(paren, p, len) != 0) {
      break;
    }
    i++;
  }
  MDEBUG(("cbackref found %d\n", i));

  /*
   * And sort it out.
   */

  if (p != end) { /* didn't consume all of it */
    return OB_REG_NOMATCH;
  }
  if (min <= i && (i <= max || max == INFINITY_1)) {
    return OB_REG_OKAY;
  }
  return OB_REG_NOMATCH; /* out of range */
}

/*
 - caltdissect - determine alternative subexpression matches (w. complications)
 ^ static int caltdissect(struct vars *, struct subre *, chr *, chr *);
 */
static int                                                   /* ob_regexec return code */
    caltdissect(struct vars* v, struct subre* t, chr* begin, /* beginning of relevant substring */
        chr* end)                                            /* end of same */
{
  struct dfa* d = NULL;
  int er;
#define UNTRIED 0 /* not yet tried at all */
#define TRYING 1  /* top matched, trying submatches */
#define TRIED 2   /* top didn't match or submatches exhausted */

  if (t == NULL) {
    return OB_REG_NOMATCH;
  }
  assert(t->op == '|');
  if (v->mem[t->retry] == TRIED) {
    return caltdissect(v, t->right, begin, end);
  }

  MDEBUG(("calt n%d\n", t->retry));
  assert(t->left != NULL);

  if (v->mem[t->retry] == UNTRIED) {
    d = newdfa(v, &t->left->cnfa, &v->g->cmap, DOMALLOC);
    if (ISERR()) {
      freedfa(d);
      return v->err;
    }
    if (longest(v, d, begin, end, NULL) != end) {
      freedfa(d);
      v->mem[t->retry] = TRIED;
      return caltdissect(v, t->right, begin, end);
    }
    freedfa(d);
    MDEBUG(("calt matched\n"));
    v->mem[t->retry] = TRYING;
  }

  er = cdissect(v, t->left, begin, end);
  if (er != OB_REG_NOMATCH) {
    return er;
  }

  v->mem[t->retry] = TRIED;
  return caltdissect(v, t->right, begin, end);
}

#include "rege_dfa.cpp"

/*
 * Local Variables:
 * mode: c
 * c-basic-offset: 4
 * fill-column: 78
 * End:
 */
