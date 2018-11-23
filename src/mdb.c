/**
 * lmdb.c -- Lightning Memory-Mapped Database (LMDB)
 *    ______      ___
 *   / ____/___  /   | _____________  __________
 *  / / __/ __ \/ /| |/ ___/ ___/ _ \/ ___/ ___/
 * / /_/ / /_/ / ___ / /__/ /__/  __(__  |__  )
 * \____/\____/_/  |_\___/\___/\___/____/____/
 *
 * The MIT License (MIT)
 * Copyright (c) 2009-2016 Gerardo Orellana <hello @ goaccess.io>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include "mdb.h"

#include "error.h"
#include "sort.h"
#include "util.h"
#include "xmalloc.h"

/* Hash tables storage */
static GLMDBStorage *db_storage;

/* tables for the whole app */
static MDB_dbi *ht_agent_keys;
static MDB_dbi *ht_agent_vals;
static MDB_dbi *ht_general_stats;
static MDB_dbi *ht_hostnames;
static MDB_dbi *ht_unique_keys;


/* Instantiate a new store */
static GLMDBStorage *
new_storage (uint32_t size)
{
  GLMDBStorage *storage = xcalloc (size, sizeof (GLMDBStorage));
  return storage;
}

/* Get the on-disk databases path.
 *
 * On success, the databases path string is returned. */
static char *
get_dbname (const char *dbname, int module)
{
  char *path = NULL;

  path = xmalloc(snprintf (NULL, 0, "%s-m%d", dbname, module) + 1);
  sprintf (path, "%s-m%d", dbname, module);

  return path;
}

static char *
set_env_path (void)
{
  struct stat info;
  const char *db_path = LMDB_DBPATH;

  /* path was provided as a config option */
  if (conf.db_path != NULL)
    db_path = conf.db_path;

  /* sanity check: Is db_path accessible and a directory? */
  if (stat (db_path, &info) != 0)
    FATAL ("Unable to access database path: %s", strerror (errno));
  if (!(info.st_mode & S_IFDIR))
    FATAL ("Database path is not a directory.");

  return xstrdup (db_path);
}

static MDB_txn *
txn_begin (void)
{
  int rc = 0;
  MDB_txn *txn = NULL;

  if ((rc = mdb_txn_begin (db_storage->env, NULL, 0, &txn)) != 0)
    FATAL ("Unable to begin transaction: %s", mdb_strerror (rc));

  return txn;
}

static int
txn_commit (MDB_txn * txn)
{
  int rc = 0;

  if ((rc = mdb_txn_commit (txn)) != 0)
    FATAL ("Unable to start transaction: %s", mdb_strerror (rc));

  return 0;
}

static MDB_env *
create_environment (void)
{
  MDB_env *env = NULL;
  char *path = NULL;
  int rc = 0;
  unsigned int flags = MDB_NOSYNC;

  /* Everything starts with an environment (database) */
  if ((rc = mdb_env_create (&env)) != 0)
    FATAL ("Unable to create DB environment: %s", mdb_strerror (rc));

  /* Set the maximum number of named databases for the environment. */
  if ((rc = mdb_env_set_maxdbs (env, (MDB_dbi) LMDB_ENV_MAX_DBS)) != 0)
    FATAL ("Unable to set environment max db: %s", mdb_strerror (rc));

  /* size of the memory map is also the maximum size of the database */
  if ((rc = mdb_env_set_mapsize (env, LMDB_DB_SIZE)) != 0)
    FATAL ("Unable to set env map size: %s", mdb_strerror (rc));

  /* Open an environment handle */
  path = set_env_path ();

  /* directory for environment must exist already */
  if ((rc = mdb_env_open (env, path, flags, 0664))) {
    free (path);
    FATAL ("Unable to open environment: %s", mdb_strerror (rc));
  }

  return env;
}

static MDB_dbi *
create_datebase (MDB_txn *txn, const char *dbname, unsigned int flags)
{
  MDB_dbi *dbi = NULL;
  int rc = 0;

  dbi = xcalloc (1, sizeof (MDB_dbi));

  if ((rc = mdb_dbi_open (txn, dbname, flags, dbi)) != 0)
    FATAL ("Unable to open database: %s", mdb_strerror (rc));

  return dbi;
}

/* Initialize map & metric hashes */
static void
init_tables (MDB_txn *txn, GModule module)
{
  GLMDBStorageMetric mtrc;
  int n = 0, i;

  /* *INDENT-OFF* */
  GLMDBStorageMetric metrics[] = {
    {MTRC_KEYMAP    , DB_KEYMAP    , NULL} ,
    {MTRC_ROOTMAP   , DB_ROOTMAP   , NULL} ,
    {MTRC_DATAMAP   , DB_DATAMAP   , NULL} ,
    {MTRC_UNIQMAP   , DB_UNIQMAP   , NULL} ,
    {MTRC_ROOT      , DB_ROOT      , NULL} ,
    {MTRC_HITS      , DB_HITS      , NULL} ,
    {MTRC_VISITORS  , DB_VISITORS  , NULL} ,
    {MTRC_BW        , DB_BW        , NULL} ,
    {MTRC_CUMTS     , DB_CUMTS     , NULL} ,
    {MTRC_MAXTS     , DB_MAXTS     , NULL} ,
    {MTRC_METHODS   , DB_METHODS   , NULL} ,
    {MTRC_PROTOCOLS , DB_PROTOCOLS , NULL} ,
    {MTRC_AGENTS    , DB_AGENTS    , NULL} ,
    {MTRC_METADATA  , DB_METADATA  , NULL} ,
  };
  /* *INDENT-ON* */

  n = ARRAY_SIZE (metrics);
  for (i = 0; i < n; ++i) {
    mtrc = metrics[i];
    mtrc.store = create_datebase (txn, get_dbname(mtrc.dbname, module), MDB_CREATE);
    db_storage[module].metrics[i] = mtrc;
  }
}

/* Initialize hash tables */
void
ginit_storage (void)
{
  MDB_txn *txn = NULL;
  GModule module;
  size_t idx = 0;

  /* global storage object */
  db_storage = new_storage (TOTAL_MODULES);
  /* create database environment */
  db_storage->env = create_environment ();

  txn = txn_begin ();
  ht_agent_keys    = create_datebase (txn, DB_AGENT_KEYS, MDB_CREATE);
  ht_agent_vals    = create_datebase (txn, DB_AGENT_VALS, MDB_CREATE);
  ht_general_stats = create_datebase (txn, DB_GEN_STATS, MDB_CREATE);
  ht_hostnames     = create_datebase (txn, DB_HOSTNAMES, MDB_CREATE);
  ht_unique_keys   = create_datebase (txn, DB_UNIQUE_KEYS, MDB_CREATE);

  FOREACH_MODULE (idx, module_list) {
    module = module_list[idx];

    db_storage[module].module = module;
    init_tables (txn, module);
  }
  txn_commit (txn);
}

static MDB_dbi *
get_db (GModule module, GSMetric metric)
{
  MDB_dbi *db = NULL;
  GLMDBStorageMetric mtrc;
  int i;

  if (!db_storage)
    return NULL;

  for (i = 0; i < GSMTRC_TOTAL; i++) {
    mtrc = db_storage[module].metrics[i];
    if (mtrc.metric != metric)
      continue;
    db = mtrc.store;
    break;
  }

  return db;
}

static size_t
db_get_size (MDB_dbi * db)
{
  MDB_stat mst;
  MDB_txn *txn = NULL;
  size_t size = 0;
  int rc = 0;

  txn = txn_begin ();
  if (rc = mdb_stat (txn, *db, &mst))
    FATAL ("Unable to stat database: %s", mdb_strerror (rc));
  size = mst.ms_entries;
  txn_commit (txn);

  return size;
}

/* Get the int value of a given string key.
 *
 * On error, or if key is not found, -1 is returned.
 * On success the int value for the given key is returned */
static int
get_si32 (MDB_dbi * db, char *k)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0, ret = -1;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = k;
  key.mv_size = strlen (k);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    ret = (*(int *) data.mv_data);
  txn_commit (txn);

  return ret;
}

/* Insert a string key and the corresponding int value.
 * Note: If the key exists, the value is not replaced.
 *
 * On error, or if key exists, -1 is returned.
 * On success 0 is returned */
static int
ins_si32 (MDB_dbi * db, char *k, int value)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = k;
  key.mv_size = strlen (k);

  data.mv_data = &value;
  data.mv_size = sizeof (int);

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Insert an int key and the corresponding string value.
 * Note: If the key exists, the value is not replaced.
 *
 * On error, or if key exists, -1 is returned.
 * On success 0 is returned */
static int
ins_is32 (MDB_dbi * db, int k, char *value)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  data.mv_data = value;
  data.mv_size = strlen (value) + 1;

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Insert a string key and auto increment int value. */
static int
ins_si32_ai (MDB_dbi * db, char *key)
{
  int value = 0;
  size_t size = 0;

  if (!db)
    return -1;

  size = db_get_size (db);
  /* the auto increment value starts at SIZE (hash table) + 1 */
  value = size > 0 ? size + 1 : 1;

  if (ins_si32 (db, key, value) == -1)
    return -1;

  return value;
}

/* Insert an int key and an int value
 * Note: If the key exists, its value is replaced by the given value.
 *
 * On error, -1 is returned.
 * On success 0 is returned */
static int
ins_ii32 (MDB_dbi * db, int k, int value)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  data.mv_data = &value;
  data.mv_size = sizeof (int);

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Increase an int value given an int key.
 * Note: If the key exists, its value is increased by the given inc.
 *
 * On error, -1 is returned.
 * On success 0 is returned */
static int
inc_ii32 (MDB_dbi * db, int k, int inc)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0, ret = inc;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    ret = (*(int *) data.mv_data) + inc;

  data.mv_data = &ret;
  data.mv_size = sizeof (int);

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Insert a string key and auto increment by uint64_t value.
 *
 * On error, -1 is returned.
 * On success the value of the key is inserted and 0 is returned */
static int
inc_su64 (MDB_dbi * db, char *k, uint64_t inc)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;
  uint64_t ret = inc;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = k;
  key.mv_size = strlen (k);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    ret = (*(uint64_t *) data.mv_data) + inc;

  data.mv_data = &ret;
  data.mv_size = sizeof (uint64_t);

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Increase a uint64_t value given an int key.
 * Note: If the key exists, its value is increased by the given inc.
 *
 * On error, -1 is returned.
 * On success 0 is returned */
static int
inc_iu64 (MDB_dbi * db, int k, uint64_t inc)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;
  uint64_t ret = inc;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    ret = (*(uint64_t *) data.mv_data) + inc;

  data.mv_data = &ret;
  data.mv_size = sizeof (uint64_t);

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Get the uint64_t value of a given int key.
 *
 * On error, or if key is not found, 0 is returned.
 * On success the uint64_t value for the given key is returned */
static uint64_t
get_iu64 (MDB_dbi * db, int k)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;
  uint64_t ret = 0;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    ret = (*(uint64_t *) data.mv_data);
  txn_commit (txn);

  return ret;
}

/* Insert an int key and a uint64_t value
 * Note: If the key exists, its value is replaced by the given value.
 *
 * On error, -1 is returned.
 * On success 0 is returned */
static int
ins_iu64 (MDB_dbi * db, int k, uint64_t value)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  data.mv_data = &value;
  data.mv_size = sizeof (uint64_t);

  if ((rc = mdb_put (txn, *db, &key, &data, 0)) != 0)
    FATAL ("Unable to access database path: %s", mdb_strerror (rc));
  txn_commit (txn);

  return 0;
}

/* Get the string value of a given string key.
 *
 * On error, or if key is not found, NULL is returned.
 * On success the string value for the given key is returned */
static char *
get_ss32 (MDB_dbi *db, char *k)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;
  char *value = NULL;

  if (!db)
    return NULL;

  txn = txn_begin ();
  key.mv_data = k;
  key.mv_size = strlen(k);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    value = xstrdup(data.mv_data);
  txn_commit (txn);

  return value;
}

/* Get the string value of a given int key.
 *
 * On error, or if key is not found, NULL is returned.
 * On success the string value for the given key is returned */
static char *
get_is32 (MDB_dbi *db, int k)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0;
  char *value = NULL;

  if (!db)
    return NULL;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    value = xstrdup(data.mv_data);
  txn_commit (txn);

  return value;
}

/* Get the int value of a given int key.
 *
 * If key is not found, 0 is returned.
 * On error, -1 is returned.
 * On success the int value for the given key is returned */
static int
get_ii32 (MDB_dbi *db, int k)
{
  MDB_val key, data;
  MDB_txn *txn = NULL;
  int rc = 0, ret = 0;

  if (!db)
    return -1;

  txn = txn_begin ();
  key.mv_data = &k;
  key.mv_size = sizeof (int);

  /* key found, return current value */
  if ((rc = mdb_get (txn, *db, &key, &data)) == 0)
    ret = (*(int *) data.mv_data);
  txn_commit (txn);

  return ret;
}

int
db_insert_unique_key (char *key)
{
  int value = -1;
  MDB_dbi *db = ht_unique_keys;

  if (!db)
    return -1;

  if ((value = get_si32 (db, key)) != -1)
    return value;

  return ins_si32_ai (db, key);
}

int
db_insert_agent_key (char *key)
{
  int value = -1;
  MDB_dbi *db = ht_agent_keys;

  if (!db)
    return -1;

  if ((value = get_si32 (db, key)) != -1)
    return value;

  return ins_si32_ai (db, key);
}

int
db_insert_agent_value (int key, char *value)
{
  MDB_dbi *db = ht_agent_vals;

  if (!db)
    return -1;

  return ins_is32 (db, key, value);
}

/* Insert a keymap string key.
 *
 * If the given key exists, its value is returned.
 * On error, -1 is returned.
 * On success the value of the key inserted is returned */
int
db_insert_keymap (GModule module, char *key)
{
  int value = -1;
  MDB_dbi *db = get_db (module, MTRC_KEYMAP);

  if (!db)
    return -1;

  if ((value = get_si32 (db, key)) != -1)
    return value;

  return ins_si32_ai (db, key);
}

int
db_insert_uniqmap (GModule module, char *key)
{
  int value = -1;
  MDB_dbi *db = get_db (module, MTRC_UNIQMAP);

  if (!db)
    return -1;

  if ((value = get_si32 (db, key)) != -1)
    return 0;

  return ins_si32_ai (db, key);
}

int
db_insert_datamap (GModule module, int key, char *value)
{
  MDB_dbi *db = get_db (module, MTRC_DATAMAP);

  if (!db)
    return -1;

  return ins_is32 (db, key, value);
}

int
db_insert_rootmap (GModule module, int key, char *value)
{
  MDB_dbi *db = get_db (module, MTRC_ROOTMAP);

  if (!db)
    return -1;

  return ins_is32 (db, key, value);
}

int
db_insert_root (GModule module, int key, int value)
{
  MDB_dbi *db = get_db (module, MTRC_ROOT);

  if (!db)
    return -1;

  return ins_ii32 (db, key, value);
}

int
db_insert_hits (GModule module, int key, int inc)
{
  MDB_dbi *db = get_db (module, MTRC_HITS);

  if (!db)
    return -1;

  return inc_ii32 (db, key, inc);
}

int
db_insert_visitor (GModule module, int key, int inc)
{
  MDB_dbi *db = get_db (module, MTRC_VISITORS);

  if (!db)
    return -1;

  return inc_ii32 (db, key, inc);
}

int
db_insert_bw (GModule module, int key, uint64_t inc)
{
  MDB_dbi *db = get_db (module, MTRC_BW);

  if (!db)
    return -1;

  return inc_iu64 (db, key, inc);
}

int
db_insert_cumts (GModule module, int key, uint64_t inc)
{
  MDB_dbi *db = get_db (module, MTRC_CUMTS);

  if (!db)
    return -1;

  return inc_iu64 (db, key, inc);
}

int
db_insert_maxts (GModule module, int key, uint64_t value)
{
  uint64_t curvalue = 0;
  MDB_dbi *db = get_db (module, MTRC_MAXTS);

  if (!db)
    return -1;

  if ((curvalue = get_iu64 (db, key)) < value)
    ins_iu64 (db, key, value);

  return 0;
}

int
db_insert_method (GModule module, int key, char *value)
{
  MDB_dbi *db = get_db (module, MTRC_METHODS);

  if (!db)
    return -1;

  return ins_is32 (db, key, value);
}

int
db_insert_protocol (GModule module, int key, char *value)
{
  MDB_dbi *db = get_db (module, MTRC_PROTOCOLS);

  if (!db)
    return -1;

  return ins_is32 (db, key, value);
}

int
db_insert_meta_data (GModule module, char *key, uint64_t value)
{
  MDB_dbi *db = get_db (module, MTRC_METADATA);

  if (!db)
    return -1;

  return inc_su64 (db, key, value);
}

char *
db_get_hostname (char *host)
{
  MDB_dbi *db = ht_hostnames;

  if (!db)
    return NULL;

  return get_ss32 (db, host);
}

char *
db_get_datamap (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_DATAMAP);

  if (!db)
    return NULL;

  return get_is32 (db, key);
}

int
db_get_hits (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_HITS);

  if (!db)
    return -1;

  return get_ii32 (db, key);
}

uint64_t
db_get_bw (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_BW);

  if (!db)
    return 0;

  return get_iu64 (db, key);
}

uint64_t
db_get_cumts (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_CUMTS);

  if (!db)
    return 0;

  return get_iu64 (db, key);
}

uint64_t
db_get_maxts (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_MAXTS);

  if (!db)
    return 0;

  return get_iu64 (db, key);
}

int
db_get_visitors (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_VISITORS);

  if (!db)
    return -1;

  return get_ii32 (db, key);
}

char *
db_get_method (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_METHODS);

  if (!db)
    return NULL;

  return get_is32 (db, key);
}

char *
db_get_protocol (GModule module, int key)
{
  MDB_dbi *db = get_db (module, MTRC_PROTOCOLS);

  if (!db)
    return NULL;

  return get_is32 (db, key);
}

char *
db_get_root (GModule module, int key)
{
  int root_key = 0;
  MDB_dbi *hashroot = get_db (module, MTRC_ROOT);
  MDB_dbi *hashrootmap = get_db (module, MTRC_ROOTMAP);

  if (!hashroot || !hashrootmap)
    return NULL;

  /* not found */
  if ((root_key = get_ii32 (hashroot, key)) == 0)
    return NULL;

  return get_is32 (hashrootmap, root_key);
}

/* Get the number of elements in a uniqmap.
 *
 * On error, 0 is returned.
 * On success the number of elements in MTRC_UNIQMAP is returned */
uint32_t
db_get_size_uniqmap (GModule module)
{
  MDB_dbi *db = get_db(module, MTRC_UNIQMAP);

  if (!db)
    return 0;

  return db_get_size (db);
}

/* Get the number of elements in a datamap.
 *
 * Return -1 if the operation fails, else number of elements. */
uint32_t
db_get_size_datamap (GModule module)
{
  MDB_dbi *db = get_db (module, MTRC_DATAMAP);

  if (!db)
    return 0;

  return db_get_size(db);
}

static GRawData *
init_new_raw_data (GModule module, uint32_t ht_size)
{
  GRawData *raw_data;

  raw_data = new_grawdata ();
  raw_data->idx = 0;
  raw_data->module = module;
  raw_data->size = ht_size;
  raw_data->items = new_grawdata_item (ht_size);

  return raw_data;
}

/* Store the key/value pairs from a hash table into raw_data and sorts
 * the hits (numeric) value.
 *
 * On error, NULL is returned.
 * On success the GRawData sorted is returned */
static GRawData *
parse_raw_num_data (GModule module)
{
  GRawData *raw_data;
  MDB_cursor *cursor;
  MDB_dbi *db = NULL;
  MDB_txn *txn = NULL;
  MDB_val key, data;
  uint32_t ht_size = 0;
  int rc = 0;

  db = get_db (module, MTRC_HITS);
  if (!db)
    return NULL;

  ht_size = db_get_size (db);
  raw_data = init_new_raw_data (module, ht_size);
  raw_data->type = INTEGER;

  txn = txn_begin ();

  rc = mdb_cursor_open(txn, *db, &cursor);
  while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
    raw_data->items[raw_data->idx].key = (*(int *) key.mv_data);
    raw_data->items[raw_data->idx].value.ivalue = (* (int *) data.mv_data);
    raw_data->idx++;
  }
  mdb_cursor_close(cursor);
  txn_commit (txn);

  sort_raw_num_data (raw_data, raw_data->idx);

  return raw_data;
}

/* Store the key/value pairs from a hash table into raw_data and sorts
 * the data (string) value.
 *
 * On error, NULL is returned.
 * On success the GRawData sorted is returned */
static GRawData *
parse_raw_str_data (GModule module)
{
  GRawData *raw_data;
  MDB_cursor *cursor;
  MDB_dbi *db = NULL;
  MDB_txn *txn = NULL;
  MDB_val key, data;
  uint32_t ht_size = 0;
  int rc = 0;

  db = get_db (module, MTRC_DATAMAP);
  if (!db)
    return NULL;

  ht_size = db_get_size (db);
  raw_data = init_new_raw_data (module, ht_size);
  raw_data->type = STRING;

  txn = txn_begin ();

  rc = mdb_cursor_open(txn, *db, &cursor);
  while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
    raw_data->items[raw_data->idx].key = (*(int *) key.mv_data);
    raw_data->items[raw_data->idx].value.svalue = (char *) data.mv_data;
    raw_data->idx++;
  }
  mdb_cursor_close(cursor);
  txn_commit (txn);

  sort_raw_str_data (raw_data, raw_data->idx);

  return raw_data;
}

GRawData *
db_parse_raw_data(GModule module)
{
  int rc = 0;
  if ((rc = mdb_env_sync(db_storage->env, 1)) != 0)
    FATAL ("Unable to sync transaction: %s", mdb_strerror (rc));

  GRawData *raw_data;

  switch (module) {
  case VISITORS:
    raw_data = parse_raw_str_data (module);
    break;
  default:
    raw_data = parse_raw_num_data (module);
  }

  return raw_data;
}
