/**
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

#ifndef MDB_H_INCLUDED
#define MDB_H_INCLUDED

#include "deps/liblmdb/lmdb.h"
#include "commons.h"
#include "gstorage.h"

#include "gslist.h"
#include "gstorage.h"
#include "parser.h"

#define LMDB_DBPATH "/tmp"
#define LMDB_DB_SIZE ((size_t)100000 * (size_t)(1024 * 1024))
#define LMDB_ENV_MAX_DBS 256
#define LMDB_ENV_NAME "goaccess.env"

#define RAND_FN 7 + 1

#define DB_AGENT_KEYS  "db_agent_keys"
#define DB_AGENT_VALS  "db_agent_vals"
#define DB_GEN_STATS   "db_gen_stats"
#define DB_HOSTNAMES   "db_hostnames"
#define DB_UNIQUE_KEYS "db_unique_keys"

#define DB_KEYMAP    "db_keymap"
#define DB_DATAMAP   "db_datamap"
#define DB_ROOTMAP   "db_rootmap"
#define DB_UNIQMAP   "db_uniqmap"
#define DB_VISITORS  "db_visitors"
#define DB_ROOT      "db_root"
#define DB_HITS      "db_hits"
#define DB_BW        "db_bw"
#define DB_CUMTS     "db_cumts"
#define DB_MAXTS     "db_maxts"
#define DB_METHODS   "db_methods"
#define DB_PROTOCOLS "db_protocols"
#define DB_AGENTS    "db_agents"
#define DB_METADATA  "db_metadata"

/* Enumerated Storage Metrics */
typedef struct GLMDBStorageMetric_
{
  GSMetric metric;
  const char *dbname;
  MDB_dbi *store;
} GLMDBStorageMetric;

/* Data Storage per module */
typedef struct GLMDBStorage_
{
  MDB_env *env;
  GModule module;
  GLMDBStorageMetric metrics[GSMTRC_TOTAL];
} GLMDBStorage;

char *db_get_datamap (GModule module, int key);
char *db_get_hostname (char *host);
char *db_get_method (GModule module, int key);
char *db_get_protocol (GModule module, int key);
char *db_get_root (GModule module, int key);
GRawData *db_parse_raw_data(GModule module);
int db_get_hits (GModule module, int key);
int db_get_visitors (GModule module, int key);
int db_insert_agent_key (char *key);
int db_insert_agent_value (int key, char *value);
int db_insert_bw (GModule module, int key, uint64_t inc);
int db_insert_cumts (GModule module, int key, uint64_t inc);
int db_insert_datamap (GModule module, int key, char *value);
int db_insert_hits (GModule module, int key, int inc);
int db_insert_keymap (GModule module, char *key);
int db_insert_maxts (GModule module, int key, uint64_t value);
int db_insert_meta_data (GModule module, char *key, uint64_t value);
int db_insert_method (GModule module, int key, char *value);
int db_insert_protocol (GModule module, int key, char *value);
int db_insert_root (GModule module, int key, int value);
int db_insert_rootmap (GModule module, int key, char *value);
int db_insert_uniqmap (GModule module, char *key);
int db_insert_unique_key (char *key);
int db_insert_visitor (GModule module, int key, int inc);
uint32_t db_get_size_datamap (GModule module);
uint32_t db_get_size_uniqmap (GModule module);
uint64_t db_get_bw (GModule module, int key);
uint64_t db_get_cumts (GModule module, int key);
uint64_t db_get_maxts (GModule module, int key);
void ginit_storage (void);

#endif // for #ifndef MDB_H
