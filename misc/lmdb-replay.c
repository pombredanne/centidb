/*
 * Copyright 2013, David Wilson.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/**
 * Take the output of acid.engines.TraceEngine and replay it against LMDB,
 * without any intervening code.
 */

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "lmdb.h"
#include "mdb.c"
#include "midl.c"


void check(int x)
{
    if(x) {
        fprintf(stderr, "eek %s\n", mdb_strerror(x));
        _exit(1);
    }
}


#define DB_PATH "/media/scratch/t6.lmdb"

MDB_dbi dbi;
MDB_txn *txn;
MDB_env *env;
MDB_cursor *cur;

MDB_cursor_op dir;

int lineno;
unsigned char linebuf[1048576*16];
unsigned char keybuf[1048576*16];
unsigned char valbuf[1048576*16];
size_t keylen;
size_t vallen;


int empty;
MDB_val keyv;
MDB_val valv;




void new_txn(void)
{
    if(txn) {
        fprintf(stderr, "commit\n");
        check(mdb_txn_commit(txn));
    }
    check(mdb_txn_begin(env, NULL, 0, &txn));
}


unsigned char nibble(unsigned char *c)
{
    if(isdigit(*c)) {
        return *c - '0';
    } else {
        return 10 + (*c - 'a');
    }
}


void decodes(void)
{
    unsigned char *c = linebuf;
    while(*c != ' ' && *c != '\n') c++; // skip cmd
    if(*c == '\n') {
        return;
    }
    c++; // skip ws

    keylen = 0;
    vallen = 0;

    while(isxdigit(*c)) {
        unsigned char b = nibble(c++) << 4;
        b |= nibble(c++);
        keybuf[keylen++] = b;
    }
    keybuf[keylen] = 0;
    c++;

    while(isxdigit(*c)) {
        unsigned char b = nibble(c++) << 4;
        b |= nibble(c++);
        valbuf[vallen++] = b;
    }
    valbuf[vallen] = 0;
}


int main(void)
{
    FILE *fp = fopen("/tmp/lmdb.trace", "r");
    check(/*!*/ fp == NULL);

    check(mdb_env_create(&env));
    check(mdb_env_set_mapsize(env, 1048576UL*1024UL*3UL));
    check(mdb_env_set_maxreaders(env, 126));
    check(mdb_env_set_maxdbs(env, 1));
    if(! access(DB_PATH, X_OK)) {
        system("rm -rf " DB_PATH);
    }
    check(mkdir(DB_PATH, 0777));
    check(mdb_env_open(env, DB_PATH, MDB_MAPASYNC|MDB_NOSYNC|MDB_NOMETASYNC, 0644));
    new_txn();
    check(mdb_dbi_open(txn, NULL, 0, &dbi));

    while(NULL != fgets(linebuf, sizeof linebuf, fp)) {
        lineno++;
        linebuf[sizeof linebuf - 1] = '\0';
        decodes();

        if(0 == strncmp(linebuf, "iter", 4)) {
            if(cur) {
                mdb_cursor_close(cur);
                cur = NULL;
            }
            //write(1, keybuf, keylen);
            //write(1, valbuf, vallen);
            check(mdb_cursor_open(txn, dbi, &cur));
            keyv.mv_data = keybuf;
            keyv.mv_size = keylen;
            int rc;
            if(valbuf[0] == 'F') { // forward
                dir = MDB_NEXT;
                rc = mdb_cursor_get(cur, &keyv, NULL, MDB_SET_RANGE);
                if(rc == MDB_NOTFOUND) {
                    rc = mdb_cursor_get(cur, &keyv, NULL, MDB_FIRST);
                }
                empty = rc == MDB_NOTFOUND;
            } else { //reverse
                dir = MDB_PREV;
                rc = mdb_cursor_get(cur, &keyv, NULL, MDB_SET_RANGE);
                if(rc == MDB_NOTFOUND) {
                    rc = mdb_cursor_get(cur, &keyv, NULL, MDB_LAST);
                }
                empty = rc == MDB_NOTFOUND;
            }
            if(rc != MDB_NOTFOUND) {
                check(rc);
            }
        } else if(0 == strncmp(linebuf, "fetch", 5)) {
            if(! empty) {
                int rc = mdb_cursor_get(cur, &keyv, &valv, MDB_GET_CURRENT);
                if(rc != MDB_NOTFOUND /*WTF NOT REQ'D IN 0.9.8*/ && rc != EINVAL) {
                    check(rc);
                }
            }
        } else if(0 == strncmp(linebuf, "iter", 4)) {
            check(strncmp(keybuf, keyv.mv_data, keylen));
            check(strncmp(valbuf, valv.mv_data, vallen));
        } else if(0 == strncmp(linebuf, "put", 3)) {
            keyv.mv_data = keybuf;
            keyv.mv_size = keylen;
            valv.mv_data = valbuf;
            valv.mv_size = vallen;
            check(mdb_put(txn, dbi, &keyv, &valv, 0));
        } else if(0 == strncmp(linebuf, "delete", 6)) {
            keyv.mv_data = keybuf;
            keyv.mv_size = keylen;
            int rc = mdb_del(txn, dbi, &keyv, NULL);
            if(rc != MDB_NOTFOUND) {
                check(rc);
            }
        } else if(0 == strncmp(linebuf, "commit", 6)) {
            new_txn();
        }
    }
}
