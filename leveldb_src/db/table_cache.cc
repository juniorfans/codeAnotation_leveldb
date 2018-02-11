// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}



/************************************************************************/
/* 
	lzh:
		TableCache 是一个很有意思的类，它使用 LRUCache 缓存了从 file_number 到
		文件描述符的对应，同时它限制了最多只能保存 entries 多个这样的映射关系

		而 LRUCahce 在用作 block 缓存具体数据时，则意指缓存的字节数
*/
/************************************************************************/

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}


/************************************************************************/
/* 
	lzh: 返回 sst 文件: file_number 的 Iterator.
	
	步骤: 若 cache_ 中不存在则打开 sst 文件，导入到内存，加入到缓存
*/
/************************************************************************/
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    Status s = env_->NewRandomAccessFile(fname, &file);
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
      return NewErrorIterator(s);
    }

    TableAndFile* tf = new TableAndFile;
    tf->file = file;
    tf->table = table;

	//lzh: 注意下面 Insert 传的参数 charge=1，这是控制只能缓存 entries 个 sst 文件的关键之二，此处与
	//lzh: 初始化时，传入 entries 是相关的(默认值为 1000-10)
    handle = cache_->Insert(key, tf, 1, &DeleteEntry);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}
