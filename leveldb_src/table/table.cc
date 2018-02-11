// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "table/block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  Block* index_block = NULL;
  if (s.ok()) {
    s = ReadBlock(file, ReadOptions(), footer.index_handle(), &index_block);
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
	  // lzh: 每一个 Table 有一个 Rep, 有一个 cache, 有一个 index block
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    *table = new Table(rep);
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

Table::~Table() {
  delete rep_;
}

/************************************************************************/
/* 
	lzh: 直接删除 arg 指示的 Block
*/
/************************************************************************/
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

/************************************************************************/
/* 
	lzh: 释放缓存 arg 中的 条目 h. Release 当缓存条目引用计数为 0 时会清理内存
*/
/************************************************************************/
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
/************************************************************************/
/* 
	lzh: 返回 Table 中 index_value 位置的 Block 的迭代器. 其被用来对一个 sst 文件构造 TwoLevelIterator。

	1.	如果此 Table 具有块缓存 block_cache 则块缓存中读取, 如果块缓存中没有该数据则从 table 文件(sst 文件)
		中读取并加入到块缓存中(注意注册 ReleaseBlock 函数).
	2.	如果此 Table 无块缓存则直接读取 table 文件, 注册 DeleteBlock 函数
*/
/************************************************************************/
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;

  //lzh: 目标  block 在缓存中的位置信息
  Cache::Handle* cache_handle = NULL;

  //lzh: 结构化的 index_value，用于表示 Block 中的索引信息(offset, size)
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    if (block_cache != NULL) {
		//lzh: 计算目标 Block 在缓存 block_cache 中的索引 key
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));

	  //lzh: key 即是目标 Block 在缓存 block_cache 中对应的索引键
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
		  //lzh: 位置信息有效则查看此位置的值. 实际上就是 ((LRUHandle*)cache_handle)->value
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
		  //lzh: 缓存无效, 则读取 table 中 handle 位置的数据(注意 handle 即等价于 index_value), 装入 block
        s = ReadBlock(table->rep_->file, options, handle, &block);

		//插入缓存
        if (s.ok() && options.fill_cache) {
          cache_handle = block_cache->Insert(
              key, block, block->size(), &DeleteCachedBlock);
        }
      }
    } else {
		//lzh: block_cache 是 NULl 不需要将当前 block 加入进去
      s = ReadBlock(table->rep_->file, options, handle, &block);
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
		//lzh: cache 具有引用计数, 所以使用 release 的方式
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}


/************************************************************************/
/* 
	lzh: 返回一个遍历此 Table 的迭代器.
	第一维的迭代器是 index_block 的迭代器，用于返回元素在 table 中的位置信息(哪一个 block).
	第二维的迭代器是 table 上的迭代器，用于返回元素的 key-value.
*/
/************************************************************************/
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),	//lzh: table 的 index_block
      &Table::BlockReader, 
	  const_cast<Table*>(this), 
	  options);
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}
