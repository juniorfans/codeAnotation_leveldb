// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

	//这里很容易看成声明类 DBImpl，继承自 CompactionState。继承是 : 符号，这里的 :: 是域作用符
struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  ClipToRange(&result.max_open_files,           20,     50000);
  ClipToRange(&result.write_buffer_size,        64<<10, 1<<30);
  ClipToRange(&result.block_size,               1<<10,  4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : env_(options.env),
      internal_comparator_(options.comparator),
      options_(SanitizeOptions(dbname, &internal_comparator_, options)),
      owns_info_log_(options_.info_log != options.info_log),
      owns_cache_(options_.block_cache != options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(new MemTable(internal_comparator_)),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      logger_(NULL),
      logger_cv_(&mutex_),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  mem_->Ref();
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options.max_open_files - 10;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}


/************************************************************************/
/* 
	lzh:
	这个函数会查找数据库中的所有文件：
	删除较老的 log 文件
	删除较老的描述文件
	删除没有在任何一个level中引用也没有正在压缩的所有table文件和对应的 tmp 文件，再从缓存中删除这些 sst 数据
*/
/************************************************************************/
void DBImpl::DeleteObsoleteFiles() {
  // Make a set of all of the live files
	//lzh: 将正在 compaction 和版本管理中的所有 table 文件均加到 live 中：它们是有效的文件
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  //lzh: 遍历数据库目录下的所有文件
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
			keep = ((number >= versions_->LogNumber()) ||
				(number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  //lzh: 读取 CURRENT 文件，获得当前的 manifest 文件，再从此文件中获得 versionedit，再通过这些 versionedit 
  //lzh: 构建出
  s = versions_->Recover();
  if (s.ok()) {
    SequenceNumber max_sequence(0);

	// lzh: 我们从比 descriptor 中的 log 更新的 log 中执行 Recover（因为这些更新的 log 可能被加入到 descriptor 中）
    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    
	// lzh: 虽然 PrevLogNumber() 不会再被使用，但是我们要防止使用隐式地恢复了一个较低版本 db
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)
          && type == kLogFile
          && ((number >= min_log) || (number == prev_log))) {
        logs.push_back(number);
      }
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);//内部调用 WriteBatch::Iterate, MemTableInserter::Put, MemTable::Add, SkipList::Insert
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

/************************************************************************/
/* 
	lzh: 将 mem 写入磁盘, 生成的 sst 文件的 level 计算方式是：首个键范围与 [min_key, max_key] 相交的层次
*/
/************************************************************************/
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;

  //lzh: 已经将 meta.number 写入到 sst 中了, 删除之	
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  // lzh: 将 meta 这个文件加入到哪一个 level 中, 判断的依据是，meta 的键范围与哪一个 level 的键范围有重叠.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
	  //lzh: 由第 0 个 level 开始，找到首个 level，使它里面的 key 落在区间 [min_user_key, max_user_key]
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL && !base->OverlapInLevel(0, min_user_key, max_user_key)) {
      // Push the new sstable to a higher level if possible to reduce
      // expensive manifest file ops.
      while (level < config::kMaxMemCompactLevel &&
             !base->OverlapInLevel(level + 1, min_user_key, max_user_key)) {
        level++;
      }
    }

	//lzh: 将 meta.number 加入到这个 level
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

/************************************************************************/
/* 
	lzh: 将 mem 写入到磁盘: 生成 sst 文件, 文件层次是首个 key 相交的层次.
	更新整个 VersionSet 的 log_number, prev_log_number, 删除旧的 log 文件.
*/
/************************************************************************/
Status DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  }

  return s;
}

void DBImpl::TEST_CompactRange(
    int level,
    const std::string& begin,
    const std::string& end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  MutexLock l(&mutex_);
  while (manual_compaction_ != NULL) {
    bg_cv_.Wait();
  }
  ManualCompaction manual;
  manual.level = level;
  manual.begin = begin;
  manual.end = end;
  manual_compaction_ = &manual;
  MaybeScheduleCompaction();
  while (manual_compaction_ == &manual) {
    bg_cv_.Wait();
  }
}

Status DBImpl::TEST_CompactMemTable() {
  MutexLock l(&mutex_);
  LoggerId self;
  AcquireLoggingResponsibility(&self);
  Status s = MakeRoomForWrite(true /* force compaction */);
  ReleaseLoggingResponsibility(&self);
  if (s.ok()) {
    // Wait until the compaction completes
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (!shutting_down_.Acquire_Load()) {
    BackgroundCompaction();
  }
  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();	//lzh: 将内存中不变的 MemTable imm_ 写到磁盘文件中，层次是 level 0
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  if (is_manual) {
    const ManualCompaction* m = manual_compaction_;
	//lZh: 选择出要进行 compact 的范围(m->begin, m->end), 下面构造了一个 InternalKey，其实这是不需要的
	//lzh: 在较新版本的代码中已经去掉了 InternalKey 对象的构造
    c = versions_->CompactRange(
        m->level,
        InternalKey(m->begin, kMaxSequenceNumber, kValueTypeForSeek),
        InternalKey(m->end, 0, static_cast<ValueType>(0)));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    CleanupCompaction(compact);
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
    if (options_.paranoid_checks && bg_error_.ok()) {
      bg_error_ = status;
    }
  }

  if (is_manual) {
    // Mark it as done
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

/************************************************************************/
/* 
	lzh: 生成 file_number，加到 outputs，新建输出文件
*/
/************************************************************************/
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

/************************************************************************/
/* 
	lzh: 将 compact 中当前的 builder 数据 dump 到磁盘的 sst 文件.
*/
/************************************************************************/
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

/************************************************************************/
/* 
	lzh:
		将 compact 中生成的新文件在 VersionEdit 中标记，注意 level 层生成的文件是要加入到 level+1 层中的
		将 compact 的输入文件(_inputs)在 VersionEdit 中标记
		删除以后再也不会用到的文件(如果有的话)
		
		然后在当前 Version 中应用 VersionEdit
*/
/************************************************************************/
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,	//lzh: level 层生成的文件，compaction 生要加入到 level+1 层
        out.number, out.file_size, out.smallest, out.largest);
    pending_outputs_.erase(out.number);
  }
  compact->outputs.clear();

  Status s = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
  if (s.ok()) {
    compact->compaction->ReleaseInputs();
    DeleteObsoleteFiles();
  } else {
    // Discard any files we may have created during this failed compaction
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      env_->DeleteFile(TableFileName(dbname_, compact->outputs[i].number));
    }
  }
  return s;
}

Iterator* getTestIterator(uint64_t& snapshot)
{
	const Comparator* comparator = leveldb::BytewiseComparator();
	InternalKeyComparator comp(comparator);

	SequenceNumber x16(16),x17(17),x18(18),x19(19);
	Slice as("a",1);
	MemTable *mt = new MemTable(comp);
	mt->Add(x16,ValueType::kTypeValue,as,Slice("x16",3));
	mt->Add(x18,ValueType::kTypeValue,as,Slice("x18",3));
	mt->Add(x19,ValueType::kTypeValue,as,Slice("x19",3));

	Iterator* its[1] = {mt->NewIterator()};

	Iterator* megerIt = leveldb::NewMergingIterator(&comp,its,1);
	snapshot = x17;
	return megerIt;
}

/************************************************************************/
/* 
	lzh: 压缩挑选出来的 level 层和 level+1 层文件集合: c[0], c[1]

	leveldb 中的 snapshot 的功能是，
	Get 时可以通过option传入snapshot参数，那么查找时会跳过SequenceNumber比snapshot大的键值对，定位到小于等于 snapshot 的最新版本，从而完成快照功能：读取历史数据。
	
	对应地，compaction 应该有如下两个逻辑：
		1.为每个 user_key 保留一个小于等于 snapshot 的最大版本号(如果有的话)。
		2.最基本的，各个 user_key 要保留其最新的版本，不论这个版本是不是小于 snapshot。
	
	由1，2两点，当 snapshot 没有被设置时，snapshot 应该被默认为整个 leveldb 中当前最大版本号。此时第1点等于无（此时它即是第2点）。
	另外，一个解析出错的键值对后面紧挨着的键值对不会被 drop。
	
*/
/************************************************************************/
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);

  //lzh: 函数注释已经阐明 snapshot 没有被设置时，应该被设置为 leveldb 中当前最大版本号。
  //lzh: 若有多个 snapshot 应该使用最小的那个，以保证更古老的版本数据能够被保存下来
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  //lzh: 得到遍历所有数据的 Iterator. 它将正序遍历所有的 Internalkey，相同的 user_key 越新的版本越前面被遍历到
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  //input = getTestIterator(compact->smallest_snapshot);

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;	//当前正遍历到的 internal key

  std::string current_user_key;	//当前遍历到的 user_key。

  //lzh: current_user_key 是否有效。还没有开始遍历，或者解析失败时此值为 false
  bool has_current_user_key = false;

  //lzh: 上一次遍历到的当前 user_key 的 sequence number。
  //lzh: 还没有开始遍历，或者解析失败时，或者当前的 user_key 是首次遍历到，则此值为 kMaxSequenceNumber。保证每第一次遍历到的 user_key 不被 drop
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  //lzh: 依次遍历, 若不 drop, 则 compact
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();	//lzh: 将内存中的 mem 导出到磁盘上的 sst 文件(作为第 0 层)
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
	//lzh: 是否应该停止往当前构建的 sst 中增加 key. 若返回 true 则需要将当前的构建写入到磁盘然后再重新开始一个 sst 的构建
	//lzh: 首次调用 ShouldStopBefore 必然返回 false. 
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else 
	{
	  // lzh: 首次遇到 ikey.user_key 这个 user_key
      if (!has_current_user_key ||	//lzh: !has_current_user_key 表示上一个 ikey 解析失败或者当前是第一次解析
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {	//lzh: 表示当前的 ikey.user_key 与 current_user_key 不相等
        // First occurrence of this user key
		
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;	//lzh: 当前 ikey.user_key 是首次遇到，设置 last_sequence_for_key=kMaxSequenceNumber 保证当前 ikey 不被 drop
      }
	  
	  //lzh: 当前 user_key 的上一个版本 <= snapshot，我们把上一个版本保留下来，以满足 snapshot 的功能：为每个 user_key 保留一个小于等于 snapshot 的最大版本号。
	  //lzh: 从当前版本开始(包括在内)，后续所有当前 user_key 的更旧版本可以 drop 了。
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) 
	  {
		  //lzh: 若当前 user_key 上一个版本 > snapshot，但当前 user_key 的 type 是 deletion，那么什么情况下可以 drop 掉这个版本呢？
		  //lzh: 1.当前是要将 level_ 层和 level_+1 层 compact，如果当前 user_key 出现在比level_+1 层更高的层次，那么这个 kv 必然不能被 drop 掉，
		  //lzh: 否则，其它层可能存在的更旧的版本就会被使用，这将导致逻辑错误。
		  //lzh: 2.若当前版本大于数据库设置的最小快照号 smallest_snapshot 仍然将它 drop 掉
		  //lzh: 注意当前的 user_key 是 deletion 版本，为了保持逻辑的正确性：相同 user_key 的 deletion 版本后面的版本是无效的，还需要将此 user_key 后续版本全部 drop
		  //lzh: 但是这样一来快照功能的逻辑就错了：我们的查找范围是所有小于等于 snapshot 的版本。因为我们已经删除了所有的较旧版本的 user_key，所以使用快照功能也无法访问了，
		  //lzh: 虽然在一个较旧的时刻，user_key 最新的版本是有效的，在快照下应该是可访问的。
		  //lzh: 综上，所以判断的条件有以上三个。
		  
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

	  //lzh: 在赋值之前 last_sequence_for_key 指的是前一次遍历到的当前 user_key 的版本。若当次是首次遍历到当前 user_key 则此值在赋值前为 kMaxSequenceNumber
      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

	//lzh: 当前 input->key 被 drop 掉, 不用 compact 到文件中
    if (!drop) {
      // Open output file if necessary
		//lzh: 如果首次进入此分支， compaction 没有初始化(没有生成 file_number, 没有生成文件, 建立 sst 对应的内存 table), 那么需要初始化
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }

	  //lzh: 如果 sst 文件对应的内存 table 中没有数据, 则当前被加入的 key 作为 smallest. (注意 key 是正序遍历的, 所以此逻辑是正确的)
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
	  
	  //lzh: key 是正序遍历的，所以每次新加入的 key 就是最大值
      compact->current_output()->largest.DecodeFrom(key);

	  //lzh: (key, value) 加入到 sst 内存 table 中
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
	  //lzh: sst 内存 table 满了，需要 dump
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }

  //lzh: 最后一轮循环中正在构建的 sst 大小没有达到阈值还没来得及写入磁盘, 此处需要处理
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  //lzh: 最终将所有生成的 sst 文件应用到内存版本管理, sst 文件层次管理中
  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  return NewInternalIterator(ReadOptions(), &ignored);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

/************************************************************************/
/* 
	lzh: 从内存中的 mem, imem, VersionSet (缓存的sst文件/磁盘上的sst文件) 中查找 key
		若是最后一种情况，可能会触发 seek_compaction 策略，详见后面注释。
*/
/************************************************************************/
Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  //lzh: 如果内存中的 mem, imm 中都没有找到 lkey, 则需要从 缓存的 sst 文件, 甚至是硬盘的 sst 文件中去查找
  //lzh: 那么需要更新统计情况: 是否从 sst 文件中 seek 的次数过多(若是则需要 compact 以提高后续的查询效率).
  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
	//lzh: 使用 userkey.length, user_key, sequencenum, keySeektype 构造 LookupKey
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  return NewDBIterator(
      &dbname_, env_, user_comparator(), internal_iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot));
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

// There is at most one thread that is the current logger.  This call
// waits until preceding logger(s) have finished and becomes the
// current logger.
void DBImpl::AcquireLoggingResponsibility(LoggerId* self) {
  while (logger_ != NULL) {
    logger_cv_.Wait();
  }
  logger_ = self;
}

void DBImpl::ReleaseLoggingResponsibility(LoggerId* self) {
  assert(logger_ == self);
  logger_ = NULL;
  logger_cv_.SignalAll();
}

/************************************************************************/
/* lzh 写入记录                                                                     */
/************************************************************************/
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Status status;
  MutexLock l(&mutex_);
  LoggerId self;
  AcquireLoggingResponsibility(&self);
  status = MakeRoomForWrite(false);  // May temporarily release lock and wait
  uint64_t last_sequence = versions_->LastSequence();
  if (status.ok()) {
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);

	//lzh: 这一点与 write_batch.cc 中 MemTableInserter 的实现相呼应：batch 中的操作依次获得 last_sequence+i 的 sequence number
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock during
    // this phase since the "logger_" flag protects against concurrent
    // loggers and concurrent writes into mem_.
    {
      assert(logger_ == &self);
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      assert(logger_ == &self);
    }

    versions_->SetLastSequence(last_sequence);
  }
  if (options.post_write_snapshot != NULL) {
    *options.post_write_snapshot =
        status.ok() ? snapshots_.New(last_sequence) : NULL;

	Log(options_.info_log, "snapshots_.New(%d)\n", last_sequence);

  }
  ReleaseLoggingResponsibility(&self);
  return status;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is the current logger
// lzh: force: force compaction even if less of memory
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(logger_ != NULL);
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
		//lzh 如果当前level-0中的文件数目达到kL0_SlowdownWritesTrigger阈值，则sleep 进行 delay。该delay只会发生一次。
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
		//如果强制直接写入则直接写入，否则如果当前memtable的size 未达到阈值write_buffer_size，则允许这次写。
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
		//如果memtable已经达到阈值，但immutable memtable仍存在，则等待compact 将其dump完成。
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
		//如果level-0中的文件数目达到kL0_StopWritesTrigger阈值，则等待compact memtable完成。
      // There are too many level-0 files.
      Log(options_.info_log, "waiting...\n");
      bg_cv_.Wait();
    } else {
		//此分支包含了 force=true
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
	  //lzh: 获得整个 leveldb 中最大的 file number + 1
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();
      impl->MaybeScheduleCompaction();
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          filenames[i] != lockname) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}
