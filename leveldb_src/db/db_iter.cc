// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().

	 /************************************************************************/
	 /* 
	 执行完 Prev 后迭代器的指针指示的位置是当前 saved_key 节点的上一个节点. 这一点不同于 Next.
	 造成这个不同的关键原因是:
	 Internalkey 排序规则是 Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序
	 那么相同 user_key 的更新的版本会排在前面(Prev 方向), 所以要找到当前 user_key 的前面一个有效 user_key`, 必须要
	 遍历把 user_key` 的所有节点都遍历完, 才会得知这个 user_key` 是否没有被删除, 或是否有更新版本的值. 
	 */
	 /************************************************************************/

  enum Direction {
    kForward,
    kReverse
  };

  DBIter(const std::string* dbname, Env* env,
         const Comparator* cmp, Iterator* iter, SequenceNumber s)
      : dbname_(dbname),
        env_(env),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false) {
  }
  virtual ~DBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  const std::string* const dbname_;
  Env* const env_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;

  Status status_;

  //lzh: 当前遍历方向是 kReverse 时, 即是当前的 key
  std::string saved_key_;     // == current key when direction_==kReverse

  //lzh: 当前遍历方向是 kReverse 时, 即是当前的值(最原始的 value)
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  //lzh: 当前函数 Next 需要向后遍历(kForward), 如果当前的遍历方向是 kReverse, 则需要更改为 kForward
  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  // Temporarily use saved_key_ as storage for key to skip.
  std::string* skip = &saved_key_;
  SaveKey(ExtractUserKey(iter_->key()), skip);	//lzh: 注意，此处间接着设置了 iter 的 user_key 到 saved_key_ 里面
  
  //lzh: 跳过 iter_key().user_key_ 更旧的版本 或者 跳过当 iter_key()已经是一个 deletion 了
  FindNextUserEntry(true, skip);
}


/************************************************************************/
/* 
	lzh: 该函数耦合了两个功能: 
		1. 遍历过程中遇到了某个 user_key 的类型是 delete, 则后面跳过此 user_key, 
			若跳过后又遇到其它 user_key 的最新记录(也即此 user_key 最先遍历到的版本)是 deletion, 则继续重复处理. -- 强制(skipping 为 true|false 都有此功能)
		2. 跳过相同 user_key 的旧版本 -- skipping 为 true 时有此功能

	skiping 的意思是, 若以后再遇到此 user_key 是否要跳过. 函数被调用之初, user_key 指的是 *skip,  后续指的是遍历到的 user_key

	lzh: 
		skipping=false 时, 有以下执行序列:
			1.若遍历到的首个 Iternalkey 的 valueType 是 delete, 则 skip 会被重置为当前的 user_key, 且 skipping=true, 转 3
			2.否则直接返回
			3.当前遍历的 Internalkey 的 valueType 是 delete, 同 1 处理.否则转 4
			4.当前遍历的 Internalkey 的 valueType 是 value, 则执行比较,若当前 user_key<=*skip 则执行跳过, 否则不执行跳过且函数结束
		
		skipping=true 时则相对复杂了, 它有因为 "deletetion 跳过"和 "user_key 旧版本跳过"这两种情况.

	传入的参数 skip 可能是 Internalkey, 但此时 skipping 是 false(见本文件中 Seek 函数中的调用),

*/
/************************************************************************/
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
			//lzh: 将 ikey.user_key 保存到 skip 中
			//lzh: Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序
			//lzh: 所以同 user_key 较新的记录在前面被遍历出, 如果先遇到了一个 deleteType 的 InternalKey,
			//lzh: 则后面同 user_key 的InternalKey 要被删除.
			//lzh: 这个函数的功能有些耦合, 若此 case 先进入且调用函数时 skip 有值则传入值会被覆盖.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
				  //lzh: 前面一个 case 中若计算得出 skipping=true 则指示了若后面遇到同样的 user_key 则跳过
				  //lzh: 若本函数本来就传入 skipping=true 且 skip 有值(user_key), 这就表示需要跳过小于等于当前 skip 的 InternalKey
				  //lzh: 综上, 所以此处判断的条件是 skipping==true 和 小于等于(不仅仅只有等于).
				  //lzh: 注意, 虽然本函数的输入 skipping 可能是 Internalkey, 但是此处比较时, skipping 一定已经被重设为 user_key 了. 见函数上面的注释
            // Entry hidden
				//lzh: 需要跳过
          } else {
			  //lzh: 如果不需要跳过或遍历到的 user_key 大于 skip, 则不需要跳过
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();	//执行跳过
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}


/************************************************************************/
/* 
	lzh: 与 Next 相对, 功能是找到仅仅比当前 user_key 小的有效的 user_key` 节点
		执行完 Prev 后迭代器的指针指示的位置是当前 saved_key 节点的上一个节点. 这一点不同于 Next.
		造成这个不同的关键原因是:
			Internalkey 排序规则是 Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序
		那么相同 user_key 的更新的版本会排在前面(Prev 方向), 所以要找到当前 user_key 的前面一个有效 user_key`, 必须要
		遍历把 user_key` 的所有节点都遍历完, 才会得知这个 user_key` 是否没有被删除, 或是否有更新版本的值. 
*/
/************************************************************************/
void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false

	//lzh: 保存当前的 user_key 到 saved_key_, 等此函数调用完毕, 即成为“上一个 user_key”
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
		//lzh: 当上一次遍历的方向是 kFroward 时, 会往 next方向(向后) 继续跳过相同 user_key 的节点.
		//lzh: 所以本函数要往 prev 方向遍历, 需要往 prev 方向跳过相同 user_key 的节点
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {
			//lzh: 已经跳完了所有相同 user_key 的节点, 可以停止
        break;
      }
    }
    direction_ = kReverse;
  }

  //lzh: 上面的循环已经使迭代器指向了首个小于当前 user_key 的节点, 现在调用 FindPrevUserEntry 是为了继续向前找到
  FindPrevUserEntry();
}

/************************************************************************/
/* 
	lzh: 本函数耦合两个功能:
	1. 向前移动当前迭代器的位置, 使其指示到更小的一个 user_key 的节点.(此节点可能是 deletion)
	2. saved_key_ 需要指示出之前一个有效的 user_key`. (暗示着, 如果之前一个节点是 deletion, 则需要继续往前遍历).
		此过程是递归的, 如果往前移动的过程中, 遇到的 user_key 的最新版本总是 deletion 则始终需要往前移动.
	
	设当前迭代器指示着 user_key` 的某个节点, 往前遍历遇到它的最新版本, 但是类型是 deletion, 则遍历到的所有 user_key` 节点
	都不再是有效的, 所以需要继续往前遍历. 这也是函数里面这段判断代码的意义: 
		if ((value_type != kTypeDeletion) &&	//上一节点的类型不能是 deletion
			user_comparator_->Compare(ikey.user_key, saved_key_) < 0)	//遇到了更小的 user_key
	举例说明:
	设 user_key: B < A, 且有如下操作流程: A.add	-->	A.add	-->	A.delete	-->	B.add --> B.delete, 则迭代器依次遍历的顺序是:(简单地以链表形式去表示)
	(提示: Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序):

	1(B.delete)	-->	2(B.add)	-->	3(A.delete)	-->	4(A.add)	-->	5(A.add)

	若当前迭代器位置指向 5, 则 FindPrevUserEntry 依次遍历的节点是 4, 3, 2, 1
	遍历到 2 时, 由于之前节点的类型是 deletion 则不应该 break, 继续遍历到 1, 再因为 !valid() 返回. 
	
	以下细节需要注意:
		1. 正常情况下函数结束时迭代器指向的位置的后一个节点不可能是 deletion, 当且仅当整个迭代器遍历到了尽头.
		2. 函数结束时迭代器所指位置可能是一个 deletion节点(这种情况下,形成这个局面的原因是做了空的 delete).举例如下：
			0(A.add)	-->	1(A.delete)	-->	2(B.add)
			当前迭代器指向 2 时, 往前遍历, 则到 1 节点时会 break. 此时迭代器会指在 1，且 saved_key_ 会保存着 2 节点的值.
			仔细观察上面的数据, 操作序列是: 增加B, 删除A, 增加A. 实际上，不管之前的数据(user_key) 有没有 A，“删除A”这个操作
			是无意义的.
	
*/
/************************************************************************/
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
				//lzh: value_type 是上一次遍历到的节点的类型, 当且仅当上一节点不是 deletion 且当前节点是一个新的 user_key 时跳出
				//lzh: 注意当前节点有可能是 deletion
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
		//lzh: 注意, 在此之前 value_type 指的是上一次遍历到的 Internalkey 的 type
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

/************************************************************************/
/* 
	lzh: Seek 调用 FindNextUserEntry 传入 skipping=false, 这点和 SeekToFirst一样, 
		但是和 Next 不一样, Next 传入的是 true.
		原因为以下: 
		考虑 Seek 和 SeekToFirst 的功能, 我们需要定位到的位置: 
			当它是 deletion, 表明当前 user_key 最新的记录是被删除了, 所以应该继续往后面 seek(即 skip); 
			当它是 valueType 时, 即使它是一个旧版本的值, 也不应该跳过, 因为有可能调用者就是想定位到旧版本.
		
		考虑 Next 的功能, 我们需要定位到当前版本 user_key 的下一个有效可访问位置. 若下个位置是 deletion 显然也应该跳过, 
		若是 valueType, 且 user_key 与上一个一致, 但版本较旧, 为了得到 “下一个有效可访问位置”, 我们也需要跳过.
*/
/************************************************************************/
void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {

	//lzh: 此时传入的 saved_key_ 是 Internalkey, 仅此一处. 
	//lzh: 但调用完 FindNextUserEntry 函数后, saved_key 被清空
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
	  //lzh: 当且仅当 iter_key() 已经是一个 deletion 时跳过它后面相同 user_key.
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();

  ParsedInternalKey ikey;
  ParseKey(&ikey);

  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(
    const std::string* dbname,
    Env* env,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    const SequenceNumber& sequence) {
  return new DBIter(dbname, env, user_key_comparator, internal_iter, sequence);
}

}
