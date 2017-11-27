// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

/************************************************************************/
/* 
	skipList 的写需要在外部加锁，而多线程读不需要加锁
*/
/************************************************************************/

/************************************************************************/
/* 
	skiplist 有两种实现方式
	第一种：由相对独立的多层单链表构成，每层单链表的节点除了有next指针指向
			自己的后续节点外，还有一个down指针用来指向自己的下一层链表的对
			应节点，并以此实现各层链表之间的联系。这种结构下节点数目要多于
			实际的 key 的数目。可能会有很大的空间浪费。
			class node
			{
				Key _key;
				void *value;
				node *left;		//同一个 level 上的 list 的下一个元素
				node *down;		//同一个元素的下一层节点
			}

	第二种：总节点数是元素的个数，每个节点有一个指针数组 next[]，next[i] 表示
	第 i 层上的下一个元素的指针。
			class node
			{
				Key _key;
				void *value;
				vector<node*> nextPoints;	//此节点的 nexts
			}

*/
/************************************************************************/

namespace leveldb {

	class Arena;

	template<typename Key, class Comparator>
	class SkipList {
	private:
		struct Node;

	public:
		// Create a new SkipList object that will use "cmp" for comparing keys,
		// and will allocate memory using "*arena".  Objects allocated in the arena
		// must remain allocated for the lifetime of the skiplist object.
		explicit SkipList(Comparator cmp, Arena* arena);

		// Insert key into the list.
		// REQUIRES: nothing that compares equal to key is currently in the list.
		void Insert(const Key& key);

		// Returns true iff an entry that compares equal to key is in the list.
		bool Contains(const Key& key) const;

		// Iteration over the contents of a skip list
		class Iterator {
		public:
			// Initialize an iterator over the specified list.
			// The returned iterator is not valid.
			explicit Iterator(const SkipList* list);

			// Returns true iff the iterator is positioned at a valid node.
			bool Valid() const;

			// Returns the key at the current position.
			// REQUIRES: Valid()
			const Key& key() const;

			// Advances to the next position.
			// REQUIRES: Valid()
			void Next();

			// Advances to the previous position.
			// REQUIRES: Valid()
			void Prev();

			// Advance to the first entry with a key >= target
			void Seek(const Key& target);

			// Position at the first entry in list.
			// Final state of iterator is Valid() iff list is not empty.
			void SeekToFirst();

			// Position at the last entry in list.
			// Final state of iterator is Valid() iff list is not empty.
			void SeekToLast();

		private:
			const SkipList* list_;
			Node* node_;
			// Intentionally copyable
		};

	private:
		enum { kMaxHeight = 12 };

		// Immutable after construction
		Comparator const compare_;
		Arena* const arena_;    // Arena used for allocations of nodes

		Node* const head_;

		// Modified only by Insert().  Read racily by readers, but stale
		// values are ok.
		port::AtomicPointer max_height_;   // Height of the entire list

		inline int GetMaxHeight() const {
			return reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load());
		}

		// Read/written only by Insert().
		Random rnd_;

		Node* NewNode(const Key& key, int height);
		int RandomHeight();
		bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

		// Return true if key is greater than the data stored in "n"
		bool KeyIsAfterNode(const Key& key, Node* n) const;

		// Return the earliest node that comes at or after key.
		// Return NULL if there is no such node.
		//
		// If prev is non-NULL, fills prev[level] with pointer to previous
		// node at "level" for every level in [0..max_height_-1].
		Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

		// Return the latest node with a key < key.
		// Return head_ if there is no such node.
		Node* FindLessThan(const Key& key) const;

		// Return the last node in the list.
		// Return head_ if list is empty.
		Node* FindLast() const;

		// No copying allowed
		SkipList(const SkipList&);
		void operator=(const SkipList&);
	};

	// Implementation details follow
	template<typename Key, class Comparator>
	struct SkipList<Key,Comparator>::Node 
	{
		explicit Node(const Key& k) : key(k) { }

		Key const key;

		// Accessors/mutators for links.  Wrapped in methods so we can
		// add the appropriate barriers as necessary.
		//lzh: acquire: 在以 acquire 方式读取共享变量之后的任意读/写发生在其之后. 保证后续使用的 Node 即是上面最新获得的
		Node* Next(int n) {
			assert(n >= 0);
			// Use an 'acquire load' so that we observe a fully initialized
			// version of the returned Node.
			return reinterpret_cast<Node*>(next_[n].Acquire_Load());
		}

		//lzh: releae: 在以 release 方式写共享变量之前的任意读/写发生在其之前. 保证前面写入到 Node 指针指向的对象各个属性中的值全部写入完毕.
		void SetNext(int n, Node* x) {
			assert(n >= 0);
			// Use a 'release store' so that anybody who reads through this
			// pointer observes a fully initialized version of the inserted node.
			next_[n].Release_Store(x);
		}

		// No-barrier variants that can be safely used in a few locations.
		Node* NoBarrier_Next(int n) {
			assert(n >= 0);
			return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
		}
		void NoBarrier_SetNext(int n, Node* x) {
			assert(n >= 0);
			next_[n].NoBarrier_Store(x);
		}

	private:
		// Array of length equal to the node height.  next_[0] is lowest level link.
		port::AtomicPointer next_[1];	//该数组作为结构体的最后一个成员，其长度可变。这是一个编程技法
										//每个节点都有一个指针数组，用于指示各个 level 上的“next”
										//这是一个长度为 1 的数组，已经占有一个空间，意味着 Node 指针数组至少有一个元素
	};

	template<typename Key, class Comparator>
	typename SkipList<Key,Comparator>::Node*
		SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
			char* mem = arena_->AllocateAligned(
				sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));	//之所以这里只分配 height - 1 个指针空间，因为
																			//Node 的定义最后已经占了一个
			return new (mem) Node(key);
	}

	template<typename Key, class Comparator>
	inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
		list_ = list;
		node_ = NULL;
	}

	template<typename Key, class Comparator>
	inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
		return node_ != NULL;
	}

	template<typename Key, class Comparator>
	inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
		assert(Valid());
		return node_->key;
	}

	template<typename Key, class Comparator>
	inline void SkipList<Key,Comparator>::Iterator::Next() {
		assert(Valid());
		node_ = node_->Next(0);
	}

	template<typename Key, class Comparator>
	inline void SkipList<Key,Comparator>::Iterator::Prev() {
		// Instead of using explicit "prev" links, we just search for the
		// last node that falls before key.
		assert(Valid());
		node_ = list_->FindLessThan(node_->key);
		if (node_ == list_->head_) {
			node_ = NULL;
		}
	}

	template<typename Key, class Comparator>
	inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
		node_ = list_->FindGreaterOrEqual(target, NULL);
	}

	template<typename Key, class Comparator>
	inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
		node_ = list_->head_->Next(0);
	}

	template<typename Key, class Comparator>
	inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
		node_ = list_->FindLast();
		if (node_ == list_->head_) {
			node_ = NULL;
		}
	}

	template<typename Key, class Comparator>
	int SkipList<Key,Comparator>::RandomHeight() {
		// Increase height with probability 1 in kBranching
		static const unsigned int kBranching = 4;
		int height = 1;
		while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
			height++;
		}
		assert(height > 0);
		assert(height <= kMaxHeight);
		return height;
	}

	template<typename Key, class Comparator>
	bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
		// NULL n is considered infinite
		return (n != NULL) && (compare_(n->key, key) < 0);
	}

	/************************************************************************/
	/* 
		找到首个不小于 key 的节点并返回
		prev : 沿途的查找路径上的每一层的最后一个节点

		节点		1	2	3	4	5	6	7				
					________.								第 4 层
							|								   3
							|___.							   2
								|                              1
								|_______+___                   0
		
		上图是寻找首个不小于 key 的节点的寻找路径：
		node1.next[4] -> node2.next[4] -> node3.next[4] ->
		node3.next[3] -> 
		node3.next[2] -> node4.next[2] -> 
		node4.next[1] -> 
		node4.next[0] -> node5.next[0] -> node6.next[0] -> [目标节点]
		则 prev 的值：
		prev[0] = node6;
		prev[1] = node4;
		prev[2] = node4;
		prev[3] = node3;
		prev[4] = node3;
		这个数组可以看成是一阻墙立在了'+'处，它里面的 next 数组正是装载了它阻断的从'左边射向右边的指针'
				
 
	*/
	/************************************************************************/
	template<typename Key, class Comparator>
	typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
		const {
			Node* x = head_;
			int level = GetMaxHeight() - 1;//_header一定有 maxHeight 层
			while (true) {
				Node* next = x->Next(level);		
				if (KeyIsAfterNode(key, next)) 
				{
					// Keep searching in this list
					x = next;			//如果这上节点比 key 小，则继续找同一层上的后面一个节点。
				} 
				else 
				{
										//否则存下这个节点，并转向下一层继续寻找
					if (prev != NULL) prev[level] = x;
					if (level == 0) 
					{
						return next;
					} 
					else 
					{
						// Switch to next list
						level--;
					}
				}
			}
	}

	/************************************************************************/
	/* 
		lzh: 从各个节点的最高层往低层寻找：找到最后一个小于 key 的节点
	*/
	/************************************************************************/
	template<typename Key, class Comparator>
	typename SkipList<Key,Comparator>::Node*
		SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
			Node* x = head_;
			int level = GetMaxHeight() - 1;
			while (true) 
			{
				assert(x == head_ || compare_(x->key, key) < 0);
				Node* next = x->Next(level);
				if (next == NULL || compare_(next->key, key) >= 0) 
				{
					if (level == 0) 
					{
						return x;
					} 
					else 
					{
						// Switch to next list
						level--;
					}
				} 
				else 
				{
					x = next;
				}
			}
	}

	/************************************************************************/
	/* 
		寻找最后一个元素：可以确定level[0]上的最后一个元素必定是整个 skiplist 的最后一个元素，其它的元素不一定是
	*/
	/************************************************************************/
	template<typename Key, class Comparator>
	typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
		const 
	{
			Node* x = head_;
			int level = GetMaxHeight() - 1;
			while (true) 
			{
				Node* next = x->Next(level);
				if (next == NULL) 
				{
					if (level == 0) //最后一个元素必定在level=0的层上
					{
						return x;
					}
					else 
					{
						// Switch to next list
						level--;
					}
				} else 
				{
					x = next;
				}
			}
	}

	template<typename Key, class Comparator>
	SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
		: compare_(cmp),
		arena_(arena),
		head_(NewNode(0 /* any key will do */, kMaxHeight)),
		max_height_(reinterpret_cast<void*>(1)),
		rnd_(0xdeadbeef) {
			for (int i = 0; i < kMaxHeight; i++) {
				head_->SetNext(i, NULL);
			}
	}


	/************************************************************************/
	/* 
	levelDB 写入 KV 对时，会先获取日志锁，所以，任意时刻从全局看， levelDB 只会有一个线程写。
	所以决定了底层的 skiplist 的 Insert 不会存在数据竞争。
	
	虽然同一时刻只有一个线程写入 skiplist(调用 Insert) ，
	但那个日志锁结合了用户态自旋锁和内核态锁(Windows 下的实现其实就是 Event，linux 下会使用 pthread_cond_)，
	所以高并发下的写性能不致于太低，
	*/
	/************************************************************************/
	template<typename Key, class Comparator>
	void SkipList<Key,Comparator>::Insert(const Key& key) {
		// TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
		// here since Insert() is externally synchronized.
		Node* prev[kMaxHeight];
		Node* x = FindGreaterOrEqual(key, prev);

		// Our data structure does not allow duplicate insertion
		assert(x == NULL || !Equal(key, x->key));

		int height = RandomHeight();
		if (height > GetMaxHeight()) {
			for (int i = GetMaxHeight(); i < height; i++) {
				prev[i] = head_;
			}
			//fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

			// It is ok to mutate max_height_ without any synchronization
			// with concurrent readers.  A concurrent reader that observes
			// the new value of max_height_ will see either the old value of
			// new level pointers from head_ (NULL), or a new value set in
			// the loop below.  In the former case the reader will
			// immediately drop to the next level since NULL sorts after all
			// keys.  In the latter case the reader will use the new node.
			// lzh: 在并发下，无需要使用同步的手段去改变 max_height_ 的值
			// 如果一个读线程看到了 max_height_ 的新值，则它通过 head_ 指针数组：
			// 要么看到各个新增加层指针的旧值(NULL, 此时还没有赋值)，要么看到在后续循环中设置的新值
			// 前一种情况下，读线程立即会下落到下一个层，因为 head_[old_max_height_] ~ head_[new_max_height_] 值均为 0，
			// 相当于 head_ 直接连到了 end_，这几层会被直接跳过。
			// 后一种情况下，读线程会读取到新加入的结点。
			// 这只会有性能的略微损耗，而不会有逻辑的错误
			max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
		}

		x = NewNode(key, height);
		for (int i = 0; i < height; i++) {
			// NoBarrier_SetNext() suffices since we will add a barrier when
			// we publish a pointer to "x" in prev[i].
			x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
			prev[i]->SetNext(i, x);	//release store
		}
	}

	template<typename Key, class Comparator>
	bool SkipList<Key,Comparator>::Contains(const Key& key) const 
	{
		Node* x = FindGreaterOrEqual(key, NULL);
		if (x != NULL && Equal(key, x->key)) 
		{
			return true;
		} 
		else 
		{
			return false;
		}
	}

}
