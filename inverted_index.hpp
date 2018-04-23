
#ifndef _ii_hpp
#define _ii_hpp

#include <iostream>
#include <vector>
#include <atomic>
#include <thread>

namespace ii_impl
{
	// Interface for creating variable byte delta encoded postings list files
	template<typename Truncate, typename FeatureObjectLists>
	class file_abstractor
	{
	private:

		Truncate& _trunc;
		FeatureObjectLists& _features;
		uint64_t _feature_count;
		std::vector<uint64_t> _feature_len; // number of items in the postings list for each feature
		std::vector<uint64_t> _feature_offset; // offset of feature start from file start
		std::size_t _size; // length of file in counts of uint64_t
		uint64_t* _begin;

		// get number of bytes needed to encode the second parameter after the first in variable byte delta encoding
		uint64_t get_delta_length(uint64_t a, uint64_t b)
		{
			if (a >= b) throw std::logic_error("Bad order for delta encoding.");
			uint64_t d = b - a;
			for (uint64_t i = 1;; ++i)
			{
				if (d < 128) return i;
				d /= 128;
			}
		}

		// Count the lengths of the feature lists and calculate size for truncate
		void get_lengths()
		{
			_size = (2 * _feature_count) + 1; // header of the file - length and index of indexes
			for (uint64_t i = 0; i < _feature_count; ++i) // calculate length of each feature
			{
				_feature_offset[i] = _size;
				bool first = true; uint64_t prev; uint64_t f_size = 0; // size of feature in bytes
				for (auto&& o : _features[i])
				{
					if (first)
					{
						first = false;
						prev = o;
						f_size += sizeof(uint64_t);
						++(_feature_len[i]);
					}
					else
					{
						auto b = get_delta_length(prev, o);
						f_size += b;
						++(_feature_len[i]);
					}
				}
				// padding after each feature to multiple of 8 bytes for aligned reading of 64bit numbers
				if (f_size % sizeof(uint64_t) != 0)	_size += (f_size / sizeof(uint64_t)) + 1;
				else _size += f_size / sizeof(uint64_t);
			}
		}

		// write value d (calculated delta from caller) as varbyte value, MSB == 1 iff this is the last byte
		void write_delta(uint8_t** bpp, uint64_t d)
		{
			while (true)
			{
				if (d < 128) // last byte -> set MSB to 1 and end writing
				{
					**bpp = (uint8_t)(d + 128); ++(*bpp); // increments the pointer in the calling function
					return;
				}
				else
				{
					**bpp = (uint8_t)(d % 128); ++(*bpp);
					d /= 128;
				}
			}
		}

		void write_data()
		{
			// header
			uint64_t* p = _begin;
			*p = _feature_count; ++p;
			for (uint64_t i = 0; i < _feature_count; ++i)
			{
				*p = _feature_len[i]; ++p;
				*p = _feature_offset[i]; ++p;
			}

			// compressed data
			for (uint64_t i = 0; i < _feature_count; ++i)
			{
				p = _begin + _feature_offset[i]; // start of block for this feature
				uint8_t* bp; // unsigned byte pointer for writing var byte deltas
				bool first = true; uint64_t prev;
				for (auto&& o : _features[i])
				{
					if (first) // first item is always uncompressed (point of reference for delta)
					{
						first = false;
						prev = o;
						*p = o; ++p;
						bp = reinterpret_cast<uint8_t*>(p);
					}
					else
					{
						write_delta(&bp, o - prev);
						prev = o;
					}
				}
			}
		}

	public:

		// Creates the actual file from parameters given in constructor
		void make()
		{
			// The file is created in two passes
			// the first pass counts how large the whole file and individual features will be after compression
			// the second pass after setting the file size writes the actual data
			get_lengths();
			_begin = _trunc(_size);
			write_data();
		}

		file_abstractor(Truncate&& truncate, FeatureObjectLists&& features) : _trunc(truncate), _features(features)
		{
			_feature_count = features.size();
			_feature_len = std::vector<uint64_t>(_feature_count, 0);
			_feature_offset = std::vector<uint64_t>(_feature_count, 0);
		}
	};


	// for storing iterators to both the mmapped lists and the vector lists inside the same container for task management during the algorithm
	// represents anything that acts like an iterator over int64_t values
	class generic_postings_list_iterator
	{
	private:
		// there are some redundant variables depending on what this iterator actually points to, but it's just a few bytes per task
		// having it this way allows me to make the search algorithm much more clear because all details about where the lists actually are
		// are abstracted away by this class
		std::unique_ptr<std::vector<uint64_t>> _list; std::vector<uint64_t>::const_iterator _iter;
		const uint8_t* _segment; uint64_t _count; uint64_t _prev; bool first = true; uint64_t _val;
		bool _memory_storage;

		// Decodes a delta into a document number and stores it in _val
		void decode_delta()
		{
			_prev = _val; uint64_t delta = 0;
			for (uint64_t i = 1;; i *= 128)
			{
				delta += i * ((*_segment) % 128);
				if (*_segment >= 128)
				{
					++_segment; break; // this means it's the last byte
				}
				++_segment;
			}
			_val += delta;
		}

	public:
		bool good; // signifies that operator* will yield a good value, updated on moving the iterator

		// if this is called when good == false, results are undefined (probably not pleasant)
		uint64_t operator*()
		{
			if (!_memory_storage)
			{
				if (first)
				{
					_val = *(reinterpret_cast<const uint64_t*>(_segment));
					return _val;
				}
				return _val;
			}
			// else vector storage
			return *_iter;
		}

		generic_postings_list_iterator& operator++()
		{
			if (!_memory_storage)
			{
				if (_count <= 1)
				{
					good = false; return *this;
				}
				if (first) // skip to first compressed byte
				{
					_segment += sizeof(uint64_t);
				}
				--_count;
				first = false;
				decode_delta();
				return *this;
			}
			// else vector storage
			++_iter;
			if (_iter == _list->cend()) good = false;
			return *this;
		}

		generic_postings_list_iterator() {}; // for constructing the vector of appropriate length (no reallocations inside it at runtime)
		generic_postings_list_iterator(const uint64_t* segment, uint64_t feature) : _memory_storage(false) // constructor that makes this class iterate over my file format
		{
			uint64_t _offset; const uint64_t* beginning = segment;
			segment += 1 + (2 * feature); // move pointer to start of this feature's header
			_count = *segment; ++segment; _offset = *segment;
			_segment = reinterpret_cast<const uint8_t*>(beginning + _offset); // move pointer to start of this feature's data
			good = _count > 0;
		}
		generic_postings_list_iterator(std::unique_ptr<std::vector<uint64_t>> list) : _list(std::move(list)), _memory_storage(true) // makes this class iterate over vectors
		{
			good = _list->size() > 0;
			_iter = _list->cbegin();
		}
	};

	template<class Fs, class OutFn>
	class search_task
	{
	private:

		const uint64_t* _segment;
		Fs& _fs;
		OutFn& _callback;
		uint64_t _feature_count;
		uint64_t _batch_size; // Number of iterators that will be used during the search (node count of binary tree from the start posting lists)
		std::vector<std::unique_ptr<generic_postings_list_iterator>> _lists;

		// All workers adhere to this policy:
		//		When checking for available work, they only look at lists between _first and _last.
		//		They always try to move _first to "reserve" the two items for merging, merging happens 2 lists into one at a time.
		//		If they succeed, other threads will not attempt to do the same job, because _first was moved by the reserving thread.
		//		Work is available iff first < last.
		//		A similar approach is used for storing the computed intersection.
		//
		//		To avoid condition variables, workers use semi-active waiting if work is not available.
		//		To prevent many workers waiting while few are computing, a heuristic is used:
		//		if there are less remaining jobs than a thread's ID given to it by the algorithm, it kills itself
		//		this can sometimes lead to an extra few low index threads waiting while high index threads are computing
		//		but it doesn't happen often and the low index threads instantly yield.
		//
		//		An alternate approach I considered was using condition variables, but I decided against it because it would 
		//		slow down the common case (lists are similar length, threads have similar work loads) by syscalling too much.
		//		It would help in the case where some threads are under disproportionate load(very long vs very short lists)
		//		but I would consider that a degenerate case, instead I optimized for the common case.
		//		Even then, the slowdown of occasionally being planned and yielding right away isn't that much.
		std::atomic<uint64_t> _first;
		std::atomic<uint64_t> _last;

	public:

		void prepare()
		{
			if (_feature_count == 0) return; // prevents nasty things from happening due to integer underflow
			_last.store(_feature_count - 1); uint64_t i = 0;
			for (auto&& f : _fs)
			{
				auto l = std::make_unique<generic_postings_list_iterator>(_segment, f);
				_lists[i] = std::move(l);
				++i;
			}
		}

		// merge the list at index with the one at index + 1 and store the result at the end of _lists
		// guarantees thread safe storing of the result
		void merge(uint64_t index)
		{
			auto l = std::make_unique<std::vector<uint64_t>>();
			generic_postings_list_iterator* l1 = (_lists[index]).get();
			generic_postings_list_iterator* l2 = (_lists[index + 1]).get();

			uint64_t v1, v2;
			while (l1->good && l2->good) // when one list is at its end, the intersection is over
			{
				v1 = **l1; v2 = **l2;
				if (v1 == v2) // add the item to the result list
				{
					l->push_back(v1);
					++(*l1); ++(*l2);
				}
				else if (v1 < v2) ++(*l1);
				else ++(*l2);
			}

			auto result = std::make_unique<generic_postings_list_iterator>(std::move(l));
			uint64_t idx;
			while (true) // store the result
			{
				idx = _last.load();
				bool success = _last.compare_exchange_weak(idx, idx + 1);
				if (success) // we managed to reserve space for storing the result
				{
					_lists[idx + 1] = std::move(result);
					_lists[index].reset(); _lists[index + 1].reset(); // we no longer need the old postings lists -> free up their memory
					break;
				}
			}
		}

		// worker threads for list merging do this function
		// guarantees thread safe acquisition of lists to merge
		void worker_func(uint64_t id)
		{
			uint64_t f, l;
			while (true)
			{
				// check if there are two lists to merge
				f = _first.load(); l = _last.load();
				if (l > f)
				{
					// try to reserve the first 2 items
					bool success = _first.compare_exchange_weak(f, f + 2);
					if (success) // managed to reserve 2 items -> merge them
					{
						merge(f);
					}
					else std::this_thread::yield(); // there is work but another thread reserved it before us -> yield
				}
				else
				{
					// there was no work -> worker dies if its ID is higher or equal to the amount of remaining jobs
					if (_batch_size - 1 - f <= id) return;
					std::this_thread::yield(); // worker shouldn't die yet but has no work -> yield and wait for other threads to finish their work
				}
			}
		}

		void run()
		{
			if (_feature_count == 0) return; // no features -> no objects
			std::vector<std::thread> workers; auto worker_count = std::thread::hardware_concurrency();
			if (worker_count == 0) worker_count = 16; // backup number of threads if hardware_concurrency() fails
			for (uint64_t i = 0; i < worker_count; ++i)
			{
				workers.push_back(std::thread([this, i] { this->worker_func(i); }));
			}
			for (auto&& w : workers) w.join();
			// last list in the vector is the result postings list -> call the callback function on its elements
			for (generic_postings_list_iterator* i = (_lists[_batch_size - 1]).get(); i->good; ++(*i))
			{
				_callback(**i);
			}
		}

		search_task(const uint64_t* segment, Fs&& fs, OutFn&& callback) : _segment(segment), _fs(fs), _callback(callback), _first(0), _last(0)
		{
			_batch_size = 0; uint64_t n = 0;
			for (auto&& f : fs) ++n; // count features manually since it is not guaranteed that fs will have size() function
			_feature_count = n;
			while (true) // calculate number of spaces needed for storing the iterators
			{
				if (n <= 1) { ++_batch_size; break; } // this includes 0 for convenience, nothing will be done with 0 requirements
				_batch_size += 2 * (n / 2); // take as many pairs as we can
				n = (n / 2) + (n % 2); // if there was anything left over, add it to the next level
			}
			_lists = std::vector<std::unique_ptr<generic_postings_list_iterator>>(_batch_size);
		}
	};
}

namespace ii
{
	// File structure:
	//	1x size_t ... feature count
	//	2nx size_t ... length of feature, offset of feature start
	//	rest of data delta encoded UTF-8 style

	template<typename Truncate, typename FeatureObjectLists>
	void create(Truncate&& truncate, FeatureObjectLists&& features)
	{
		auto file = ii_impl::file_abstractor<Truncate, FeatureObjectLists>(std::forward<Truncate>(truncate), std::forward<FeatureObjectLists>(features));
		file.make();
	}

	// Search algorithm:
	//	There is a task queue containing iterators over posting lists
	//	Worker threads periodically try to pull two oldest lists from the queue and intersect them into a new one, adding it to the end of the queue
	//	The whole algorithm is lock-free (if the compiler generates atomic instructions for std::atomic<uint64_t>, it may decide to use mutexes instead)
	//	This is accomplished with the queue actually being a vector of unique_pointers of length roughly 2 * feature count
	//	Worker threads adhere to a policy that is described in detail in the search_task class, using atomic uints for synchronization
	template<class Fs, class OutFn>
	void search(const uint64_t* segment, size_t size, Fs&& fs, OutFn&& callback)
	{
		// size parameter is unused, the file format itself knows how long it is
		// Initialize the task with input data
		ii_impl::search_task<Fs, OutFn> task(segment, std::forward<Fs>(fs), std::forward<OutFn>(callback));
		task.prepare();

		// Start worker threads on task queue
		task.run();
	}

}; //namespace ii

#endif
