
#ifndef _ii_hpp
#define _ii_hpp


#include <iostream>
#include <vector>
#include <atomic>
#include <thread>

namespace ii_impl
{
template<typename Truncate, typename FeatureObjectLists>
class file_abstractor // simple version without delta compression
{
private:

	Truncate& _trunc;
	FeatureObjectLists& _features;
	uint64_t _feature_count;
	std::vector<uint64_t> _feature_len;
	std::vector<uint64_t> _feature_offset; // offset of feature start from file start
	std::size_t _size; // length of file in counts of uint64_t


	// Count the lengths of the feature lists and calculate size for truncate
	void get_lengths()
	{
		_size = (2 * _feature_count) + 1; // header of the file - length and index of indexes
		for (uint64_t i = 0; i < _feature_count; ++i) // calculate length of each feature
		{
			_feature_offset[i] = _size;
			for (auto&& o : _features[i])
			{
				_feature_len[i] += 1;
			}
			_size += _feature_len[i];
		}
	}

	void write_data()
	{
		// header
		uint64_t* p = _trunc.data();
		*p = _feature_count; ++p;
		for (uint64_t i = 0; i < _feature_count; ++i)
		{
			*p = _feature_len[i]; ++p;
			*p = _feature_offset[i]; ++p;
		}

		// compressed data
		for (uint64_t i = 0; i < _feature_count; ++i)
		{
			p = _trunc.data() + _feature_offset[i]; // start of block for this feature
			for (auto&& o : _features[i])
			{
				*p = o; ++p;
			}
		}
	}

public:

	// Creates the actual file from parameters given in constructor
	void make()
	{
		get_lengths();
		_trunc(_size);
		write_data();
	}

	file_abstractor(Truncate&& truncate, FeatureObjectLists&& features) : _trunc(truncate), _features(features)
	{
		_feature_count = features.size();
		_feature_len = std::vector<uint64_t>(_feature_count, 0);
		_feature_offset = std::vector<uint64_t>(_feature_count, 0);
	}
};

// for storing iterators to both the mmapped lists and the vector lists inside the same container
class generic_postings_list_iterator
{
private:
	std::unique_ptr<std::vector<uint64_t>> _list; std::vector<uint64_t>::const_iterator _iter;
	const uint64_t* _segment; uint64_t _count;
	bool _memory_storage;


public:
	bool good; // signifies that operator* will yield a good value, updated on moving the iterator

			   // if this is called when good == false, results are undefined
	uint64_t operator*() const
	{
		if (!_memory_storage)
		{
			return *_segment;
		}
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
			--_count;
			_segment += 1;
			return *this;
		}
		// else vector storage
		++_iter;
		if (_iter == _list->cend()) good = false;
		return *this;
	}

	generic_postings_list_iterator() {}; // for constructing the vector of appropriate length (no reallocations inside it at runtime)
	generic_postings_list_iterator(const uint64_t* segment, uint64_t feature) : _memory_storage(false)
	{
		const uint64_t* beginning = segment;
		segment += 1 + (2 * feature); // move pointer to start of this feature's header
		_count = *segment; ++segment;
		_segment = beginning + *segment; // move pointer to start of this feature's data
		good = _count > 0;
	}
	generic_postings_list_iterator(std::unique_ptr<std::vector<uint64_t>> list) : _list(std::move(list)), _memory_storage(true)
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
		std::vector<uint64_t> _thread_ids; // IDs of threads taking certain tasks
		std::atomic<uint64_t> _first;
		std::atomic<uint64_t> _last;

	public:

		void prepare()
		{
			if (_feature_count == 0) return;
			_last.store(_feature_count - 1); uint64_t i = 0;
			for (auto&& f : _fs)
			{
				auto l = std::make_unique<generic_postings_list_iterator>(_segment, f);
				_lists[i] = std::move(l);
				++i;
			}
		}

		// merge the list at index with the one at index + 1 and store the result at the end of _lists
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
					break;
				}
			}
		}

		void worker_func(uint64_t id)
		{
			uint64_t f, l;
			while (true)
			{
				// check if there are two lists to merge
				f = _first.load(); l = _last.load();
				if (l > f && l - f >= 1)
				{
					// try to reserve the first 2 items
					bool success = _first.compare_exchange_weak(f, f + 2);
					if (success) // managed to reserve 2 items -> merge them
					{
						_thread_ids[f] = id; _thread_ids[f + 1] = id;
						merge(f);
					}
					else std::this_thread::yield(); // there is work but another thread reserved it before us -> yield
				}
				else
				{
					// there was no work -> worker dies if its ID is higher or equal to the amount of remaining jobs
					if (_batch_size - 1 -f <= id) return;
					std::this_thread::yield(); // worker shouldn't die yet but has no work -> yield and wait for other threads to finish their work
				}
			}
		}

		void run()
		{
			if (_feature_count == 0) return;
			std::vector<std::thread> workers; auto worker_count = std::thread::hardware_concurrency();
			if (worker_count == 0) worker_count = 16; // backup number of threads
			for (uint64_t i = 0; i < worker_count; ++i)
			{
				workers.push_back(std::thread([this, i] { this->worker_func(i); }));
			}
			for (auto&& w : workers) w.join();
			// last item in the vector is the result postings list -> call the callback function on it
			for (generic_postings_list_iterator* i = (_lists[_batch_size - 1]).get(); i->good; i->operator++())
			{
				_callback(**i);
			}
		}

		search_task(const uint64_t* segment, Fs&& fs, OutFn&& callback) : _segment(segment), _fs(fs), _callback(callback), _first(0), _last(0)
		{
			_batch_size = 0; uint64_t n = 0;
			for (auto&& f : fs) ++n; // count features
			_feature_count = n;
			while (true) // calculate number of spaces needed for storing the iterators
			{
				if (n <= 1) { ++_batch_size; break; } // this includes 0 for convenience, nothing will be done with 0 requirements
				_batch_size += 2 * (n / 2); // take as many pairs as we can
				n = (n / 2) + (n % 2); // if there was anything left over, add it to the next level
			}
			_lists = std::vector<std::unique_ptr<generic_postings_list_iterator>>(_batch_size);
			_thread_ids = std::vector<uint64_t>(_batch_size, 69);
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

	template<class Fs, class OutFn>
	void search(const uint64_t* segment, size_t size, Fs&& fs, OutFn&& callback)
	{
		// Prepare iterators from segment into the task queue

		// Compute total amount of tasks needed to obtain result (for killing threads that are reduntant at the end)

		ii_impl::search_task<Fs, OutFn> task(segment, std::forward<Fs>(fs), std::forward<OutFn>(callback));
		task.prepare();

		// Start worker threads on task queue

		task.run();

		// Workers infinite loop, waiting for work, and trying to compare_exchange_weak to reserve when they think there is work

		// Worker thread dies if its index is bigger than number of tasks remaining and it has nothing to do
		// This doesn't always work perfectly (high index threads could be doing last bit of work while low index threads yield in a loop)

		// The task queue contains the result at the end
	}

}; //namespace ii

#endif
