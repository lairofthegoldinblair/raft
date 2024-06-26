#ifndef __LOG_HH__
#define __LOG_HH__

#include <deque>

#include "slice.hh"
#include "util/call_on_delete.hh"

namespace raft {

  // TODO: FIX
  class log_header_write
  {
  public:
    virtual ~log_header_write() {};
    virtual void async_write_log_header(uint64_t current_term, uint64_t voted_for) =0;
  };

  // Probably template out the Raft log header stuff (current_term_ and voted_for_).
  template<typename _LogEntry, typename _LogEntryTraits>
  class in_memory_log
  {
  public:
    typedef uint64_t index_type;
    typedef _LogEntry entry_type;
    typedef _LogEntryTraits traits_type;
  private:
    index_type start_index_;
    std::deque<std::pair<const entry_type *, raft::util::call_on_delete>> entries_;
    uint64_t current_term_;
    uint64_t voted_for_;
    log_header_write * writer_;
    
  public:
    // TODO: voted_for_ initialization should use raft::server::INVALID_PEER_ID.
    in_memory_log()
      :
      start_index_(0),
      current_term_(0),
      voted_for_(std::numeric_limits<uint64_t>::max()),
      writer_(nullptr)
    {
    }

    // TODO: Fix.  I don't like this way of doing things.
    void set_log_header_writer(log_header_write * writer)
    {
      writer_ = writer;
    }

    /**
     * Returns index range [begin,end) that was appended
     */
    std::pair<index_type, index_type> append(std::pair<const entry_type *, raft::util::call_on_delete> && e)
    {
      index_type start_added = index_end();
      entries_.push_back(std::move(e));
      return std::make_pair(start_added, start_added+1);
    }

    template<typename InputIterator>
    std::pair<index_type, index_type> append(InputIterator begin, InputIterator end, raft::util::call_on_delete && del)
    {
      index_type start_added = index_end();
      index_type last_added = start_added + std::distance(begin, end);
      for(; begin != end; ++begin) {
	entries_.push_back(std::make_pair(&*begin, raft::util::call_on_delete()));
      }
      if (start_added != last_added) {
	// If we added anything set the deleter to the last entry
	entries_.back().second = std::move(del);
      }
      return std::make_pair(start_added, last_added);
    }

    /**
     * Log header stuff.  
     */
    void update_header(uint64_t current_term, uint64_t voted_for)
    {
      current_term_ = current_term;
      voted_for_ = voted_for;
    }

    uint64_t current_term() const {
      return current_term_;
    }
    
    uint64_t voted_for() const {
      return voted_for_;
    }

    void sync_header()
    {
      if (writer_ != nullptr) {
	writer_->async_write_log_header(current_term_, voted_for_);
      }
    }

    /*
     * A FOLLOWER doesn't actually need access to full entries but does need
     * to examine terms in order to enforce the Log Matching Property.  We provide methods
     *  to access terms without assuming that a full entry is available.  This opens the door to
     * optimizations of the in-memory cache on a follower.
     */
    uint64_t term(index_type i) const
    {
      return traits_type::term(entries_.at(i - index_begin()).first);
    }

    uint64_t cluster_time(index_type i) const
    {
      return traits_type::cluster_time(entries_.at(i - index_begin()).first);
    }

    /**
     * Only call if !empty().  Returns term(index_end()-1)
     */
    uint64_t last_entry_term() const
    {
      return traits_type::term(entries_.back().first);
    }

    uint64_t current_time(index_type i) const
    {
      return traits_type::current_time(entries_.at(i - index_begin()).first);
    }

    /**
     * Only call if !empty().  Returns cluster_time(index_end()-1)
     */
    uint64_t last_entry_cluster_time() const
    {
      return traits_type::cluster_time(entries_.back().first);
    }

    /**
     * A LEADER needs sequential access to it log in order to append entries
     * to FOLLOWERs.  Each FOLLOWER is at a different point in the log and that point may
     * move forward or backward (a FOLLOWER may tell a LEADER that it is missing entries that
     * the LEADER assumes it has).
     */
    const entry_type & entry(index_type i) const
    {
      return *entries_.at(i - index_begin()).first;
    }

    /**
     * Index of first entry in log.  Only valid if !empty() or equivalently
     * if index_begin() != index_end().
     */
    index_type index_begin() const
    {
      return start_index_;
    }

    /**
     * Points one past the last index inserted.  Always valid.
     */
    index_type index_end() const
    {
      return start_index_ + entries_.size();
    }

    /** 
     * Is the log empty.  It will be immediately after a checkpoint.
     */
    bool empty() const
    {
      return entries_.size() == 0;
    }

    /**
     * Throw away the entries of the log in the range [index_begin(), idx)
     */
    void truncate_prefix(index_type idx)
    {
      index_type i = start_index_;
      while(i++ < idx && !entries_.empty()) {
	entries_.pop_front();
      }
      start_index_ = idx;
    }

    /**
     * Throw away the entries of the log in the range [idx, index_end())
     */
    void truncate_suffix(index_type idx)
    {
      index_type last = index_end();
      while (idx++ < last && !entries_.empty()) {
	// TODO: If we are not throwing away all of the entries covered by the delete then
	// it's a big problem.   If that is the case then we should save the deleter and
	// put it at the back of entries_.
	entries_.pop_back();
      }
    }
  };
}

#endif
