#ifndef __LOG_HH__
#define __LOG_HH__

#include <deque>

namespace raft {
  struct log_entry
  {
  public:
    enum entry_type { COMMAND, CHECKPOINT, CONFIGURATION };
    entry_type type;
    uint64_t term;
    std::string data;
  };
  
  class in_memory_log
  {
  public:
    typedef uint64_t index_type;
  private:
    index_type start_index_;
    std::deque<log_entry> entries_;
    uint64_t current_term_;
    uint64_t voted_for_;
    
  public:
    in_memory_log()
      :
      start_index_(0),
      current_term_(0),
      voted_for_(0)
    {
    }

    /**
     * Returns index range [begin,end) that was appended
     */
    std::pair<index_type, index_type> append(const std::vector<log_entry>& entries)
    {
      index_type start_added = last_index();
      index_type last_added = start_added + entries.size();
      for(auto & e : entries) {
	entries_.push_back(e);
      }
      return std::make_pair(start_added, last_added);
    }

    void append(uint64_t current_term, uint64_t voted_for)
    {
      current_term_ = current_term;
      voted_for_ = voted_for;
    }

    const log_entry & entry(index_type i) const
    {
      return entries_.at(i - start_index());
    }

    /**
     * Index of first entry in log.  Only valid if !empty()
     */
    index_type start_index() const
    {
      return start_index_;
    }

    /**
     * Points one past the last index inserted.
     */
    index_type last_index() const
    {
      return start_index_ + entries_.size();
    }

    bool empty() const
    {
      return entries_.size() == 0;
    }

    /**
     * Throw away the entries of the log in the range [start_index(), idx)
     */
    void truncate_prefix(index_type idx)
    {
      // TODO:
    }
    /**
     * Throw away the entries of the log in the range (idx, last_index()]
     */
    void truncate_suffix(index_type idx)
    {
      // TODO:
    }
  };
}

#endif
