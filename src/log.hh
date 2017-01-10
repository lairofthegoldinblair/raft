#ifndef __LOG_HH__
#define __LOG_HH__

#include <deque>

namespace raft {

  template<typename _Description>
  struct log_entry
  {
  public:
    typedef _Description configuration_description_type;
    enum entry_type { COMMAND, CHECKPOINT, CONFIGURATION, NOOP };
    entry_type type;
    uint64_t term;
    // TODO: Investigate use of boost::variant, boost::any, etc to handle
    // union
    // Populated if type==COMMAND
    std::string data;
    // Populated if type==CONFIGURATION
    configuration_description_type configuration;
  };
  
  template<typename _Description>
  class in_memory_log
  {
  public:
    typedef uint64_t index_type;
    typedef _Description configuration_description_type;
    typedef log_entry<_Description> entry_type;
  private:
    index_type start_index_;
    std::deque<entry_type> entries_;
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
    std::pair<index_type, index_type> append(const std::vector<entry_type>& entries)
    {
      index_type start_added = last_index();
      index_type last_added = start_added + entries.size();
      for(auto & e : entries) {
	entries_.push_back(e);
      }
      return std::make_pair(start_added, last_added);
    }

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
      // TODO: Implement
    }

    const entry_type & entry(index_type i) const
    {
      return entries_.at(i - start_index());
    }

    const entry_type & last_entry() const
    {
      return entries_.back();
    }

    /**
     * Index of first entry in log.  Only valid if !empty() or equivalently
     * if start_index() != last_index().
     */
    index_type start_index() const
    {
      return start_index_;
    }

    /**
     * Points one past the last index inserted.  Always valid.
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
      index_type i = start_index_;
      while(i++ < idx && !entries_.empty()) {
	entries_.pop_front();
      }
      start_index_ = idx;
    }

    /**
     * Throw away the entries of the log in the range [idx, last_index())
     */
    void truncate_suffix(index_type idx)
    {
      index_type last = last_index();
      while (idx++ < last && !entries_.empty()) {
	entries_.pop_back();
      }
    }
  };
}

#endif
