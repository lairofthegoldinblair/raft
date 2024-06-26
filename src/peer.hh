#ifndef __RAFTPEER_HH__
#define __RAFTPEER_HH__

#include <chrono>
#include <vector>

#include "boost/logic/tribool.hpp"

#include "slice.hh"

namespace raft {
  // Raft only really cares about the fact that a checkpoint maintains the log index, term and configuration
  // at which the checkpoint is taken.  What client data and how that client data is communicated
  // to peers and disk is more or less opaque.  I want to structure things so that these latter
  // aspects are decoupled.
  template<typename checkpoint_data_store_type>
  class peer_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::block_type block_type;
    typedef typename checkpoint_data_store_type::header_type header_type;
    
    // One past last byte to written in checkpoint file.
    uint64_t checkpoint_next_byte_;
    // Header of the checkpoint
    const header_type & checkpoint_last_header_;
    // Checkpoint data we are sending to this peer
    checkpoint_data_ptr data_;
    // Last block sent.  We do not assume that the checkpoint bytes are contiguous in memory
    // so cannot use checkpoint_next_byte_ to know where the next chunk is in data_.
    block_type last_block_sent_;
    // The time the last block was sent
    std::chrono::steady_clock::time_point last_block_sent_time_;
    // Has the last block been acked?  TODO: Generalize to a window/credit system?
    bool awaiting_ack_;
    // Has the peer requested the next block from data_
    bool request_outstanding_;

    peer_checkpoint(const header_type & header, checkpoint_data_ptr data)
      :
      checkpoint_next_byte_(0),
      checkpoint_last_header_(header),
      data_(data),
      awaiting_ack_(false),
      request_outstanding_(false)
    {
    }
  };

  class peer_scheduler
  {
  private:
    uint64_t max_index_sent_=0;
    std::chrono::time_point<std::chrono::steady_clock> retransmit_timeout_;
    uint32_t delay_milliseconds_=1;

  public:
    void init(std::chrono::time_point<std::chrono::steady_clock> now)
    {
      max_index_sent_ = 0;
      retransmit_timeout_ = now;
      // Should we remember this from the last time we were leader?
      delay_milliseconds_ = 1;
    }
    
    bool can_send(std::chrono::time_point<std::chrono::steady_clock> now, uint64_t max_index)
    {
      if (max_index > max_index_sent_) {
        // No flow control yet, always allow new index to be sent
        max_index_sent_ = max_index;
        retransmit_timeout_ = now + std::chrono::milliseconds(delay_milliseconds_);
        return true;
      } else if (now > retransmit_timeout_) {
        // This implies a dropped packet, so increase delay
        delay_milliseconds_ *= 2;
        retransmit_timeout_ = now + std::chrono::milliseconds(delay_milliseconds_);
        return true;
      }
      return false;
    }

    void ack(std::chrono::time_point<std::chrono::steady_clock> now, uint64_t max_index)
    {
      if (max_index != max_index_sent_)
        return;

      // TOOD: How to account for retransmits (e.g. Karn's algorithm)?   We may be getting an ack for a previously
      // sent message.
      if (delay_milliseconds_ > 0) {
        delay_milliseconds_ -= std::min(delay_milliseconds_-1, 10U);
      } else {
        delay_milliseconds_ = 1;
      }
    }

    std::chrono::time_point<std::chrono::steady_clock> retransmit_timeout() const
    {
      return retransmit_timeout_;
    }
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  template<typename checkpoint_data_store_type, typename configuration_change_type>
  class peer
  {
  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
    // TODO: Templatize; do we actually want multiple addresses?  Does protocol really
    // need to know whether this is one or more addresses?
    std::string address;
    // Leader specific state about peers
    // Index of next log entry to send
    uint64_t next_index_;
    // One past the index of last log entry known to be replicated to peer
    uint64_t match_index_;
    // The last request_id this peer has acknowledged
    uint64_t last_request_id_;
    // Vote we got from this peer.  boost::logic::indeterminate means we haven't heard back yet.
    boost::logic::tribool vote_;
    // Used only when LEADER; when does this peer need another heartbeat?
    std::chrono::time_point<std::chrono::steady_clock> requires_heartbeat_;
    // Is the value of next_index_ a guess or has it been confirmed by communication with the peer
    bool is_next_index_reliable_;
    // TODO: Implement the exit protocol
    bool exiting_;
    // Checkpoint state for sending a checkpoint from a server to this peer
    std::shared_ptr<peer_checkpoint<checkpoint_data_store_type> > checkpoint_;
    // State for tracking whether a peer that is newly added to a configuration tracking
    // log appends
    std::shared_ptr<configuration_change_type> configuration_change_;
    // Used only when LEADER; this is for congestion control when sending append entries to a potentially slow peer.
    // tells us when we can next send an append entries.
    peer_scheduler scheduler_;
    
    void acknowledge_request_id(uint64_t request_id)
    {
      // TODO: Should we allow request id to go backwards?
      last_request_id_ = request_id;
    }

    void exit()
    {
      exiting_ = true;
    }
  };

  // Peer side of a checkpoint transfer
  template<typename checkpoint_data_store_type>
  class in_progress_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::header_type header_type;
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
  public:
    uint64_t end_;
    checkpoint_data_ptr file_;
    std::string leader_address_;
    
    uint64_t end() const {
      return end_;
    }

    template<typename _Callback>
    void write(std::chrono::time_point<std::chrono::steady_clock> clock_now, slice && data, _Callback && cb)
    {
      auto sz = slice::buffer_size(data);
      end_ += sz;
      file_->write(clock_now, slice::buffer_cast<const uint8_t *>(data), slice::buffer_size(data), std::move(cb));
    }

    in_progress_checkpoint(checkpoint_data_store_type & store,
			   const header_type * header,
                           std::string && leader_address,
			   raft::util::call_on_delete && deleter)
      :
      end_(0),
      file_(store.create(header, std::move(deleter))),
      leader_address_(std::move(leader_address))
    {
    }
  };
}

#endif
