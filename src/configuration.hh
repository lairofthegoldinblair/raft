#ifndef __RAFTCONFIGURATION_HH__
#define __RAFTCONFIGURATION_HH__

#include <chrono>
#include <set>
#include <string>
#include <vector>

#include "boost/assert.hpp"

namespace raft {

  struct server_description
  {
    uint64_t id;
    std::string address;
  };

  struct simple_configuration_description
  {
    typedef server_description server_type;
    std::vector<server_description> servers;
  };

  struct configuration_description
  {
    typedef simple_configuration_description simple_type;
    typedef server_description server_type;
    std::vector<server_description> from;
    std::vector<server_description> to;
  };

  // Track how far behind a newly added peer is.  The idea is that we don't want to transition the peer
  // from staging until it is pretty close to having all the state it needs (e.g. it could take some time
  // to load a checkpoint).  This is a fuzzy concept and is heurisitc.  The logic here is from logcabin and
  // says that we give the peer an interval of time in which to meet a goal of replicating up the position
  // of the log at the beginning of the interval.  If it gets there by the end of the interval then we say it
  // is in good shape.  If it doesn't then we try again.
  class peer_configuration_change
  {
  private:
    // TODO: Get into a configuration object
    static const int64_t ELECTION_TIMEOUT = 300;
    bool is_caught_up_;
    std::chrono::time_point<std::chrono::steady_clock> current_catch_up_iteration_start_;
    uint64_t current_catch_up_iteration_goal_;
    int64_t last_catch_up_iteration_millis_;
  public:
    // We start out with an unattainable goal but this will get fixed up in the next interval.
    peer_configuration_change(std::chrono::time_point<std::chrono::steady_clock> clock_now)
      :
      is_caught_up_(false),
      current_catch_up_iteration_start_(clock_now),
      current_catch_up_iteration_goal_(std::numeric_limits<uint64_t>::max()),
      last_catch_up_iteration_millis_(std::numeric_limits<uint64_t>::max())
    {
    }
    
    void on_append_response(std::chrono::time_point<std::chrono::steady_clock> clock_now,
			    uint64_t match_index,
			    uint64_t last_log_index)
    {
      if (!is_caught_up_ && current_catch_up_iteration_goal_ <= match_index) {
	auto duration = clock_now - current_catch_up_iteration_start_;
	int64_t millis = (int64_t) std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
	if (std::labs(millis - last_catch_up_iteration_millis_) < ELECTION_TIMEOUT) {
	  is_caught_up_ = true;
	} else {
	  last_catch_up_iteration_millis_ = millis;
	  current_catch_up_iteration_start_ = clock_now;
	  current_catch_up_iteration_goal_ = last_log_index;
	}
      }
    }

    bool is_caught_up() const
    {
      return is_caught_up_;
    }
  };

  template <typename _Peer>
  class simple_configuration
  {
  public:
    simple_configuration();
    ~simple_configuration();

    // Remote peers
    std::vector<std::shared_ptr<_Peer> > peers_;
  };

  template <typename _Peer, typename _Description>
  class configuration
  {
  public:
    typedef typename _Description::server_type server_description_type;
  private:
    // The cluster = all known peers which may not be in a configuration yet
    std::vector<std::shared_ptr<_Peer> > cluster_;

    // My cluster id/index
    std::size_t cluster_idx_;

    // Log entry in which this configuration was written to the log
    uint64_t configuration_id_;

    // EMPTY - First time a server is ever started for a cluster
    // STABLE - Configuration is currently functional and agreed on
    // STAGING - On a leader we have requested a new configuration, we are propagating
    // entries to any new peers and are waiting for them to catchup.
    // TRANSITIONAL - On a leader a new configuration has caught up
    enum State { EMPTY, STABLE, STAGING, TRANSITIONAL };
    State state_;

    // TODO: Are old_peers_ and new_peers_ necessariliy disjoint?  I suspect not.

    simple_configuration<_Peer> old_peers_;

    // If STAGING then these servers get log entries but do not participate in
    // elections (TODO: What does that mean? simply that they do not vote for leader?)
    // If TRANSITIONAL then we need a majority of these for a quorum (in leader election?)
    simple_configuration<_Peer> new_peers_;

    // TODO: Description = is this just for serializing to log and for initializing?
    _Description description_;

    // Sync up the server description address with the peer type.
    std::shared_ptr<_Peer> & get_or_create_peer(const server_description_type & s)
    {
      if (cluster_.size() <= s.id) {
	cluster_.resize(s.id+1);
      }
      if (!cluster_[s.id]) {
	cluster_[s.id].reset(new _Peer());
	cluster_[s.id]->peer_id = s.id;
	cluster_[s.id]->address = s.address;
	cluster_[s.id]->configuration_change.reset(new peer_configuration_change(std::chrono::steady_clock::now()));
      }
      return cluster_[s.id];
    }
  public:
    std::size_t num_known_peers() const
    {
      return cluster_.size();
    }

    std::size_t cluster_idx() const
    {
      return cluster_idx_;
    }

    void set_configuration(uint64_t configuration_id, const _Description & desc)
    {
      state_ = desc.to.servers.size() == 0 ? STABLE : TRANSITIONAL;
      configuration_id_ = configuration_id;
      description_ = desc;

      old_peers_.clear();
      new_peers_.clear();

      std::set<uint64_t> new_known_peers;
      for(auto & s : description_.from.servers) {
	old_peers_.peers_.push_back(get_or_create_peer(s));
	new_known_peers.insert(old_peers_.peers_.back()->peer_id);
      }
      for(auto & s : description_.to.servers) {
	new_peers_.peers_.push_back(get_or_create_peer(s));
	new_known_peers.insert(new_peers_.peers_.back()->peer_id);
      }

      for(auto p : cluster_) {
	if (0 == new_known_peers.count(p.peer_id) && !!cluster_[p.peer_id]) {
	  cluster_[p.peer_id]->exit();
	  // TODO: Who else is holding a reference to this object????
	  cluster_[p.peer_id].reset();
	}
      }
    }

    void set_staging_configuration(const typename _Description::simple_type & desc)
    {
      BOOST_ASSERT(state_ == STABLE);
      state_ = STAGING;
      for(auto & s : desc.servers) {
	new_peers_.peers_.push_back(get_or_create_peer(s));
      }
    }
  };
}

#endif
