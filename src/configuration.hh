#ifndef __RAFTCONFIGURATION_HH__
#define __RAFTCONFIGURATION_HH__

#include <chrono>
#include <set>
#include <string>
#include <vector>

#include "boost/assert.hpp"
#include "boost/iterator/filter_iterator.hpp"
#include "boost/iterator/transform_iterator.hpp"
#include "boost/log/trivial.hpp"

namespace raft {

  struct server_description
  {
    typedef std::string address_type;
    uint64_t id;
    std::string address;
  };

  struct simple_configuration_description
  {
    typedef server_description server_type;
    std::vector<server_description> servers;
  };

  // Data about a configuration that has to be stored in a checkpoint
  template<typename configuration_description_type>
  struct configuration_checkpoint
  {
    typedef typename configuration_description_type::address_type address_type;
    uint64_t index;
    configuration_description_type description;

    configuration_checkpoint()
      :
      index(std::numeric_limits<uint64_t>::max())
    {
    }

    configuration_checkpoint(uint64_t i, const configuration_description_type & desc)
      :
      index(i),
      description(desc)
    {
    }

    bool is_valid() const
    {
      return index != std::numeric_limits<uint64_t>::max();
    }
  };

  struct configuration_description
  {
    typedef simple_configuration_description simple_type;
    typedef server_description server_type;
    typedef configuration_checkpoint<configuration_description> checkpoint_type;
    typedef server_description::address_type address_type;
    simple_type from;
    simple_type to;
  };

  template<typename _Description>
  struct configuration_simple_type
  {
    typedef typename _Description::simple_type type;
  };

  template<typename _Description>
  struct configuration_server_type
  {
    typedef typename _Description::server_type type;
  };

  template<typename _Description>
  struct configuration_checkpoint_type
  {
    typedef typename _Description::checkpoint_type type;
  };

  // Track how far behind a newly added peer is.  The idea is that we don't want to transition the peer
  // from staging until it is pretty close to having all the state it needs (e.g. it could take some time
  // to load a checkpoint).  This is a fuzzy concept and is heurisitc.  The logic here is from logcabin and
  // says that we give the peer an interval of time in which to meet a goal of replicating up the position
  // of the log at the beginning of the interval.  If it gets there by the end of the interval then we say it
  // is in good shape.  If it doesn't then we try again.  At some point we may decide that the peer is hopelessly
  // slow and we reject the configuration change.
  // It seems to me that this protocol should be kept quite separate from the rest of Raft.  
  class peer_configuration_change
  {
  private:
    // TODO: Get into a configuration object
    static const int64_t ELECTION_TIMEOUT = 300;
    bool is_caught_up_;
    std::chrono::time_point<std::chrono::steady_clock> current_catch_up_iteration_start_;
    uint64_t current_catch_up_iteration_goal_;
    int64_t last_catch_up_iteration_millis_;
    std::size_t cluster_idx_;
    uint64_t peer_id_;
  public:
    // We start out with an unattainable goal but this will get fixed up in the next interval.
    peer_configuration_change(std::size_t cluster_idx,
			      uint64_t peer_id,
			      std::chrono::time_point<std::chrono::steady_clock> clock_now)
      :
      is_caught_up_(false),
      current_catch_up_iteration_start_(clock_now),
      current_catch_up_iteration_goal_(0),
      last_catch_up_iteration_millis_(std::numeric_limits<int64_t>::max()),
      cluster_idx_(cluster_idx),
      peer_id_(peer_id)
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
	  BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") peer " << peer_id_ << " is caught up at match_index " <<
	    match_index;
	} else {
	  BOOST_LOG_TRIVIAL(info) << "Server(" << cluster_idx_ << ") peer " << peer_id_ << " achieved previous goal " <<
	    current_catch_up_iteration_goal_ << " in " << millis << " milliseconds but has not yet caught up. New match index goal " <<
	    last_log_index;
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
    simple_configuration()
    {
    }
    
    ~simple_configuration()
    {
    }

    bool includes(std::size_t myself) const
    {
      for(auto & p : peers_) {
	if (p->peer_id == myself) {
	  return true;
	}
      }
      return false;
    }

    bool has_majority_vote(std::size_t myself) const
    {
      // Majority quorum logic
      std::size_t num_votes(0);
      for(auto & p : peers_) {
	if(p->peer_id == myself || p->vote_) {
	  num_votes += 1;
	}
      }
      return num_votes > (peers_.size()/2);
    }

    // Match index quorum
    uint64_t get_committed(std::size_t leader, uint64_t last_synced_index) const
    {
      // Figure out the minimum ack'd index over a quorum.
      std::vector<uint64_t> acked;
      for(auto & p : peers_) {
	if(p->peer_id != leader) {
	  // For peers we need an append response to get an ack
	  acked.push_back(p->match_index_);
	} else {
	  // For a leader, syncing to a log is "acking"
	  acked.push_back(last_synced_index);
	}
      }
      std::sort(acked.begin(), acked.end());
      return acked[(acked.size()-1)/2];
    }

    void clear()
    {
      return peers_.clear();
    }
    
    // Remote peers
    std::vector<std::shared_ptr<_Peer> > peers_;
  };

  class configuration_description_view
  {
  private:
    const configuration_description & description_;
  public:
    configuration_description_view(const configuration_description & desc)
      :
      description_(desc)
    {
    }

    std::size_t from_size() const
    {
      return description_.from.servers.size();
    }

    std::size_t from_id(std::size_t i) const
    {
      return description_.from.servers[i].id;
    }

    const std::string & from_address(std::size_t i) const
    {
      return description_.from.servers[i].address;
    }

    std::size_t to_size() const
    {
      return description_.to.servers.size();
    }

    std::size_t to_id(std::size_t i) const
    {
      return description_.to.servers[i].id;
    }

    const std::string & to_address(std::size_t i) const
    {
      return description_.to.servers[i].address;
    }
  };
  
  template<typename configuration_type>
  class transitional_configuration_view
  {
  private:
    const configuration_type & configuration_;
  public:
    transitional_configuration_view(const configuration_type & config)
      :
      configuration_(config)
    {
    }

    std::size_t from_size() const
    {
      return configuration_.description_.from.servers.size();
    }

    std::size_t from_id(std::size_t i) const
    {
      return configuration_.description_.from.servers[i].id;
    }

    const std::string & from_address(std::size_t i) const
    {
      return configuration_.description_.from.servers[i].address;
    }

    std::size_t to_size() const
    {
      return configuration_.new_peers_.peers_.size();
    }

    std::size_t to_id(std::size_t i) const
    {
      return configuration_.new_peers_.peers_[i]->peer_id;
    }

    const std::string & to_address(std::size_t i) const
    {
      return configuration_.new_peers_.peers_[i]->address;
    }
  };
  
  template<typename configuration_type>
  class stable_configuration_view
  {
  private:
    const configuration_type & configuration_;
  public:
    stable_configuration_view(const configuration_type & config)
      :
      configuration_(config)
    {
    }

    std::size_t from_size() const
    {
      return configuration_.description_.to.servers.size();
    }

    std::size_t from_id(std::size_t i) const
    {
      return configuration_.description_.to.servers[i].id;
    }

    const std::string & from_address(std::size_t i) const
    {
      return configuration_.description_.to.servers[i].address;
    }

    std::size_t to_size() const
    {
      return 0;
    }

    std::size_t to_id(std::size_t i) const
    {
      return 0;
    }

    const std::string & to_address(std::size_t i) const
    {
      static std::string empty;
      return empty;
    }
  };
  
  // Implements Ongaro's Joint Consensus configuration algorithm.  See Section 4.3 of Ongaro's thesis and
  // the "In search of understandable consensus algorithm" paper.
  template <typename _Peer, typename _Description>
  class configuration
  {
  private:
    struct is_not_null
    {
      bool operator() (std::shared_ptr<_Peer> p) const { return !!p; }
    };
    
    struct deref
    {
      _Peer & operator() (std::shared_ptr<_Peer> & elt) const
      {
	return *elt.get();
      }
    };
    
    typedef boost::filter_iterator<is_not_null, typename std::vector<std::shared_ptr<_Peer> >::iterator> peer_filter_iterator;

  public:
    typedef transitional_configuration_view<configuration<_Peer, _Description> > transitional_configuration_type;
    typedef stable_configuration_view<configuration<_Peer, _Description> > stable_configuration_type;
    typedef typename configuration_server_type<_Description>::type server_description_type;
    typedef typename configuration_simple_type<_Description>::type simple_configuration_description_type;
    typedef _Peer peer_type;
    typedef simple_configuration<peer_type> simple_description_type;
    typedef boost::transform_iterator<deref, peer_filter_iterator> peer_iterator;

    friend transitional_configuration_type;
    friend stable_configuration_type;
  private:
    // The cluster = all known peers which may not be in a configuration yet
    std::vector<std::shared_ptr<_Peer> > cluster_;

    // Number of non null peers in cluster_
    std::size_t num_known_peers_;

    // My cluster id/index
    std::size_t cluster_idx_;

    // Log entry in which this configuration was written to the log
    uint64_t configuration_id_;

    // EMPTY - First time a server is ever started for a cluster
    // STABLE - Configuration is currently functional and agreed on
    // STAGING - On a leader we have requested a new configuration, we are propagating
    // entries to any new peers and are waiting for them to catchup.
    // TRANSITIONAL - On a leader, a new configuration has caught up and been added to a
    // transitional configuration in which we require a majority from both the old set of
    // servers and a new set of servers.  Once transitional is committed (in both old and new set)
    // then the old configuration may be shut down.
    enum State { EMPTY, STABLE, STAGING, TRANSITIONAL };
    State state_;

    // These are the peers in the "current" configuration
    simple_configuration<_Peer> old_peers_;

    // If STAGING then these servers get log entries but do not participate in
    // quorums for voting or commitment decisions.
    // If TRANSITIONAL then we need a majority of these for a quorum (in leader election and committing)
    // as well as needing a majority of old_peers_.
    // It not STAGING or TRANSITIONAL then this should be empty.
    simple_configuration<_Peer> new_peers_;

    // Description = is for serializing to log/checkpoint and for initializing
    _Description description_;

    // Remember if a transitional configuration was created by staging in this instance (as opposed
    // to being received in a log).  This is synonymous with being the LEADER where the config change
    // was initiated.  The reason we have to remember this is that by the time the config is committed
    // we may have lost leadership.
    bool initiated_new_configuration_;

    // Sync up the server description address with the peer type.
    std::shared_ptr<_Peer> get_or_create_peer(const server_description_type & s, bool is_staging)
    {
      if (cluster_.size() <= s.id) {
	cluster_.resize(s.id+1);
      }
      if (!cluster_[s.id]) {
	cluster_[s.id].reset(new _Peer());
	cluster_[s.id]->peer_id = s.id;
	// Only staging servers need to be monitored for catchup.
	if (is_staging) {
	  cluster_[s.id]->configuration_change_.reset(new peer_configuration_change(my_cluster_id(), s.id, std::chrono::steady_clock::now()));
	}
	++num_known_peers_;
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") creating new peer with id " << s.id;
      }
      // NOTE: We always update the address even if peer is not created
      cluster_[s.id]->address = s.address;
      return cluster_[s.id];
    }
  public:
    configuration(uint64_t self)
      :
      num_known_peers_(0),
      cluster_idx_(self),
      configuration_id_(std::numeric_limits<uint64_t>::max()),
      state_(EMPTY),
      initiated_new_configuration_(false)
    {
    }
    
    std::size_t num_known_peers() const
    {
      return num_known_peers_;
    }

    peer_iterator begin_peers()
    {
      return boost::make_transform_iterator(boost::make_filter_iterator<is_not_null>(cluster_.begin(), cluster_.end()), deref());
    }

    peer_iterator end_peers()
    {
      return boost::make_transform_iterator(boost::make_filter_iterator<is_not_null>(cluster_.end(), cluster_.end()), deref());
    }

    std::size_t my_cluster_id() const
    {
      return cluster_idx_;
    }

    peer_type & self()
    {
      return *cluster_[cluster_idx_];
    }

    peer_type & peer_from_id(uint64_t peer_id)
    {
      return *cluster_[peer_id];
    }

    const peer_type & get_peer_from_id(uint64_t peer_id) const
    {
      return *cluster_.at(peer_id);
    }

    bool has_quorum() const
    {
      // Always check old peers for quorum
      bool ret = old_peers_.has_majority_vote(cluster_idx_);
      // If transitional must also check new peers
      if (ret && state_ == TRANSITIONAL) {
	ret = new_peers_.has_majority_vote(cluster_idx_);
      }
      return ret;
    }

    uint64_t get_committed(uint64_t last_synced_index) const {
      uint64_t ret = old_peers_.get_committed(cluster_idx_, last_synced_index);
      if (state_ == TRANSITIONAL) {
	uint64_t new_committed_index = new_peers_.get_committed(cluster_idx_, last_synced_index);
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") with transitional config has old commit index " << ret <<
	  " new commit index " << new_committed_index;
	ret = (std::min)(ret, new_committed_index);
      }
      return ret;
    }

    // TODO: LogCabin has support for using a quorum to track versions of clients/state machines
    // in order to do consistent upgrades.
    
    uint64_t configuration_id() const
    {
      return configuration_id_;
    }

    bool is_valid() const
    {
      return configuration_id_ != std::numeric_limits<uint64_t>::max();
    }

    void set_configuration(uint64_t configuration_id, const _Description & desc)
    {
      state_ = desc.to.servers.size() == 0 ? STABLE : TRANSITIONAL;
      configuration_id_ = configuration_id;
      description_ = desc;

      // The in-progress config change is done (either we've rolled back to previous STABLE
      // or committed the new STABLE).
      if (state_ == STABLE) {
	initiated_new_configuration_ = false;
      }

      old_peers_.clear();
      new_peers_.clear();

      std::set<uint64_t> new_known_peers;
      for(auto & s : description_.from.servers) {
	old_peers_.peers_.push_back(get_or_create_peer(s, false));
	new_known_peers.insert(old_peers_.peers_.back()->peer_id);
      }
      for(auto & s : description_.to.servers) {
	new_peers_.peers_.push_back(get_or_create_peer(s, false));
	new_known_peers.insert(new_peers_.peers_.back()->peer_id);
      }

      for(auto p : cluster_) {
	if (!!p && 0 == new_known_peers.count(p->peer_id)) {
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") deleting peer with id " << p->peer_id;
	  p->exit();
	  // TODO: Who else is holding a reference to this object????
	  p.reset();
	  --num_known_peers_;
	}
      }
    }

    void set_staging_configuration(const simple_configuration_description_type & desc)
    {
      BOOST_ASSERT(state_ == STABLE);
      state_ = STAGING;
      initiated_new_configuration_ = true;
      for(auto & s : desc.servers) {
	// This may update address of any existing peer
	new_peers_.peers_.push_back(get_or_create_peer(s, true));
      }
    }

    void reset_staging_servers()
    {
      if (state_ == STAGING) {
	// Cancel the STAGING and also restore any addresses that may have been
	// updated by the staging...
	set_configuration(configuration_id_, description_);
	BOOST_ASSERT(initiated_new_configuration_ == false);
      }
    }

    bool staging_servers_caught_up() const
    {
      if (state_ != STAGING) {
	return true;
      }
      
      for(auto & p : new_peers_.peers_) {
	if (!!p->configuration_change_ && !p->configuration_change_->is_caught_up()) {
	  return false;
	}
      }
      return true;
    }

    bool staging_servers_making_progress(simple_configuration_description_type & desc) const
    {
      if (state_ != STAGING) {
	return true;
      }

      // TODO: Implement
      return true;
    }

    void get_transitional_configuration(_Description & desc)
    {
      desc.from = description_.from;
      for(auto & p : new_peers_.peers_) {
	desc.to.servers.push_back(server_description_type());
	desc.to.servers.back().id = p->peer_id;
	desc.to.servers.back().address = p->address;
      }
    }
    
    transitional_configuration_type get_transitional_configuration() const
    {
      return transitional_configuration_type(*this);
    }
    
    void get_stable_configuration(_Description & desc)
    {
      desc.from = description_.to;
    }
    
    stable_configuration_type get_stable_configuration() const
    {
      return stable_configuration_type(*this);
    }
    
    void reset()
    {
      state_ = EMPTY;
      initiated_new_configuration_ = false;
      configuration_id_ = std::numeric_limits<uint64_t>::max();
      description_ = _Description();
      old_peers_.clear();
      new_peers_.clear();
      for(auto p : cluster_) {
	if (!!p && cluster_idx_ != p->peer_id) {
	  p->exit();
	  // TODO: Who else is holding a reference to this object????
	  p.reset();
	  --num_known_peers_;
	}
      }
    }

    bool includes_self() const {
      bool ret = old_peers_.includes(cluster_idx_);
      if (!ret && state_ == TRANSITIONAL) {
	ret = new_peers_.includes(cluster_idx_);
      }
      return ret;
    }

    bool is_transitional() const {
      return state_ == TRANSITIONAL;
    }

    bool is_staging() const {
      return state_ == STAGING;
    }

    bool is_stable() const {
      return state_ == STABLE;
    }

    bool is_transitional_initiator() const {
      return is_transitional() && initiated_new_configuration_;
    }

    const _Description description() const {
      return description_;
    }
  };

  // Not exactly sure what this is about yet...
  // On the one hand this class is a view (in the database sense) of the configuration
  // entries in the log.
  // In particular it knows what configuration was active at each
  // point of the log (needed when taking a checkpoint at a point in the log that
  // was using an out of date configuration).
  // Also it keeps a configuration object sync'd up with the latest
  // logged description.
  // This might be a good candidate for a mixin with a log class or maybe not...
  template <typename _Peer, typename _Description>
  class configuration_manager
  {
  public:
    typedef _Description description_type;
    typedef typename _Peer::template apply<peer_configuration_change>::type peer_type;
    typedef configuration<peer_type, _Description> configuration_type;
    typedef typename description_type::checkpoint_type checkpoint_type;
  private:

    // The current configuration of the cluster (could be transitioning).
    configuration_type configuration_;

    // The union of configuration descriptions in the current log plus
    // the configuration in the most recent checkpoint.

    // log index => description.  Included checkpoint description as well.
    std::map<uint64_t, description_type> logged_descriptions_;

    // Valid if and only if checkpoint_description_.first != std::numeric_limits<uint64_t>::max()
    // Not sure I need to track this separately from logged descriptions as I am storing the checkpointed
    // description in server_checkpoint::last_checkpoint_configuration_ (or I can get rid of that and use this).
    checkpoint_type checkpoint_description_;

    void on_update()
    {
      if (checkpoint_description_.is_valid()) {
	logged_descriptions_.insert(std::make_pair(checkpoint_description_.index, checkpoint_description_.description));
      }

      // Make sure configuration_ always reflects the last logged config
      if (logged_descriptions_.empty()) {
	configuration_.reset();
      } else {
	auto it = logged_descriptions_.rbegin();
	if (configuration_.configuration_id() != it->first) {
	  configuration_.set_configuration(it->first, it->second);
	}
      }
    }
  
    description_type get_configuration_description_at(uint64_t log_index) const
    {
      auto it = logged_descriptions_.upper_bound(log_index);
      if (it == logged_descriptions_.begin()) {
	// Nothing is less that or equal to log_index
	return description_type();
      } else if (it == logged_descriptions_.end()) {
	// Everything is less than or equal to log_index
	return logged_descriptions_.rbegin()->second;
      } else {
	return (--it)->second;
      }
    }

    uint64_t get_configuration_index_at(uint64_t log_index) const
    {
      auto it = logged_descriptions_.upper_bound(log_index);
      if (it == logged_descriptions_.begin()) {
	// Nothing is less that or equal to log_index
	return 0;
      } else if (it == logged_descriptions_.end()) {
	// Everything is less than or equal to log_index
	return logged_descriptions_.rbegin()->first;
      } else {
	return (--it)->first;
      }
    }

  public:
    configuration_manager(uint64_t self)
      :
      configuration_(self)
    {
    }

    void add_logged_description(uint64_t log_index, const description_type & description)
    {
      logged_descriptions_[log_index] = description;
      on_update();
    }

    void add_transitional_description(uint64_t log_index)
    {
      description_type description;
      configuration_.get_transitional_configuration(description);
      add_logged_description(log_index, description);
    }

    void add_stable_description(uint64_t log_index)
    {
      description_type description;
      configuration_.get_stable_configuration(description);
      add_logged_description(log_index, description);
    }

    void set_checkpoint(const checkpoint_type & ckpt)
    {
      // TODO: Error when invalid checkpoint data
      if (ckpt.is_valid()) {
	checkpoint_description_ = ckpt;
	on_update();
      }
    }

    const checkpoint_type & get_checkpoint() const
    {
      return checkpoint_description_;
    }

    // Remove entries with index < idx
    void truncate_prefix(uint64_t idx)
    {
      logged_descriptions_.erase(logged_descriptions_.begin(), logged_descriptions_.lower_bound(idx));
      on_update();
    }
  
    // Remove entries with index >= idx
    void truncate_suffix(uint64_t idx)
    {
      logged_descriptions_.erase(logged_descriptions_.lower_bound(idx), logged_descriptions_.end());
      on_update();
    }

    // Get configuration in effect at log_index.  
    bool has_configuration_at(uint64_t log_index) const
    {
      // This points to the first element strictly greater than log_index.
      // What we want is the last element that is less than or equal to
      // log_index
      auto it = logged_descriptions_.upper_bound(log_index);
      if (it == logged_descriptions_.begin()) {
	// Nothing is less that or equal to log_index
	return false;
      } else {
	return true;
      }
    }
      
    void get_checkpoint_state(uint64_t log_index, checkpoint_type & ck) const
    {
      if (has_configuration_at(log_index)) {
	ck.index = get_configuration_index_at(log_index);
	ck.description = get_configuration_description_at(log_index);
      }
    }

    const configuration_type & configuration() const
    {
      return configuration_;
    }

    configuration_type & configuration()
    {
      return configuration_;
    }
  };
}

#endif
