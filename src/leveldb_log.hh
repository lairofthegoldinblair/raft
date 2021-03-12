#ifndef _RAFT_LEVELDB_LOG_HH_
#define _RAFT_LEVELDB_LOG_HH_

#include "boost/crc.hpp"
#include "boost/dynamic_bitset.hpp"
#include "boost/endian/arithmetic.hpp"

#include "slice.hh"

namespace raft {
  namespace leveldb {

    
    enum record_type { ZERO, FULL, FIRST, MIDDLE, LAST };
    static const int MAX_RECORD_TYPE = LAST;
    static const std::size_t BLOCK_SIZE = 32768;
    // checksum + length + type
    static const std::size_t HEADER_SIZE = 4 + 2 + 1;

    static const uint32_t MASK_DELTA = 0xa282ead8ul;    
    uint32_t mask_checksum(uint32_t crc)
    {
      return ((crc >> 15) | (crc << 17)) + MASK_DELTA;
    }

    uint32_t unmask_checksum(uint32_t crc)
    {
      uint32_t tmp = crc - MASK_DELTA;
      return ((tmp >> 17) | (tmp << 15));
    }

    struct fragment_header
    {
      boost::endian::little_uint32_t crc;
      boost::endian::little_uint16_t length;
      char type;
    };


    enum class result_code { ZERO, FULL, FIRST, MIDDLE, LAST, BAD, eEOF };

    template<typename Scheduler>
    class fragment_reader_operator
    {
    public:
      typedef Scheduler scheduler_type;
      typedef raft::slice input_type;
      typedef std::pair<raft::slice,result_code> output_type;
    private:
      enum state { BEGIN, READ, WRITE, WRITE_EOF };
      scheduler_type & scheduler_;
      state state_;
      raft::slice slice_;
      std::pair<raft::slice, result_code> output_;
      bool eof_;
      bool syncing_;

      const fragment_header * header() const
      {
	return raft::slice::buffer_cast<const fragment_header *>(slice_);
      }

      result_code header_type() const
      {
	switch(header()->type) {
	case ZERO:
	  return result_code::ZERO;
	case FULL:
	  return result_code::FULL;
	case FIRST:
	  return result_code::FIRST;
	case MIDDLE:
	  return result_code::MIDDLE;
	case LAST:
	  return result_code::LAST;
	default:
	  return result_code::BAD;
	}
      }

      const char * header_string() const
      {
	switch(header()->type) {
	case ZERO:
	  return "ZERO";
	case FULL:
	  return "FULL";
	case MIDDLE:
	  return "MIDDLE";
	case LAST:
	  return "LAST";
	default:
	  return "BAD";
	}
      }

      std::pair<raft::slice, result_code> next_fragment()
      {
	auto buffer_size = raft::slice::buffer_size(slice_);
	BOOST_ASSERT(buffer_size >= HEADER_SIZE);
	if (HEADER_SIZE + header()->length > buffer_size) {
	  auto expected = (HEADER_SIZE + header()->length);
	  const char * header_type_str = header_string();
	  slice_.clear();
	  if (!eof_) {
	    // Don't give up just drop this block
	    // Output something indicating that a block was in fact dropped.
	    BOOST_LOG_TRIVIAL(debug) << "fragment_reader_operator::next_fragment incomplete non EOF block marked BAD: expected fragment "
	      "of type " << header_type_str << " and size (including header) " <<
	      expected << " available block size " << buffer_size;
	    return std::make_pair(raft::slice(nullptr, 0), result_code::BAD);
	  } else {
	    // Stop processing but don't make this an error (could be unclean shutdown)
	    BOOST_LOG_TRIVIAL(info) << "fragment_reader_operator::next_fragment incomplete EOF block: expected fragment "
	      "of type " << header_type_str << " and size (including header) " <<
	      expected << " available block size " << buffer_size;
	    return std::make_pair(raft::slice(nullptr, 0), result_code::eEOF);
	  }
	}

	if (int(result_code::ZERO) == header()->type && 0 == header()->length) {
	  slice_.clear();
	  return std::make_pair(raft::slice(nullptr, 0), result_code::BAD);
	}

	BOOST_ASSERT(header_type() != result_code::BAD);
	auto tmp = std::make_pair(slice_.share(0, HEADER_SIZE + header()->length), header_type());
	slice_.trim_prefix(HEADER_SIZE + header()->length);
	return tmp;
      }
	
    public:

      fragment_reader_operator(scheduler_type & scheduler, bool syncing=false)
	:
	scheduler_(scheduler),
	state_(BEGIN),
	slice_(nullptr, 0),
	output_({raft::slice(nullptr, 0), result_code::ZERO }),
	eof_(false),
	syncing_(syncing)
      {
      }
	
      void start()
      {
	state_ = BEGIN;
	eof_ = false;
	on_event();
      }

      void on_event()
      {
	switch(state_) {
	case BEGIN:
	  while(true) {
	    if (raft::slice::buffer_size(slice_) < HEADER_SIZE) {
	      if (!eof_) {
		slice_.clear();
		scheduler_.request_read(*this);
		state_ = READ;
		return;
	      case READ:
		slice_ = scheduler_.read(*this);
		if (raft::slice::buffer_size(slice_) != BLOCK_SIZE) {
		  BOOST_LOG_TRIVIAL(debug) << "fragment_reader_operator::on_event received block of size "  <<
		    raft::slice::buffer_size(slice_) << " setting EOF";
		  eof_ = true;
		}
	      } else {
		// Close up shop.  This is probably just an incomplete header written
		// out by a crashing writer (or it could just be an empty block indicating that the
		// input block stream is done).
		BOOST_LOG_TRIVIAL(info) << "fragment_reader_operator::on_event received block of size "  <<
		  raft::slice::buffer_size(slice_) << " sending EOF";
		slice_.clear();
		scheduler_.request_write(*this);
		state_ = WRITE_EOF;
		return;
		case WRITE_EOF:
		  scheduler_.write(*this, std::make_pair(raft::slice(nullptr, 0), result_code::eEOF));
		  break;
	      }
	    }

	    output_ = next_fragment();
	    if (!syncing_ || (output_.second != result_code::MIDDLE && output_.second != result_code::LAST)) {
	      if (syncing_ && output_.second != result_code::BAD) {
		syncing_ = false;
	      }
	      scheduler_.request_write(*this);
	      state_ = WRITE;
	      return;
	    case WRITE:
	      {
		bool was_eof = output_.second == result_code::eEOF;
		scheduler_.write(*this, std::move(output_));
		if (was_eof) {
		  break;
		}
	      }
	    }
	  }
	}
      }
    };

    template<typename Scheduler>
    class record_reader_operator
    {
    public:
      typedef Scheduler scheduler_type;
      typedef std::pair<raft::slice,result_code> input_type;
      typedef std::vector<raft::slice> output_type;

    private:
      enum class state { BEGIN, READ, WRITE_FULL, WRITE_LAST, WRITE_EOF };
      scheduler_type & scheduler_;
      std::vector<raft::slice> record_slices_;
      state state_;
      std::pair<raft::slice, result_code> fragment_;

    public:
      record_reader_operator(scheduler_type & scheduler)
	:
	scheduler_(scheduler),
	state_(state::BEGIN),
	fragment_({ raft::slice(nullptr, 0), result_code::BAD })
      {
      }

      void start()
      {
	state_ = state::BEGIN;
	record_slices_.clear();
	on_event();
      }
	
      void on_event()
      {
	switch(state_) {

	case state::BEGIN:	    

	  while(true) {
	    // Get next fragment 
	    scheduler_.request_read(*this);
	    state_ = state::READ;
	    return;
	  case state::READ:
	    fragment_ = scheduler_.read(*this);

	    // Incorporate fragment
	    if(result_code::FULL == fragment_.second) {
	      scheduler_.request_write(*this);
	      state_ = state::WRITE_FULL;
	      return;
	    case state::WRITE_FULL:
	      fragment_.first.trim_prefix(HEADER_SIZE);
	      scheduler_.write(*this, std::move(fragment_.first));		
	    } else if (result_code::MIDDLE == fragment_.second) {
	      if (0 < record_slices_.size()) {
		fragment_.first.trim_prefix(HEADER_SIZE);
		record_slices_.push_back(std::move(fragment_.first));
	      } else {
		// TODO: Report corruption
		BOOST_LOG_TRIVIAL(debug) << "record_reader_operator::on_event unexpected MIDDLE fragment";
	      }
	    } else if (result_code::FIRST == fragment_.second) {
	      if (0 == record_slices_.size()) {
		fragment_.first.trim_prefix(HEADER_SIZE);
		record_slices_.push_back(std::move(fragment_.first));
	      } else {
		// TODO: Report corruption
		BOOST_LOG_TRIVIAL(debug) << "record_reader_operator::on_event unexpected FIRST fragment";
	      }
	    } else if (result_code::LAST == fragment_.second) {
	      if (0 < record_slices_.size()) {
		fragment_.first.trim_prefix(HEADER_SIZE);
		record_slices_.push_back(std::move(fragment_.first));
		scheduler_.request_write(*this);
		state_ = state::WRITE_LAST;
		return;
	      case state::WRITE_LAST:
		scheduler_.write(*this, std::move(record_slices_));
	      } else {
		// TODO: Report corruption
		BOOST_LOG_TRIVIAL(debug) << "record_reader_operator::on_event unexpected LAST fragment";
	      }
	    } else if (result_code::BAD == fragment_.second) {
	      record_slices_.clear();
	    } else if (result_code::eEOF == fragment_.second) {
	      BOOST_LOG_TRIVIAL(debug) << "record_reader_operator::on_event write eof";
	      record_slices_.clear();
	      scheduler_.request_write(*this);
	      state_ = state::WRITE_EOF;
	      return;
	    case state::WRITE_EOF:
	      scheduler_.write(*this, std::move(record_slices_));
	      // Return because the state machine is done
	      return;
	    } else {
	      // TODO: Report error
	      BOOST_LOG_TRIVIAL(error) << "record_reader_operator::on_event unexpected fragment type";
	    }
	  }
	}
      }
    };

    class record_reader
    {
      fragment_reader_operator<record_reader> fragment_op_;
      record_reader_operator<record_reader> record_op_;

      // These are the links/fifos that connect operators.
      // No buffering or read ahead configured right now so these have capacity 1.
      raft::slice block_;
      typename fragment_reader_operator<record_reader>::output_type fragment_;	
      typename record_reader_operator<record_reader>::output_type record_;

      // Conceptually (I didn't properly model the data structures yet) each of the above
      // fifos has an input port and output port attached to it (each port belonging to an operator).
      // For each port (i.e. input or output of an operator) we maintain 2 booleans
      // The first indicates whether an operator wants to access the port (requests_)
      // The second indicates whether a port can be access (i.e. fifo corresponding to an output port is empty or 
      // fifo corresponding to an input port is non-empty).
      boost::dynamic_bitset<> requests_;
      boost::dynamic_bitset<> buffers_;

      // HACK:
      enum { FRAGMENT_READ, FRAGMENT_WRITE, RECORD_READ, RECORD_WRITE, NUM_PORTS };

      const char * port_name(std::size_t i)
      {
	switch(i) {
	case 0:
	  return "FRAGMENT_READ";
	case 1:
	  return "FRAGMENT_WRITE";
	case 2:
	  return "RECORD_READ";
	case 3:
	  return "RECORD_WRITE";
	default:
	  return "INVALID PORT";
	}
      }

    public:
      record_reader(bool syncing=false)
	:
	fragment_op_(*this, syncing),
	record_op_(*this),
	block_(nullptr, 0),
	fragment_({ raft::slice(nullptr, 0), result_code::BAD }),
	requests_(NUM_PORTS),
	buffers_(NUM_PORTS)
      {
	// Writes are unblocked to start since the corresponding
	// fifo is empty to begin
	buffers_.set(FRAGMENT_WRITE);
	buffers_.set(RECORD_WRITE);
	// Now we can start operators
	fragment_op_.start();
	record_op_.start();
      }
	
      void request_write(const fragment_reader_operator<record_reader> & op)
      {
	requests_.set(FRAGMENT_WRITE);
      }
      void request_write(const record_reader_operator<record_reader> & op)
      {
	requests_.set(RECORD_WRITE);
      }
      void request_read(const fragment_reader_operator<record_reader> & op)
      {
	requests_.set(FRAGMENT_READ);
      }
      void request_read(const record_reader_operator<record_reader> & op)
      {
	requests_.set(RECORD_READ);
      }
      typename fragment_reader_operator<record_reader>::input_type && read(const fragment_reader_operator<record_reader> & op)
      {
	buffers_.reset(FRAGMENT_READ);
	return std::move(block_);
      }
      typename record_reader_operator<record_reader>::input_type && read(const record_reader_operator<record_reader> & op)
      {
	buffers_.reset(RECORD_READ);
	buffers_.set(FRAGMENT_WRITE);
	return std::move(fragment_);
      }
      void write(const fragment_reader_operator<record_reader> & op,
		 typename fragment_reader_operator<record_reader>::output_type && frag)
      {
	fragment_ = std::move(frag);
	buffers_.reset(FRAGMENT_WRITE);
	buffers_.set(RECORD_READ);
      }
      void write(const record_reader_operator<record_reader> & op,
		 typename record_reader_operator<record_reader>::output_type && rec)
      {
	record_ = std::move(rec);
	buffers_.reset(RECORD_WRITE);
	// TODO: Invoke callback
      }
      void write(const record_reader_operator<record_reader> & op,
		 raft::slice && rec)
      {
	record_.clear();
	record_.push_back(std::move(rec));
	buffers_.reset(RECORD_WRITE);
	// TODO: Invoke callback
      }

      void write_block(raft::slice && rec)
      {
	block_ = std::move(rec);
	buffers_.set(FRAGMENT_READ);
      }

      bool can_read_record() const
      {
	return !buffers_.test(RECORD_WRITE);
      }

      typename record_reader_operator<record_reader>::output_type read_record()
      {
	typename record_reader_operator<record_reader>::output_type tmp = std::move(record_);
	buffers_.set(RECORD_WRITE);
	return tmp;
      }

      void run_all_enabled()
      {
	// Run until all requests are blocked (it will
	// require an external event to unblock something).
	bool all_requests_blocked = false;
	while(!all_requests_blocked) {
	  all_requests_blocked = true;
	  auto pos = requests_.find_first();
	  while (pos != boost::dynamic_bitset<>::npos) {
	    if (buffers_.test(pos)) {
	      all_requests_blocked = false;
	      requests_.reset(pos);
	      buffers_.reset(pos);
	      switch(pos) {
	      case FRAGMENT_READ:
	      case FRAGMENT_WRITE:
		fragment_op_.on_event();
		break;
	      case RECORD_READ:
	      case RECORD_WRITE:
		record_op_.on_event();
		break;
	      }
	    }
	    pos = requests_.find_next(pos);
	  }
	}
      }
    };
	
    template<typename _WritableFileType>
    class log_writer
    {
    public:
      // TODO: Double check this is correct
      typedef boost::crc_optimal<32, 0x1EDC6F41, 0xFFFFFFFF, 0xFFFFFFFF, true, true> crc_32_c_type;
      typedef _WritableFileType writable_file_type;
    private:
      writable_file_type & file_;
      std::size_t offset_;
      uint32_t type_checksums_[MAX_RECORD_TYPE+1];

      template<typename ConstSliceSequence>
      void append_fragment(record_type ty, const ConstSliceSequence & s)
      {
	fragment_header header;
	// Compute checksum and write to header
	boost::crc_32_type cksummer(type_checksums_[ty]);
	std::size_t total_size = 0;
	auto it = s.begin();
	auto end = s.end();
	for(; it != end; ++it) {
	  cksummer.process_bytes(slice::buffer_cast<const uint8_t *>(*it), slice::buffer_size(*it));
	  total_size += slice::buffer_size(*it);
	}
	header.crc = mask_checksum(cksummer.checksum());
	header.length = total_size;
	header.type = static_cast<char>(ty);

	file_.write(reinterpret_cast<uint8_t *>(&header), sizeof(header));
	it = s.begin();
	for(; it != end; ++it) {
	  file_.write(slice::buffer_cast<const uint8_t *>(*it), slice::buffer_size(*it));
	}
      }

    public:
      log_writer(writable_file_type & f)
	:
	file_(f),
	offset_(BLOCK_SIZE)
      {
	for(int i=0; i<=MAX_RECORD_TYPE; ++i) {
	  crc_32_c_type cksum;
	  cksum.process_byte(static_cast<char>(i));
	  type_checksums_[i] = cksum.checksum();
	}
      }

      void append_record(const uint8_t * data, std::size_t sz)
      {
	std::array<slice, 1> slices = { slice(data, sz) };
	append_record(slices);
      }

      template<typename ConstSliceSequence>
      void append_record(const ConstSliceSequence & slices)
      {
	bool first=true;
	std::size_t sz = slice::total_size(slices);
	std::size_t next = 0;
	// Use do/while because we want a zero length record even if the data is zero length
	do {
	  if (BLOCK_SIZE - offset_ < HEADER_SIZE) {
	    // Need a new block. Output trailer and release the block.
	    static const uint8_t zeros[HEADER_SIZE] = { 0, 0, 0, 0, 0, 0, 0 };
	    file_.write(zeros, BLOCK_SIZE - offset_);
	    offset_ = 0;
	  }

	  BOOST_ASSERT(BLOCK_SIZE - offset_ >= HEADER_SIZE);

	  // Figure out what the next fragment looks like
	  std::size_t available_in_block = BLOCK_SIZE - offset_ - HEADER_SIZE;
	  std::size_t fragment_sz = sz; 
	  bool is_full_fragment = true;
	  if (fragment_sz > available_in_block) {
	    fragment_sz = available_in_block;
	    is_full_fragment = false;
	  }
	  record_type ty;
	  if (first && is_full_fragment) {
	    ty = FULL;
	  } else if (first) {
	    ty = FIRST;
	  } else if (is_full_fragment) {
	    ty = LAST;
	  } else {
	    ty = MIDDLE;
	  }

	  if (ty == FULL) {
	    append_fragment(ty, slices);
	  } else {
	    std::vector<slice> v;
	    slice::share(slices, next, fragment_sz, std::back_inserter(v));
	    append_fragment(ty, v);
	  }
	  
	  first = false;
	  next += fragment_sz;
	  sz -= fragment_sz;
	  offset_ += fragment_sz+HEADER_SIZE;
	} while(sz > 0);
	
	file_.flush();
      }
    };

  }
}

#endif
