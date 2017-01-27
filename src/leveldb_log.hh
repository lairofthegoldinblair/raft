#ifndef _RAFT_LEVELDB_LOG_HH_
#define _RAFT_LEVELDB_LOG_HH_

#include "boost/crc.hpp"
#include "boost/endian/arithmetic.hpp"

namespace raft {
  namespace leveldb {

    
    enum record_type { ZERO, FULL, FIRST, MIDDLE, LAST };
    static const int MAX_RECORD_TYPE = LAST;
    static const int BLOCK_SIZE = 32768;
    // checksum + length + type
    static const int HEADER_SIZE = 4 + 2 + 1;

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

    struct mutable_buffer
    {
      uint8_t * buffer;
      std::size_t sz;
    };
    
    struct fragment_header
    {
      boost::endian::little_uint32_t crc;
      boost::endian::little_uint16_t length;
      char type;
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

      void append_fragment(record_type ty, const uint8_t * data, std::size_t sz)
      {
	fragment_header header;
	// Compute checksum and write to header
	boost::crc_32_type cksummer(type_checksums_[ty]);
	cksummer.process_bytes(data, sz);
	header.crc = mask_checksum(cksummer.checksum());
	header.length = sz;
	header.type = static_cast<char>(ty);

	file_.write(reinterpret_cast<uint8_t *>(&header), sizeof(header));
	file_.write(data, sz);
      }
      
    public:
      log_writer(writable_file_type & f)
	:
	file_(f),
	offset_(0)
      {
	for(int i=0; i<=MAX_RECORD_TYPE; ++i) {
	  crc_32_c_type cksum;
	  cksum.process_byte(static_cast<char>(i));
	  type_checksums_[i] = cksum.checksum();
	}
      }

      void append_record(const uint8_t * data, std::size_t sz)
      {
	bool first=true;
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
	  if (sz > available_in_block) {
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

	  append_fragment(ty, data, fragment_sz);
	  
	  first = false;
	  data += fragment_sz;
	  sz -= fragment_sz;
	} while(sz > 0);
	
	file_.flush();
      }
    };

    // template<typename _BlockDevice>
    // struct log_appender
    // {
    //   enum state { START, NEXT };
    //   log_writer<_BlockDevice> & writer_;
    //   state state_;
    //   bool first_;

    //   log_appender(log_writer<_BlockDevice> & writer)
    // 	:
    // 	writer_(writer),
    // 	state_(START),
    // 	first_(true)
    //   {
    //   }

      
    //   mutable_buffer next(std::size_t requested)
    //   {
    // 	advance(requested, std::numeric_limits<std::size_t>::max());
    //   }

    //   mutable_buffer finish(std::size_t consumed=std::numeric_limits<std::size_t>::max())
    //   {
    // 	advance(0, std::numeric_limits<std::size_t>::max());
    //   }

    //   mutable_buffer advance(std::size_t requested, std::size_t consumed)
    //   {
    // 	switch(state_) {
    // 	case START:
    // 	first_=true;
    // 	// Use do/while because we want a zero length record even if the data is zero length
    // 	do {
    // 	  if (BLOCK_SIZE-writer_.block_offset_ < HEADER_SIZE) {
    // 	    // Need a new block. TODO: Output trailer. And release the block.
    // 	    writer_.device_.exchange_block(block_);
    // 	    writer_.block_offset_ = 0;
    // 	  }

    // 	  // Figure out what the next fragment looks like
    // 	  {
    // 	    std::size_t available_in_block = BLOCK_SIZE - writer_.block_offset_ - HEADER_SIZE;
    // 	    std::size_t fragment_sz = (std::min)(requested, available_in_block);
    // 	    state_ = NEXT;
    // 	    mutable_buffer buf;
    // 	    buf.buffer = writer.block_;
    // 	    buf.sz = fragment_sz;
    // 	    return buf;
    // 	  }
    // 	  case NEXT:
	  

    // 	  bool is_full_fragment = true;
    // 	  if (sz_ > available_in_block) {
    // 	    fragment_sz = available_in_block;
    // 	    is_full_fragment = false;
    // 	  }
    // 	  record_type ty;
    // 	  if (first && is_full_fragment) {
    // 	    ty = FULL;
    // 	  } else if (first) {
    // 	    ty = FIRST;
    // 	  } else if (is_full_fragment) {
    // 	    ty = LAST;
    // 	  } else {
    // 	    ty = MIDDLE;
    // 	  }

    // 	  append_fragment(type, data, fragment_sz);
	  
    // 	  first = false;
    // 	  data += fragment_sz;
    // 	  sz -= fragment_sz;
    // 	} while(sz > 0);
    // 	}
    //   }
    // };

    // template<typename _BlockDevice>
    // void log_writer<_BlockDevice>::append_record2(const char * data, std::size_t sz)
    // {
    //   log_appender<_BlockDevice> appender(*this);
    //   while(sz != 0) {
    // 	std::size_t granted=0;
    // 	mutable_buffer b = appender.next(sz);
    // 	std::copy(b.buffer, data, b.sz);
    // 	data += b.sz;
    // 	sz -= b.sz;
    //   }
    //   appender.finish();
    // }

  }
}

#endif
