#ifndef __SLICE_HH__
#define __SLICE_HH__

#include <iomanip>
#include <iostream>
#include "boost/io/ios_state.hpp"


namespace raft {

  class slice
  {
  private:
    const uint8_t * buffer;
    std::size_t sz;
  public:
    // Non-copyable only moveable
    slice(const slice & s) = delete;
    const slice & operator=(const slice & s) = delete;

    slice()
      :
      buffer(nullptr),
      sz(0)
    {
    }

    slice(const uint8_t * b, std::size_t s)
      :
      buffer(b),
      sz(s)
    {
    }

    slice(slice && s)
      :
      buffer(s.buffer),
      sz(s.sz)
    {
      s.buffer = nullptr;
      s.sz = 0;
    }

    slice & operator=(slice && s)
    {
      buffer = s.buffer;
      sz = s.sz;
      s.buffer = nullptr;
      s.sz = 0;
      return *this;
    }

    slice share() const
    {
      return slice(buffer, sz);
    }

    slice share(std::size_t pos, std::size_t len) const
    {
      slice s = share();
      s.buffer += pos;
      s.sz = len;
      return s;
    }

    void clear()
    {
      buffer = nullptr;
      sz = 0;
    }

    void trim_prefix(std::size_t len)
    {
      buffer += len;
      sz -= len;
    }

    slice & operator+=(std::size_t len)
    {
      trim_prefix(len);
      return *this;
    }
    
    const void * data() const
    {
      return buffer;
    }

    size_t size() const
    {
      return sz;
    }

    static slice create(const std::string & str)
    {
      return slice(reinterpret_cast<const uint8_t *>(&str[0]), str.size());
    }

    template<typename _T>
    static _T buffer_cast(const slice & s)
    {
      return reinterpret_cast<_T>(s.buffer);
    }

    static std::size_t buffer_size(const slice & s)
    {
      return s.sz;
    }

    // Get the total size of a sequence of slices
    template<typename ConstSliceSequence>
    static std::size_t total_size(const ConstSliceSequence & slices)
    {
      std::size_t total = 0;
      auto it = slices.begin();
      auto end = slices.end();
      for(; it != end; ++it) {
	total += slice::buffer_size(*it);
      }
      return total;
    }
      
      
    // Return slices corresponding to the byte range [left,left+len)
    template<typename ConstSliceSequence, typename SliceOutputIterator>
    static void share(const ConstSliceSequence & slices, std::size_t left, std::size_t len,
		      SliceOutputIterator out)
    {
      auto it = slices.begin();
      auto end = slices.end();

      // Find the slice containing left and add it
      std::size_t total_bytes = 0;
      for(; it != end; ++it) {
	std::size_t sz = slice::buffer_size(*it);
	if (total_bytes + sz > left) {
	  std::size_t to_share = (std::min)(len, std::size_t(total_bytes + sz - left));
	  *out++ = it->share(left-total_bytes, to_share);
	  len -= to_share;
	  total_bytes += sz;
	  ++it;
	  break;
	} else {
	  total_bytes += sz;
	}
      }

      // Keep adding until we get all of the bytes or hit the end of the sequence
      for(; len > 0 && it != end; ++it) {
	std::size_t sz = slice::buffer_size(*it);
	if (sz > len) {
	  *out++ = slice(slice::buffer_cast<const uint8_t *>(*it), len);
	  len = 0;
	} else {
	  len -= sz;
	  *out++ = it->share();
	}
      }	
    }
  };

  class mutable_slice
  {
  private:
    uint8_t * buffer;
    std::size_t sz;
  public:
    // Non-copyable only moveable
    mutable_slice(const mutable_slice & s) = delete;
    const mutable_slice & operator=(const mutable_slice & s) = delete;

    mutable_slice(uint8_t * b, std::size_t s)
      :
      buffer(b),
      sz(s)
    {
    }

    mutable_slice(mutable_slice && s)
      :
      buffer(s.buffer),
      sz(s.sz)
    {
      s.buffer = nullptr;
      s.sz = 0;
    }

    mutable_slice & operator=(mutable_slice && s)
    {
      buffer = s.buffer;
      sz = s.sz;
      s.buffer = nullptr;
      s.sz = 0;
      return *this;
    }

    void clear()
    {
      buffer = nullptr;
      sz = 0;
    }

    mutable_slice share() const
    {
      return mutable_slice(buffer, sz);
    }

    void trim_prefix(std::size_t len)
    {
      buffer += len;
      sz -= len;
    }

    mutable_slice & operator+=(std::size_t len)
    {
      trim_prefix(len);
      return *this;
    }
    
    void * data()
    {
      return buffer;
    }

    size_t size() const
    {
      return sz;
    }

    static std::size_t buffer_size(const mutable_slice & s)
    {
      return s.sz;
    }
  };

  inline std::size_t buffer_copy(mutable_slice && target, slice && source)
  {
    auto to_copy = (std::min)(mutable_slice::buffer_size(target), slice::buffer_size(source));
    ::memcpy(target.data(), source.data(), to_copy);
    return to_copy;
  }

  inline slice operator+(const slice & s, std::size_t len)
  {
    slice ret = s.share();
    ret.trim_prefix(len);
    return ret;
  }

  inline mutable_slice operator+(const mutable_slice & s, std::size_t len)
  {
    mutable_slice ret = s.share();
    ret.trim_prefix(len);
    return ret;
  }

  inline std::ostream & operator<<(std::ostream & ostr, const slice &  s)
  {
    boost::io::ios_flags_saver ifs(ostr);
    ostr << "0x" << std::hex << std::setfill ('0');
    auto d = reinterpret_cast<const uint8_t *>(s.data());
    for(std::size_t i=0; i<s.size(); ++i) {
      ostr << std::setw (2) << (unsigned) d[i];
    }
    return ostr;
  }
}

#endif
