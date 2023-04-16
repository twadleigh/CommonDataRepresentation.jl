module CommonDataRepresentation

export CdrBuffer, buffer, bytes
export Endianness, BigEndian, LittleEndian, HostEndian
export alignment, unsafe_read_sequence, unsafe_write_sequence

@enum Endianness BigEndian LittleEndian HostEndian

const HOST_IS_LE = Base.ENDIAN_BOM == 0x04030201
function requireswap(endianness::Endianness)
  endianness == HostEndian && return false
  endianness == BigEndian && return !HOST_IS_LE
  endianness == LittleEndian && return HOST_IS_LE
end

mutable struct CdrBuffer
  buf::IOBuffer
  doswap::Bool

  CdrBuffer(buf::IOBuffer; endianness::Endianness=HostEndian) = new(buf,requireswap(endianness))
end

function CdrBuffer(buf::AbstractVector{UInt8}; endianness::Endianness=HostEndian)
  CdrBuffer(IOBuffer(buf); endianness)
end

function CdrBuffer(endianness::Endianness=HostEndian)
  CdrBuffer(IOBuffer(); endianness)
end

buffer(cdr::CdrBuffer) = cdr.buf
bytes(cdr::CdrBuffer) = cdr.buf.data


#*** core internal operations

function alignment(pos, size)
  offset = pos % size
  offset > 0 ? size - offset : 0
end

function swap_in_place!(ptr::Ptr, n::Integer = 1)
  for ix in 1:n
    Base.unsafe_store!(ptr, bswap(Base.unsafe_load(ptr, ix)), ix)
  end
  nothing
end

function unsafe_read_sequence(buf::IOBuffer, ptr::Ptr{T}, n::Integer = 1, doswap::Bool = false) where {T}
  size = sizeof(T)
  align = alignment(position(buf),size)
  if align > 0
    skip(buf, align)
  end
  unsafe_read(buf, convert(Ptr{UInt8}, ptr), size*n)
  if doswap
    swap_in_place!(ptr, n)
  end
end

function unsafe_write_sequence(buf::IOBuffer, ptr::Ptr{T}, n::Integer = 1, doswap::Bool = false) where {T}
  if doswap
    swap_in_place!(ptr, n)
  end
  size = sizeof(T)
  align = alignment(position(buf), size)
  padding = align > 0 ? write(buf, zeros(UInt8,align)) : 0
  padding + unsafe_write(buf, convert(Ptr{UInt8}, ptr), size*n)
end


#*** I/O interface

function Base.read!(cdr::CdrBuffer, ref::Ref{T}) where {T}
  unsafe_read_sequence(cdr.buf, Base.unsafe_convert(Ptr{T}, ref), 1, cdr.doswap)
  ref
end

function Base.read!(cdr::CdrBuffer, ref::Ref{NTuple{N,T}}) where {T,N}
  unsafe_read_sequence(cdr.buf, Base.unsafe_convert(Ptr{T}, ref), N, cdr.doswap)
  ref
end

function Base.read!(cdr::CdrBuffer, ary::Array{T}) where {T}
  unsafe_read_sequence(cdr.buf, Base.unsafe_convert(Ptr{T}, ary), length(ary), cdr.doswap)
  ary
end

function Base.read(cdr::CdrBuffer, ::Type{T}) where {T}
  ref = Ref{T}()
  read!(cdr, ref)
  ref[]
end

end # module Cdr
