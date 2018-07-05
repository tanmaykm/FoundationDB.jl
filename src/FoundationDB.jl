__precompile__(true)
module FoundationDB

using Compat
using Compat.Distributed

if VERSION < v"0.7.0-DEV.4442"
    import Base: finalizer
    finalizer(f::Function, o) = finalizer(o, f)

    function unsafe_wrap(Atype::Union{Type{Array},Type{Array{T}},Type{Array{T,N}}}, p::Ptr{T}, dims::NTuple{N,Int}; own::Bool = false) where {T,N}
        Base.unsafe_wrap(Atype, p, dims, own)
    end
    function unsafe_wrap(Atype::Union{Type{Array},Type{Array{T}},Type{Array{T,1}}}, p::Ptr{T}, d::Integer; own::Bool = false) where {T}
        Base.unsafe_wrap(Atype, p, d, own)
    end
    unsafe_wrap(Atype::Type, p::Ptr, dims::NTuple{N,<:Integer}; own::Bool = false) where {N} = unsafe_wrap(Atype, p, convert(Tuple{Vararg{Int}}, dims), own = own)
end

import Base: show, open, close, reset, isopen, getkey
export FDBCluster, FDBDatabase, FDBTransaction, FDBError, FDBKeySel, FDBFuture, start_client, stop_client, is_client_running
export FDBNetworkOption, FDBDatabaseOption, FDBTransactionOption, FDBMutationType, FDBStreamingMode, FDBConflictRangeType
export reset, cancel, commit, get_read_version, set_read_version, get_committed_version
export clearkey, clearkeyrange, getval, setval, watchkey, keysel, getkey, getrange, conflict
export atomic, atomic_add, atomic_and, atomic_or, atomic_xor, atomic_max, atomic_min, atomic_setval, atomic_integer, prep_atomic_key!

include("capi/capi.jl")
include("base.jl")

end # module
