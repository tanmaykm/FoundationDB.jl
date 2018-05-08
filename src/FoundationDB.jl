__precompile__(true)
module FoundationDB

import Base: show, open, close, reset, isopen, getkey
export FDBCluster, FDBDatabase, FDBTransaction, FDBError, FDBKeySel, start_client, stop_client, is_client_running
export reset, cancel, commit, get_read_version, set_read_version, get_committed_version
export clearkey, clearkeyrange, getval, setval, watchkey, keysel, getkey, getrange

include("capi/capi.jl")
include("base.jl")

end # module
