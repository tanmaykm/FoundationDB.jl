using .CApi

#-------------------------------------------------------------------------------
# FDBError
#-------------------------------------------------------------------------------
struct FDBError
    code::fdb_error_t
    desc::String
end

strerrdesc(cs::Cstring) = (cs == C_NULL) ? "unknown" : unsafe_string(cs)
FDBError(code::fdb_error_t) = FDBError(code, strerrdesc(fdb_get_error(code)))
function FDBError(future::fdb_future_ptr_t)
    desc = Ref{Cstring}(C_NULL)
    block_until(future)
    errcode = fdb_future_get_error(future, desc)
    if desc[] == C_NULL
        # try getting it with a different API
        desc[] = fdb_get_error(errcode)
    end
    FDBError(errcode, strerrdesc(desc[]))
end

function throw_on_error(result)
    (result == 0) || throw(FDBError(result))
    nothing
end

function throw_on_error(future::fdb_future_ptr_t)
    err = FDBError(future)
    if err.code != 0
        fdb_future_destroy(future)
        throw(err)
    end
    nothing
end

function show(io::IO, err::FDBError)
    code = Int(err.code)
    print(io, "FDB error ", code, " - ", err.desc)
end

#-------------------------------------------------------------------------------
# Working with futures and starting/stopping network
#-------------------------------------------------------------------------------
block_until(future::fdb_future_ptr_t) = (wait(@schedule fdb_future_block_until_ready_in_thread(future)); future)
block_until(errcode) = errcode

function with_err_check(on_success, result::Union{fdb_future_ptr_t,fdb_error_t}, on_error=throw_on_error)
    err = FDBError(result)
    (err.code == 0) ? on_success(result) : on_error(result)
end

function err_check(result::Union{fdb_future_ptr_t,fdb_error_t}, on_error=throw_on_error)
    with_err_check((x)->x, result, on_error)
    nothing
end

"""
Encapsulates starting and stopping of the FDB Network
"""
struct FDBNetwork
    addr::String
    version::Cint
    task::Task

    function FDBNetwork(addr::String="127.0.0.1:4500", version::Cint=FDB_API_VERSION)
        throw_on_error(fdb_select_api_version(version))
        throw_on_error(fdb_setup_network(addr))
        network_task = @schedule fdb_run_network_in_thread()
        network = new(addr, version, network_task)
    end
end

const network = Ref{Union{FDBNetwork,Void}}(nothing)

is_client_running() = (network[] !== nothing) && !istaskdone((network[]).task)

function start_client(addr::String="127.0.0.1:4500", version::Cint=FDB_API_VERSION)
    if network[] === nothing
        network[] = FDBNetwork(addr, version)
    elseif istaskdone((network[]).task)
        error("Client stopped. Can only start one client in the lifetime of a process.")
    end
    nothing
end

function stop_client()
    if network[] !== nothing
        if !istaskdone((network[]).task)
            fdb_stop_network()
            wait((network[]).task)
        end
    end
    nothing
end

#-------------------------------------------------------------------------------
# Opening cluster, database, transaction
#-------------------------------------------------------------------------------

"""
An opaque type that represents a Cluster in the FoundationDB C API.
"""
mutable struct FDBCluster
    cluster_file::String
    ptr::fdb_cluster_ptr_t

    function FDBCluster(cluster_file::String="/etc/foundationdb/fdb.cluster")
        cluster = new(cluster_file, C_NULL)
        finalizer(cluster, (cluster)->close(cluster))
        cluster
    end
end

function show(io::IO, cluster::FDBCluster)
    print("FDBCluster(", cluster.cluster_file, ") - ", (cluster.ptr == C_NULL) ? "closed" : "open")
end

open(fn::Function, cluster::FDBCluster) = try fn(open(cluster)) finally close(cluster) end
function open(cluster::FDBCluster)
    if !isopen(cluster)
        @assert is_client_running()
        handle = with_err_check(fdb_create_cluster(cluster.cluster_file)) do future
            h = Ref{fdb_cluster_ptr_t}(C_NULL)
            fdb_future_get_cluster(future, h)
            fdb_future_destroy(future)
            h[]
        end
        cluster.ptr = handle
    end
    cluster
end

function close(cluster::FDBCluster)
    if is_client_running() && isopen(cluster)
        fdb_cluster_destroy(cluster.ptr)
        cluster.ptr = C_NULL
    end
    nothing
end

"""
An opaque type that represents a database in the FoundationDB C API.

An FDBDatabase represents a FoundationDB database - a mutable, lexicographically
ordered mapping from binary keys to binary values. Modifications to a database
are performed via transactions.
"""
mutable struct FDBDatabase
    cluster::FDBCluster
    name::String
    ptr::fdb_database_ptr_t

    function FDBDatabase(cluster::FDBCluster, name::String="DB")
        db = new(cluster, name, C_NULL)
        finalizer(db, (db)->close(db))
        db
    end
end

function show(io::IO, db::FDBDatabase)
    print("FDBDatabase(", db.name, ") - ", (db.ptr == C_NULL) ? "closed" : "open")
end

open(fn::Function, db::FDBDatabase) = try fn(open(db)) finally close(db) end
function open(db::FDBDatabase)
    if !isopen(db)
        @assert is_client_running()
        cl = db.cluster.ptr
        name = convert(Vector{UInt8}, db.name)
        lname = Cint(length(name))
        handle = with_err_check(fdb_cluster_create_database(cl, name, lname)) do future
            h = Ref{fdb_database_ptr_t}(C_NULL)
            fdb_future_get_database(future, h)
            fdb_future_destroy(future)
            h[]
        end
        db.ptr = handle
    end
    db
end

function close(db::FDBDatabase)
    if is_client_running() && isopen(db)
        fdb_database_destroy(db.ptr)
        db.ptr = C_NULL
    end
    nothing
end

"""
An opaque type that represents a transaction in the FoundationDB C API.

In FoundationDB, a transaction is a mutable snapshot of a database. All read and
write operations on a transaction see and modify an otherwise-unchanging version
of the database and only change the underlying database if and when the
transaction is committed. Read operations do see the effects of previous write
operations on the same transaction. Committing a transaction usually succeeds in
the absence of conflicts.

Applications must provide error handling and an appropriate retry loop around
the application code for a transaction. See the documentation for
fdb_transaction_on_error().

Transactions group operations into a unit with the properties of atomicity,
isolation, and durability. Transactions also provide the ability to maintain an
application’s invariants or integrity constraints, supporting the property of
consistency. Together these properties are known as ACID.

Transactions are also causally consistent: once a transaction has been
successfully committed, all subsequently created transactions will see the
modifications made by it.
"""
mutable struct FDBTransaction
    db::FDBDatabase
    ptr::fdb_transaction_ptr_t

    function FDBTransaction(db::FDBDatabase)
        tran = new(db, C_NULL)
        finalizer(tran, (tran)->close(tran))
        tran
    end
end

function show(io::IO, tran::FDBTransaction)
    print("FDBTransaction - ", (tran.ptr == C_NULL) ? "closed" : "open")
end

open(fn::Function, tran::FDBTransaction) = try fn(open(tran)) finally close(tran) end
function open(tran::FDBTransaction)
    if !isopen(tran)
        @assert is_client_running()
        db = tran.db.ptr
        h = Ref{fdb_database_ptr_t}(C_NULL)
        handle = with_err_check(fdb_database_create_transaction(db, h)) do result
            h[]
        end
        tran.ptr = handle
    end
    tran
end

function close(tran::FDBTransaction)
    if is_client_running() && isopen(tran)
        fdb_transaction_destroy(tran.ptr)
        tran.ptr = C_NULL
    end
    nothing
end

"""
Check if it is open.
"""
isopen(x::Union{FDBCluster,FDBDatabase,FDBTransaction}) = !(x.ptr === C_NULL)

#-------------------------------------------------------------------------------
# Transaction Ops
#-------------------------------------------------------------------------------

function reset(tran::FDBTransaction)
    fdb_transaction_reset(tran.ptr)
    nothing
end

function cancel(tran::FDBTransaction)
    fdb_transaction_cancel(tran.ptr)
    nothing
end

"""
- returns true on success
- returns false on a retryable error
- throws error on non-retryable error
"""
function retry_on_error(tran::FDBTransaction, future::fdb_future_ptr_t)
    err = FDBError(future)
    if err.code != 0
        throw_on_error(fdb_transaction_on_error(tran.ptr, err.code))
        return false
    end
    true
end

function commit(tran::FDBTransaction, on_error=retry_on_error)
    err_check(fdb_transaction_commit(tran.ptr), on_error)
end

function get_read_version(tran::FDBTransaction)
    ver = Ref{Int64}(0)
    with_err_check(fdb_transaction_get_read_version(tran.ptr)) do result
        fdb_future_get_version(result, ver)
        fdb_future_destroy(result)
    end
    ver[]
end

function set_read_version(tran::FDBTransaction, version)
    fdb_transaction_set_read_version(tran.ptr, Int64(version))
end

function get_committed_version(tran::FDBTransaction)
    ver = Ref{Int64}(0)
    err_check(fdb_transaction_get_committed_version(tran.ptr, ver))
    ver[]
end

#-------------------------------------------------------------------------------
# Get Set Ops
#-------------------------------------------------------------------------------

function setkey(tran::FDBTransaction, key::Vector{UInt8}, val::Vector{UInt8})
    fdb_transaction_set(tran.ptr, key, Cint(length(key)), val, Cint(length(val)))
end

function clearkey(tran::FDBTransaction, key::Vector{UInt8})
    fdb_transaction_clear(tran.ptr, key, Cint(length(key)))
end

function clearkeyrange(tran::FDBTransaction, begin_key::Vector{UInt8}, end_key::Vector{UInt8})
    fdb_transaction_clear_range(tran.ptr, begin_key, Cint(length(begin_key)), end_key, Cint(length(end_key)))
end

copyval(tran::FDBTransaction, present::Bool, val::Vector{UInt8}) = present ? copy(val) : nothing
getval(tran::FDBTransaction, key::Vector{UInt8}) = getval(copyval, tran, key)
function getval(fn::Function, tran::FDBTransaction, key::Vector{UInt8})
    val = nothing
    present = Ref{fdb_bool_t}(false)
    valptr = Ref{Ptr{UInt8}}(C_NULL)
    vallen = Ref{Cint}(0)
    with_err_check(fdb_transaction_get(tran.ptr, key, Cint(length(key)))) do result
        err_check(fdb_future_get_value(result, present, valptr, vallen))
        val = fn(tran, Bool(present[]), unsafe_wrap(Array, valptr[], (vallen[],), false))
        fdb_future_destroy(result)
    end
    val
end

function setval(tran::FDBTransaction, key::Vector{UInt8}, val::Vector{UInt8})
    fdb_transaction_set(tran.ptr, key, Cint(length(key)), val, Cint(length(val)))
end
