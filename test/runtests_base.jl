using FoundationDB

@static if VERSION < v"0.7.0-DEV.2005"
    using Base.Test
else
    using Test
end

@testset "start network" begin
    @test !is_client_running()
    @test start_client() === nothing
    @test is_client_running()
end

try
    @testset "basic julia apis" begin
        chk_closed = Vector{Any}()

        open(FDBCluster()) do cluster
            push!(chk_closed, cluster)
            @test cluster.ptr !== C_NULL

            open(FDBDatabase(cluster)) do db
                push!(chk_closed, db)
                @test db.name == "DB"
                @test db.ptr !== C_NULL

                open(FDBTransaction(db)) do tran
                    push!(chk_closed, tran)
                    @test tran.ptr !== C_NULL
                end
            end
        end
    end

    @testset "stop network" begin
        @test is_client_running()
        @test stop_client() === nothing
        @test !is_client_running()
        @test_throws Exception start_client()
    end
finally
    stop_client()
end
