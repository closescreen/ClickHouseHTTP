"""
using ClickHouseHTTP 

( You may `unset http_proxy` before run julia program )

s1 = ClickHouseHTTP.RemoteServer(\"http://myserver:8123\")

readonly( s1, \"select 1\")|>readstring 

modify!( s,  \"select 1\", [\"FORMAT Pretty\"]) |>println

modify!( s,  [\"select 1\", ""])

modify!( s,  \"select 1\")

"""
module ClickHouseHTTP
export readonly, modify!

type RemoteServer
 address::AbstractString
end

import Base.string
string( s::RemoteServer) = s.address

using Requests

isopen( io::Requests.ChunkedStream ) = isopenio.io

if !isnull( Requests.SETTINGS.http_proxy )
    Requests.set_proxy( Nullable{URI}() )
    info("ClickHouseHTTP: http_proxy disabled")
end


""" s |> readonly( query::AbstractString, ...) """
readonly( query::AbstractString, args... ) = (s::ClickHouseHTTP.RemoteServer)->readonly( s, query, args...)
    

"""readonly( s, \"select 1\")|>readstring"""
readonly( s::ClickHouseHTTP.RemoteServer, query::AbstractString )::Requests.ResponseStream = 
    Requests.get_streaming( s.address, query=Dict( "query"=>query))


"""readonly( s, \"select 1\", STDOUT)"""
function readonly( s::ClickHouseHTTP.RemoteServer, query::AbstractString, out::IO )::Int
 stream = Requests.get_streaming( s.address, query=Dict( "query"=>query))
 write( out, stream)
end 

"""readonly( s, \"select 1\", \"result.txt\" )"""
function readonly( s::ClickHouseHTTP.RemoteServer, query::AbstractString, out::AbstractString )::Int
 stream = Requests.get_streaming( s.address, query=Dict( "query"=>query))
 open( _->write( _, stream),  out, "w")
end


import Base.write
function write( io::IO, stream::Requests.ResponseStream )::Int
 bytes=0
 while !eof( stream)
    bytes += write( io, readavailable( stream))
 end
 bytes
end


typealias Iter Union{Array,IO,Base.Generator}

"s |> modify!(...)"
modify!{T<:Union{AbstractString,Iter}}( arg1::T, args...) = (s::ClickHouseHTTP.RemoteServer)->modify!( s, arg1, args...)


"""
modify! allow you to do not-readonly actions in database.

modify!( s,  \"select 1\") |> print


"""
function modify!( s::ClickHouseHTTP.RemoteServer, query::AbstractString)::AbstractString
    post( s.address, data=query )|>readstring
end



"""
modify!( s,  [\"select 1 FORMAT Pretty\"])|>print
"""
function modify!( s::ClickHouseHTTP.RemoteServer, iter::Iter)::AbstractString
 
 stream = Requests.post_streaming( s.address, headers=Dict("Transfer-Encoding"=>"chunked"), write_body=false)
 try
    for data_chunk in iter
        write_chunked( stream, data_chunk)
    end
    write_chunked( stream, "")  # Signal that the body is complete
 catch e
    warn(e)
    for i in catch_stacktrace()
     println(STDERR, i)
    end

 finally    
    return stream|>readstring
 end
end



"""
modify!( s,  \"select 1\", [\"FORMAT Pretty\"])|>print
"""
function modify!( s::ClickHouseHTTP.RemoteServer, query::AbstractString, iter::Iter; debug::Bool=false)::AbstractString
 
 stream = Requests.post_streaming( s.address, headers=Dict("Transfer-Encoding"=>"chunked"), write_body=false)
 stop = false
 try
    write_chunked(stream, "$query\n" )
    for data_chunk in iter
        stop && break
        debug && info("DATA_CHUNK:", data_chunk)
        write_chunked( stream, data_chunk)
    end
    write_chunked(stream, "")  # Signal that the body is complete
 catch e
    warn(e)
    for i in catch_stacktrace()
     println(STDERR, i)
    end
    stop = true
 finally    
    return stream|>readstring
 end

end







end # --module





