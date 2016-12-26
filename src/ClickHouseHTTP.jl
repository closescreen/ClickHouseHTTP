"""
using ClickHouseHTTP 

( You may `unset http_proxy` before run julia program )

s1 = ClickHouseHTTP.RemoteServer(\"http://myserver:8123\")

READONLY( s1, \"select 1\")|>readstring 

MODIFY( s,  \"select 1\", [\"FORMAT Pretty\"]) |>println

MODIFY( s,  [\"select 1\", ""])

MODIFY( s,  \"select 1\")

"""
module ClickHouseHTTP
export READONLY, MODIFY

type RemoteServer
 address::AbstractString
end

import Base.string
string( s::RemoteServer) = s.address

using Requests

if !isnull( Requests.SETTINGS.http_proxy )
    Requests.set_proxy( Nullable{URI}() )
    info("ClickHouseHTTP: http_proxy disabled")
end

    
"""READONLY( s, \"select 1\")|>readstring"""
READONLY( s::ClickHouseHTTP.RemoteServer, query::AbstractString )::Requests.ResponseStream = 
    Requests.get_streaming( s.address, query=Dict( "query"=>query))

"""READONLY( s, \"select 1\", STDOUT)"""
function READONLY( s::ClickHouseHTTP.RemoteServer, query::AbstractString, out::IO )::Int
 stream = Requests.get_streaming( s.address, query=Dict( "query"=>query))
 write( out, stream)
end 

"""READONLY( s, \"select 1\", \"result.txt\" )"""
function READONLY( s::ClickHouseHTTP.RemoteServer, query::AbstractString, out::AbstractString )::Int
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


typealias Iter Union{Array,IO}

"""
MODIFY allow you to do not-readonly actions in database.

MODIFY( s,  \"select 1\")


"""
function MODIFY( s::ClickHouseHTTP.RemoteServer, query::AbstractString)::AbstractString
 post( s.address, data=query )|>readall
end


"""
MODIFY( s,  [\"select 1 FORMAT Pretty\"])|>print
"""
function MODIFY( s::ClickHouseHTTP.RemoteServer, iter::Iter)::AbstractString
 
 stream = Requests.post_streaming( s.address, headers=Dict("Transfer-Encoding"=>"chunked"), write_body=false)
 
 for data_chunk in iter
    write_chunked( stream, data_chunk)
 end
 write_chunked( stream, "")  # Signal that the body is complete

 stream|>readstring 
end


"""
MODIFY( s,  \"select 1\", [\"FORMAT Pretty\"])|>print
"""
function MODIFY( s::ClickHouseHTTP.RemoteServer, query::AbstractString, iter::Iter)::AbstractString
 
 stream = Requests.post_streaming( s.address, headers=Dict("Transfer-Encoding"=>"chunked"), write_body=false)
 
 write_chunked(stream, "$query\n" )
 for data_chunk in iter
    write_chunked( stream, data_chunk)
 end
 write_chunked(stream, "")  # Signal that the body is complete

 stream|>readstring
end







end # --module





