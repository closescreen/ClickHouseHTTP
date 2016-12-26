"""
using ClickHouseHTTP 

( You may `unset http_proxy` before run julia program )

const CH = ClickHouseHTTP

s1 = CH.RemoteServer(\"http://myserver:8123\")

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

#if !isnull( Requests.SETTINGS.https_proxy )
#    Requests.set_https_proxy( Nullable{URI}() )
#    info("ClickHouseHTTP: https_proxy disabled")
#end
    

READONLY( s::ClickHouseHTTP.RemoteServer, query::AbstractString )::Requests.ResponseStream = 
    Requests.get_streaming( s.address, query=Dict( "query"=>query))


function READONLY( s::ClickHouseHTTP.RemoteServer, query::AbstractString, out::IO )::Requests.ResponseStream
 stream = Requests.get_streaming( s.address, query=Dict( "query"=>query))
 write( out, stream)
end 


function READONLY( s::ClickHouseHTTP.RemoteServer, query::AbstractString, out::AbstractString )::Requests.ResponseStream
 stream = Requests.get_streaming( s.address, query=Dict( "query"=>query))
 open( _->write( _, stream),  out, "w")
end


import Base.write
function write( io::IO, stream::Requests.ResponseStream )::Void
 bytes=0
 while !eof( stream)
    bytes += write( io, readavailable( stream))
 end
 bytes
end


typealias Iter Union{Array,IO}

function MODIFY( s::ClickHouseHTTP.RemoteServer, query::AbstractString)::AbstractString
 post( s.address, data=query )|>readall
end


function MODIFY( s::ClickHouseHTTP.RemoteServer, iter::Iter)
 
 stream = Requests.post_streaming( s.address, headers=Dict("Transfer-Encoding"=>"chunked"), write_body=false)
 
 for data_chunk in iter
    write_chunked( stream, data_chunk)
 end
 write_chunked( stream, "")  # Signal that the body is complete

 stream|>readstring 
end


function MODIFY( s::ClickHouseHTTP.RemoteServer, query::AbstractString, iter::Iter)
 
 stream = Requests.post_streaming( s.address, headers=Dict("Transfer-Encoding"=>"chunked"), write_body=false)
 
 write_chunked(stream, "$query\n" )
 for data_chunk in iter
    write_chunked( stream, data_chunk)
 end
 write_chunked(stream, "")  # Signal that the body is complete

 stream|>readstring
end







end # --module





