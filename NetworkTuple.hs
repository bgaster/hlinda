{-
  Copyright (c) 2010-2012, Benedict R. Gaster
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.


   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
   TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
   PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
   OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-}

module NetworkTuple (
  Port,
  NetID,
  NetReply(..),
  ClientReq(..),
  RemoteServerInfo,
  Network,
  ServerConn(..),
  
  initNetwork,
  sendOutTupleNet,
  sendInReqNet,
  sendAcceptNet,
  sendCancelNet,
  replyNet
  
  ) where

import Network (listenOn, accept, connectTo, withSocketsDo, PortID(..))
import System.IO
import Control.Concurrent 
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan

import Control.Concurrent.MVar

import Control.Monad.Identity

import Text.Parsec

import Data.Maybe

--------------------------------------------------------------------------------

import TupleType

------------------------------------------------------------------------------

data NetMessage = OutTupleNetMsg Tuple
                | InReqNetMsg Int Template
                | ReadReqNetMsg Int Template
                | AcceptNetMsg Int
                | CancelNetMsg Int
                | InReplyNetMsg NetReply
                  
instance Show NetMessage
         where 
           show (OutTupleNetMsg t)  = "OutTupleNetMsg" ++ show t
           show (InReqNetMsg _ _)   = "InReqNetMsg"           
           show (ReadReqNetMsg _ _) = "ReadReqNetMsg"           
           show (AcceptNetMsg _)    = "AcceptNetMsg"           
           show (CancelNetMsg _)    = "CancelNetMsg"           
           show (InReplyNetMsg nr)  = "InReplyNetMsg"

type Port = Int    

type NetID = Int

data NetReply = NetReply Int [AtomicVal]

instance Show NetReply where
  show (NetReply id binds) = show "NetReply " ++ show id ++ " " ++ show binds

data ClientReq = NetOutTuple Tuple
               | NetInReq NetID                -- network id 
                          Int                  -- trans ID
                          Bool                 -- remove
                          Template             -- pattern
                          (NetReply -> IO ())  -- reply
               | NetAccept NetID Int           -- NetID x transID
               | NetCancel NetID Int           -- NetID x transID

instance Show ClientReq
         where 
           show (NetOutTuple t)  = "NetOutTuple " ++ show t
           show (NetInReq netID id b template f) = "NetInReg"
           show (NetAccept netID id) = "NetAccept"
           show (NetCancel netID id) = "NetCancel"

type RemoteServerInfo = (String, NetID, ServerConn)

type Network = (NetID, [RemoteServerInfo])

data ServerConn = ServerConn (NetMessage -> IO ()) (TChan NetReply)

defaultPort :: Port
defaultPort = 8001

--------------------------------------------------------------------------------

receiveMessage :: Handle -> IO NetMessage
receiveMessage sock =
  do msg    <- hGetLine sock
     return $ decodeMsg msg
  where
    decodeMsg ('0':xs) = OutTupleNetMsg $ decodeTuple xs
    decodeMsg ('1':xs) = runParse (
      do { transID <- fromIntegral `fmap` integer;
         template <- parseTemplate;
         return $ InReqNetMsg transID template; }) xs
              
    decodeMsg ('2':xs) = runParse (
      do { transID <- fromIntegral `fmap` integer;
         template <- parseTemplate;
         return $ ReadReqNetMsg transID template; }) xs
                         
    decodeMsg ('3':xs) = AcceptNetMsg ((read xs) :: Int)
    decodeMsg ('4':xs) = CancelNetMsg ((read xs) :: Int)
    decodeMsg ('5':xs) = runParse (
      do { transID <- fromIntegral `fmap` integer;
         values <- many parseAtomicVal;
         return $ InReplyNetMsg (NetReply transID values); }) xs 
                         
    decodeMsg _ = error "ERROR: unxpected message receiveMessage"
    
    runParse p input = case runParser p () "" input of
       Left _  -> error "failed to decode received message"
       Right t -> t                        

    
sendMessage :: Handle -> NetMessage -> IO ()
sendMessage sock msg = 
  do let m = encodeMsg msg
     print ("msg: " ++ show m)
     hPutStrLn sock (encodeMsg msg)
     hFlush sock
  where
    encodeMsg (OutTupleNetMsg tuple)           = 
      "0" ++ encodeTuple tuple
    encodeMsg (InReqNetMsg transID template)   = 
      "1" ++ show transID ++ encodeTemplate template 
    encodeMsg (ReadReqNetMsg transID template) = 
      "2" ++ show transID ++ encodeTemplate template
    encodeMsg (AcceptNetMsg transID)           = 
      "3" ++ show transID
    encodeMsg (CancelNetMsg transID)           = 
      "4" ++ show transID
    encodeMsg (InReplyNetMsg (NetReply transID values)) = 
      "5" ++ show transID ++ encodeValues values
      
--------------------------------------------------------------------------------
        
spawnBuffers :: NetID -> Handle -> TChan ClientReq -> IO ServerConn
spawnBuffers id sock tsReqChan =
  do outChan <- atomically $ newTChan
     inChan  <- atomically $ newTChan
     forkIO $ outLoop outChan
     forkIO $ inLoop outChan inChan
     return (ServerConn (atomically . writeTChan outChan) inChan)
  where
    reply outChan r = do print "in reply network" 
                         atomically $ writeTChan outChan $ InReplyNetMsg r
    
    outLoop outChan = do print "about to read message"
                         msg <- atomically $ readTChan outChan
                         print ("send msg: " ++ show msg)
                         sendMessage sock msg
                         outLoop outChan
                         
    inLoop outChan inChan =
      do msg <- receiveMessage sock
         print ("reveived msg: " ++ show msg)
         case msg of
           OutTupleNetMsg t -> atomically $ writeTChan tsReqChan (NetOutTuple t)
           
           InReqNetMsg transID template ->
             atomically $ writeTChan tsReqChan (NetInReq id 
                                                         transID 
                                                         True 
                                                         template 
                                                         (reply outChan))
             
           ReadReqNetMsg transID template ->
             atomically $ writeTChan tsReqChan (NetInReq id 
                                                         transID 
                                                         False 
                                                         template 
                                                         (reply outChan))
             
           AcceptNetMsg transID -> atomically $ writeTChan tsReqChan 
                                                           (NetAccept id transID)
           
           CancelNetMsg transID -> atomically $ writeTChan tsReqChan 
                                                           (NetCancel id transID)
           
           InReplyNetMsg reply -> atomically $ writeTChan inChan reply
         inLoop outChan inChan
         
--------------------------------------------------------------------------------

spawnNetworkServer myPort startID tsReqChan addTS =
  do let port = fromMaybe defaultPort myPort
     forkIO $ loop (fromIntegral port) startID
     return ()
  where       
    loop myPort nextID = 
      do servSock <- listenOn $ PortNumber myPort
         --print $ "listening on: " ++ show myPort         
         (handle, host, port) <- accept servSock
         --print $ "accepted: " ++ host ++ " on: " ++ show myPort         
         proxyConn <- spawnBuffers nextID handle tsReqChan                     
         addTS (host, nextID, proxyConn)
         loop myPort (nextID + 1)

initNetwork :: Maybe Port -> 
                 [(String, Port)] -> 
                   TChan ClientReq -> 
                     (RemoteServerInfo -> IO ()) ->
                       IO Network
initNetwork port remotes tsReqChan addTS = 
  withSocketsDo $ do let startID = length remotes + 1
                     (_, servers) <- foldM mkServer (1, []) remotes
                     spawnNetworkServer port startID tsReqChan addTS
                     return (0, servers)
  where
    mkServer (id, cs) (host, port) =
      do sock <- connectTo host $ PortNumber (fromIntegral port)
         conn <- spawnBuffers id sock tsReqChan
         return (id+1, (host, id, conn):cs)

--------------------------------------------------------------------------------

sendOutTupleNet :: ServerConn -> Tuple -> IO ()
sendOutTupleNet (ServerConn f _) tuple = f $ OutTupleNetMsg tuple

sendInReqNet :: ServerConn -> Int -> Bool -> Template -> IO ()
sendInReqNet (ServerConn f _) id _ template =  f $ InReqNetMsg id template 

sendAcceptNet :: ServerConn -> Int -> IO ()
sendAcceptNet (ServerConn f _) id =  f $ AcceptNetMsg id

sendCancelNet :: ServerConn -> Int -> IO ()
sendCancelNet (ServerConn f _) id =  f $ CancelNetMsg id

replyNet :: ServerConn -> TChan NetReply
replyNet (ServerConn _ reply) = reply
