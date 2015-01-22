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

Short Description: Distributed Linda implementation for Haskell
Description      : 

An implementation of distributed Linda-style tuple spaces, based on the CML 
implementation by John Reppy; from his book Concurrent Programming in ML.
          
The implementation is built on STM and has a number of things that could
be done better!

It is worth noting that while it would have been possible to build an
abstraction on top of STM channels to support CML style events the 
implementation passes channels explicitly. This has the disadvantage
of having to explictly read/write from these channels in interfaces
that could simply sync on an event in Reppy's implementation. The advantage
is that we can use STM channels directly without an additonal layer of
abstraction. It would be nice to see an event abstraction provided as 
part of STM, not simply in the CML package, but as a first class part 
of the STM implementation.
-}

module Linda (
  TupleSpace,
  
  joinTupleSpace,
  inLinda,
  rdLinda,
  outLinda,
  
  AtomicVal(..),
  AtomicPat(..),
  TupleRep(..),
  Tuple,
  Template
  ) where

import System.IO
import System.IO.Error (isEOFError)

import Control.Concurrent 
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan

import Control.Concurrent.MVar

--------------------------------------------------------------------------------

import TupleType
import NetworkTuple
import TupleServer
import TupleStore
import OutputServer

--------------------------------------------------------------------------------
    
data TupleSpace = TupleSpace (TupleServerMsg -> IO ())  (Tuple -> IO ())

joinTupleSpace :: Maybe Port -> 
                    [(String, Port)] -> 
                      IO TupleSpace
joinTupleSpace localPort remoteHosts =
  do reqChan          <- atomically newTChan
     tsReqChan        <- atomically newTChan
     let out tuple    = atomically $ writeTChan tsReqChan (NetOutTuple tuple)
     (output, addTS)  <- createOutputServer out
     logChan          <- atomically $ dupTChan reqChan
     getInReqLog      <- createLogServer logChan
     (netID, servers) <- initNetwork localPort 
                                     remoteHosts 
                                     tsReqChan 
                                     (newRemoteTS reqChan addTS getInReqLog)
     port             <- atomically $ dupTChan reqChan
     createTupleServer netID tsReqChan port
     mapM (newRemoteTS reqChan addTS getInReqLog) servers
     return $ TupleSpace (\msg -> atomically $ writeTChan reqChan msg ) output
  where
    newRemoteTS reqChan addTS getInReqLog (name, netID, conn) = 
      do (port, initInReqs) <- getInReqLog ()
         addTS (sendOutTupleNet conn)
         chan          <- atomically $ dupTChan reqChan
         createProxyServer netID conn chan initInReqs --port initInReqs
         
doInputOp tid (TupleSpace request _) template removeFlg nack =
  do replyChan <- atomically $ newTChan
     forkIO $ transactionMngr replyChan
     return replyChan
  where
    handleReply replyChan (msg,tsID) tid = 
      do r <- atomically $ (Left `fmap` readTChan nack)
              `orElse`(Right `fmap` writeTChan replyChan msg)
         case r of
           Left  _ -> request $ TSMsgCancel tid 
           Right _ -> request $ TSMsgAccept tid tsID 
    
    transactionMngr replyChan = 
      do replyMB <- atomically $ newTChan
         request $ TSMsgIn tid 
                           removeFlg 
                           template 
                           (\x y -> atomically $ writeTChan replyMB (x,y))
         r <- atomically $ (Left `fmap` readTChan nack)
              `orElse`(Right `fmap` readTChan replyMB)
         case r of
           Left  _ -> request $ TSMsgCancel tid ; 
           Right msg -> handleReply replyChan msg tid 
           
inLinda tid ts template = 
  do nack      <- atomically $ newTChan 
     replyChan <- doInputOp tid ts template True nack
     tuple     <- atomically $ readTChan replyChan
     return tuple
     
rdLinda tid ts template = 
  do nack      <- atomically $ newTChan 
     replyChan <- doInputOp tid ts template False nack
     binds     <- atomically $ readTChan replyChan
     return binds
     
outLinda (TupleSpace _ output) tuple = output tuple

--------------------------------------------------------------------------------

