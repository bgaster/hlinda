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

module TupleServer (
  TupleServerMsg(..),
  
  createTupleServer,
  createProxyServer,
  createLogServer
  
  ) where

import Control.Concurrent 
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan

import Control.Concurrent.MVar

import Data.HashTable as HT
import Data.Map as MP

--------------------------------------------------------------------------------

import TupleType
import NetworkTuple
import TupleStore

------------------------------------------------------------------------------

data TupleServerMsg = TSMsgIn Int                             -- thread ID
                              Bool                            -- remove
                              Template                        -- pattern
                              ([AtomicVal] -> NetID -> IO ()) -- reply fn
                    | TSMsgCancel Int                         -- thread ID
                    | TSMsgAccept Int NetID                   -- thread ID * NetID
                      
instance Show TupleServerMsg where
  show (TSMsgIn id b template f) = "TSMsgIn " ++ 
                                   show id ++ " " ++ show b ++ " " ++ show template 
  show (TSMsgCancel id)          = "TSMsgCancel " ++ show id
  show (TSMsgAccept id netID)    = "TSMsgAccept " ++ show id ++ " " ++ show netID

data ConOps = ConOps (Int -> Bool -> Template -> IO ()) -- sendInReg
                     (Int -> IO ())                     -- sendAccept
                     (Int -> IO ())                     -- sendCancel
                     (TChan NetReply)                   -- replyChannel
              
data TransactionInfo = TransInfo Int ([AtomicVal] -> Int -> IO ())

instance Show TransactionInfo where
  show (TransInfo id f) = "TransInfo " ++ show id

data TransactionKey = TransKey Int Int

instance Show TransactionKey where
  show (TransKey x y) = "TransKey " ++ show x ++ " " ++ show y

instance Eq TransactionKey where
  TransKey x x' == TransKey y y' | (x' == -1 || y' == -1) = (x == y)
                                 | (x == -1) || (y == -1) = (x' == y')
                                 | otherwise = (x == y) && (x' == y')

instance Ord TransactionKey where
  TransKey x x' <= TransKey y y' | (x' == -1 || y' == -1) = (x <= y)
                                 | (x == -1) || (y == -1) = (x' <= y')
                                 | otherwise = (x <= y) && (x' <= y')


type TransactionTable = MP.Map TransactionKey TransactionInfo
                            
--------------------------------------------------------------------------------

proxyServer :: NetID -> 
                 ConOps -> 
                   TChan TupleServerMsg -> 
                     [TupleServerMsg] -> 
                       IO ()
proxyServer myID (ConOps sInReg sAccept sCancel replyChan) reqChan initInReqs =
  do tableVar <- newMVar (MP.empty :: TransactionTable)
     idVar    <- newMVar (0 :: Int)
     mapM (handleMsg tableVar idVar) initInReqs
     forkIO $ loop tableVar idVar
     return ()
  where
    loop tableVar idVar = 
      do r <- atomically $ (Left `fmap` readTChan reqChan)
              `orElse`(Right `fmap` readTChan replyChan)
         case r of
           Left tsMsg     -> 
             handleMsg tableVar idVar tsMsg >> loop tableVar idVar
           Right netReply -> 
             handleReply tableVar netReply >> loop tableVar idVar
    
    handleMsg tableVar idVar (TSMsgIn transId remove template replyFn) =
      do id         <- nextID idVar
         table      <- takeMVar tableVar
         print ("insert: " ++ show transId)
         putMVar tableVar $ MP.insert (TransKey transId id) 
                                      (TransInfo id replyFn) 
                                      table 
         sInReg id remove template
         
    handleMsg tableVar _ (TSMsgCancel transId) =
      do table  <- takeMVar tableVar
         let mb = MP.lookup (TransKey transId (-1::Int)) table
         case mb of 
           Nothing -> error "unexpected loopup failure proxyServer"
           Just (TransInfo id _) -> 
             do takeMVar tableVar >>= \_ -> 
                 putMVar tableVar $ MP.delete (TransKey transId (-1::Int)) table
                sCancel id
    
    handleMsg tableVar _ (TSMsgAccept transId tsID) =
      do table  <- readMVar tableVar
         print ("transID: " ++ show transId)
         let mb = MP.lookup (TransKey transId (-1::Int)) table
         case mb of 
           Nothing -> return () --error "unexpected lookup failure proxyServer"
           Just (TransInfo id _) -> 
             do takeMVar tableVar >>= \_ -> 
                 putMVar tableVar $ MP.delete (TransKey transId (-1::Int)) table  
                if tsID == myID 
                  then sAccept id
                  else sCancel id
                       
    handleReply tableVar (NetReply transID vals) =
      do table  <- readMVar tableVar
         let mb = MP.lookup (TransKey (-1::Int) transID) table
         case mb of 
           Nothing -> return ()
           Just (TransInfo id replyFn) -> replyFn vals myID

    nextID idVar = do id <- takeMVar idVar
                      putMVar idVar (id+1)
                      return id

createProxyServer :: NetID -> 
                       ServerConn -> 
                         TChan TupleServerMsg -> 
                           [TupleServerMsg] -> 
                             IO ()
createProxyServer myID conn reqChan initInReqs = 
  proxyServer myID (ConOps (sendInReqNet conn) 
                           (sendAcceptNet conn) 
                           (sendCancelNet conn)
                           (replyNet conn)) reqChan initInReqs

--------------------------------------------------------------------------------

tupleServer :: TChan ClientReq -> IO ()
tupleServer msgChan =
  do tupleStore <- createTupleStore ()
     forkIO $ serverLoop tupleStore
     return ()
  where
    serverLoop tupleStore = do msg <- atomically (readTChan msgChan)
                               handleReq tupleStore msg
                               serverLoop tupleStore
    
    replyIfMatch Nothing                         = return ()
    replyIfMatch (Just (Match (_,id) reply ext)) = reply (NetReply id ext)
         
    handleReq tupleStore (NetOutTuple tuple) = 
      addTS tupleStore tuple >>= replyIfMatch
    
    handleReq tupleStore (NetInReq netID transID remove template reply) =
      do mb <-inputTS tupleStore (Match (netID, transID) reply template)
         case mb of
           Nothing                     -> return ()
           Just (Match id reply binds) -> reply (NetReply transID binds)
    
    handleReq tupleStore (NetAccept netID transID) = 
      removeTS tupleStore (netID, transID) 
    
    handleReq tupleStore (NetCancel netID transID) = 
      cancelTS tupleStore (netID, transID) >>= replyIfMatch

createTupleServer :: NetID -> 
                       TChan ClientReq -> 
                         TChan TupleServerMsg -> 
                           IO ()
createTupleServer netID msgChan reqChan =
  do rChan         <- atomically newTChan
     tupleServer msgChan
     proxyServer netID (conn rChan) (reqChan) []
     return ()
  where
    conn rChan = ConOps (sendInReg rChan) sendAccept sendCancel rChan
    
    sendInReg rChan transID remove template = 
      atomically $ writeTChan msgChan 
           (NetInReq netID transID remove template (replyFn rChan))
           
    sendAccept transID =
      atomically $ writeTChan msgChan (NetAccept netID transID)
            
    sendCancel transID =
      atomically $ writeTChan msgChan (NetCancel netID transID)
                             
    replyFn rChan reply = atomically $ writeTChan rChan reply;

--------------------------------------------------------------------------------

createLogServer :: TChan TupleServerMsg ->
                     IO (() -> IO (TChan TupleServerMsg, [TupleServerMsg]))
createLogServer masterChan = do log <- HT.new (==) hashInt
                                (call, entry) <- makeRPC (handleGetLog log)
                                forkIO $ serverLoop entry log
                                return call
  where
    serverLoop entry log = 
      do e <- entry
         r <- atomically $ (Left `fmap` readTChan e)
              `orElse`(Right `fmap` readTChan masterChan)
         case r of
           Left  _ -> serverLoop entry log
           Right t -> handleTrans log t >> serverLoop entry log
                                
                              
    handleGetLog log _ = do newChan <- atomically $ dupTChan masterChan
                            logs    <- HT.toList log
                            return (newChan, Prelude.map snd logs)
    
    handleTrans log (trans@(TSMsgIn tid _ _ _)) = HT.insert log tid trans
    
    handleTrans log (trans@(TSMsgCancel tid))   = HT.delete log tid
    
    handleTrans log (trans@(TSMsgAccept tid _)) = HT.delete log tid 
    
    makeRPC f = do reqChan   <- atomically $ newTChan
                   return (call reqChan, entry reqChan)
      where
        call reqChan arg = do reply <- atomically $ newTChan 
                              atomically $ writeTChan reqChan (arg, reply)
                              (a,b) <- atomically $ readTChan reply
                              return (a,b)
                          
        entry reqChan = do (arg, replyV) <- atomically $ readTChan reqChan
                           (newChan, result) <- f arg
                           atomically $ writeTChan replyV (newChan, result)
                           return newChan
