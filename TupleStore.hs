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

module TupleStore (
  ID,
  Match(..),
  Bindings,
  TupleStore(..),
  
  createTupleStore,
  addTS,
  inputTS,
  cancelTS,
  removeTS
  ) where

import Control.Concurrent 
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan

import Control.Concurrent.MVar

import Data.HashTable as HT
import Data.Map as MP

import Data.Maybe

--------------------------------------------------------------------------------

import TupleType
import NetworkTuple

--------------------------------------------------------------------------------

type ID = (NetID, Int)

data Match a = Match ID (NetReply -> IO ()) a

type Bindings = [AtomicVal]

data Bucket = Bucket [Match Template] -- waiting
                     [(ID, Tuple)]    -- holds
                     [Tuple]          -- items

data QueryStatus = Held | Waiting

data TupleStore = TS (HT.HashTable ID (QueryStatus,Bucket)) 
                     (HT.HashTable AtomicVal Bucket)

--------------------------------------------------------------------------------

createTupleStore :: () -> IO TupleStore
createTupleStore _ = do queries <- HT.new (==) hashValQueryTbl
                        tuples  <- HT.new (==) hashValTupleTbl
                        return (TS queries tuples) 
  where
    hashValTupleTbl (IVal i)     = hashInt i
    hashValTupleTbl (BVal False) = 0
    hashValTupleTbl (BVal True)  = 1
    hashValTupleTbl (SVal s)     = hashString s
    
    hashValQueryTbl (_,id)       = hashInt id
    
--------------------------------------------------------------------------------    
    
addTS :: TupleStore -> Tuple -> IO (Maybe (Match Bindings))
addTS (TS queries tuples) tuple@(TupleRep key _) = 
          do mb <- HT.lookup tuples key
             case mb of
               Nothing -> do HT.insert tuples key (Bucket [] [] [tuple])
                             return Nothing
               Just (Bucket waiting holds items) -> 
                 let scan [] _ = 
                       do _ <- HT.update tuples 
                                         key 
                                         (Bucket waiting holds (tuple : items))
                          return Nothing
                     scan (x@(Match id reply ext):r) wl = 
                       case match tuple ext of
                         Nothing -> scan r (x:wl)
                         Just binds -> 
                           do let bucket = (Bucket (r ++ wl) 
                                                   ((id, tuple):holds) 
                                                   items)
                              HT.update tuples key bucket
                              HT.insert queries id (Held, bucket)
                              return $ Just (Match id reply binds)
                 in scan waiting []                    
                    
                    

inputTS :: TupleStore -> Match Template -> IO (Maybe (Match Bindings))
inputTS (TS queries tuples) (Match id reply ext@(TupleRep key _)) = 
  do mb <- HT.lookup tuples key
     case mb of
       Nothing -> do let bucket = Bucket [Match id reply ext] [] []
                     HT.insert tuples key bucket
                     HT.insert queries id (Waiting, bucket)
                     return Nothing
       Just (Bucket waiting holds items) -> 
         let look _ [] = 
               do let bucket = Bucket (waiting ++ [Match id reply ext]) 
                                      holds 
                                      items
                  HT.update tuples key bucket
                  HT.insert queries id (Waiting, bucket)
                  return Nothing
             look prefix (item : r) = 
               case match item ext of
                    Nothing    -> look (item:prefix) r
                    Just binds -> 
                      do let bucket = Bucket waiting 
                                             ((id, item):holds) 
                                             (prefix ++ r)
                         HT.update tuples key bucket
                         HT.insert queries id (Held, bucket)
                         return $ Just (Match id reply binds)
         in look [] items



cancelTS ts@(TS queries tuples) id =
  do mb <- HT.lookup queries id
     result <- case mb of
       Nothing -> error "unexpected lookup failure cancelTS"
       
       Just (Waiting, Bucket waiting holds items) -> 
         do case Prelude.foldr (\m@(Match id' reply ext) (ws,rs) -> 
                                   if id == id' 
                                   then (ws, m:rs) 
                                   else (m : ws, rs)) ([], []) waiting of
              ([], (Match _ _ (TupleRep key _)):_) -> 
                if Prelude.null items && Prelude.null holds
                then HT.delete tuples key >> return Nothing
                else do HT.update tuples key (Bucket [] holds items)
                        return Nothing
              (ws@((Match _ _ (TupleRep key _)):_), _) -> 
                HT.update tuples key (Bucket ws holds items) >> return Nothing
              
       Just (Held, Bucket waiting holds items) -> 
         do let ([(_,tuple)],hs) = Prelude.foldr (\t@(id',_) (ts,hs) -> 
                                           if id == id' 
                                           then ([t],hs)
                                           else (ts,t:hs)) ([],[]) holds
            addTS ts tuple
     HT.delete queries id
     return result
     


removeTS ts@(TS queries tuples) id =
  do mb <- HT.lookup queries id
     result <- case mb of
       Nothing -> error "unexpected lookup failure removeTS"
       
       Just (_, Bucket waiting holds items) -> 
         do let ([(_,TupleRep key _)],hs) = Prelude.foldr (\t@(id',_) (ts,hs) -> 
                                           if id == id' 
                                           then ([t],hs)
                                           else (ts,t:hs)) ([],[]) holds
            if Prelude.null hs && Prelude.null items && Prelude.null waiting
               then HT.delete tuples key
               else HT.update tuples key (Bucket waiting hs items) >> return ()
     HT.delete queries id
