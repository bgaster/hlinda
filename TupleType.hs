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

module TupleType (
  AtomicVal(..),
  AtomicPat(..),
  TupleRep(..),
  Tuple,
  Template,
  
  encodeTuple,
  decodeTuple,
  encodeTemplate,
  decodeTemplate,
  encodeValues,
  decodeValues,
  
  integer,
  parseTemplate,
  parseAtomicVal,
  
  match
  ) where

import Control.Monad.Identity
import Debug.Trace
import Text.Parsec
import Text.Parsec.Combinator
import Text.Parsec.Language
import qualified Text.Parsec.Token as T

--------------------------------------------------------------------------------

data AtomicVal = IVal Int
               | SVal String
               | BVal Bool
           deriving (Eq, Show)
                    
{- 
data AtomicPat 

We differ slightly from Reppy's original implementation to provide support for 
wildcard formal bindings. In partiuclar, WFormal and WFormals can be 
used to bind any value or all remaining values, respectively.
-}
                    
data AtomicPat = IPat Int    -- Match and discard integer value
               | SPat String -- Match and discard string value
               | BPat Bool   -- Match and discard boolean value
               | WPat        -- Match and discard any value                 
               | IFormal     -- Match and bind integer value
               | SFormal     -- Match and bind string value
               | BFormal     -- Match and bind boolean value
               | WFormals    -- Match and bind current and all remaining values
               | WFormal     -- Match and bind a single value
           deriving (Eq, Show)

data TupleRep a = TupleRep AtomicVal [a] 
           deriving (Eq, Show)
                                
type Tuple = TupleRep AtomicVal

type Template = TupleRep AtomicPat

--------------------------------------------------------------------------------

type Parser = Parsec String ()
                       
tupleStyle  :: LanguageDef st
tupleStyle   = emptyDef
                { T.commentStart   = "/*"
                , T.commentEnd     = "*/"
                , T.commentLine    = "//"
                , T.nestedComments = False
                , T.identStart     = letter <|> char '$'
                , T.identLetter    = alphaNum <|> char '$'
                , T.reservedNames  = [ "IVal", "SVal", "BVal", 
                                       "True", "False",
                                       "IPat", "SPat", "BPat", 
                                       "IFormal", "SFormal", "BFormal", "Wild",
                                       "WFormal", "WFormals",
                                       "TupleRep"]
                , T.reservedOpNames= []
                , T.caseSensitive  = True
                }

lexer = T.makeTokenParser tupleStyle

reserved      = T.reserved lexer
integer       = T.integer lexer
stringLiteral = T.stringLiteral lexer
whiteSpace    = T.whiteSpace lexer

parseAtomicVal :: Parser AtomicVal
parseAtomicVal = choice [ bVal, iVal, sVal ] 
  where iVal = do reserved "IVal" 
                  whiteSpace 
                  i <- fromIntegral `fmap` integer
                  return $ IVal i
                  
        sVal = reserved "SVal" >> whiteSpace >> stringLiteral >>= return . SVal
        
        bVal = do reserved "BVal"
                  whiteSpace 
                  b <- choice [mapM char "True" >> return True, 
                               mapM char "False" >> return False]
                  return $ BVal b                    
               
parseAtomicPat :: Parser AtomicPat
parseAtomicPat = choice [ iPat, sPat, bPat, 
                          mapM char "IFormal" >> return IFormal,  
                          mapM char "SFormal" >> return SFormal,
                          mapM char "BFormal" >> return BFormal,
                          try $ reserved "WFormals" >> return WFormals,
                          reserved "WFormal" >> return WFormal] 
  where iPat = do reserved "IPat" 
                  whiteSpace 
                  p <- fromIntegral `fmap` integer 
                  return $ IPat p
               
        sPat = reserved "SPat" >> whiteSpace >> stringLiteral >>= return . SPat
        
        bPat = do reserved "BPat"
                  whiteSpace  
                  b <- choice [mapM char "True" >> return True, 
                               mapM char "False" >> return False]
                  return $ BPat b

parseTuple :: Parser Tuple
parseTuple = do av  <- parseAtomicVal
                avs <- many parseAtomicVal
                return (TupleRep av avs)
                
parseTemplate :: Parser Template
parseTemplate = do av  <- parseAtomicVal
                   avs <- many parseAtomicPat
                   return (TupleRep av avs)
             
--------------------------------------------------------------------------------

encodeTuple :: Tuple -> String
encodeTuple (TupleRep av avs) = show av ++ foldr (\x xs -> show x ++ xs) "" avs

decodeTuple :: String -> Tuple 
decodeTuple input = case runParser parseTuple () "" input of
                      Left _  -> error "failed to decode tuple"
                      Right t -> t                        

encodeTemplate :: Template -> String
encodeTemplate (TupleRep av avs) = show av ++ foldr (\x xs -> show x ++ xs) "" avs

decodeTemplate :: String -> Template
decodeTemplate input = case runParser parseTemplate () "" input of
                         Left _  -> error "failed to decode template"
                         Right t -> t                        

encodeValues :: [AtomicVal] -> String
encodeValues = concat . Prelude.map show

decodeValues :: String -> [AtomicVal]
decodeValues input = case runParser (many parseAtomicVal) () "" input of
                         Left _  -> error "failed to decode values"
                         Right t -> t                        

--------------------------------------------------------------------------------

match :: Tuple -> Template -> Maybe [AtomicVal]
match (TupleRep av avs) (TupleRep av' aps) = 
  if av == av'
  then matchAux avs aps (Just [])
  else Nothing
  where
    matchAux _      _               Nothing = Nothing
    matchAux (_:xs)  []              _      = Nothing
    matchAux []     (_:ys)           _      = Nothing
    matchAux xs     [WFormals] (Just binds) = Just $ binds ++ xs 
    
    matchAux (x:xs) (y:ys)          binds   = 
      matchAux xs ys (matchAtomics binds x y)
    
    matchAux []     []              binds        = binds
    
    matchAtomics Nothing      _          _                     = Nothing
    matchAtomics (Just binds) (IVal iv)  (IPat ip) | iv == ip  = Just binds
                                                   | otherwise = Nothing
    matchAtomics (Just binds) (SVal sv)  (SPat sp) | sv == sp  = Just binds
                                                   | otherwise = Nothing
    matchAtomics (Just binds) (BVal bv)  (BPat bp) | bv == bp  = Just binds
                                                   | otherwise = Nothing
    matchAtomics (Just binds) x@(IVal _) (IFormal)             = Just (x:binds)
    matchAtomics (Just binds) x@(SVal _) (SFormal)             = Just (x:binds)
    matchAtomics (Just binds) x@(BVal _) (BFormal)             = Just (x:binds)
    matchAtomics (Just binds) (_)        (WFormal)             = Just binds
    matchAtomics _            _          _                     = Nothing

