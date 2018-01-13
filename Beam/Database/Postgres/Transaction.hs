module Beam.Database.Postgres.Transaction where

{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module App.Back.Db (
    Transaction,
    Conn,
    MonadQuery,
    unsafeLiftTransaction,
    unsafeLiftIO,
    runTransaction, runTransaction', runQuery, runQuery',
    newConnPool,

    MonadBeam
  ) where

import           Control.Exception.Safe                 (MonadThrow)
import           Data.Pool                              (Pool, createPool, withResource)
import           Database.Beam                          (MonadBeam, withDatabase, withDatabaseDebug)
import qualified Database.PostgreSQL.Simple             as Psql
import           Database.PostgreSQL.Simple.Transaction (withTransactionSerializable)


data TransactionEnv = TransactionEnv{
  _transactionEnvConn   :: Psql.Connection,
  _transactionEnvLogger :: Maybe (String -> IO ())
  } deriving (Generic)

newtype Transaction mode a = Transaction (ReaderT TransactionEnv IO a)
                    deriving (Functor, Applicative, Monad, MonadThrow)

type Conn = Pool Psql.Connection

-- | A simplified constraint for monads that are Beam queries.
type MonadQuery syntax be m = MonadBeam syntax be Psql.Connection m

-- | Runs a database transaction with full serialization picking the connection from a pool
--   using no logging.
runTransaction :: MonadIO io => Conn -> Transaction mode a -> io a
runTransaction connPool = runTransaction' connPool Nothing


-- | Runs a database transaction with full serialization picking the connection from a pool
--   and using the given [optional] logger for database queries.
runTransaction' :: MonadIO io => Conn -> Maybe (String -> IO ()) -> Transaction mode a -> io a
runTransaction' connPool logger_ (Transaction act) = io $ withResource connPool $ \conn_ ->
  withTransactionSerializable conn_ $
    runReaderT act (TransactionEnv conn_ logger_)


-- | Lifts an action using a database connection into a 'Db' action. This is generally unsafe because the
-- IO action may be repeated multiple times if the transaction needs to be retried. Use with caution!
unsafeLiftTransaction :: (Psql.Connection -> IO a) -> Transaction mode a
unsafeLiftTransaction act = Transaction $ do
  conn <- asks _transactionEnvConn
  io $ act conn


-- | Lifts an arbitrary IO action into a 'Db' action. This is generally unsafe because the IO action may
-- be repeated multiple times if the transaction needs to be retried. Use with caution!
unsafeLiftIO :: IO a -> Transaction mode a
unsafeLiftIO = Transaction . io


-- | Runs a Beam query in a 'Transaction' using the Transaction's configured logger.
runQuery :: MonadQuery syntax be m => m a -> Transaction mode a
runQuery act = Transaction $ do
  TransactionEnv{..} <- ask
  io $ case _transactionEnvLogger of
    Nothing     -> withDatabase _transactionEnvConn act
    Just logger -> withDatabaseDebug logger _transactionEnvConn act


-- | Runs a Beam query in a 'Transaction' but overrides the transaction logger.
runQuery' :: MonadQuery syntax be m => (String -> IO ()) -> m a -> Transaction mode a
runQuery' logger act = Transaction $ do
  TransactionEnv{_transactionEnvConn} <- ask
  io $ withDatabaseDebug logger _transactionEnvConn act


-- | Builds a new PostgreSQL connection pool with default settings.
newConnPool :: MonadIO io => IO Psql.Connection -> io (Pool Psql.Connection)
newConnPool mkConnection = io $ createPool
  mkConnection
  Psql.close
  stripeCount
  maxResourceAge
  maxResourceCount
  where
    stripeCount      = 2
    maxResourceAge   = 60 -- seconds
    maxResourceCount = 5

