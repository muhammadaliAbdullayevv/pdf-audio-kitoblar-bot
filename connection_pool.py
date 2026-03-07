"""
Optimized connection pooling for 100+ concurrent users.
"""

import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional
import psycopg2
from psycopg2 import pool
from db import _dsn

logger = logging.getLogger(__name__)

# Global connection pools
_db_pool: Optional[pool.ThreadedConnectionPool] = None
_async_pool: Optional[Dict[str, Any]] = None

def get_optimized_pool(min_connections: int = 10, max_connections: int = 50):
    """Get optimized database connection pool."""
    global _db_pool
    if _db_pool is None or _db_pool.closed:
        try:
            _db_pool = pool.ThreadedConnectionPool(
                minconn=min_connections,
                maxconn=max_connections,
                **_dsn(),
                options="-c statement_timeout=30s -c idle_in_transaction_session_timeout=10s"
            )
            logger.info(f"Optimized DB pool created: {min_connections}-{max_connections} connections")
        except Exception as e:
            logger.error(f"Failed to create DB pool: {e}")
            raise
    return _db_pool

@asynccontextmanager
async def get_db_connection():
    """Async context manager for database connections."""
    pool = get_optimized_pool()
    conn = None
    try:
        # Get connection from pool
        loop = asyncio.get_event_loop()
        conn = await loop.run_in_executor(None, pool.getconn)
        
        # Optimize connection settings
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30s'")
            cur.execute("SET idle_in_transaction_session_timeout = '10s'")
        
        yield conn
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise e
    finally:
        if conn:
            try:
                # Return connection to pool
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, pool.putconn, conn)
            except:
                pass

async def execute_query(query: str, params: tuple = None, fetch: str = "all"):
    """Execute database query with optimized connection handling."""
    async with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            
            if fetch == "all":
                result = cur.fetchall()
            elif fetch == "one":
                result = cur.fetchone()
            elif fetch == "many":
                result = cur.fetchmany()
            else:
                result = None
            
            return result

async def execute_batch(queries: list[tuple[str, tuple]]):
    """Execute multiple queries in a single transaction."""
    async with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                for query, params in queries:
                    cur.execute(query, params)
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                raise e

# Connection pool monitoring
def get_pool_stats() -> Dict[str, Any]:
    """Get connection pool statistics."""
    pool = get_optimized_pool()
    if pool:
        return {
            "min_connections": pool.minconn,
            "max_connections": pool.maxconn,
            "closed": pool.closed,
        }
    return {}

# Health check
async def health_check() -> bool:
    """Check database connection health."""
    try:
        async with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        return False
