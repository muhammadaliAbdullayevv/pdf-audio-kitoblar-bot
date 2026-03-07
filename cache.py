"""
Redis-based distributed caching for 100+ concurrent users performance.
"""

import os
import json
import logging
from typing import Any, Optional, Union
from functools import wraps
import redis

logger = logging.getLogger(__name__)

# Redis connection
_redis_client: Optional[redis.Redis] = None

def get_redis_client() -> Optional[redis.Redis]:
    """Get Redis client instance."""
    global _redis_client
    if _redis_client is None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        try:
            _redis_client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            # Test connection
            _redis_client.ping()
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Using memory cache.")
            _redis_client = None
    return _redis_client

def cache_result(key_prefix: str, ttl: int = 300):
    """Decorator to cache function results in Redis."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = f"{key_prefix}:{hash(str(args) + str(kwargs))}"
            
            # Try to get from Redis
            redis_client = get_redis_client()
            if redis_client:
                try:
                    cached = redis_client.get(cache_key)
                    if cached:
                        logger.debug(f"Cache hit: {cache_key}")
                        return json.loads(cached)
                except Exception as e:
                    logger.debug(f"Redis get failed: {e}")
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Cache result in Redis
            if redis_client:
                try:
                    redis_client.setex(
                        cache_key, 
                        ttl, 
                        json.dumps(result, default=str)
                    )
                    logger.debug(f"Cached result: {cache_key}")
                except Exception as e:
                    logger.debug(f"Redis set failed: {e}")
            
            return result
        return wrapper
    return decorator

def cache_get(key: str) -> Optional[Any]:
    """Get value from Redis cache."""
    redis_client = get_redis_client()
    if not redis_client:
        return None
    
    try:
        value = redis_client.get(key)
        if value:
            return json.loads(value)
    except Exception as e:
        logger.debug(f"Cache get error for {key}: {e}")
    return None

def cache_set(key: str, value: Any, ttl: int = 300) -> bool:
    """Set value in Redis cache."""
    redis_client = get_redis_client()
    if not redis_client:
        return False
    
    try:
        redis_client.setex(key, ttl, json.dumps(value, default=str))
        return True
    except Exception as e:
        logger.debug(f"Cache set error for {key}: {e}")
        return False

def cache_delete(key: str) -> bool:
    """Delete key from Redis cache."""
    redis_client = get_redis_client()
    if not redis_client:
        return False
    
    try:
        redis_client.delete(key)
        return True
    except Exception as e:
        logger.debug(f"Cache delete error for {key}: {e}")
        return False

def cache_clear_pattern(pattern: str) -> int:
    """Clear keys matching pattern from Redis cache."""
    redis_client = get_redis_client()
    if not redis_client:
        return 0
    
    try:
        keys = redis_client.keys(pattern)
        if keys:
            return redis_client.delete(*keys)
    except Exception as e:
        logger.debug(f"Cache clear pattern error for {pattern}: {e}")
    return 0

# Memory cache fallback
_memory_cache: dict[str, dict] = {}

def memory_cache_get(key: str) -> Optional[Any]:
    """Get value from memory cache."""
    if key in _memory_cache:
        entry = _memory_cache[key]
        import time
        if time.time() < entry['expires']:
            return entry['value']
        else:
            del _memory_cache[key]
    return None

def memory_cache_set(key: str, value: Any, ttl: int = 300) -> None:
    """Set value in memory cache."""
    import time
    _memory_cache[key] = {
        'value': value,
        'expires': time.time() + ttl
    }

def memory_cache_delete(key: str) -> bool:
    """Delete key from memory cache."""
    return _memory_cache.pop(key, None) is not None
