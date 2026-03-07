"""
Async optimizations for 100+ concurrent users performance.
"""

import asyncio
import logging
from functools import wraps
from typing import Any, Callable, Awaitable
from concurrent.futures import ThreadPoolExecutor
import time

logger = logging.getLogger(__name__)

# Dedicated executor for I/O operations
_io_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="io")

# Dedicated executor for CPU-intensive operations
_cpu_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="cpu")

def async_io(func: Callable) -> Callable[..., Awaitable]:
    """Decorator to run I/O-bound functions in dedicated thread pool."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_io_executor, func, *args, **kwargs)
    return wrapper

def async_cpu(func: Callable) -> Callable[..., Awaitable]:
    """Decorator to run CPU-bound functions in dedicated thread pool."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_cpu_executor, func, *args, **kwargs)
    return wrapper

async def gather_with_concurrency(*coroutines, max_concurrency: int = 10):
    """Run coroutines with limited concurrency to prevent resource exhaustion."""
    semaphore = asyncio.Semaphore(max_concurrency)
    
    async def limited_coro(coro):
        async with semaphore:
            return await coro
    
    limited_coros = [limited_coro(coro) for coro in coroutines]
    return await asyncio.gather(*limited_coros, return_exceptions=True)

class AsyncCache:
    """Simple async in-memory cache with TTL."""
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
    
    async def get(self, key: str) -> Any:
        """Get value from cache if not expired."""
        if key in self._cache and key in self._ttl:
            if time.time() < self._ttl[key]:
                return self._cache[key]
            else:
                await self.delete(key)
        return None
    
    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set value in cache with TTL."""
        self._cache[key] = value
        self._ttl[key] = time.time() + ttl
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        return self._cache.pop(key, None) is not None
    
    async def clear_expired(self) -> int:
        """Clear expired entries."""
        current_time = time.time()
        expired_keys = [k for k, expiry in self._ttl.items() if expiry < current_time]
        for key in expired_keys:
            await self.delete(key)
        return len(expired_keys)

# Global async cache instance
async_cache = AsyncCache()

def batch_processor(batch_size: int = 100):
    """Decorator to process items in batches for better performance."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(items: list[Any], *args, **kwargs):
            if not items:
                return []
            
            results = []
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_result = await func(batch, *args, **kwargs)
                results.extend(batch_result if isinstance(batch_result, list) else [batch_result])
            
            return results
        return wrapper
    return decorator

# Performance monitoring
class PerformanceMonitor:
    def __init__(self):
        self.metrics: Dict[str, list[float]] = {}
    
    def record(self, operation: str, duration: float):
        """Record operation duration."""
        if operation not in self.metrics:
            self.metrics[operation] = []
        self.metrics[operation].append(duration)
        
        # Keep only last 100 measurements
        if len(self.metrics[operation]) > 100:
            self.metrics[operation] = self.metrics[operation][-100:]
    
    def get_stats(self, operation: str) -> dict:
        """Get performance statistics for an operation."""
        if operation not in self.metrics or not self.metrics[operation]:
            return {}
        
        durations = self.metrics[operation]
        return {
            "count": len(durations),
            "avg": sum(durations) / len(durations),
            "min": min(durations),
            "max": max(durations),
        }

# Global performance monitor
perf_monitor = PerformanceMonitor()

def monitor_performance(operation_name: str):
    """Decorator to monitor function performance."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                perf_monitor.record(operation_name, duration)
                if duration > 1.0:  # Log slow operations
                    logger.warning(f"Slow operation: {operation_name} took {duration:.2f}s")
        return wrapper
    return decorator

# Cleanup function
async def cleanup():
    """Cleanup resources."""
    _io_executor.shutdown(wait=True)
    _cpu_executor.shutdown(wait=True)
    await async_cache.clear_expired()
