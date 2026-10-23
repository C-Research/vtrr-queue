-- Reclaim expired leases (also runs at the start of every dequeue). Returns the re-enqueued task ids.
return vtrr_reclaim(redis_now())
