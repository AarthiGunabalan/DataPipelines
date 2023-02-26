
1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Changed maxOffsetsPerTrigger to limit the number of records to fetch per trigger
Changed maxRatePerPartition to set the maximum number of messages per partition per batch.


2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
startingOffsets determined how the records are to be processed in the stream after its processed by one consumer
Changed maxOffsetsPerTrigger to limit the number of records to fetch per trigger. Found 200 to be optimal. 
Changed maxRatePerPartition to set the maximum number of messages per partition per batch.
