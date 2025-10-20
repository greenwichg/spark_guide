## Batch and Stream Processing

**One of the data engineer's responsibilities is to capture the real-life data, add spices, cook, and serve it. There are different ways to do it. We wait for the collected data to reach a certain threshold (e.g., daily, weekly,…), then process them all at once. This is called batch processing. That might be too long. Instead of waiting, we can process a piece of data right after it happens. After completing this piece, the next one will follow, and things will continue to unfold in this manner. This is called stream processing.**

## tl;dr
**Batch: A Simple and more familiar data processing paradigm. The complete view of the data simplifies the data processing and re-processing. However, users have to wait → High latency.**

**Stream: Far lower latency and treat data as an unbounded flow. To facilitate efficient and reliable data processing, users must consider several key aspects, including windowing, event-time vs processing time, watermark state, and checkpointing. This approach, however, introduces a higher learning curve. Implement this only if your organization truly gets benefit from stream processing.**
