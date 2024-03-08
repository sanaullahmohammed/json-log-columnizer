using System.Collections.Concurrent;

public class ClosableConcurrentQueue<T> : ConcurrentQueue<T>
{
    private bool isClosed = false;

    public bool IsClosed => isClosed;

    public void Close()
    {
        isClosed = true;
    }

    public new void Enqueue(T item)
    {
        if (isClosed)
        {
            throw new InvalidOperationException("Queue is closed and cannot be enqueued.");
        }

        base.Enqueue(item);
    }
}