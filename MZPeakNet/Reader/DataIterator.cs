namespace MZPeak.Reader;

using System.Collections;
using System.Threading.Tasks;

using Apache.Arrow;
using Apache.Arrow.Ipc;
using MZPeak.Compute;
using System.Threading;


class BaseDataArraysIter
{
    protected BaseLayoutReader LayoutReader;
    protected IArrowArrayStream StreamReader;
    protected ulong? CurrentIndex = null;
    protected bool init = false;
    protected StructArray? CurrentBatch = null;
    protected (ulong, StructArray)? NextItem = null;

    public bool CurrentBatchProcessed;

    public (ulong, StructArray) Current => NextItem == null ? throw new InvalidOperationException() : ((ulong, StructArray))NextItem;

    public BaseDataArraysIter(BaseLayoutReader layoutReader, IArrowArrayStream stream)
    {
        LayoutReader = layoutReader;
        StreamReader = stream;

        CurrentIndex = null;
        CurrentBatch = null;
        NextItem = null;
        CurrentBatchProcessed = false;
    }

    protected ulong? FirstIndexInBatch()
    {
        if (CurrentBatch == null) return null;
        var idxCol = (UInt64Array)CurrentBatch.Fields[0];
        if (idxCol.Length == 0) return null;
        return idxCol.GetValue(0);
    }

    protected bool BatchHasCurrentIndex()
    {
        if (CurrentBatch == null || CurrentIndex == null) return false;
        var idx = Compute.BinarySearch((UInt64Array)CurrentBatch.Fields[0], (ulong)CurrentIndex);
        if (idx == -1) return false;
        return ((UInt64Array)CurrentBatch.Fields[0]).GetValue(idx) == (ulong)CurrentIndex;
    }

    public ulong? BatchMaxIndex()
    {
        if (CurrentBatch == null) return null;
        var arr = (UInt64Array)CurrentBatch.Fields[0];
        var valIdx = Compute.LastNotNull(arr);
        if (valIdx.HasValue) return valIdx.Value.Item1;
        else return null;
    }

    protected bool InitializeInner()
    {
        if (CurrentBatch == null)
        {
            return false;
        }
        var idxCol = (UInt64Array)CurrentBatch.Fields[0];
        CurrentIndex = Compute.Min(idxCol);
        init = true;
        return init;
    }

    protected (int, int, SliceIndex, StructArray)? ExtractCurrentIndexWithinCurrentBatch()
    {
        if (CurrentBatch == null || CurrentIndex == null) return null;
        var span = Compute.BinarySearchBetween((UInt64Array)CurrentBatch.Fields[0], (ulong)CurrentIndex);
        var lastPossibleRowIndex = CurrentBatch.Length - 1;
        int n;
        StructArray chunk;
        if (span == null)
        {
            n = CurrentBatch.Length;
            span = SliceIndex.Empty;
            chunk = (StructArray)CurrentBatch.Slice(0, 0);
        }
        else
        {
            int start = span.Start;
            n = span.Count;
            chunk = (StructArray)CurrentBatch.Slice(start, n);
        }
        return (n, lastPossibleRowIndex, span, chunk);
    }

    public void ProcessNextBatch()
    {
        if (NextItem.HasValue && !CurrentBatchProcessed)
        {
            var batch = LayoutReader.ProcessSegment(NextItem.Value.Item1, NextItem.Value.Item2);
            NextItem = (NextItem.Value.Item1, batch);
            CurrentBatchProcessed = true;
        }
    }
}


class AsyncDataArraysIter : BaseDataArraysIter, IAsyncEnumerator<(ulong, StructArray)>, IAsyncEnumerable<(ulong, StructArray)>
{
    public CancellationToken CancellationToken;

    public AsyncDataArraysIter(BaseLayoutReader layoutReader, IArrowArrayStream stream) : base(layoutReader, stream)
    {
        CancellationToken = default;
    }

    public async ValueTask<bool> ReadNextBatch(bool updateIndex = false)
    {
        CurrentBatch = null;
        var batch = await StreamReader.ReadNextRecordBatchAsync(CancellationToken);
        if (batch == null)
        {
            return false;
        }

        var root = batch.Column(0);

        var rootStruct = (StructArray?)root;
        if (rootStruct == null)
        {
            return false;
        }

        CurrentBatch = rootStruct;

        var idxCol = (UInt64Array)CurrentBatch.Fields[0];
        var lowestIndex = Compute.Min(idxCol);
        if (updateIndex && ((CurrentIndex != null && lowestIndex > CurrentIndex) || CurrentIndex == null))
        {
            CurrentIndex = lowestIndex;
        }
        return true;
    }

    async Task<bool> Initialize()
    {
        if (!await ReadNextBatch()) return false;
        return InitializeInner();
    }

    async ValueTask<StructArray?> ExtractForCurrentIndex()
    {
        var extracted = ExtractCurrentIndexWithinCurrentBatch();
        if (extracted == null || CurrentBatch == null) return null;
        var (n, lastPossibleRowIndex, indices, chunk) = extracted.Value;

        if (n == CurrentBatch.Length || indices.Contains(lastPossibleRowIndex))
        {
            if (await ReadNextBatch(false))
            {
                if (BatchHasCurrentIndex())
                {
                    var rest = await ExtractForCurrentIndex();
                    if (rest != null)
                        chunk = (StructArray)ArrowArrayConcatenator.Concatenate([chunk, rest]);
                }
            }
        }
        else
        {
            CurrentBatch = (StructArray)CurrentBatch.Slice(n, CurrentBatch.Length - n);
        }
        return chunk;
    }

    public async ValueTask<bool> MoveNextAsyncWithProcess(bool doProcess)
    {
        if (CurrentIndex == null)
        {
            if (!await Initialize()) return false;
        }
        if (CurrentIndex == null) return false;
        var nextBatch = await ExtractForCurrentIndex();
        if (nextBatch == null) return false;

        NextItem = ((ulong)CurrentIndex, nextBatch);
        CurrentBatchProcessed = false;
        if (doProcess) ProcessNextBatch();
        var nextIndex = FirstIndexInBatch();
        if (nextIndex < CurrentIndex) throw new InvalidDataException($"Next index {nextIndex} < current index {CurrentIndex}");
        CurrentIndex = nextIndex;
        return true;
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        return await MoveNextAsyncWithProcess(true);
    }

    public ValueTask DisposeAsync()
    {
        return new ValueTask();
    }

    public IAsyncEnumerator<(ulong, StructArray)> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        CancellationToken = cancellationToken;
        return this;
    }
}


/// <summary>
/// A seekable, peekable iterator over a batch stream
/// </summary>
public class PeekableAsyncDataArraysIter : IAsyncEnumerator<(ulong, StructArray)>, IAsyncEnumerable<(ulong, StructArray)>
{
    AsyncDataArraysIter Inner;
    LinkedList<(ulong, StructArray)> Peeked;
    (ulong, StructArray)? Value;

    public (ulong, StructArray) Current => Value != null ? Value.Value : Peeked.First == null ? throw new InvalidOperationException() : Peeked.First.Value;

    public PeekableAsyncDataArraysIter(BaseLayoutReader layoutReader, IArrowArrayStream stream)
    {
        Inner = new AsyncDataArraysIter(layoutReader, stream);
        Peeked = [];
        Value = null;
    }

    /// <summary>
    /// Peek at the *next* value in the queue, not the *current* value.
    ///
    /// This may trigger I/O and/or consume
    /// </summary>
    /// <returns>The next value or <c>null</c></returns>
    public async ValueTask<(ulong, StructArray)?> Peek()
    {
        if (Peeked.Count == 0)
            await NextFromInner();
        return Peeked.First?.Value;
    }

    /// <summary>
    /// Pull the next value from the inner iterator and add it to the internal queue
    /// </summary>
    /// <returns></returns>
    async ValueTask<bool> NextFromInner()
    {
        if (await Inner.MoveNextAsync())
        {
            Peeked.AddLast(Inner.Current);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Put a value back into the queue. This becomes the *current* value
    /// </summary>
    /// <param name="value"></param>
    public void Prepend((ulong, StructArray) value)
    {
        if (Value != null)
            Peeked.Prepend(Value.Value);
        Value = value;
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        if (Peeked.First != null)
        {
            Value = Peeked.First.Value;
            Peeked.RemoveFirst();
            return true;
        }
        else
        {
            if (await NextFromInner())
            {
                if (Peeked.First == null) throw new InvalidOperationException();
                Value = Peeked.First.Value;
                Peeked.RemoveFirst();
                return true;
            }
            return false;
        }
    }

    public ValueTask DisposeAsync()
    {
        return new ValueTask();
    }

    /// <summary>
    /// Peek at the *next* value's index slot if one exists
    /// </summary>
    /// <returns></returns>
    public async Task<ulong?> PeekIndex()
    {
        var value = await Peek();
        return value?.Item1;
    }

    /// <summary>
    /// Consume the iterator until the *next* value's index is greater than or equal to the requested index.
    /// </summary>
    /// <param name="index"></param>
    /// <returns>Whether the next index matches <c>index</c></returns>
    /// <exception cref="InvalidOperationException">
    /// If the <c>index</c> is < <cref>PeekeIndex</cref>
    /// </exception>
    public async Task<bool> Seek(ulong index)
    {
        var currentIndex = await PeekIndex();
        if (index < currentIndex)
            throw new InvalidOperationException($"Cannot move an iterator to an earlier position in the stream. Current index is {currentIndex}, requested {index}");
        if (index == currentIndex)
            return true;
        if (currentIndex == null)
            return false;
        // while(true) {
        //     var maxIdx = Inner.BatchMaxIndex();
        //     if (maxIdx == null || maxIdx >= index) break;
        //     await Inner.ReadNextBatch(true);
        //     Peeked.Clear();
        // }
        (ulong, StructArray)? currentValue = null;
        while (await PeekIndex() < index)
        {
            if (await MoveNextAsync())
                currentValue = Value;
            else
                break;
        }
        if (currentValue.HasValue) Prepend(currentValue.Value);
        return await PeekIndex() == index;
    }

    /// <summary>
    /// Consume the next value from the iterator and return it
    /// </summary>
    /// <returns></returns>
    public async Task<(ulong, StructArray)?> Consume()
    {
        return await MoveNextAsync() ? Value : null;
    }

    public IAsyncEnumerator<(ulong, StructArray)> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        Inner.CancellationToken = cancellationToken;
        return this;
    }
}


class SyncDataArraysIter : BaseDataArraysIter, IEnumerator<(ulong, StructArray)>, IEnumerable<(ulong, StructArray)>
{
    object IEnumerator.Current => Current;

    public SyncDataArraysIter(BaseLayoutReader layoutReader, IArrowArrayStream stream) : base(layoutReader, stream) { }

    public bool ReadNextBatch(bool updateIndex = false)
    {
        CurrentBatch = null;
        var batch = StreamReader.ReadNextRecordBatchAsync().Result;
        if (batch == null)
        {
            return false;
        }

        var root = batch.Column(0);

        var rootStruct = (StructArray?)root;
        if (rootStruct == null)
        {
            return false;
        }

        CurrentBatch = rootStruct;

        var idxCol = (UInt64Array)CurrentBatch.Fields[0];
        // var lowestIndex = Compute.Min(idxCol);
        var lowestIndexI = Compute.FirstNotNull(idxCol);
        if (!lowestIndexI.HasValue) return false;
        var lowestIndex = lowestIndexI.Value.Item1;
        if (updateIndex && ((CurrentIndex != null && lowestIndex > CurrentIndex) || CurrentIndex == null))
        {
            CurrentIndex = lowestIndex;
        }
        return true;
    }

    bool Initialize()
    {
        if (!ReadNextBatch()) return false;
        return InitializeInner();
    }

    StructArray? ExtractForCurrentIndex()
    {
        var extracted = ExtractCurrentIndexWithinCurrentBatch();
        if (extracted == null || CurrentBatch == null) return null;
        var (n, lastPossibleRowIndex, indices, chunk) = extracted.Value;

        if (n == CurrentBatch.Length || indices.Contains(lastPossibleRowIndex))
        {
            if (ReadNextBatch(false))
            {
                if (BatchHasCurrentIndex())
                {
                    var rest = ExtractForCurrentIndex();
                    if (rest != null)
                        chunk = (StructArray)ArrowArrayConcatenator.Concatenate([chunk, rest]);
                }
            }
        }
        else
        {
            CurrentBatch = (StructArray)CurrentBatch.Slice(n, CurrentBatch.Length - n);
        }
        return chunk;
    }

    public bool MoveNextWithProcess(bool doProcess)
    {
        if (CurrentIndex == null)
        {
            if (!Initialize())
            {
                return false;
            }
        }
        if (CurrentIndex == null)
        {
            return false;
        }
        var nextBatch = ExtractForCurrentIndex();
        if (nextBatch == null)
        {
            return false;
        }

        NextItem = ((ulong)CurrentIndex, nextBatch);
        CurrentBatchProcessed = false;
        if (doProcess) ProcessNextBatch();
        var nextIndex = FirstIndexInBatch();

        if (nextIndex < CurrentIndex) throw new InvalidDataException($"Next index {nextIndex} < current index {CurrentIndex}");
        CurrentIndex = nextIndex;
        return true;
    }

    public bool MoveNext()
    {
        return MoveNextWithProcess(true);
    }

    public void Reset()
    {
        throw new NotSupportedException();
    }

    public void Dispose()
    {
        return;
    }

    public IEnumerator<(ulong, StructArray)> GetEnumerator()
    {
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}


/// <summary>
/// A seekable, peekable iterator over a batch stream that blocks the calling thread.
/// </summary>
public class PeekableSyncDataArraysIter : IEnumerator<(ulong, StructArray)>, IEnumerable<(ulong, StructArray)>
{
    SyncDataArraysIter Inner;
    LinkedList<(ulong, StructArray)> Peeked;
    (ulong, StructArray)? Value;

    public (ulong, StructArray) Current => Value != null ? Value.Value : Peeked.First == null ? throw new InvalidOperationException() : Peeked.First.Value;

    object IEnumerator.Current => Current;

    public PeekableSyncDataArraysIter(BaseLayoutReader layoutReader, IArrowArrayStream stream)
    {
        Inner = new SyncDataArraysIter(layoutReader, stream);
        Peeked = [];
        Value = null;
    }

    /// <summary>
    /// Peek at the *next* value in the queue, not the *current* value.
    ///
    /// This may trigger I/O and/or consume
    /// </summary>
    /// <returns>The next value or <c>null</c></returns>
    public (ulong, StructArray)? Peek()
    {
        if (Peeked.Count == 0)
            NextFromInner();
        return Peeked.First?.Value;
    }

    /// <summary>
    /// Pull the next value from the inner iterator and add it to the internal queue
    /// </summary>
    /// <returns></returns>
    bool NextFromInner()
    {
        if (Inner.MoveNext())
        {
            Peeked.AddLast(Inner.Current);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Put a value back into the queue. This becomes the *current* value
    /// </summary>
    /// <param name="value"></param>
    public void Prepend((ulong, StructArray) value)
    {
        if (Value != null)
            Peeked.Prepend(Value.Value);
        Value = value;
    }

    public bool MoveNext()
    {
        if (Peeked.First != null)
        {
            Value = Peeked.First.Value;
            Peeked.RemoveFirst();
            return true;
        }
        else
        {
            if (NextFromInner())
            {
                if (Peeked.First == null) throw new InvalidOperationException();
                Value = Peeked.First.Value;
                Peeked.RemoveFirst();
                return true;
            }
            return false;
        }
    }

    public void Dispose()
    {
        return;
    }

    /// <summary>
    /// Peek at the *next* value's index slot if one exists
    /// </summary>
    /// <returns></returns>
    public ulong? PeekIndex()
    {
        var value = Peek();
        return value?.Item1;
    }

    /// <summary>
    /// Consume the iterator until the *next* value's index is greater than or equal to the requested index.
    /// </summary>
    /// <param name="index"></param>
    /// <returns>Whether the next index matches <c>index</c></returns>
    /// <exception cref="InvalidOperationException">
    /// If the <c>index</c> is < <cref>PeekeIndex</cref>
    /// </exception>
    public bool Seek(ulong index)
    {
        var currentIndex = PeekIndex();
        if (index < currentIndex)
            throw new InvalidOperationException($"Cannot move an iterator to an earlier position in the stream. Current index is {currentIndex}, requested {index}");
        if (index == currentIndex)
            return true;
        if (currentIndex == null)
            return false;
        (ulong, StructArray)? currentValue = null;
        while (PeekIndex() < index)
        {
            if (MoveNext())
                currentValue = Value;
            else
                break;
        }
        if (currentValue.HasValue) Prepend(currentValue.Value);
        return PeekIndex() == index;
    }

    /// <summary>
    /// Consume the next value from the iterator and return it
    /// </summary>
    /// <returns></returns>
    public (ulong, StructArray)? Consume()
    {
        return MoveNext() ? Value : null;
    }

    public void Reset()
    {
        throw new NotSupportedException();
    }

    public IEnumerator<(ulong, StructArray)> GetEnumerator()
    {
        return this;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
