using Apache.Arrow;
using MZPeak.Storage;

namespace MZPeak.Writer.Visitors;


public interface IArrowBuilder<T>
{
    public void AppendNull();

    public void Append(T value);

    public List<Field> ArrowType();

    public List<IArrowArray> Build();

    public RecordBatch BuildRecordBatch(IEnumerable<KeyValuePair<string, string>>? metadata=null)
    {
        var fields = ArrowType();
        var arrays = Build();
        var schema = new Schema(fields, metadata ?? []);
        return new RecordBatch(schema, arrays, arrays[0].Length);
    }

    /// <summary>
    /// Clear the member builders and reset them to an empty state.
    ///
    /// When building nested list arrays, they SHOULD immediately call Append() to create an
    /// offset = 0 entry that is required for compatibility with the C++ implementation.
    /// See <a>https://github.com/apache/arrow-dotnet/discussions/321</a>
    /// </summary>
    public void Clear();

    public int Length { get; }

    public List<ColumnMapping> ColumnMappings();
}

