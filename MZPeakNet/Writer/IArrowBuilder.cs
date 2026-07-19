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

    public void Clear();

    public int Length { get; }

    public List<ColumnMapping> ColumnMappings();
}

