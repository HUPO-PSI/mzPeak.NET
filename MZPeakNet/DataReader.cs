namespace MZPeak.Reader;

using System.Numerics;
using System.Collections;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Apache.Arrow;
using Apache.Arrow.Ipc;
using ParquetSharp.Arrow;

using MZPeak.Metadata;
using Apache.Arrow.Types;
using MZPeak.Compute;
using Microsoft.Extensions.Logging;
using MZPeak.ControlledVocabulary;
using System.Security;
using ParquetSharp;

/// <summary>
/// Represents a range of values associated with a key.
/// </summary>
/// <typeparam name="T">The comparable type for range bounds.</typeparam>
public record struct GroupTagBounds<T> where T : IComparable<T>
{
    /// <summary>The key identifying this group.</summary>
    public ulong Key;
    /// <summary>The start of the range (inclusive).</summary>
    public T Start;
    /// <summary>The end of the range (inclusive).</summary>
    public T End;

    /// <summary>Creates a new group tag bounds.</summary>
    /// <param name="key">The group key.</param>
    /// <param name="start">The start of the range.</param>
    /// <param name="end">The end of the range.</param>
    public GroupTagBounds(ulong key, T start, T end)
    {
        Key = key;
        Start = start;
        End = end;
    }


    /// <summary>Checks if a value falls within the range.</summary>
    /// <param name="value">The value to check.</param>
    public bool Contains(T value)
    {
        return (Start.CompareTo(value) <= 0) && (value.CompareTo(End) <= 0);
    }
}

class KeyComparator<T> : IComparer<GroupTagBounds<T>> where T : IComparable<T>
{
    public int Compare(GroupTagBounds<T> x, GroupTagBounds<T> y)
    {
        return x.Key.CompareTo(y.Key);
    }
}

/// <summary>
/// Index mapping keys to row ranges within Parquet row groups.
/// </summary>
public class RangeIndex : IEnumerable<GroupTagBounds<ulong>>
{
    /// <summary>The list of range bounds.</summary>
    public List<GroupTagBounds<ulong>> Ranges;

    /// <summary>Gets the number of ranges.</summary>
    public long Length { get => Ranges.Count; }

    /// <summary>Creates a range index from the specified bounds.</summary>
    /// <param name="bounds">The list of group tag bounds.</param>
    public RangeIndex(List<GroupTagBounds<ulong>> bounds)
    {
        Ranges = bounds;
    }

    /// <summary>Gets an enumerator over the ranges.</summary>
    public IEnumerator<GroupTagBounds<ulong>> GetEnumerator()
    {
        return ((IEnumerable<GroupTagBounds<ulong>>)Ranges).GetEnumerator();
    }

    /// <summary>Finds a range by its key using binary search.</summary>
    /// <param name="key">The key to search for.</param>
    public GroupTagBounds<ulong>? FindByKey(ulong key)
    {
        if (Length == 0)
        {
            return null;
        }
        var i = Ranges.BinarySearch(new GroupTagBounds<ulong>(key, 0, 0), new KeyComparator<ulong>());
        if (i < 0)
        {
            return null;
        }
        else
        {
            return Ranges[i];
        }
    }

    /// <summary>Gets all keys whose ranges contain the specified index.</summary>
    /// <param name="index">The index to search for.</param>
    public List<ulong> KeysFor(ulong index)
    {
        List<ulong> groups = new List<ulong>();
        foreach (var group in Ranges)
        {
            if (group.Contains(index))
            {
                groups.Add(group.Key);
            }
        }
        return groups;
    }

    public override string ToString()
    {
        var builder = new StringBuilder();
        builder.Append("RowGroupIndex {\n");
        foreach (var x in Ranges)
        {
            builder.AppendFormat("\t{0}\n", x);
        }
        builder.Append("}");
        return builder.ToString();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return ((IEnumerable)Ranges).GetEnumerator();
    }
}


/// <summary>
/// Metadata for reading data arrays from Parquet files.
/// </summary>
public class DataArraysReaderMeta
{
    /// <summary>The buffer context (spectrum or chromatogram).</summary>
    public BufferContext Context;
    /// <summary>The array index describing available arrays.</summary>
    public ArrayIndex ArrayIndex;
    /// <summary>Index mapping entry keys to row groups.</summary>
    public RangeIndex RowGroupIndex;
    /// <summary>Index mapping entry keys to row spans.</summary>
    public RangeIndex EntrySpanIndex;
    /// <summary>The buffer format used.</summary>
    public BufferFormat Format;

    /// <summary>Optional spacing interpolation models keyed by entry index.</summary>
    public Dictionary<ulong, SpacingInterpolationModel<double>>? SpacingModels;

    /// <summary>Creates metadata with the specified parameters.</summary>
    /// <param name="context">The buffer context.</param>
    /// <param name="arrayIndex">The array index.</param>
    /// <param name="rowGroupIndex">The row group index.</param>
    /// <param name="entrySpanIndex">The entry span index.</param>
    /// <param name="bufferFormat">The buffer format.</param>
    /// <param name="spacingModels">Optional spacing models.</param>
    public DataArraysReaderMeta(BufferContext context, ArrayIndex arrayIndex, RangeIndex rowGroupIndex, RangeIndex entrySpanIndex, BufferFormat bufferFormat, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null)
    {
        Context = context;
        ArrayIndex = arrayIndex;
        RowGroupIndex = rowGroupIndex;
        EntrySpanIndex = entrySpanIndex;
        Format = bufferFormat;
        SpacingModels = spacingModels;
    }

    /// <summary>Creates metadata by reading from a Parquet file.</summary>
    /// <param name="reader">The Parquet file reader.</param>
    /// <param name="context">The buffer context.</param>
    public DataArraysReaderMeta(FileReader reader, BufferContext context)
    {
        Context = context;
        ArrayIndex = new ArrayIndex
        {
            Entries = new List<ArrayIndexEntry>(),
            Prefix = "?"
        };
        RowGroupIndex = new RangeIndex(new List<GroupTagBounds<ulong>>());
        EntrySpanIndex = new RangeIndex(new List<GroupTagBounds<ulong>>());
        InferBufferFormat(reader);
        LoadArrayIndex(reader);
        AnnotateSchemaIndices(reader);
        BuildRowGroupIndex(reader);
        SpacingModels = null;
    }

    void InferBufferFormat(FileReader reader)
    {
        var field = reader.Schema.GetFieldByIndex(0);
        switch (field.Name)
        {
            case "point":
                {
                    Format = BufferFormat.Point;
                    break;
                }
            case "chunk":
                {
                    Format = BufferFormat.ChunkValues;
                    break;
                }
            default:
                {
                    throw new NotImplementedException(string.Format("Root schema name {0} isn't recognized", field.Name));
                }
        }
    }

    void LoadArrayIndex(FileReader reader)
    {
        var key = string.Format("{0}_array_index", Context.ToString().ToLower());
        var arrayIndex = JsonSerializer.Deserialize<ArrayIndex>(reader.ParquetReader.FileMetaData.KeyValueMetadata[key]);
        if (arrayIndex == null)
        {
            throw new KeyNotFoundException("Array index is missing");
        }
        ArrayIndex = arrayIndex;
    }

    void AnnotateSchemaIndices(FileReader reader)
    {
        var schema = reader.ParquetReader.FileMetaData.Schema;
        var nCols = schema.NumColumns;
        for (var i = 0; i < nCols; i++)
        {
            var col = schema.Column(i);
            var pathOf = col.Path.ToDotString();
            if (pathOf.EndsWith(".list.item"))
            {
                pathOf = pathOf.Replace(".list.item", "");
            }
            foreach (var arrEnt in ArrayIndex.Entries)
            {
                if (arrEnt.Path == pathOf)
                {
                    arrEnt.SchemaIndex = i;
                }
            }
        }
    }

    void BuildRowGroupIndex(FileReader reader)
    {
        List<GroupTagBounds<ulong>> index = new();
        ulong start = 0;
        ulong lastIdx = 0;
        ulong offset = 0;
        List<GroupTagBounds<ulong>> rowSpans = new();
        for (ulong i = 0; i < (ulong)reader.ParquetReader.FileMetaData.NumRowGroups; i++)
        {
            var rg = reader.ParquetReader.RowGroup((int)i);
            var indexMeta = rg.MetaData.GetColumnChunkMetaData(0);
            if (indexMeta.Statistics != null && indexMeta.Statistics.HasMinMax)
            {
                var min = Convert.ToUInt64(indexMeta.Statistics.MinUntyped);
                var max = Convert.ToUInt64(indexMeta.Statistics.MaxUntyped);
                var bounds = new GroupTagBounds<ulong>
                {
                    Key = i,
                    Start = min,
                    End = max,
                };
                index.Add(bounds);
            }
            var col = rg.Column(0);
            var colReader = col.LogicalReader<ulong?>();
            foreach (ulong? srcIdx in colReader)
            {
                offset += 1;
                if (srcIdx == null) continue;
                if (srcIdx != lastIdx)
                {
                    rowSpans.Add(new GroupTagBounds<ulong>
                    {
                        Key = lastIdx,
                        Start = start,
                        End = offset - 1
                    });
                    lastIdx = (ulong)srcIdx;
                    start = offset;
                }
            }
        }
        rowSpans.Add(new GroupTagBounds<ulong>
        {
            Key = lastIdx,
            Start = start,
            End = offset - 1
        });
        RowGroupIndex = new RangeIndex(index);
        EntrySpanIndex = new RangeIndex(rowSpans);
    }
}


/// <summary>
/// Reader for data arrays stored in Parquet format.
/// </summary>
public class DataArraysReader
{
    /// <summary>The buffer context (spectrum or chromatogram).</summary>
    public BufferContext BufferContext;
    FileReader FileReader;

    /// <summary>The reader metadata.</summary>
    public DataArraysReaderMeta Metadata;

    /// <summary>Gets the array index from metadata.</summary>
    public ArrayIndex ArrayIndex { get => Metadata.ArrayIndex; }
    /// <summary>Gets the row group index from metadata.</summary>
    public RangeIndex RowGroupIndex { get => Metadata.RowGroupIndex; }
    /// <summary>Gets the entry span index from metadata.</summary>
    public RangeIndex EntrySpanIndex { get => Metadata.EntrySpanIndex; }

    /// <summary>Gets or sets the spacing interpolation models.</summary>
    public Dictionary<ulong, SpacingInterpolationModel<double>>? SpacingModels
    {
        get
        {
            return Metadata.SpacingModels;
        }
        set
        {
            Metadata.SpacingModels = value;
        }
    }

    /// <summary>Creates a reader with existing metadata.</summary>
    /// <param name="reader">The Parquet file reader.</param>
    /// <param name="meta">The reader metadata.</param>
    public DataArraysReader(FileReader reader, DataArraysReaderMeta meta)
    {
        FileReader = reader;
        Metadata = meta;
    }

    /// <summary>Creates a reader that builds metadata from the file.</summary>
    /// <param name="reader">The Parquet file reader.</param>
    /// <param name="context">The buffer context.</param>
    public DataArraysReader(FileReader reader, BufferContext context)
    {
        BufferContext = context;
        FileReader = reader;
        Metadata = new DataArraysReaderMeta(reader, context);
    }

    /// <summary>Gets the buffer format from metadata.</summary>
    public BufferFormat Format => Metadata.Format;

    /// <summary>Creates an empty struct array matching the schema.</summary>
    public StructArray EmptyArrays()
    {
        List<Field> fields = new();
        List<Array> arrays = new();
        HashSet<Field> arrayTypes = new();
        foreach (var arrType in ArrayIndex.Entries)
        {
            var dtype = arrType.GetArrowType();
            var name = arrType.CreateColumnName();
            var field = new Field(name, dtype, true);
            if (arrayTypes.Contains(field)) continue;
            fields.Add(field);
            arrayTypes.Add(field);
        }
        var structDtype = new StructType(fields);
        return new StructArray(structDtype, 0, [], default);
    }

    /// <summary>Reads data arrays for a specific entry index.</summary>
    /// <param name="key">The entry index to read.</param>
    public async Task<ChunkedArray?> ReadForIndex(ulong key)
    {
        var rowGroups = RowGroupIndex.KeysFor(key);
        if (0 == rowGroups.Count)
        {
            return null;
        }
        var rowSpanExists = EntrySpanIndex.FindByKey(key);
        if (rowSpanExists == null)
        {
            return null;
        }
        var rowSpan = (GroupTagBounds<ulong>)rowSpanExists;
        ulong offset = 0;
        for (var i = 0; (ulong)i < rowGroups[0]; i++)
        {
            offset += (ulong)FileReader.ParquetReader.RowGroup(i).MetaData.NumRows;
        }

        int[] rowGroupsArr = new int[rowGroups.Count];
        for (var i = 0; i < rowGroups.Count; i++)
        {
            rowGroupsArr[i] = Convert.ToInt32(rowGroups[i]);
        }

        BaseLayoutReader reader;
        if (Metadata.Format == BufferFormat.Point)
        {
            reader = new PointLayoutReader(FileReader.GetRecordBatchReader(rowGroupsArr), ArrayIndex, SpacingModels);
        }
        else if (Metadata.Format == BufferFormat.ChunkValues)
        {
            reader = new ChunkLayoutReader(FileReader.GetRecordBatchReader(rowGroupsArr), ArrayIndex, SpacingModels);
        }
        else
        {
            throw new InvalidDataException("Data layout not recognized");
        }

        var startFrom = rowSpan.Start - offset;
        var endAt = rowSpan.End - offset;
        var result = await reader.ReadRowsOf(key, startFrom, endAt);
        return result;
    }

    /// <summary>Gets the number of entries.</summary>
    public long Length { get => Metadata.EntrySpanIndex.Length; }

    /// <summary>Asynchronously enumerates all entries with their index and data.</summary>
    public async IAsyncEnumerable<(ulong, StructArray)> Enumerate()
    {
        BaseLayoutReader reader;
        if (Metadata.Format == BufferFormat.Point)
        {
            reader = new PointLayoutReader(FileReader.GetRecordBatchReader(), ArrayIndex, SpacingModels);
        }
        else if (Metadata.Format == BufferFormat.ChunkValues)
        {
            reader = new ChunkLayoutReader(FileReader.GetRecordBatchReader(), ArrayIndex, SpacingModels);
        }
        else
        {
            throw new InvalidDataException("Data layout not recognized");
        }
        await foreach (var chunk in reader.EnumerateEntries())
        {
            yield return chunk;
        }
    }
}


/// <summary>
/// Base class for reading data arrays from different storage layouts.
/// </summary>
public class BaseLayoutReader
{
    public static ILogger? Logger = null;

    protected ArrayIndex ArrayIndex;
    protected IArrowArrayStream Reader;
    protected Dictionary<ulong, SpacingInterpolationModel<double>>? SpacingModels;

    /// <summary>Creates a layout reader with the specified configuration.</summary>
    /// <param name="reader">The Arrow array stream.</param>
    /// <param name="arrayIndex">The array index metadata.</param>
    /// <param name="spacingModels">Optional spacing interpolation models.</param>
    public BaseLayoutReader(IArrowArrayStream reader, ArrayIndex arrayIndex, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null)
    {
        Reader = reader;
        ArrayIndex = arrayIndex;
        SpacingModels = spacingModels;
    }

    protected virtual StructArray ProcessSegment(ulong entryIndex, StructArray rootStruct, ref ulong rowCountRead, ref ulong startFrom, ref ulong endAt)
    {
        var indexArr = (UInt64Array)rootStruct.Fields[0];
        var mask = Compute.Equal(indexArr, entryIndex);
        rootStruct = (StructArray)Compute.Filter(rootStruct, mask);
        rowCountRead += (ulong)rootStruct.Length;
        return rootStruct;
    }

    /// <summary>Asynchronously enumerates all entries.</summary>
    public async IAsyncEnumerable<(ulong, StructArray)> EnumerateEntries()
    {
        ulong rowCountRead = 0;
        var chunks = new List<IArrowArray>();
        ulong? lastIndex = null;
        while (true)
        {
            var batch = await Reader.ReadNextRecordBatchAsync();
            if (batch == null)
            {
                break;
            }
            var root = batch.Column(0);

            var rootStruct = (StructArray?)root;
            if (rootStruct == null)
            {
                rowCountRead += (ulong)batch.Length;
                continue;
            }
            var indexCol = (UInt64Array)rootStruct.Fields[0];
            var offsetIn = 0;
            for (int c = 0; c < indexCol.Length; c++)
            {
                ulong? v = indexCol.GetValue(c);
                if (v != lastIndex && v != null)
                {
                    if (c > offsetIn && lastIndex != null)
                    {
                        var block = (StructArray)rootStruct.Slice(offsetIn, c - offsetIn);
                        ulong z0 = 0;
                        ulong z1 = 0;
                        ulong zn = (ulong)block.Length;
                        block = ProcessSegment((ulong)lastIndex, block, ref z0, ref z1, ref zn);
                        chunks.Add(block);

                        yield return ((ulong)lastIndex, (StructArray)ArrowArrayConcatenator.Concatenate(chunks));
                    }
                    chunks = new();
                    offsetIn = c;
                    lastIndex = v;
                }
            }
            if (offsetIn < indexCol.Length && lastIndex != null)
            {
                var block = (StructArray)rootStruct.Slice(offsetIn, indexCol.Length - offsetIn);
                ulong z0 = 0;
                ulong z1 = 0;
                ulong zn = (ulong)block.Length;
                block = ProcessSegment((ulong)lastIndex, block, ref z0, ref z1, ref zn);
                chunks.Add(block);
            }
        }
        if (chunks.Count > 0 && lastIndex != null)
        {
            yield return ((ulong)lastIndex, (StructArray)ArrowArrayConcatenator.Concatenate(chunks));
        }
    }

    /// <summary>Reads rows for a specific entry within a row range.</summary>
    /// <param name="entryIndex">The entry index.</param>
    /// <param name="startFrom">The starting row offset.</param>
    /// <param name="endAt">The ending row offset.</param>
    public async Task<ChunkedArray> ReadRowsOf(ulong entryIndex, ulong startFrom, ulong endAt)
    {
        ulong rowCountRead = 0;
        var chunks = new List<Array>();
        while (true)
        {
            var batch = await Reader.ReadNextRecordBatchAsync();
            if (batch == null)
            {
                break;
            }
            var root = batch.Column(0);

            var rootStruct = (StructArray?)root;
            if (rootStruct == null)
            {
                rowCountRead += (ulong)batch.Length;
                continue;
            }

            ulong rowCountReadPlusBatch = rowCountRead + (ulong)batch.Length;
            if (rowCountReadPlusBatch < startFrom)
            {
                rowCountRead += (ulong)batch.Length;
                continue;
            }

            if (rowCountRead >= endAt)
            {
                break;
            }

            // Console.WriteLine("{0}-{1} out of {2}-{3}", rowCountRead, rowCountReadPlusBatch, startFrom, endAt);
            var chunk = ProcessSegment(entryIndex, rootStruct, ref rowCountRead, ref startFrom, ref endAt);
            chunks.Add(chunk);
        }
        return new ChunkedArray(chunks);
    }
}


/// <summary>
/// Reader for point-format data layouts where each row is a single data point.
/// </summary>
public class PointLayoutReader : BaseLayoutReader
{
    /// <summary>Creates a point layout reader.</summary>
    /// <param name="reader">The Arrow array stream.</param>
    /// <param name="arrayIndex">The array index metadata.</param>
    /// <param name="spacingModels">Optional spacing interpolation models.</param>
    public PointLayoutReader(IArrowArrayStream reader, ArrayIndex arrayIndex, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null) : base(reader, arrayIndex, spacingModels) { }

    protected override StructArray ProcessSegment(ulong entryIndex, StructArray rootStruct, ref ulong rowCountRead, ref ulong startFrom, ref ulong endAt)
    {
        var rows = base.ProcessSegment(entryIndex, rootStruct, ref rowCountRead, ref startFrom, ref endAt);
        // Console.WriteLine("Processing {0}", rowCountRead);
        var fields = ((StructType)rows.Data.DataType).Fields;
        var columnsAfter = new List<IArrowArray?>(fields.Count);
        Dictionary<int, IArrowArray> converted = new();
        foreach (var entry in ArrayIndex.Entries)
        {
            var name = entry.Path.Split(".").Last();
            IArrowArray? column = null;
            int index = 0;
            for (var i = 0; i < fields.Count; i++)
            {
                if (name == fields[i].Name)
                {
                    index = i;
                    column = rows.Fields[i];
                }
            }

            if (column == null)
            {
                continue;
            }
            if (entry.Transform == NullInterpolation.NullInterpolateCURIE && column.NullCount > 0)
            {
                if (SpacingModels == null || !SpacingModels.ContainsKey(entryIndex))
                {
                    continue;
                }
                SpacingInterpolationModel<double> model = SpacingModels[entryIndex];
                switch (column.Data.DataType.TypeId)
                {
                    case ArrowTypeId.Float:
                        {
                            var builder = new FloatArray.Builder();
                            var modelFloat = new SpacingInterpolationModel<float>(model.Coefficients.Select((v) => (float)v).ToList());
                            NullInterpolation.FillNullsWithModel((FloatArray)column, modelFloat, builder);
                            converted[index] = builder.Build();
                            break;
                        }
                    case ArrowTypeId.Double:
                        {
                            var builder = new DoubleArray.Builder();
                            NullInterpolation.FillNullsWithModel((DoubleArray)column, model, builder);
                            converted[index] = builder.Build();
                            break;
                        }
                    default:
                        {
                            throw new InvalidOperationException("Cannot interpolate non-float");
                        }
                }
            }
            else if (entry.Transform == NullInterpolation.NullZeroCURIE)
            {
                switch (column.Data.DataType.TypeId)
                {
                    case ArrowTypeId.Float:
                        {
                            converted[index] = Compute.NullToZero((FloatArray)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.Double:
                        {
                            converted[index] = Compute.NullToZero((DoubleArray)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.Int32:
                        {
                            converted[index] = Compute.NullToZero((Int32Array)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.Int64:
                        {
                            converted[index] = Compute.NullToZero((Int64Array)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.UInt32:
                        {
                            converted[index] = Compute.NullToZero((UInt32Array)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.UInt64:
                        {
                            converted[index] = Compute.NullToZero((UInt64Array)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.UInt8:
                        {
                            converted[index] = Compute.NullToZero((UInt8Array)column) ?? throw new InvalidDataException();
                            break;
                        }
                    case ArrowTypeId.Int8:
                        {
                            converted[index] = Compute.NullToZero((Int8Array)column) ?? throw new InvalidDataException();
                            break;
                        }
                    default:
                        {
                            throw new InvalidOperationException(string.Format("Data type {0} not supported", column.Data.DataType.Name));
                        }
                }
            }
            else
            {
                converted[index] = column;
            }
        }

        for (var i = 0; i < fields.Count; i++)
        {
            var column = rows.Fields[i];
            if (converted.TryGetValue(i, out column))
                columnsAfter.Add(column);
            else
                columnsAfter.Add(rows.Fields[i]);
        }
        return new StructArray(rows.Data.DataType, rows.Length, columnsAfter, default); ;
    }
}

record TransformKey(ArrayType? ArrayType, string ArrayName, BinaryDataType BinaryDataType, Unit? Unit, string? DataProcessesingId)
{
    public override int GetHashCode()
    {
        return (ArrayType, ArrayName, BinaryDataType, Unit, DataProcessesingId).GetHashCode();
    }

    public static TransformKey FromArrayIndexEntry(ArrayIndexEntry entry)
    {
        BinaryDataType dataType;
        if (!BinaryDataTypeMethods.FromCURIE.TryGetValue(entry.DataTypeCURIE, out dataType)) throw new InvalidDataException();
        return new(entry.GetArrayType(), entry.ArrayName, dataType, entry.GetUnit(), entry.DataProcessesingId);
    }
}

/// <summary>
/// Reader for chunk-format data layouts where each row contains compressed array chunks.
/// </summary>
public class ChunkLayoutReader : BaseLayoutReader
{
    const string NUMPRESS_LINEAR_CURIE = "MS:1002312";
    const string NUMPRESS_SLOF_CURIE = "MS:1002314";

    ArrayIndexEntry? mainAxis;
    int chunkStartIndex = -1;
    int chunkEndIndex = -1;
    int chunkEncodingIndex = -1;
    int chunkValuesIndex = -1;
    HashSet<ArrayIndexEntry> secondaryIndices;
    Dictionary<TransformKey, List<ArrayIndexEntry>> transformMap;


    void ConfigureIndices()
    {

        foreach (var entry in ArrayIndex.Entries)
        {
            if (entry.SchemaIndex == null)
            {
                throw new InvalidOperationException(string.Format("ArrayIndex entries cannot have null indices at this point: {0}", entry));
            }
            var index = (int)entry.SchemaIndex;
            switch (entry.BufferFormat)
            {
                case BufferFormat.ChunkStart:
                    {
                        chunkStartIndex = index;
                        break;
                    }
                case BufferFormat.ChunkEnd:
                    {
                        chunkEndIndex = index;
                        break;
                    }
                case BufferFormat.ChunkEncoding:
                    {
                        chunkEncodingIndex = index;
                        break;
                    }
                case BufferFormat.ChunkValues:
                    {
                        mainAxis = entry;
                        chunkValuesIndex = index;
                        break;
                    }
                case BufferFormat.ChunkSecondary:
                    {
                        secondaryIndices.Add(entry);
                        break;
                    }
                case BufferFormat.ChunkTransform:
                    {
                        var key = TransformKey.FromArrayIndexEntry(entry);
                        if (transformMap.ContainsKey(key))
                            transformMap[key].Add(entry);
                        else
                            transformMap[key] = [entry];
                        secondaryIndices.Add(entry);
                        break;
                    }
                default:
                    throw new NotImplementedException(string.Format("Unsupported buffer format {0} for the chunked layout", entry.BufferFormat));
            }
        }

        if (chunkEncodingIndex == -1)
            throw new InvalidOperationException("Chunk encoding column not found");

        if (mainAxis == null)
            throw new InvalidOperationException("Main axis cannot be null");
    }

    public ChunkLayoutReader(IArrowArrayStream reader, ArrayIndex arrayIndex, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null) : base(reader, arrayIndex, spacingModels)
    {
        mainAxis = null;
        secondaryIndices = new();
        transformMap = new();
        ConfigureIndices();
    }

    IArrowArray DecodeNoCompression(ulong entryIndex, double startValue, IArrowArray chunkValues, ArrayIndexEntry entryMeta)
    {
        IArrowArray result;
        switch (chunkValues.Data.DataType.TypeId)
        {
            case ArrowTypeId.Int32:
                {
                    var builder = new Int32Array.Builder();
                    NoCompressionCodec.Decode((int)startValue, (Int32Array)chunkValues, builder);
                    result = builder.Build();
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    var builder = new Int64Array.Builder();
                    NoCompressionCodec.Decode((long)startValue, (Int64Array)chunkValues, builder);
                    result = builder.Build();
                    break;
                }
            case ArrowTypeId.Float:
                {
                    var builder = new FloatArray.Builder();
                    NoCompressionCodec.Decode((float)startValue, (FloatArray)chunkValues, builder);
                    result = builder.Build();
                    if (entryMeta.Transform == NullInterpolation.NullInterpolateCURIE && SpacingModels != null && result.NullCount > 0)
                    {
                        var model = SpacingModels[entryIndex];
                        var floatModel = new SpacingInterpolationModel<float>(model.Coefficients.Select(v => (float)v).ToList());
                        builder = new FloatArray.Builder();
                        NullInterpolation.FillNullsWithModel((FloatArray)result, floatModel, builder);
                        result = builder.Build();
                    }
                    break;
                }
            case ArrowTypeId.Double:
                {
                    var builder = new DoubleArray.Builder();
                    NoCompressionCodec.Decode((double)startValue, (DoubleArray)chunkValues, builder);
                    result = builder.Build();
                    if (entryMeta.Transform == NullInterpolation.NullInterpolateCURIE && SpacingModels != null && result.NullCount > 0)
                    {
                        var model = SpacingModels[entryIndex];
                        builder = new DoubleArray.Builder();
                        NullInterpolation.FillNullsWithModel((DoubleArray)result, model, builder);
                        result = builder.Build();
                    }
                    break;
                }
            default:
                {
                    throw new NotImplementedException("Unsupported data type: " + chunkValues.Data.DataType.Name);
                }
        }
        return result;
    }

    IArrowArray DecodeDelta(ulong entryIndex, double startValue, IArrowArray chunkValues, ArrayIndexEntry entryMeta)
    {
        IArrowArray result;
        switch (chunkValues.Data.DataType.TypeId)
        {
            case ArrowTypeId.Int32:
                {
                    var builder = new Int32Array.Builder();
                    DeltaCodec.Decode((int)startValue, (Int32Array)chunkValues, builder);
                    result = builder.Build();
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    var builder = new Int64Array.Builder();
                    DeltaCodec.Decode((long)startValue, (Int64Array)chunkValues, builder);
                    result = builder.Build();
                    break;
                }
            case ArrowTypeId.Float:
                {
                    var builder = new FloatArray.Builder();
                    DeltaCodec.Decode((float)startValue, (FloatArray)chunkValues, builder);
                    result = builder.Build();
                    if (entryMeta.Transform == NullInterpolation.NullInterpolateCURIE && SpacingModels != null && result.NullCount > 0)
                    {
                        var model = SpacingModels[entryIndex];
                        var floatModel = new SpacingInterpolationModel<float>(model.Coefficients.Select(v => (float)v).ToList());
                        builder = new FloatArray.Builder();
                        NullInterpolation.FillNullsWithModel((FloatArray)result, floatModel, builder);
                        result = builder.Build();
                    }
                    break;
                }
            case ArrowTypeId.Double:
                {
                    var builder = new DoubleArray.Builder();
                    DeltaCodec.Decode((double)startValue, (DoubleArray)chunkValues, builder);
                    result = builder.Build();
                    if (entryMeta.Transform == NullInterpolation.NullInterpolateCURIE && SpacingModels != null && result.NullCount > 0)
                    {
                        var model = SpacingModels[entryIndex];
                        builder = new DoubleArray.Builder();
                        NullInterpolation.FillNullsWithModel((DoubleArray)result, model, builder);
                        result = builder.Build();
                    }
                    break;
                }
            default:
                {
                    throw new NotImplementedException("Unsupported data type: " + chunkValues.Data.DataType.Name);
                }
        }
        return result;
    }

    ArrayIndexEntry FindEntryForTransform(ArrayIndexEntry query, string transform)
    {
        foreach (var ent in transformMap[TransformKey.FromArrayIndexEntry(query)])
            if (ent.Transform == transform)
                return ent;
        throw new KeyNotFoundException($"No entry was found for {TransformKey.FromArrayIndexEntry(query)} with transform  = {transform}");
    }

    protected override StructArray ProcessSegment(ulong entryIndex, StructArray rootStruct, ref ulong rowCountRead, ref ulong startFrom, ref ulong endAt)
    {
        var rows = base.ProcessSegment(entryIndex, rootStruct, ref rowCountRead, ref startFrom, ref endAt);
        var encodingMethod = (StringArray)rows.Fields[chunkEncodingIndex];
        var chunkStart = rows.Fields[chunkStartIndex];
        var chunkValues = rows.Fields[chunkValuesIndex];
        var chunkValuesIsLarge = chunkValues.Data.DataType.TypeId == ArrowTypeId.LargeList;

        var chunkStartType = chunkStart.Data.DataType;
        if (!chunkStartType.IsFloatingPoint() || chunkStartType.TypeId == ArrowTypeId.HalfFloat)
        {
            throw new InvalidOperationException(string.Format("The chunk start type must be Float or Double, not {0}", chunkStartType));
        }
        var chunkStartDouble = chunkStartType.TypeId == ArrowTypeId.Double;

        if (mainAxis == null) throw new InvalidOperationException("mainAxis cannot be null");
        var mainAxisKey = TransformKey.FromArrayIndexEntry(mainAxis);
        List<IArrowArray> decodedValues = new();
        Dictionary<ArrayIndexEntry, List<IArrowArray>> secondaryValues = new();
        var nRows = encodingMethod.Length;
        for (var i = 0; i < nRows; i++)
        {
            var startValue = chunkStartDouble ? ((DoubleArray)chunkStart).GetValue(i) : ((FloatArray)chunkStart).GetValue(i);
            if (startValue == null)
            {
                continue;
            }
            var valueList = chunkValuesIsLarge ? ((LargeListArray)chunkValues).GetSlicedValues(i) : ((ListArray)chunkValues).GetSlicedValues(i);
            var method = encodingMethod.GetString(i);
            switch (method)
            {
                case NoCompressionCodec.CURIE:
                    {
                        decodedValues.Add(DecodeNoCompression(entryIndex, (double)startValue, valueList, mainAxis));
                        break;
                    }
                case DeltaCodec.CURIE:
                    {
                        decodedValues.Add(DecodeDelta(entryIndex, (double)startValue, valueList, mainAxis));
                        break;
                    }
                case NUMPRESS_LINEAR_CURIE:
                    {
                        var tfmEntry = FindEntryForTransform(mainAxis, NUMPRESS_LINEAR_CURIE);
                        if (tfmEntry.SchemaIndex == null) throw new InvalidOperationException("Array index entry transform not mapped to column!");
                        var arr = rows.Fields[(int)tfmEntry.SchemaIndex];
                        if (arr.IsNull(i)) throw new InvalidOperationException("Transformed main axis array slot cannot be null");
                        var values = (PrimitiveArray<byte>)((arr.Data.DataType.TypeId == ArrowTypeId.LargeList) ? ((LargeListArray)arr).GetSlicedValues(i) : ((ListArray)arr).GetSlicedValues(i));

                        var valuesNat = Numpress.MSNumpress.decode(NUMPRESS_LINEAR_CURIE, values.ValueBuffer.Span, values.Length * 3);
                        decodedValues.Add(valueList.Data.DataType.TypeId == ArrowTypeId.Float ? Compute.CastFloat(valuesNat) : Compute.CastDouble(valuesNat));
                        break;
                    }
                default: throw new NotImplementedException("Unknown chunk encoding: " + method);
            }
        }

        foreach (var entry in secondaryIndices)
        {
            if (entry.SchemaIndex == null)
                throw new InvalidOperationException($"ArrayIndexEntry schema index somehow made null!?: {entry}");
            List<IArrowArray> chunks = new();
            secondaryValues.Add(entry, chunks);
            var col = rows.Fields[(int)entry.SchemaIndex];
            var colIsLarge = col.Data.DataType.TypeId == ArrowTypeId.LargeList;
            var eltType = colIsLarge ? ((LargeListType)col.Data.DataType).ValueDataType : ((ListType)col.Data.DataType).ValueDataType;
            switch (eltType.TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        for (var i = 0; i < nRows; i++)
                        {
                            if (col.IsNull(i))
                                chunks.Add(new FloatArray.Builder().Build());
                            else
                            {
                                var valsAt = colIsLarge ? ((LargeListArray)col).GetSlicedValues(i) : ((ListArray)col).GetSlicedValues(i);
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                    valsAt = (FloatArray)Compute.NullToZero((FloatArray)valsAt);
                                else if (entry.Transform == NUMPRESS_SLOF_CURIE)
                                {
                                    var decoded = Numpress.MSNumpress.decode(NUMPRESS_SLOF_CURIE, ((UInt8Array)valsAt).ValueBuffer.Span, valsAt.Length * 3);
                                    valsAt = Compute.CastFloat(decoded);
                                }
                                chunks.Add((FloatArray)valsAt);
                            }
                        }
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        for (var i = 0; i < nRows; i++)
                        {
                            if (col.IsNull(i))
                                chunks.Add(new DoubleArray.Builder().Build());
                            else
                            {
                                var valsAt = colIsLarge ? ((LargeListArray)col).GetSlicedValues(i) : ((ListArray)col).GetSlicedValues(i);
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                    valsAt = Compute.NullToZero((DoubleArray)valsAt);
                                else if (entry.Transform == NUMPRESS_SLOF_CURIE)
                                {
                                    var decoded = Numpress.MSNumpress.decode(NUMPRESS_SLOF_CURIE, ((UInt8Array)valsAt).ValueBuffer.Span, valsAt.Length * 3);
                                    valsAt = Compute.CastDouble(decoded);
                                }
                                chunks.Add(valsAt);
                            }
                        }
                        break;
                    }
                case ArrowTypeId.Int32:
                    {
                        for (var i = 0; i < nRows; i++)
                        {
                            if (col.IsNull(i))
                                chunks.Add(new Int32Array.Builder().Build());
                            else
                            {
                                var valsAt = (Int32Array)(colIsLarge ? ((LargeListArray)col).GetSlicedValues(i) : ((ListArray)col).GetSlicedValues(i));
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                    valsAt = (Int32Array)Compute.NullToZero(valsAt);
                                chunks.Add(valsAt);
                            }
                        }
                        break;
                    }
                case ArrowTypeId.Int64:
                    {
                        for (var i = 0; i < nRows; i++)
                        {
                            if (col.IsNull(i))
                                chunks.Add(new Int64Array.Builder().Build());
                            else
                            {
                                var valsAt = (Int64Array)(colIsLarge ? ((LargeListArray)col).GetSlicedValues(i) : ((ListArray)col).GetSlicedValues(i));
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                    valsAt = (Int64Array)Compute.NullToZero(valsAt);
                                chunks.Add(valsAt);
                            }
                        }
                        break;
                    }
                default:
                    throw new NotImplementedException(string.Format("Secondary chunk array type {0} not yet implemented", eltType));
            }
        }

        if (mainAxis == null)
            throw new InvalidOperationException("Main axis cannot be null");

        var mainName = mainAxis.Path.Split(".").Last().Replace("_chunk_values", "");
        var fields = new List<Field>
        {
            new Field(mainAxis.Context.IndexName(), new UInt64Type(), true),
            new Field(mainName, mainAxis.GetArrowType(), true)
        };

        foreach (var ent in secondaryValues)
        {
            var name = ent.Key.Path.Split(".").Last();
            if (ent.Value.Count == 0)
                fields.Add(new Field(name, ent.Key.GetArrowType(), true));
            else
                fields.Add(new Field(name, ent.Value[0].Data.DataType, true));
        }

        var dataType = new StructType(fields);

        List<IArrowArray> rowChunks = new();
        for (int i = 0; i < decodedValues.Count; i++)
        {
            var n = decodedValues[i].Length;

            var indexBuild = new UInt64Array.Builder();
            indexBuild.AppendRange(Enumerable.Repeat(entryIndex, n));

            List<IArrowArray> cols = new()
            {
                indexBuild.Build(),
                decodedValues[i]
            };

            foreach (var ent in secondaryValues)
            {
                cols.Add(ent.Value[i]);
            }
            var bitmapBuilder = new ArrowBuffer.BitmapBuilder();
            bitmapBuilder.AppendRange(Enumerable.Repeat(true, n));
            var chunk = new StructArray(dataType, n, cols, bitmapBuilder.Build());
            rowChunks.Add(chunk);
        }
        var combined = (StructArray)ArrowArrayConcatenator.Concatenate(rowChunks);

        return combined;
    }

}


public class DataIter
{
    BaseLayoutReader LayoutReader;
    IArrowArrayStream StreamReader;
    ulong RowCountRead = 0;
    List<IArrowArray> Chunks;
    ulong? LastIndex = null;
    ulong? CurrentIndex = null;

    StructArray? CurrentBatch = null;

    public DataIter(BaseLayoutReader layoutReader, IArrowArrayStream stream)
    {
        LayoutReader = layoutReader;
        StreamReader = stream;
        RowCountRead = 0;
        Chunks = new();
        LastIndex = null;
        CurrentIndex = null;
        CurrentBatch = null;
    }

    public async Task<bool> ReadNextBatch()
    {
        var batch = await StreamReader.ReadNextRecordBatchAsync();
        if (batch == null)
            return false;

        var root = batch.Column(0);

        var rootStruct = (StructArray?)root;
        if (rootStruct == null)
            return false;

        CurrentBatch = rootStruct;
        return true;
    }

    public async Task<bool> Initialize()
    {
        if (!await ReadNextBatch()) return false;
        if (CurrentBatch == null) return false;
        var idxCol = (UInt64Array)CurrentBatch.Fields[0];
        CurrentIndex = idxCol.GetValue(0);
        return true;
    }

    public async Task GetNextSegment()
    {
        var isInitializing = CurrentIndex == null;
    }
}
