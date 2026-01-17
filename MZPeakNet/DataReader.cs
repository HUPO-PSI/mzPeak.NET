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

public record struct GroupTagBounds<T> where T : IComparable<T>
{
    public ulong Key;
    public T Start;
    public T End;

    public GroupTagBounds(ulong key, T start, T end)
    {
        Key = key;
        Start = start;
        End = end;
    }


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

public class RangeIndex : IEnumerable<GroupTagBounds<ulong>>
{
    public List<GroupTagBounds<ulong>> Ranges;

    public long Length { get => Ranges.Count; }

    public RangeIndex(List<GroupTagBounds<ulong>> bounds)
    {
        Ranges = bounds;
    }

    public IEnumerator<GroupTagBounds<ulong>> GetEnumerator()
    {
        return ((IEnumerable<GroupTagBounds<ulong>>)Ranges).GetEnumerator();
    }

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


public class SpacingInterpolationModel<T> where T: struct, INumber<T>
{
    List<T> coefficients;

    public SpacingInterpolationModel(List<T> coefficients)
    {
        this.coefficients = new();
        Coefficients = coefficients;
    }

    public List<T> Coefficients
    {
        get => coefficients;
        set
        {
            coefficients = value;
            if (value.Count < 1)
            {

                throw new ArgumentOutOfRangeException(message: "Spacing Interpolation Model's coefficients must not be empty!", paramName: "value");
            }
        }
    }

    public T Predict(T value)
    {
        var acc = T.One * Coefficients[0];
        for (int i = 1; i < Coefficients.Count; i++)
        {
            var x = value;
            for (int j = 1; j < i; j++)
            {
                x *= value;
            }
            acc += x * Coefficients[i];
        }
        return acc;
    }
}


public class DataArraysReaderMeta
{
    public BufferContext Context;
    public ArrayIndex ArrayIndex;
    public RangeIndex RowGroupIndex;
    public RangeIndex EntrySpanIndex;
    public BufferFormat Format;

    public Dictionary<ulong, SpacingInterpolationModel<double>>? SpacingModels;

    public DataArraysReaderMeta(BufferContext context, ArrayIndex arrayIndex, RangeIndex rowGroupIndex, RangeIndex entrySpanIndex, BufferFormat bufferFormat, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null)
    {
        Context = context;
        ArrayIndex = arrayIndex;
        RowGroupIndex = rowGroupIndex;
        EntrySpanIndex = entrySpanIndex;
        Format = bufferFormat;
        SpacingModels = spacingModels;
    }

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


public class DataArraysReader
{
    public BufferContext BufferContext;
    FileReader FileReader;

    public DataArraysReaderMeta Metadata;

    public ArrayIndex ArrayIndex { get => Metadata.ArrayIndex; }
    public RangeIndex RowGroupIndex { get => Metadata.RowGroupIndex; }
    public RangeIndex EntrySpanIndex { get => Metadata.EntrySpanIndex; }

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

    public DataArraysReader(FileReader reader, DataArraysReaderMeta meta)
    {
        FileReader = reader;
        Metadata = meta;
    }

    public DataArraysReader(FileReader reader, BufferContext context)
    {
        BufferContext = context;
        FileReader = reader;
        Metadata = new DataArraysReaderMeta(reader, context);
    }

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

    public long Length { get => Metadata.EntrySpanIndex.Length; }

    //TODO: Factor this into a type
    public async IAsyncEnumerable<ChunkedArray> Enumerate()
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


public class BaseLayoutReader
{
    protected ArrayIndex ArrayIndex;
    protected IArrowArrayStream Reader;
    protected Dictionary<ulong, SpacingInterpolationModel<double>>? SpacingModels;

    public BaseLayoutReader(IArrowArrayStream reader, ArrayIndex arrayIndex, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null)
    {
        Reader = reader;
        ArrayIndex = arrayIndex;
        SpacingModels = spacingModels;
    }

    protected virtual StructArray ProcessSegment(ulong entryIndex, StructArray rootStruct, ref ulong rowCountRead, ref ulong startFrom, ref ulong endAt)
    {
        var skipAhead = startFrom - rowCountRead;

        var rowsToTake = (int)(endAt - startFrom);
        bool continuesInNextBatch = (rowsToTake + (int)skipAhead) > rootStruct.Length;

        if (continuesInNextBatch)
        {
            var rowsToTakeHere = (int)((ulong)rootStruct.Length - skipAhead);
            rootStruct = (StructArray)rootStruct.Slice((int)skipAhead, rowsToTakeHere);
            startFrom += (ulong)rowsToTakeHere;
        }
        else
        {
            rootStruct = (StructArray)rootStruct.Slice((int)skipAhead, rowsToTake);
        }

        rowCountRead += (ulong)rootStruct.Length;
        return rootStruct;
    }

    public async IAsyncEnumerable<ChunkedArray> EnumerateEntries()
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
                        yield return new ChunkedArray(chunks);
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
        if (chunks.Count > 0)
        {
            yield return new ChunkedArray(chunks);
        }
    }

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
            if (rowCountRead > endAt)
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


public class PointLayoutReader : BaseLayoutReader
{
    public PointLayoutReader(IArrowArrayStream reader, ArrayIndex arrayIndex, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null) : base(reader, arrayIndex, spacingModels) { }

    protected override StructArray ProcessSegment(ulong entryIndex, StructArray rootStruct, ref ulong rowCountRead, ref ulong startFrom, ref ulong endAt)
    {
        var rows = base.ProcessSegment(entryIndex, rootStruct, ref rowCountRead, ref startFrom, ref endAt);
        var fields = ((StructType)rows.Data.DataType).Fields;
        var columnsAfter = new List<IArrowArray?>(fields.Count);
        Dictionary<int, IArrowArray> converted = new();
        foreach(var entry in ArrayIndex.Entries)
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
            if(column == null)
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
            else if(entry.Transform == NullInterpolation.NullZeroCURIE)
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

        for(var i = 0; i < fields.Count; i++)
        {
            var column = rows.Fields[i];
            if (converted.TryGetValue(i, out column))
                columnsAfter.Add(column);
            else
                columnsAfter.Add(rows.Fields[i]);
        }
        return new StructArray(rows.Data.DataType, rows.Length, columnsAfter, rows.NullBitmapBuffer); ;
    }
}


public class ChunkLayoutReader : BaseLayoutReader
{
    const string NUMPRESS_LINEAR_CURIE = "MS:1002312";

    ArrayIndexEntry? mainAxis;
    int chunkStartIndex;
    int chunkEndIndex;
    int chunkEncodingIndex;
    int chunkValuesIndex;
    HashSet<ArrayIndexEntry> secondaryIndices;

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
                        break;
                    }
                default:
                    {
                        throw new NotImplementedException(string.Format("Unsupported buffer format {0} for the chunked layout", entry.BufferFormat));
                    }
            }
        }

        if (mainAxis == null)
        {
            throw new InvalidOperationException("Main axis cannot be null");
        }
    }

    public ChunkLayoutReader(IArrowArrayStream reader, ArrayIndex arrayIndex, Dictionary<ulong, SpacingInterpolationModel<double>>? spacingModels = null) : base(reader, arrayIndex, spacingModels)
    {
        mainAxis = null;
        secondaryIndices = new();
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

    protected override StructArray ProcessSegment(ulong entryIndex, StructArray rootStruct, ref ulong rowCountRead, ref ulong startFrom, ref ulong endAt)
    {
        var rows = base.ProcessSegment(entryIndex, rootStruct, ref rowCountRead, ref startFrom, ref endAt);
        var encodingMethod = (StringArray)rows.Fields[chunkEncodingIndex];
        var chunkStart = rows.Fields[chunkStartIndex];
        var chunkValues = (LargeListArray)rows.Fields[chunkValuesIndex];

        var chunkStartType = chunkStart.Data.DataType;
        if (!chunkStartType.IsFloatingPoint() || chunkStartType.TypeId == ArrowTypeId.HalfFloat)
        {
            throw new InvalidOperationException(string.Format("The chunk start type must be Float or Double, not {0}", chunkStartType));
        }
        var chunkStartDouble = chunkStartType.TypeId == ArrowTypeId.Double;

        if(mainAxis == null) throw new InvalidOperationException("mainAxis cannot be null");

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
            var valueList = chunkValues.GetSlicedValues(i);
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
                        throw new NotImplementedException("Numpress decoding not yet implemented");
                    }
                default:
                    {
                        throw new NotImplementedException("Unknown chunk encoding: " + method);
                    }
            }
        }
        foreach (var entry in secondaryIndices)
        {
            if (entry.SchemaIndex == null)
            {
                throw new InvalidOperationException(string.Format("ArrayIndexEntry schema index somehow made null!?: {0}", entry));
            }
            List<IArrowArray> chunks = new();
            secondaryValues.Add(entry, chunks);
            var col = (LargeListArray)rows.Fields[(int)entry.SchemaIndex];
            var dType = (LargeListType)col.Data.DataType;
            var eltType = dType.ValueDataType;
            switch (eltType.TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        for (var i = 0; i < nRows; i++)
                        {
                            if (col.IsNull(i))
                            {
                                chunks.Add(new FloatArray.Builder().Build());
                            }
                            else
                            {
                                var valsAt = (FloatArray)col.GetSlicedValues(i);
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                {
                                    valsAt = (FloatArray)Compute.NullToZero(valsAt);
                                }
                                chunks.Add(valsAt);
                            }
                        }
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        for (var i = 0; i < nRows; i++)
                        {
                            if (col.IsNull(i))
                            {
                                chunks.Add(new DoubleArray.Builder().Build());
                            }
                            else
                            {
                                var valsAt = (DoubleArray)col.GetSlicedValues(i);
                                if(entry.Transform == NullInterpolation.NullZeroCURIE)
                                {
                                    valsAt = (DoubleArray)Compute.NullToZero(valsAt);
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
                            {
                                chunks.Add(new Int32Array.Builder().Build());
                            }
                            else
                            {
                                var valsAt = (Int32Array)col.GetSlicedValues(i);
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                {
                                    valsAt = (Int32Array)Compute.NullToZero(valsAt);
                                }
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
                            {
                                chunks.Add(new Int64Array.Builder().Build());
                            }
                            else
                            {
                                var valsAt = (Int64Array)col.GetSlicedValues(i);
                                if (entry.Transform == NullInterpolation.NullZeroCURIE)
                                {
                                    valsAt = (Int64Array)Compute.NullToZero(valsAt);
                                }
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
        {
            throw new InvalidOperationException("Main axis cannot be null");
        }
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
            {
                fields.Add(new Field(name, ent.Key.GetArrowType(), true));
            }
            else
            {
                fields.Add(new Field(name, ent.Value[0].Data.DataType, true));
            }
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
