using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using Array = Apache.Arrow.Array;

namespace MZPeak.Writer.Data;

using ComputeFn = Compute.Compute;



public abstract class BaseLayoutBuilder
{
    public BufferContext BufferContext { get; protected set; }

    public bool ShouldRemoveZeroRuns { get; set; } = true;
    public ulong NumberOfPoints = 0;

    protected UInt64Array.Builder Index;
    protected List<IArrowArrayBuilder> Arrays;
    protected List<IArrowType> DataTypes;
    public ArrayIndex ArrayIndex { get; protected set; }

    public int BufferedRows => Index.Length;
    public int BufferedSize => Index.Length + DataTypes.Zip(Arrays).Sum(dtBuilder => dtBuilder.Second.Length);

    public abstract string LayoutName();

    public BaseLayoutBuilder(ArrayIndex arrayIndex)
    {
        ArrayIndex = arrayIndex;
        Index = new();
        Arrays = new();
        DataTypes = new();
        InitializeBuilders();
    }

    public bool HasArrayType(ArrayType arrayType) => ArrayIndex.HasArrayType(arrayType);

    protected virtual void InitializeBuilders()
    {
        int i = 1;
        foreach (var entry in ArrayIndex.Entries)
        {
            entry.SchemaIndex = i;
            i++;
            BufferContext = entry.Context;
            switch (entry.DataTypeCURIE)
            {
                case "MS:1000523":
                    {
                        DataTypes.Add(new DoubleType());
                        Arrays.Add(new DoubleArray.Builder());
                        break;
                    }
                case "MS:1000521":
                    {
                        DataTypes.Add(new FloatType());
                        Arrays.Add(new FloatArray.Builder());
                        break;
                    }
                case "MS:1000519":
                    {
                        DataTypes.Add(new Int32Type());
                        Arrays.Add(new Int32Array.Builder());
                        break;
                    }
                case "MS:1000522":
                    {
                        DataTypes.Add(new Int64Type());
                        Arrays.Add(new Int64Array.Builder());
                        break;
                    }
                default:
                    {
                        throw new NotImplementedException(entry.DataTypeCURIE);
                    }
            }
        }
    }

    protected Dictionary<ArrayIndexEntry, Array> RemoveZeroIntensityRuns(Dictionary<ArrayIndexEntry, Array> arrays, IArrowArray intensityArrayVal)
    {
        switch (intensityArrayVal.Data.DataType.TypeId)
        {
            case ArrowTypeId.Float:
                {
                    var indices = ZeroRunRemoval.WhereNotZeroRun((FloatArray)intensityArrayVal);
                    arrays = ComputeFn.Take(arrays, indices);
                    break;
                }
            case ArrowTypeId.Double:
                {
                    var indices = ZeroRunRemoval.WhereNotZeroRun((DoubleArray)intensityArrayVal);
                    arrays = ComputeFn.Take(arrays, indices);
                    break;
                }
            case ArrowTypeId.Int8:
                {
                    var indices = ZeroRunRemoval.WhereNotZeroRun((Int8Array)intensityArrayVal);
                    arrays = ComputeFn.Take(arrays, indices);
                    break;
                }
            case ArrowTypeId.Int16:
                {
                    var indices = ZeroRunRemoval.WhereNotZeroRun((Int16Array)intensityArrayVal);
                    arrays = ComputeFn.Take(arrays, indices);
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    var indices = ZeroRunRemoval.WhereNotZeroRun((Int32Array)intensityArrayVal);
                    arrays = ComputeFn.Take(arrays, indices);
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    var indices = ZeroRunRemoval.WhereNotZeroRun((Int64Array)intensityArrayVal);
                    arrays = ComputeFn.Take(arrays, indices);
                    break;
                }
            default:
                throw new NotImplementedException();
        }
        return arrays;
    }

    protected Dictionary<ArrayIndexEntry, Array> MarkNulls(Dictionary<ArrayIndexEntry, Array> arrays, ArrayIndexEntry intensityEntry, ArrayIndexEntry coordinateEntry)
    {
        BooleanArray mask;
        var weights = arrays[intensityEntry];
        if (weights.Data.DataType.TypeId == ArrowTypeId.Float)
        {
            mask = ComputeFn.Invert(ZeroRunRemoval.IsZeroPairMask((FloatArray)weights));
            arrays[intensityEntry] = ComputeFn.NullifyAt((FloatArray)weights, mask);
        }
        else if (weights.Data.DataType.TypeId == ArrowTypeId.Double)
        {
            mask = ComputeFn.Invert(ZeroRunRemoval.IsZeroPairMask((DoubleArray)weights));
            arrays[intensityEntry] = ComputeFn.NullifyAt((DoubleArray)weights, mask);
        }
        else if (weights.Data.DataType.TypeId == ArrowTypeId.Int32)
        {
            mask = ComputeFn.Invert(ZeroRunRemoval.IsZeroPairMask((Int32Array)weights));
            arrays[intensityEntry] = ComputeFn.NullifyAt((Int32Array)weights, mask);
        }
        else if (weights.Data.DataType.TypeId == ArrowTypeId.Int64)
        {
            mask = ComputeFn.Invert(ZeroRunRemoval.IsZeroPairMask((Int64Array)weights));
            arrays[intensityEntry] = ComputeFn.NullifyAt((Int64Array)weights, mask);
        }
        else
        {
            var v = ComputeFn.CastFloat(weights);
            mask = ComputeFn.Invert(ZeroRunRemoval.IsZeroPairMask(v));
            arrays[intensityEntry] = ComputeFn.NullifyAt(v, mask);
        }

        var coordinatesToNull = arrays[coordinateEntry];
        if (coordinatesToNull.Data.DataType.TypeId == ArrowTypeId.Float)
        {
            arrays[coordinateEntry] = ComputeFn.NullifyAt((FloatArray)coordinatesToNull, mask);
        }
        else if (coordinatesToNull.Data.DataType.TypeId == ArrowTypeId.Double)
        {
            arrays[coordinateEntry] = ComputeFn.NullifyAt((DoubleArray)coordinatesToNull, mask);
        }
        else throw new InvalidDataException($"Unsupported data type {coordinatesToNull.Data.DataType.Name}");
        return arrays;
    }

    protected record _ArrayFilterResult(
        Dictionary<ArrayIndexEntry, Array> arrays,
        List<(ArrayIndexEntry, Array)> notCoveredArrays,
        ArrayIndexEntry? nullInterpolate = null,
        ArrayIndexEntry? nullZero = null,
        ArrayIndexEntry? intensityArray = null
    )
    { }

    protected virtual _ArrayFilterResult FilterArrays(Dictionary<ArrayIndexEntry, Array> arrays)
    {
        List<(ArrayIndexEntry, Array)> notCoveredArrays = new();
        ArrayIndexEntry? nullInterpolate = null;
        ArrayIndexEntry? nullZero = null;
        ArrayIndexEntry? intensityArray = null;
        foreach (var col in arrays)
        {
            if (!ArrayIndex.Entries.Contains(col.Key))
            {
                notCoveredArrays.Add((col.Key, col.Value));
                continue;
            }
            if (col.Key.ArrayTypeCURIE == ArrayType.IntensityArray.CURIE()) intensityArray = col.Key;
            if (col.Key.Transform == NullInterpolation.NullInterpolateCURIE) nullInterpolate = col.Key;
            else if (col.Key.Transform == NullInterpolation.NullZeroCURIE) nullZero = col.Key;
        }
        return new _ArrayFilterResult(arrays, notCoveredArrays, nullInterpolate, nullZero, intensityArray);
    }

    public (Dictionary<ArrayIndexEntry, Array>, SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Preprocess(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null)
    {
        SpacingInterpolationModel<double>? deltaModel = null;
        List<AuxiliaryArray> auxiliaryArrays = [];

        (arrays, var notCoveredArrays, var nullInterpolate, var nullZero, var intensityArray) = FilterArrays(arrays);

        foreach (var (k, v) in notCoveredArrays)
        {
            // Console.WriteLine($"{k} is treated as an auxiliary array");
            arrays.Remove(k);
            auxiliaryArrays.Add(AuxiliaryArray.FromValues(v, k));
        }

        if (intensityArray != null && ShouldRemoveZeroRuns && (isProfile ?? true))
        {
            var intensityArrayVal = arrays[intensityArray];
            arrays = RemoveZeroIntensityRuns(arrays, intensityArrayVal);
        }

        if (isProfile ?? false)
        {
            if (nullInterpolate != null && nullZero != null)
            {
                var coordinates = ComputeFn.CastDouble(arrays[nullInterpolate]);
                var weights = arrays[nullZero];
                deltaModel = SpacingInterpolationModel<double>.Fit(coordinates, weights);
                arrays = MarkNulls(arrays, nullZero, nullInterpolate);
            }
            else if (nullInterpolate != null || nullZero != null) throw new InvalidOperationException();
        }

        return (arrays, deltaModel, auxiliaryArrays);
    }

    public abstract (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null);
    public abstract (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, IEnumerable<Array> arrays, bool? isProfile = null);
    public (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, IEnumerable<IArrowArray> arrays, bool? isProfile = null)
    {
        return Add(entryIndex, arrays.Select(a => (Array)a), isProfile);
    }

    public abstract RecordBatch GetRecordBatch();

    public virtual Schema ArrowSchema()
    {
        List<Field> fields = [new Field(BufferContext.IndexName(), new UInt64Type(), true)];

        foreach (var entry in ArrayIndex.Entries)
        {
            var name = entry.CreateColumnName();
            fields.Add(new Field(name, entry.GetArrowType(), true));
        }
        var root = new Field(LayoutName(), new StructType(fields), true);
        Dictionary<string, string> meta = new();
        meta[$"{BufferContext.Name()}_array_index"] = JsonSerializer.Serialize(ArrayIndex);
        return new Schema([root], meta);
    }

    protected void AppendNullsTo(IArrowArrayBuilder builder, IArrowType dtype, int k)
    {
        switch (dtype.TypeId)
        {
            case ArrowTypeId.Double:
                {
                    for (var j = 0; j < k; j++)
                        ((DoubleArray.Builder)builder).AppendNull();
                    break;
                }
            case ArrowTypeId.Float:
                {
                    for (var j = 0; j < k; j++)
                        ((FloatArray.Builder)builder).AppendNull();
                    break;
                }
            case ArrowTypeId.Int8:
                {
                    for (var j = 0; j < k; j++)
                        ((Int8Array.Builder)builder).AppendNull();
                    break;
                }
            case ArrowTypeId.Int16:
                {
                    for (var j = 0; j < k; j++)
                        ((Int16Array.Builder)builder).AppendNull();
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    for (var j = 0; j < k; j++)
                        ((Int32Array.Builder)builder).AppendNull();
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    for (var j = 0; j < k; j++)
                        ((Int64Array.Builder)builder).AppendNull();
                    break;
                }
            default: throw new NotImplementedException();
        }
    }

    protected void AppendArrayTo(IArrowArrayBuilder builder, IArrowType dtype, IArrowArray array)
    {
        switch (dtype.TypeId)
        {
            case ArrowTypeId.Double:
                {
                    DoubleArray valArray = ComputeFn.CastDouble(array);
                    DoubleArray.Builder valBuilder = (DoubleArray.Builder)builder;
                    foreach (var v in valArray) valBuilder.Append(v);
                    break;
                }
            case ArrowTypeId.Float:
                {
                    FloatArray valArray = ComputeFn.CastFloat(array);
                    FloatArray.Builder valBuilder = (FloatArray.Builder)builder;
                    foreach (var v in valArray) valBuilder.Append(v);
                    break;
                }
            case ArrowTypeId.Int8:
                {
                    Int8Array valArray = (Int8Array)array;
                    Int8Array.Builder valBuilder = (Int8Array.Builder)builder;
                    foreach (var v in valArray) valBuilder.Append(v);
                    break;
                }
            case ArrowTypeId.Int16:
                {
                    Int16Array valArray = (Int16Array)array;
                    Int16Array.Builder valBuilder = (Int16Array.Builder)builder;
                    foreach (var v in valArray) valBuilder.Append(v);
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    Int32Array valArray = ComputeFn.CastInt32(array);
                    Int32Array.Builder valBuilder = (Int32Array.Builder)builder;
                    foreach (var v in valArray) valBuilder.Append(v);
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    Int64Array valArray = ComputeFn.CastInt64(array);
                    Int64Array.Builder valBuilder = (Int64Array.Builder)builder;
                    foreach (var v in valArray) valBuilder.Append(v);
                    break;
                }
            default: throw new NotImplementedException();
        }
    }
}


public class PointLayoutBuilder : BaseLayoutBuilder
{
    public PointLayoutBuilder(ArrayIndex arrayIndex) : base(arrayIndex) { }

    public override string LayoutName()
    {
        return "point";
    }

    public override (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null)
    {
        (arrays, var deltaModel, var auxiliaryArrays) = Preprocess(entryIndex, arrays, isProfile);

        int k = 0;
        foreach (var val in arrays.Values)
        {
            if (k > 0 && k != val.Length) throw new InvalidDataException("Arrays do not have equal lengths");
            else k = val.Length;
        }
        Index.AppendRange(Enumerable.Repeat(entryIndex, k));
        foreach (var entry in ArrayIndex.Entries)
        {
            if (entry.SchemaIndex == null) throw new InvalidOperationException("Cannot be null");
            Array? array;
            if (arrays.TryGetValue(entry, out array))
            {
                var builder = Arrays[(int)entry.SchemaIndex - 1];
                var dtype = DataTypes[(int)entry.SchemaIndex - 1];
                AppendArrayTo(builder, dtype, array);
            }
            else
            {
                var builder = Arrays[(int)entry.SchemaIndex - 1];
                var dtype = DataTypes[(int)entry.SchemaIndex - 1];
                AppendNullsTo(builder, dtype, k);
            }
        }
        NumberOfPoints += (ulong)k;

        return (deltaModel, auxiliaryArrays);
    }

    public override (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, IEnumerable<Array> arrays, bool? isProfile = null)
    {
        var kvs = ArrayIndex.Entries.Zip(arrays).ToDictionary();
        return Add(entryIndex, kvs, isProfile);
    }

    public override RecordBatch GetRecordBatch()
    {
        List<Array> cols = [Index.Build()];
        foreach (var (dtype, builder) in DataTypes.Zip(Arrays))
        {
            switch (dtype.TypeId)
            {
                case ArrowTypeId.Double:
                    {
                        var builderOf = (DoubleArray.Builder)builder;
                        cols.Add(builderOf.Build());
                        builderOf.Clear();
                        break;
                    }
                case ArrowTypeId.Float:
                    {
                        var builderOf = (FloatArray.Builder)builder;
                        cols.Add(builderOf.Build());
                        builderOf.Clear();
                        break;
                    }
                case ArrowTypeId.Int8:
                    {
                        var builderOf = (Int8Array.Builder)builder;
                        cols.Add(builderOf.Build());
                        builderOf.Clear();
                        break;
                    }
                case ArrowTypeId.Int16:
                    {
                        var builderOf = (Int16Array.Builder)builder;
                        cols.Add(builderOf.Build());
                        builderOf.Clear();
                        break;
                    }
                case ArrowTypeId.Int32:
                    {
                        var builderOf = (Int32Array.Builder)builder;
                        cols.Add(builderOf.Build());
                        builderOf.Clear();
                        break;
                    }
                case ArrowTypeId.Int64:
                    {
                        var builderOf = (Int64Array.Builder)builder;
                        cols.Add(builderOf.Build());
                        builderOf.Clear();
                        break;
                    }
                default: throw new NotImplementedException();
            }
        }

        var schema = ArrowSchema();
        var dtypeOf = schema.GetFieldByIndex(0).DataType;
        var layer = new StructArray(dtypeOf, Index.Length, cols, cols[0].NullBitmapBuffer, cols[0].NullCount);
        Index.Clear();

        return new RecordBatch(schema, [layer], layer.Length);
    }
}


public class ChunkLayoutBuilder : BaseLayoutBuilder
{
    public string DefaultMainAxisEncodingCURIE { get; set; }

    public string CurrentMainAxisEncodingCURIE { get; set; }

    public double ChunkSize { get; set; } = 50.0;
    public ArrayIndexEntry MainAxisEntry {get; set;}

    int MainAxisBuilderIdx;
    int StartValueBuilderIdx;
    int EndValueBuilderIdx;
    int EncodingBuilderIdx;

    public ChunkLayoutBuilder(ArrayIndex arrayIndex, string mainAxisEncodingCURIE=DeltaCodec.CURIE, double chunkSize=50.0) : base(arrayIndex)
    {
        DefaultMainAxisEncodingCURIE = mainAxisEncodingCURIE;
        CurrentMainAxisEncodingCURIE = DefaultMainAxisEncodingCURIE;
        ChunkSize = chunkSize;
        MainAxisEntry = arrayIndex.Entries.Find(
            entry => entry.BufferFormat == BufferFormat.ChunkValues) ?? throw new InvalidDataException(
            $"No main axis array found in {BufferContext} array index");

    }

    protected override void InitializeBuilders()
    {
        int i = 1;
        foreach (var entry in ArrayIndex.Entries)
        {
            entry.SchemaIndex = i;
            i++;
            BufferContext = entry.Context;
            switch (entry.BufferFormat)
            {
                case BufferFormat.ChunkEncoding:
                    {
                        DataTypes.Add(new StringType());
                        Arrays.Add(new StringArray.Builder());
                        break;
                    }
                case BufferFormat.ChunkStart:
                case BufferFormat.ChunkEnd:
                    {
                        switch (entry.DataTypeCURIE)
                        {
                            case "MS:1000523":
                                {
                                    DataTypes.Add(new DoubleType());
                                    Arrays.Add(new DoubleArray.Builder());
                                    break;
                                }
                            case "MS:1000521":
                                {
                                    DataTypes.Add(new FloatType());
                                    Arrays.Add(new FloatArray.Builder());
                                    break;
                                }
                            default: throw new NotImplementedException($"{entry.DataTypeCURIE} not supported for {entry.BufferFormat}");
                        }
                        break;
                    }
                case BufferFormat.ChunkSecondary:
                case BufferFormat.ChunkValues:
                    {
                        DataTypes.Add(entry.GetArrowType());
                        Arrays.Add(new ListArray.Builder(entry.GetArrowType()).Append());

                        break;
                    }
                case BufferFormat.ChunkTransform:
                    {
                        DataTypes.Add(new UInt8Type());
                        Arrays.Add(new ListArray.Builder(new UInt8Type()));
                        break;
                    }
                default: throw new InvalidDataException($"{entry.BufferFormat} is not supported");
            }
        }

        foreach(var entry in ArrayIndex.Entries)
        {
            var idx = (entry.SchemaIndex ?? throw new InvalidOperationException()) - 1;
            if (entry.BufferFormat == BufferFormat.ChunkValues)
                MainAxisBuilderIdx = idx;
            else if (entry.BufferFormat == BufferFormat.ChunkEncoding)
                EncodingBuilderIdx = idx;
            else if (entry.BufferFormat == BufferFormat.ChunkStart)
                StartValueBuilderIdx = idx;
            else if (entry.BufferFormat == BufferFormat.ChunkEnd)
                EndValueBuilderIdx = idx;
        }
    }

    protected override _ArrayFilterResult FilterArrays(Dictionary<ArrayIndexEntry, Array> arrays)
    {
        List<(ArrayIndexEntry, Array)> notCoveredArrays = new();
        ArrayIndexEntry? nullInterpolate = null;
        ArrayIndexEntry? nullZero = null;
        ArrayIndexEntry? intensityArray = null;
        foreach (var col in arrays)
        {
            if (!ArrayIndex.Entries.Contains(col.Key))
            {
                notCoveredArrays.Add((col.Key, col.Value));
                continue;
            }
            if (col.Key.ArrayTypeCURIE == ArrayType.IntensityArray.CURIE()) intensityArray = col.Key;
            if (col.Key.Transform == NullInterpolation.NullInterpolateCURIE) nullInterpolate = col.Key;
            else if (col.Key.Transform == NullInterpolation.NullZeroCURIE) nullZero = col.Key;
        }
        return new _ArrayFilterResult(arrays, notCoveredArrays, nullInterpolate, nullZero, intensityArray);
    }

    public override (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null)
    {
        (arrays, var deltaModel, var auxiliaryArrays) = Preprocess(entryIndex, arrays, isProfile);
        if (isProfile != null && (bool)isProfile)
        {
            CurrentMainAxisEncodingCURIE = DefaultMainAxisEncodingCURIE;
        }
        else if (isProfile != null && !(bool)isProfile)
        {
            CurrentMainAxisEncodingCURIE = NoCompressionCodec.CURIE;
        }
        var mainAxis = arrays[MainAxisEntry];

        var spans = Chunking.ChunkEvery(mainAxis, ChunkSize);

        int k = spans.Count;
        foreach (var val in arrays.Values)
        {
            if (mainAxis.Length != val.Length) throw new InvalidDataException("Arrays do not have equal lengths");
        }

        Index.AppendRange(Enumerable.Repeat(entryIndex, k));

        HashSet<int> visited = new()
        {
            MainAxisBuilderIdx,
            EncodingBuilderIdx,
            StartValueBuilderIdx,
            EndValueBuilderIdx
        };
        var mainAxisBuilder = (ListArray.Builder)Arrays[MainAxisBuilderIdx];
        var mainAxisValueBuilder = mainAxisBuilder.ValueBuilder;
        var startValBuilder = Arrays[StartValueBuilderIdx];
        var endValBuilder = Arrays[EndValueBuilderIdx];
        foreach (var (startIdx, endIdx) in spans)
        {
            var chunk = mainAxis.Slice(startIdx, endIdx - startIdx);
            var startVal = ComputeFn.Min(chunk, NullHandling.Skip);
            var endVal = ComputeFn.Max(chunk, NullHandling.Skip);
            if (startVal == null) continue;

            switch (DataTypes[StartValueBuilderIdx].TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        ((FloatArray.Builder)startValBuilder).Append((float?)startVal);
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        ((DoubleArray.Builder)startValBuilder).Append(startVal);
                        break;
                    }
                default: throw new NotImplementedException($"{chunk.Data.DataType.Name}");
            }
            switch (DataTypes[EndValueBuilderIdx].TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        ((FloatArray.Builder)endValBuilder).Append((float?)endVal);
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        ((DoubleArray.Builder)endValBuilder).Append(endVal);
                        break;
                    }
                default: throw new NotImplementedException($"{chunk.Data.DataType.Name}");
            }

            if (CurrentMainAxisEncodingCURIE == DeltaCodec.CURIE)
            {
                ((StringArray.Builder)Arrays[EncodingBuilderIdx]).Append(DeltaCodec.CURIE);
                switch (MainAxisEntry.GetArrowType().TypeId)
                {
                    case ArrowTypeId.Double:
                        {

                            var builder = (DoubleArray.Builder)mainAxisValueBuilder;
                            DeltaCodec.Encode(startVal, ComputeFn.CastDouble(chunk.Slice(1, chunk.Length - 1)), builder);
                            mainAxisBuilder.Append();
                            break;
                        }
                    case ArrowTypeId.Float:
                        {
                            var builder = (FloatArray.Builder)mainAxisValueBuilder;
                            DeltaCodec.Encode((float?)startVal, ComputeFn.CastFloat(chunk.Slice(1, chunk.Length - 1)), builder);
                            mainAxisBuilder.Append();
                            break;
                        }
                    default: throw new NotImplementedException($"{chunk.Data.DataType.Name}");
                }
            }
            else if (CurrentMainAxisEncodingCURIE == NoCompressionCodec.CURIE)
            {
                ((StringArray.Builder)Arrays[EncodingBuilderIdx]).Append(NoCompressionCodec.CURIE);
                switch (MainAxisEntry.GetArrowType().TypeId)
                {
                    case ArrowTypeId.Double:
                        {
                            var builder = (DoubleArray.Builder)mainAxisValueBuilder;
                            NoCompressionCodec.Encode((double)startVal, ComputeFn.CastDouble(chunk.Slice(1, chunk.Length - 1)), builder);
                            mainAxisBuilder.Append();
                            break;
                        }
                    case ArrowTypeId.Float:
                        {
                            var builder = (FloatArray.Builder)mainAxisValueBuilder;
                            NoCompressionCodec.Encode((float)startVal, ComputeFn.CastFloat(chunk.Slice(1, chunk.Length - 1)), builder);
                            mainAxisBuilder.Append();
                            break;
                        }
                    default: throw new NotImplementedException($"{chunk.Data.DataType.Name}");
                }
            }
            else throw new NotImplementedException(CurrentMainAxisEncodingCURIE);

            foreach (var entry in ArrayIndex.Entries)
            {
                if (entry.SchemaIndex == null) throw new InvalidOperationException("Cannot be null");
                if (visited.Contains((int)entry.SchemaIndex - 1)) continue;
                Array? array;
                if (arrays.TryGetValue(entry, out array))
                {
                    var arrayChunk = array.Slice(startIdx, endIdx - startIdx);
                    var builder = (ListArray.Builder)Arrays[(int)entry.SchemaIndex - 1];
                    var dtype = DataTypes[(int)entry.SchemaIndex - 1];
                    AppendArrayTo(builder.ValueBuilder, dtype, arrayChunk);
                    builder.Append();
                }
                else
                {
                    var builder = (ListArray.Builder)Arrays[(int)entry.SchemaIndex - 1];
                    builder.Append();
                }
            }
        }

        NumberOfPoints += (ulong)mainAxis.Length;
        CurrentMainAxisEncodingCURIE = DefaultMainAxisEncodingCURIE;
        return (deltaModel, auxiliaryArrays);
    }



    public override (SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Add(ulong entryIndex, IEnumerable<Array> arrays, bool? isProfile = null)
    {
        var kvs = ArrayIndex.Entries.Where(e => e.BufferFormat switch
        {
            BufferFormat.ChunkSecondary => true,
            BufferFormat.ChunkValues => true,
            _ => false,
        }).Zip(arrays).ToDictionary();
        return Add(entryIndex, kvs, isProfile);
    }

    public override Schema ArrowSchema()
    {
        List<Field> fields = [new Field(BufferContext.IndexName(), new UInt64Type(), true)];

        foreach (var entry in ArrayIndex.Entries)
        {
            var name = entry.Path.Split(".").Last();
            switch (entry.BufferFormat)
            {
                case BufferFormat.ChunkEncoding:
                    {
                        fields.Add(new Field(name, new StringType(), true));
                        break;
                    }
                case BufferFormat.ChunkStart:
                case BufferFormat.ChunkEnd:
                    {
                        fields.Add(new Field(name, entry.GetArrowType(), true));
                        break;
                    }
                case BufferFormat.ChunkSecondary:
                case BufferFormat.ChunkValues:
                    {
                        fields.Add(new Field(name, new ListType(entry.GetArrowType()), true));
                        break;
                    }
                case BufferFormat.ChunkTransform:
                    {
                        fields.Add(new Field(name, new ListType(new UInt8Type()), true));
                        break;
                    }
                default: throw new InvalidDataException($"{entry.BufferFormat} is not supported");
            }
        }
        var root = new Field(LayoutName(), new StructType(fields), true);
        Dictionary<string, string> meta = new();
        meta[$"{BufferContext.Name()}_array_index"] = JsonSerializer.Serialize(ArrayIndex);
        return new Schema([root], meta);
    }

    public override RecordBatch GetRecordBatch()
    {
        List<Array> cols = [Index.Build()];
        foreach (var (entry, (dtype, builder)) in ArrayIndex.Entries.Zip(DataTypes.Zip(Arrays)))
        {
            switch (entry.BufferFormat)
            {
                case BufferFormat.ChunkEncoding:
                    {
                        cols.Add(((StringArray.Builder)builder).Build());
                        break;
                    }
                case BufferFormat.ChunkStart:
                case BufferFormat.ChunkEnd:
                    {
                        switch (dtype.TypeId)
                        {
                            case ArrowTypeId.Double:
                                {
                                    var builderOf = (DoubleArray.Builder)builder;
                                    cols.Add(builderOf.Build());
                                    builderOf.Clear();
                                    break;
                                }
                            case ArrowTypeId.Float:
                                {
                                    var builderOf = (FloatArray.Builder)builder;
                                    cols.Add(builderOf.Build());
                                    builderOf.Clear();
                                    break;
                                }
                            default: throw new InvalidDataException($"{dtype.Name} is not supported as a chunk boundary");
                        }
                        break;
                    }
                case BufferFormat.ChunkSecondary:
                case BufferFormat.ChunkValues:
                case BufferFormat.ChunkTransform:
                    {
                        cols.Add(((ListArray.Builder)builder).Build());
                        break;
                    }
                default: throw new InvalidDataException($"{entry.BufferFormat} is not supported");
            }
        }

        var schema = ArrowSchema();
        var dtypeOf = schema.GetFieldByIndex(0).DataType;
        var layer = new StructArray(dtypeOf, Index.Length, cols, cols[0].NullBitmapBuffer, cols[0].NullCount);
        Index.Clear();

        return new RecordBatch(schema, [layer], layer.Length);
    }

    public override string LayoutName()
    {
        return "chunk";
    }
}