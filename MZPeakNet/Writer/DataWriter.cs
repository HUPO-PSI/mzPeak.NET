using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using Array = Apache.Arrow.Array;

namespace MZPeak.Writer.Data;


public abstract class BaseLayoutBuilder
{
    public BufferContext BufferContext { get; protected set; }

    public bool ShouldRemoveZeroRuns { get; set; } = true;
    public bool UseNullMarking { get; set; } = false;

    protected UInt64Array.Builder Index;
    protected List<IArrowArrayBuilder> Arrays;
    protected List<IArrowType> DataTypes;
    public ArrayIndex ArrayIndex { get; protected set; }

    public int BufferedRows => Index.Length;
    public int BufferedSize => (Index.Length * 8) + DataTypes.Zip(Arrays).Sum(dt_builder => dt_builder.Second.Length);

    public abstract string LayoutName();

    public BaseLayoutBuilder(ArrayIndex arrayIndex)
    {
        ArrayIndex = arrayIndex;
        Index = new();
        Arrays = new();
        DataTypes = new();
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

    public abstract (Dictionary<ArrayIndexEntry, Array>, SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Preprocess(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile=null);
    public abstract int Add(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays);
    public abstract int Add(ulong entryIndex, IEnumerable<Array> arrays);

    public abstract RecordBatch GetRecordBatch();

    public Schema ArrowSchema()
    {
        List<Field> fields = [new Field(BufferContext.IndexName(), new UInt64Type(), true)];

        foreach(var entry in ArrayIndex.Entries)
        {
            var name = entry.CreateColumnName();
            fields.Add(new Field(name, entry.GetArrowType(), true));
        }
        var root = new Field(LayoutName(), new StructType(fields), true);
        Dictionary<string, string> meta = new();
        meta[$"{BufferContext.Name()}_array_index"] = JsonSerializer.Serialize(ArrayIndex);
        return new Schema([root], meta);
    }
}


public class PointLayoutBuilder : BaseLayoutBuilder
{
    public ulong NumberOfPoints = 0;

    public PointLayoutBuilder(ArrayIndex arrayIndex) : base(arrayIndex)
    {}

    public override string LayoutName()
    {
        return "point";
    }

    public override (Dictionary<ArrayIndexEntry, Array>, SpacingInterpolationModel<double>?, List<AuxiliaryArray>) Preprocess(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays, bool? isProfile = null)
    {
        List<(ArrayIndexEntry, Array)> notCoveredArrays = new();
        ArrayIndexEntry? nullInterpolate = null;
        ArrayIndexEntry? nullZero = null;
        ArrayIndexEntry? intensityArray = null;
        SpacingInterpolationModel<double>? deltaModel = null;
        List<AuxiliaryArray> auxiliaryArrays = [];
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

        // TODO: Construct Auxiliary Array here
        foreach(var (k, v) in notCoveredArrays) {
            arrays.Remove(k);
        }

        if (intensityArray != null)
        {
            var intensityArrayVal = arrays[intensityArray];
            switch (intensityArrayVal.Data.DataType.TypeId)
            {
                case ArrowTypeId.Float:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((FloatArray)intensityArrayVal);
                        arrays = Compute.Compute.Take(arrays, indices);
                        break;
                    }
                case ArrowTypeId.Double:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((DoubleArray)intensityArrayVal);
                        arrays = Compute.Compute.Take(arrays, indices);
                        break;
                    }
                case ArrowTypeId.Int8:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((Int8Array)intensityArrayVal);
                        arrays = Compute.Compute.Take(arrays, indices);
                        break;
                    }
                case ArrowTypeId.Int16:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((Int16Array)intensityArrayVal);
                        arrays = Compute.Compute.Take(arrays, indices);
                        break;
                    }
                case ArrowTypeId.Int32:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((Int32Array)intensityArrayVal);
                        arrays = Compute.Compute.Take(arrays, indices);
                        break;
                    }
                case ArrowTypeId.Int64:
                    {
                        var indices = ZeroRunRemoval.WhereNotZeroRun((Int64Array)intensityArrayVal);
                        arrays = Compute.Compute.Take(arrays, indices);
                        break;
                    }
                default:
                    throw new NotImplementedException();
            }
        }

        if (nullInterpolate != null && nullZero != null)
        {

        } else if (nullInterpolate != null || nullZero != null) throw new InvalidOperationException();

        return (arrays, deltaModel, auxiliaryArrays);
    }

    public override int Add(ulong entryIndex, Dictionary<ArrayIndexEntry, Array> arrays)
    {
        Preprocess(entryIndex, arrays);

        int k = 0;
        foreach(var val in arrays.Values)
        {
            if(k > 0 && k != val.Length) throw new InvalidDataException("Arrays do not have equal lengths");
            else k = val.Length;
        }
        Index.AppendRange(Enumerable.Repeat(entryIndex, k));
        foreach(var entry in ArrayIndex.Entries)
        {
            if (entry.SchemaIndex == null) throw new InvalidOperationException("Cannot be null");
            Array? array;
            if (arrays.TryGetValue(entry, out array))
            {
                var builder = Arrays[(int)entry.SchemaIndex - 1];
                var dtype = DataTypes[(int)entry.SchemaIndex - 1];
                switch (dtype.TypeId)
                {
                    case ArrowTypeId.Double:
                        {
                            DoubleArray valArray = (DoubleArray)array;
                            DoubleArray.Builder valBuilder = (DoubleArray.Builder)builder;
                            foreach(var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    case ArrowTypeId.Float:
                        {
                            FloatArray valArray = (FloatArray)array;
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
                            Int32Array valArray = (Int32Array)array;
                            Int32Array.Builder valBuilder = (Int32Array.Builder)builder;
                            foreach (var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    case ArrowTypeId.Int64:
                        {
                            Int64Array valArray = (Int64Array)array;
                            Int64Array.Builder valBuilder = (Int64Array.Builder)builder;
                            foreach (var v in valArray) valBuilder.Append(v);
                            break;
                        }
                    default: throw new NotImplementedException();
                }
            }
            else
            {
                var builder = Arrays[(int)entry.SchemaIndex - 1];
                var dtype = DataTypes[(int)entry.SchemaIndex - 1];
                switch (dtype.TypeId)
                {
                    case ArrowTypeId.Double:
                        {
                            for(var j = 0; j < k; j++)
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
        }
        NumberOfPoints += (ulong)k;

        return 0;
    }

    public override int Add(ulong entryIndex, IEnumerable<Array> arrays)
    {
        var kvs = ArrayIndex.Entries.Zip(arrays).ToDictionary();
        return Add(entryIndex, kvs);
    }

    public override RecordBatch GetRecordBatch()
    {
        List<Array> cols = [Index.Build()];
        foreach(var (dtype, builder) in DataTypes.Zip(Arrays))
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