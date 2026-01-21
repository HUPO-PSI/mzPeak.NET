namespace MZPeak.Compute;

using System.Numerics;

using Apache.Arrow;
using Apache.Arrow.Types;

using MathNet.Numerics.LinearAlgebra;

public class SpacingInterpolationModel<T> where T : struct, INumber<T>
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

    public T MeanSquaredError(List<T?> coordinates, List<T> deltas)
    {
        var acc = T.Zero;
        var n = T.Zero;
        foreach(var (x, y) in coordinates.Zip(deltas))
        {
            if (x == null) continue;
            var e = y - Predict((T)x);
            acc += e * e;
            n += T.One;
        }
        return acc / n;
    }

    public static SpacingInterpolationModel<U> FitMedian<U>(List<U?> coordinates) where U: struct, INumber<U>
    {
        var value = NullInterpolation.LocalMedianDelta(coordinates);
        return new(new(){value});
    }
    public static SpacingInterpolationModel<U> FitRegression<U>(List<U?> coordinates, List<U> deltas, List<U?>? weights = null, U? deltaThreshold = null, int rank = 2) where U: struct, INumber<U>, IRootFunctions<U>
    {
        if (deltaThreshold == null) deltaThreshold = U.One;

        var columns = new List<List<U>>();
        for (var i = 0; i <= rank; i++)
        {
            columns.Add(new());
        }
        var weightsTransformed = new List<U>();
        var deltasFiltered = new List<U>();
        for (var i = 0; i < coordinates.Count; i++)
        {
            var viMaybe = coordinates[i];
            if (viMaybe == null) throw new InvalidDataException("Values cannot be null");
            if (deltas[i] > deltaThreshold) continue;
            deltasFiltered.Add(deltas[i]);
            var vi = (U)viMaybe;
            var v = U.One;
            var w = weights == null ? U.One : U.RootN(weights[i] ?? U.One, 2);

            weightsTransformed.Add(w);
            for (var r = 0; r <= rank; r++)
            {
                columns[r].Add(v);
                v *= vi;
            }
        }
        var cholWeights = MathNet.Numerics.LinearAlgebra.Vector<U>.Build.DenseOfEnumerable(weightsTransformed);
        var y = MathNet.Numerics.LinearAlgebra.Vector<U>.Build.DenseOfEnumerable(deltasFiltered);

        var data = Matrix<U>.Build.DenseOfColumns(columns);
        var QR = data.MapIndexed((i, j, v) => cholWeights[i] * v).QR();
        var cholY = cholWeights.PointwiseMultiply(y);
        var V = QR.Q.Transpose().Multiply(cholY);
        var betas = QR.R.Solve(V);

        SpacingInterpolationModel<U> model = new(betas.ToList());
        return model;
    }

    public static SpacingInterpolationModel<U> Fit<U>(List<U?> coordinates, List<U> deltas, List<U?>? weights = null, U? deltaThreshold = null, int rank = 2) where U : struct, INumber<U>, IRootFunctions<U>
    {
        var simpleModel = FitMedian(coordinates);
        if (deltas.Count <= 3) return simpleModel;
        var regressionModel = FitRegression(coordinates, deltas, weights, deltaThreshold, rank);
        var simpleErr = simpleModel.MeanSquaredError(coordinates, deltas);
        var regressionErr = regressionModel.MeanSquaredError(coordinates, deltas);
        if (simpleErr < regressionErr) return simpleModel;
        else return regressionModel;
    }
}

public static class ZeroRunRemoval
{
    public static List<int> WhereNotZeroRun<T>(IList<T?> data) where T: INumber<T>
    {
        List<int> acc = new();

        int n = data.Count;
        int n1 = n - 1;
        bool wasZero = false;
        int i = 0;
        while(i < n)
        {
            var v = data[i];
            if (v != null)
            {
                if (v == T.Zero)
                {
                    if (wasZero || (acc.Count == 0 && i < n1 && data[i + 1] == T.Zero) || i == n1)
                    {}
                    else
                    {
                        acc.Add(i);
                    }
                    wasZero = true;
                }
                else
                {
                    acc.Add(i);
                    wasZero = false;
                }
            }
            i += 1;
        }
        return acc;
    }

    public static List<int> WhereNotZeroRun<T>(PrimitiveArray<T> data) where T : struct, INumber<T>
    {
        List<int> acc = new();

        int n = data.Length;
        int n1 = n - 1;
        bool wasZero = false;
        int i = 0;
        while (i < n)
        {
            var v = data.GetValue(i);
            if (v != null)
            {
                if (v == T.Zero)
                {
                    if (wasZero || (acc.Count == 0 && i < n1 && data.GetValue(i + 1) == T.Zero) || i == n1)
                    { }
                    else
                    {
                        acc.Add(i);
                    }
                    wasZero = true;
                }
                else
                {
                    acc.Add(i);
                    wasZero = false;
                }
            }
            i += 1;
        }
        return acc;
    }

    public static BooleanArray IsZeroPairMask<T>(IList<T?> data) where T: INumber<T>
    {
        int n = data.Count;
        int n1 = n - 1;
        bool wasZero = false;
        var acc = new BooleanArray.Builder();
        for(var i = 0; i < data.Count; i++)
        {
            var v = data[i];
            if (v == null)
            {
                acc.Append(true);
            }
            else
            {
                if (v == T.Zero)
                {
                    if (wasZero || (i < n1 && data[i + 1] == T.Zero))
                    {
                        acc.Append(true);
                    }
                    else
                    {
                        acc.Append(false);
                    }
                    wasZero = true;
                }
                else
                {
                    acc.Append(false);
                    wasZero = false;
                }
            }
        }
        return acc.Build();
    }

    public static BooleanArray IsZeroPairMask<T>(PrimitiveArray<T> data) where T : struct, INumber<T>
    {
        int n = data.Length;
        int n1 = n - 1;
        bool wasZero = false;
        var acc = new BooleanArray.Builder();
        for (var i = 0; i < data.Length; i++)
        {
            var v = data.GetValue(i);
            if (v == null)
            {
                acc.Append(true);
            }
            else
            {
                if (v == T.Zero)
                {
                    if (wasZero || (i < n1 && data.GetValue(i + 1) == T.Zero))
                    {
                        acc.Append(true);
                    }
                    else
                    {
                        acc.Append(false);
                    }
                    wasZero = true;
                }
                else
                {
                    acc.Append(false);
                    wasZero = false;
                }
            }
        }
        return acc.Build();
    }
}

public static class NullInterpolation
{
    public const string NullInterpolateCURIE = "MS:1003901";
    public const string NullZeroCURIE = "MS:1003902";

    public static List<T> CollectDeltas<T>(IEnumerable<T?> values, bool sort=true) where T : struct, INumber<T>
    {
        List<T> deltas = new();
        T last = default;
        int i = 0;
        foreach (var value in values)
        {
            if (value == null)
            {
                continue;
            }
            if (i == 0)
            {
                last = (T)value;
                i++;
            }
            else
            {
                var delta = (T)value - last;
                if (delta < T.Zero) throw new Exception($"{delta} = {value} - {last}");
                deltas.Add(delta);
                last = (T)value;
            }
        }
        if (sort) deltas.Sort();
        return deltas;
    }

    public static T SortedMedian<T>(List<T> values) where T : struct, INumber<T>
    {
        if (values.Count == 0)
        {
            return T.Zero;
        }
        else if (values.Count <= 2)
        {
            return values[0];
        }
        else
        {
            int mid = values.Count / 2;
            if (values.Count % 2 == 0)
            {
                return values[mid];
            }
            else
            {
                return (values[mid] + values[mid + 1]) / (T.One + T.One);
            }
        }
    }

    public static List<(int, int)> FindNullBounds(IArrowArray arrayValues)
    {
        List<(int, int)> bounds = new();
        if (arrayValues.Length == 0) return bounds;
        List<int> nullHere = new();
        for (int i = 0; i < arrayValues.Length; i++)
        {
            if (arrayValues.IsNull(i))
            {
                nullHere.Add(i);
            }
        }
        if (nullHere.Count == 0)
        {
            bounds.Add((0, arrayValues.Length));
            return bounds;
        }
        if (nullHere[0] != 0)
        {
            List<int> tmp = [0, .. nullHere];
            nullHere = tmp;
        }
        if (nullHere.Last() != arrayValues.Length - 1)
        {
            nullHere.Add(arrayValues.Length);
        }
        if (nullHere.Count % 2 != 0)
        {
            throw new InvalidDataException("The nulls in this data array are not properly paired");
        }
        for (int i = 0; i < nullHere.Count; i += 2)
        {
            bounds.Add((nullHere[i], nullHere[i + 1]));
        }
        return bounds;
    }

    public static void FillNullsWithModel<T, TBuilder>(PrimitiveArray<T> arrayValues, SpacingInterpolationModel<T> model, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> builder) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        var nBefore = arrayValues.Length;
        var bounds = FindNullBounds(arrayValues);
        var nVisited = 0;
        foreach (var (startIdx, endIdx) in bounds)
        {
            var chunk = (PrimitiveArray<T>)arrayValues.Slice(startIdx, endIdx - startIdx + 1);
            nVisited += chunk.Length;
            var startSize = builder.Length;
            var n = chunk.Length;
            // NullCount can only be 0, 1, or 2
            var nHasReal = n - chunk.NullCount;

            if (nHasReal == 1)
            {
                if (n == 2)
                {
                    if (chunk.IsNull(0))
                    {
                        var vAt = chunk.GetValue(1);
                        if (vAt == null) throw new InvalidDataException("Cannot both be null");
                        var vFill = (T)vAt - model.Predict((T)vAt);
                        builder.Append(vFill);
                        builder.Append((T)vAt);
                    }
                    else
                    {
                        var vAt = chunk.GetValue(0);
                        if (vAt == null) throw new InvalidDataException("Cannot both be null");
                        var vFill = (T)vAt + model.Predict((T)vAt);
                        builder.Append((T)vAt);
                        builder.Append(vFill);
                    }
                }
                else if (n == 3)
                {
                    var vAt = chunk.GetValue(1);
                    if (vAt == null) throw new InvalidDataException("Cannot both be null");
                    var vFill = (T)vAt - model.Predict((T)vAt);
                    builder.Append(vFill);
                    builder.Append((T)vAt);
                    vFill = (T)vAt + model.Predict((T)vAt);
                    builder.Append(vFill);
                }
                else throw new InvalidOperationException("This is impossible");
            }
            else
            {
                var delta = LocalMedianDelta(chunk);
                if (chunk.IsNull(0))
                {
                    var vAt = chunk.GetValue(1);
                    if (vAt == null) throw new InvalidDataException("Cannot both be null");
                    var vFill = (T)vAt - delta;
                    builder.Append(vFill);
                } else
                {
                    var vAt = chunk.GetValue(0);
                    if (vAt == null) throw new InvalidOperationException("This should not happen");
                    builder.Append((T)vAt);
                }


                foreach (var v in (PrimitiveArray<T>)chunk.Slice(1, chunk.Length - 2))
                {
                    if (v == null) throw new InvalidDataException("Cannot both be null");
                    builder.Append((T)v);
                }
                if (chunk.IsNull(chunk.Length - 1))
                {
                    var vAt = chunk.GetValue(chunk.Length - 2);
                    if (vAt == null) throw new InvalidDataException("Cannot both be null");
                    builder.Append((T)vAt + delta);
                } else
                {
                    var vAt = chunk.GetValue(chunk.Length - 1);
                    if (vAt == null) throw new InvalidOperationException("This should not happen");
                    builder.Append((T)vAt);
                }
            }

            var endSize = builder.Length;
            if ((endSize - startSize) != chunk.Length) throw new InvalidOperationException(string.Format("chunk size {0} did not get fully copied", chunk.Length));
        }

        var nAfter = builder.Length;
        if (nBefore != nAfter) throw new InvalidOperationException(string.Format("Failed to preserve all data points during slicing {0} != {1}, {2}", nBefore, nAfter, nVisited));
    }

    public static T LocalMedianDelta<T>(IEnumerable<T?> arrayValues) where T : struct, INumber<T>
    {
        var deltas = CollectDeltas(arrayValues);
        if (deltas.Count == 0)
        {
            return T.Zero;
        }
        var median = SortedMedian(deltas);
        var deltasBelow = deltas.Where(v => v <= median).ToList();
        if (deltasBelow.Count == 0)
        {
            return median;
        }
        else
        {
            return SortedMedian(deltasBelow);
        }
    }
}

public static class NoCompressionCodec
{
    public const string CURIE = "MS:1000576";

    public static int Encode<T, TBuilder>(T startValue, IEnumerable<T?> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;
        foreach (var value in values)
        {
            if (value == null)
            {
                nNulls += 1;
                accumulator.AppendNull();
            }
            else
            {
                accumulator.Append((T)value);
            }
        }
        return nNulls;
    }

    public static int Decode<T, TBuilder>(T startValue, PrimitiveArray<T> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;
        accumulator.Append(startValue);
        foreach (var value in values)
        {
            if (value == null)
            {
                nNulls += 1;
                accumulator.AppendNull();
            }
            else
            {
                accumulator.Append((T)value);
            }
        }
        return nNulls;
    }
}

public static class DeltaCodec
{
    public const string CURIE = "MS:1003089";

    public static int Encode<T, TBuilder>(T? startValue, IEnumerable<T?> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;

        T? last = startValue;

        if (last == null)
        {
            nNulls += 1;
            accumulator.AppendNull();
        }
        foreach (var value in values)
        {
            if (value != null)
            {
                if (last != null)
                {
                    accumulator.Append((T)value - (T)last);
                }
                else
                {
                    accumulator.Append((T)value);
                }
                last = value;
            }
            else
            {
                accumulator.AppendNull();
                last = value;
                nNulls += 1;
            }
        }

        return nNulls;
    }

    public static int Decode<T, TBuilder>(T startValue, PrimitiveArray<T> values, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        int nNulls = 0;
        if (values.Length < 2)
        {
            throw new IndexOutOfRangeException("Cannot have a delta encoded chunk value slice size of less than two");
        }
        T? last = startValue;
        if (values.ElementAt(0) == null)
        {
            if (values.ElementAt(1) == null)
            {
                accumulator.Append(startValue);
            }
            last = default;
        }
        else
        {
            accumulator.Append(startValue);
        }

        foreach (var value in values)
        {
            if (value != null)
            {
                if (last == null)
                {
                    last = value;
                    accumulator.Append((T)value);
                }
                else
                {
                    last = last + value;
                    accumulator.Append((T)last);
                }
            }
            else
            {
                nNulls += 1;
                last = value;
                accumulator.AppendNull();
            }
        }
        return nNulls;
    }
}


public static class Compute
{
    static void NullToZero<T, TBuilder>(PrimitiveArray<T> array, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator) where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        foreach (var value in array)
        {
            accumulator.Append(value == null ? T.Zero : (T)value);
        }
    }

    public static IArrowArray IndicesToMask(IList<int> indices, int n)
    {
        BooleanArray.Builder acc = new();
        int j = 0;
        int m = indices.Count;
        int i = 0;
        for(i = 0; i < n && j < m; i++)
        {
            if (i < indices[j])
            {
                acc.Append(false);
            }
            else if (i == indices[j])
            {
                acc.Append(true);
                j += 1;
            }
            else if (i > indices[j])
            {
                var step = i - indices[j];
                acc.AppendRange(Enumerable.Repeat(false, step));
            }
        }
        while (i < n) acc.Append(false);
        if (acc.Length != n) throw new InvalidOperationException();
        return acc.Build();
    }

    public static List<(int, int)> IndicesToSpans(IList<int> indices)
    {
        List<(int, int)> acc = new();
        int? start = null;
        int? last = null;
        foreach(var i in indices)
        {
            if (last == null)
            {
                start = i;
                last = i;
            }
            else
            {
                if (i - last == 1)
                {
                    last = i;
                }
                else if (start != null)
                {
                    acc.Add(((int)start, (int)last));
                    start = i;
                    last = i;
                }
            }
        }
        if (start != null && last != null)
        {
            acc.Add(((int)start, indices.Last()));
        }
        return acc;
    }
    public static IArrowArray NullToZero<T>(PrimitiveArray<T> array) where T : struct, INumber<T>
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                {
                    var builder = new DoubleArray.Builder();
                    NullToZero((DoubleArray)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Float:
                {
                    var builder = new FloatArray.Builder();
                    NullToZero((FloatArray)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int32:
                {
                    var builder = new Int32Array.Builder();
                    NullToZero((Int32Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int64:
                {
                    var builder = new Int64Array.Builder();
                    NullToZero((Int64Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt32:
                {
                    var builder = new UInt32Array.Builder();
                    NullToZero((UInt32Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt64:
                {
                    var builder = new UInt64Array.Builder();
                    NullToZero((UInt64Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int16:
                {
                    var builder = new Int16Array.Builder();
                    NullToZero((Int16Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.Int8:
                {
                    var builder = new Int8Array.Builder();
                    NullToZero((Int8Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt16:
                {
                    var builder = new UInt16Array.Builder();
                    NullToZero((UInt16Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            case ArrowTypeId.UInt8:
                {
                    var builder = new UInt8Array.Builder();
                    NullToZero((UInt8Array)(IArrowArray)array, builder);
                    return builder.Build();
                }
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static IArrowArray Equal<T>(PrimitiveArray<T> lhs, T rhs) where T: struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var flag = a == rhs;
            cmp.Append(flag);
        }
        return cmp.Build();
    }

    public static IArrowArray Equal<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs) where T: struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
        if(lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        for(int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a == b;
            cmp.Append(flag);
        }
        return cmp.Build();
    }

    public static IArrowArray Filter(Array array, BooleanArray mask)
    {
        if (array.Length != mask.Length) throw new InvalidOperationException("Array and mask must have the same length");
        List<(int, int)> spans = new();
        int? start = null;
        for(int i = 0; i < mask.Length; i++)
        {
            var v = mask.GetValue(i);
            if(v != null && (bool)v)
            {
                if (start != null) {}
                else start = i;
            } else if (v != null && !(bool)v)
            {
                if (start != null)
                {
                    // Slices in Take include the trailing index
                    spans.Add(((int)start, i - 1));
                    start = null;
                }
                else {}
            }
        }
        if(start != null)
        {
            spans.Add(((int)start, mask.Length - 1));
        }
        return Take(array, spans);
    }

    public static IArrowArray Take(Array array, IList<(int, int)> spans)
    {
        if (spans.Count == 0)
        {
            return array.Slice(0, 0);
        }
        List<IArrowArray> chunks = new();
        foreach(var (start, end) in spans)
        {
            if (end < start || end < 0 || start < 0) throw new InvalidOperationException(string.Format("Invalid span: {0} {1}", start, end));
            chunks.Add(array.Slice(start, end - start + 1));
        }
        return ArrowArrayConcatenator.Concatenate(chunks);
    }

    public static IArrowArray Take(Array array, IList<int> indices)
    {
        if (indices.Count == 0)
        {
            return array.Slice(0, 0);
        }
        List<IArrowArray> chunks = new();
        for (var i = 0; i < indices.Count; i++)
        {
            chunks.Add(array.Slice(i, 1));
        }
        return ArrowArrayConcatenator.Concatenate(chunks);
    }
}