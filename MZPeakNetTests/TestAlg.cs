namespace MzPeakTests;

using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using MZPeak.Compute;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Reader;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;


public class NullInterpolationTest
{
    IMZPeakArchiveStorage PointArchive;

    public NullInterpolationTest()
    {
        string fileName = "small.mzpeak";
        string baseDirectory = AppContext.BaseDirectory; // Gets the directory where tests are running
        string fullPath = Path.Combine(baseDirectory, fileName);
        PointArchive = new LocalZipArchive(fullPath);
    }

    [Fact]
    public void TestMasking()
    {
        var v = new List<int>()
        {
            1,2,3,4,
            6,7,8,9
        };

        var spans = Compute.IndicesToSpans(v);
        Assert.Equal(2, spans.Count);
        Assert.Equal((1, 4), spans[0]);
        Assert.Equal((6, 9), spans[1]);

        v =
        [
            1,2,4,
            6,7,8,9
        ];
        spans = Compute.IndicesToSpans(v);
        Assert.Equal((1, 2), spans[0]);
        Assert.Equal((4, 4), spans[1]);
        Assert.Equal((6, 9), spans[2]);

        var builder = new Int32Array.Builder();
        builder.AppendRange([
            0,
            1,
            2,
            4,
            5,
            6,
            7,
            8,
            9,
            10
        ]);
        var vals = builder.Build();
        var subset = (Int32Array)Compute.Take(vals, spans);

        foreach (var i in v)
        {
            var j = vals.GetValue(i);
            Assert.NotNull(j);
            Assert.Contains(j, subset);
        }
    }

    [Fact]
    public async Task TestLearnDelta()
    {
        var reader = new MzPeakReader(PointArchive);
        var specData = await reader.GetSpectrumData(0);
        Assert.NotNull(specData);

        var chunk = specData;
        Assert.Equal(0, chunk.NullCount);

        var mzsArr = (DoubleArray)chunk.Fields[1];
        Assert.Equal(0, mzsArr.NullCount);
        var mzs = mzsArr.ToList();
        var intensitiesArr = (FloatArray)chunk.Fields[2];
        Assert.Equal(0, intensitiesArr.NullCount);
        var intensities = intensitiesArr.Select((v) => (double?)v).ToList();
        Assert.Equal(mzs.Count, intensities.Count);

        var deltas = NullInterpolation.CollectDeltas(mzs, sort: false);
        var model = SpacingInterpolationModel<double>.Fit(
            mzs.Skip(1).ToList(),
            deltas,
            intensities.Skip(1).ToList()
        );
        Assert.Equal(3, model.Coefficients.Count);
        for (var i = 0; i < model.Coefficients.Count; i++)
        {
            Assert.True(Math.Abs(model.Coefficients[i]) < 1e-6);
        }
    }

    [Fact]
    public void TestChunking()
    {
        var reader = new MzPeakReader(PointArchive);
        var specData = reader.GetSpectrumDataSync(0);
        Assert.NotNull(specData);

        var chunk = specData;
        Assert.Equal(0, chunk.NullCount);

        var mzsArr = (DoubleArray)chunk.Fields[1];
        Assert.Equal(0, mzsArr.NullCount);
        var intensitiesArr = (FloatArray)chunk.Fields[2];

        var splits = Chunking.ChunkEvery(mzsArr, 50.0);

        var mask = Compute.Invert(ZeroRunRemoval.IsZeroPairMask(intensitiesArr));
        var maskedMzs = Compute.NullifyAt(mzsArr, mask);
        Assert.Equal(11213, maskedMzs.NullCount);
        var splitsMasked = Chunking.ChunkEvery(maskedMzs, 50.0);
        foreach (var (ii, jj) in splitsMasked.Zip(splits))
        {
            Assert.Equal(ii, jj);
        }
    }
}


public class GridTest
{
    [Fact]
    public void TestLinearGridRoundTrip()
    {
        var grid = new LinearGrid(intercept: 400.0, slope: 0.001, scale: 1.0);

        Assert.Equal(400.0, grid.FromIndex(0), 10);
        Assert.Equal(400.1, grid.FromIndex(100), 10);
        Assert.Equal(401.0, grid.FromIndex(1000), 10);

        Assert.Equal(0u, (uint)grid.ToIndex(400.0));
        Assert.Equal(100u, (uint)grid.ToIndex(400.1));
        Assert.Equal(1000u, (uint)grid.ToIndex(401.0));
    }

    [Fact]
    public void TestLinearGridRoundTripWithScale()
    {
        // A non-unit scale rescales the physical coordinate before it is mapped to an index,
        // e.g. converting from Th to some internal fixed-point unit.
        var grid = new LinearGrid(intercept: 50.0, slope: 0.25, scale: 2.0);

        double value = grid.FromIndex(4);
        Assert.Equal(25.5, value, 10);
        Assert.Equal(4u, (uint)grid.ToIndex(value));
    }

    [Fact]
    public void TestSquareRootLinearGridRoundTrip()
    {
        var grid = new SquareRootLinearGrid(intercept: 2.0, slope: 0.1, scale: 1.0);

        double value = grid.FromIndex(5);
        Assert.Equal(6.25, value, 10);
        Assert.Equal(5u, (uint)grid.ToIndex(value));

        value = grid.FromIndex(0);
        Assert.Equal(4.0, value, 10);
        Assert.Equal(0u, (uint)grid.ToIndex(value));
    }

    [Fact]
    public void TestLinearGridFit()
    {
        double low = 400.0;
        double high = 1600.0;
        double scale = 1.0;
        double slots = uint.MaxValue;

        // Build a "ground truth" grid whose slope matches the step size Fit will assume,
        // so the recovered parameters should match the ones used to generate the data.
        var trueGrid = new LinearGrid(low, (high * scale - low * scale) / slots, scale);

        var values = new List<double>();
        for (int i = 0; i <= 1000; i++)
        {
            uint index = (uint)((ulong)i * uint.MaxValue / 1000);
            values.Add(trueGrid.FromIndex(index));
        }

        var fitted = LinearGrid.Fit(values, low, high, scale);

        Assert.True(Math.Abs(fitted.Intercept - trueGrid.Intercept) < 1e-3);
        Assert.True(Math.Abs(fitted.Slope - trueGrid.Slope) < 1e-13);
        Assert.True(((GridLike)fitted).MaxError(values) < 1e-6);
    }

    [Fact]
    public void TestSquareRootLinearGridFit()
    {
        double low = 400.0;
        double high = 1600.0;
        double scale = 1.0;
        double slots = uint.MaxValue;

        // Same idea as the linear case, but in sqrt-space: Fit buckets sqrt(value) using the
        // same (non-sqrt-adjusted) step size, so we match that when generating the ground truth.
        double trueSlope = (high * scale - low * scale) / slots;
        double trueIntercept = Math.Sqrt(low * scale);
        var trueGrid = new SquareRootLinearGrid(trueIntercept, trueSlope, scale);

        var values = new List<double>();
        for (int i = 0; i <= 1000; i++)
        {
            uint index = (uint)((ulong)i * uint.MaxValue / 1000);
            values.Add(trueGrid.FromIndex(index));
        }

        var fitted = SquareRootLinearGrid.Fit(values, low, high, scale);

        Assert.True(Math.Abs(fitted.Intercept - trueGrid.Intercept) < 1e-3);
        Assert.True(Math.Abs(fitted.Slope - trueGrid.Slope) < 1e-13);
        Assert.True(((GridLike)fitted).MaxError(values) < 1e-3);
    }

    [Fact]
    public void TestBrukerTimsTOFTimsLinearGrid2RoundTrip()
    {
        var grid = new BrukerTimsTOFTimsLinearGrid2(c6: 0.02, c7: 500.0, intercept: 50.0, slope: 0.001);

        for (uint index = 0; index <= 200_000; index += 5_000)
        {
            double value = grid.FromIndex(index);
            double recovered = grid.ToIndex(value);
            Assert.True(Math.Abs(recovered - index) <= 1, $"index {index} -> value {value} -> recovered {recovered}");
        }
    }

    [Fact]
    public void TestBrukerTimsTOFTimsLinearGrid2Parameters()
    {
        var grid = new BrukerTimsTOFTimsLinearGrid2(c6: 0.02, c7: 500.0, intercept: 50.0, slope: 0.001);

        var round = (BrukerTimsTOFTimsLinearGrid2)BrukerTimsTOFTimsLinearGrid2.FromParameters(
            BrukerTimsTOFTimsLinearGrid2.Accession, grid.Parameters());

        Assert.Equal(grid.C6, round.C6);
        Assert.Equal(grid.C7, round.C7);
        Assert.Equal(grid.Intercept, round.Intercept);
        Assert.Equal(grid.Slope, round.Slope);
    }

    [Fact]
    public void TestBrukerTimsTOFMzGrid2RoundTripQuadratic()
    {
        // c3 = 0 exercises the closed-form quadratic branch of FromIndex.
        var grid = new BrukerTimsTOFMzGrid2(c0: 1000.0, beta: 100.0, c2: 1e-3, c3: 0.0, c4: 5.0, slope: 0.0025, intercept: 1000.0);

        for (uint index = 0; index <= 2_000_000; index += 50_000)
        {
            double value = grid.FromIndex(index);
            double recovered = grid.ToIndex(value);
            Assert.True(Math.Abs(recovered - index) <= 1, $"index {index} -> value {value} -> recovered {recovered}");
        }
    }

    [Fact]
    public void TestBrukerTimsTOFMzGrid2RoundTripCubic()
    {
        // c3 != 0 exercises the Newton-Raphson branch of FromIndex.
        var grid = new BrukerTimsTOFMzGrid2(c0: 1000.0, beta: 100.0, c2: 1e-3, c3: 1e-6, c4: 5.0, slope: 0.0025, intercept: 1000.0);

        for (uint index = 0; index <= 2_000_000; index += 50_000)
        {
            double value = grid.FromIndex(index);
            double recovered = grid.ToIndex(value);
            Assert.True(Math.Abs(recovered - index) <= 1, $"index {index} -> value {value} -> recovered {recovered}");
        }
    }

    [Fact]
    public void TestBrukerTimsTOFMzGrid2Parameters()
    {
        var grid = new BrukerTimsTOFMzGrid2(c0: 1000.0, beta: 100.0, c2: 1e-3, c3: 1e-6, c4: 5.0, slope: 0.0025, intercept: 1000.0);

        var round = (BrukerTimsTOFMzGrid2)BrukerTimsTOFMzGrid2.FromParameters(
            BrukerTimsTOFMzGrid2.Accession, grid.Parameters());

        Assert.Equal(grid.C0, round.C0);
        Assert.Equal(grid.Beta, round.Beta);
        Assert.Equal(grid.C2, round.C2);
        Assert.Equal(grid.C3, round.C3);
        Assert.Equal(grid.C4, round.C4);
        Assert.Equal(grid.Slope, round.Slope);
        Assert.Equal(grid.Intercept, round.Intercept);
    }
}