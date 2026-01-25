using MZPeak.Storage;
using MZPeak.Reader;
using MZPeak.Writer;
using Apache.Arrow;
using MZPeak.ControlledVocabulary;
using Apache.Arrow.Types;
using System.Text.Json.Nodes;
using System.Text.Json;
using MZPeak.Reader.Visitors;
using MZPeak.Metadata;
using MZPeak.Writer.Data;
using MZPeak.Compute;

namespace MzPeakTests;


public class WriteTest
{
    IMZPeakArchiveStorage PointArchive;
    public WriteTest() {
        string fileName = "small.mzpeak";
        string baseDirectory = AppContext.BaseDirectory; // Gets the directory where tests are running
        string fullPath = Path.Combine(baseDirectory, fileName);
        PointArchive = new LocalZipArchive(fullPath);
    }

    [Fact]
    public void BuildArrayIndexTest()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        var index = builder.Build();
        Assert.Equal("point.mz", index.Entries[0].Path);
        Assert.Equal(1u, index.Entries[0].SortingRank);
        Assert.Equal("point.intensity", index.Entries[1].Path);
        Assert.Null(index.Entries[1].SortingRank);
    }

    [Fact]
    public void PointLayoutBuilderTest()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        var index = builder.Build();
        var writer = new PointLayoutBuilder(index);
        writer.Add(0, [
            new DoubleArray.Builder().AppendRange([250.0]).Build(),
            new FloatArray.Builder().AppendRange([1023.1f]).Build()
        ]);
        writer.Add(1, [
            new DoubleArray.Builder().AppendRange([252.0]).Build(),
            new FloatArray.Builder().AppendRange([1026.1f]).Build()
        ]);
        Assert.Equal(2ul, writer.NumberOfPoints);
    }

    [Fact]
    public async Task TranscribePointsArrow()
    {
        var reader = new MzPeakReader(PointArchive);

        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);

        ChunkedArray? data = await reader.GetSpectrumData(0);
        Assert.NotNull(data);
        var chunk = (StructArray)data.Array(0);
        var n0 = chunk.Length;

        var index = builder.Build();
        var writer = new PointLayoutBuilder(index);

        writer.Add(0, [(Apache.Arrow.Array)chunk.Fields[1], (Apache.Arrow.Array)chunk.Fields[2]]);

        data = await reader.GetSpectrumData(1);
        Assert.NotNull(data);
        chunk = (StructArray)data.Array(0);
        var n1 = chunk.Length;

        writer.Add(1, [(Apache.Arrow.Array)chunk.Fields[1], (Apache.Arrow.Array)chunk.Fields[2]]);

        var batch = writer.GetRecordBatch();
        Assert.Equal(1, batch.ColumnCount);
        var points = (StructArray)batch.Column(0);
        Assert.Equal(3, points.Fields.Count);

        var idxArr = (UInt64Array)points.Fields[0];
        var mask0 = Compute.Equal(idxArr, 0ul);
        var points0 = Compute.Filter(points, mask0);
        Assert.Equal(n0, points0.Length);

        var mask1 = Compute.Equal(idxArr, 1ul);
        var points1 = Compute.Filter(points, mask1);
        Assert.Equal(n1, points1.Length);
    }

    [Fact]
    public async Task TranscribePointsParquet()
    {
        var stream = new MemoryStream();
        var writer = new MZPeakWriter(new ZipStreamArchiveWriter<MemoryStream>(stream));

        var reader = new MzPeakReader(PointArchive);
        Assert.NotNull(reader);
        var dat0 = await reader.GetSpectrumData(0);
        var meta0 = reader.GetSpectrumMeta(0);
        Assert.NotNull(dat0);
        Assert.NotNull(meta0);
        writer.AddSpectrumData(
            writer.CurrentSpectrum,
            ((StructArray)dat0.Array(0)).Fields.Skip(1).Select(a => (Apache.Arrow.Array)a)
        );

        writer.AddSpectrum(meta0.Id, meta0.Time, null, meta0.MzDeltaModel, meta0.Parameters, []);
    }

    [Fact]
    public void WriteMemory_Test()
    {
        var stream = new MemoryStream();
        var writer = new MZPeakWriter(new ZipStreamArchiveWriter<MemoryStream>(stream));
        writer.AddSpectrum("foobar", 299.0, null, null, [new Param("baz", 5)], []);
        writer.WriteSpectrumMetadata();
        writer.Dispose();
        stream.Flush();
        stream.Seek(0, SeekOrigin.Begin);

        Assert.True(stream.CanRead);

        var readerStorage = new ZipArchiveStream<MemoryStream>(stream);
        var reader = new MzPeakReader(readerStorage);

        var meta = reader.SpectrumMetadata;
        Assert.NotNull(meta);

        var idArr = (StringArray)meta.Column("id");
        Assert.Equal("foobar", idArr.GetString(0));
        var indexArr = (UInt64Array)meta.Column("index");
        Assert.Equal(0ul, indexArr.GetValue(0));

        var paramsList = (ListArray)meta.Column("parameters");
        var visitor = new ParamListVisitor();
        paramsList.Accept(visitor);
        Assert.Single(visitor.ParamsLists);
        Assert.Single(visitor.ParamsLists[0]);
        var paramVal = visitor.ParamsLists[0][0];
        Assert.Equal("baz", paramVal.Name);
        Assert.True(paramVal.IsLong());
        Assert.Equal(5L, paramVal.AsLong());
    }
}