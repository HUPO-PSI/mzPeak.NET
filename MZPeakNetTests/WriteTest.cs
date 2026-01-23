using MZPeak.Storage;
using MZPeak.Reader;
using MZPeak.Writer;
using Apache.Arrow;

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
    public void WriteMemory_Test()
    {
        var stream = new MemoryStream();
        var writer = new MZPeakWriter(new ZipStreamArchiveWriter<MemoryStream>(stream));
        writer.AddSpectrum("foobar", 299.0, null, 0, null, []);
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
    }
}