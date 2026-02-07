
using System.Threading.Tasks;
using MZPeak.ControlledVocabulary;
using MZPeak.Storage;
using MZPeak.Thermo;

using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.RandomAccessReaderPlugin;
using ThermoFisher.CommonCore.RawFileReader;

namespace MzPeakTests;
public class ThermoTranslationTest
{
    string TestRAWPath;

    public ThermoTranslationTest()
    {
        string fileName = "small.RAW";
        string baseDirectory = AppContext.BaseDirectory; // Gets the directory where tests are running
        TestRAWPath = Path.Combine(baseDirectory, fileName);
    }

    [Fact]
    public void TranslateThermoInMemory()
    {
        var readerManager = RawFileReaderAdapter.RandomAccessThreadedFileFactory(TestRAWPath, RandomAccessFileManager.Instance);
        var accessor = readerManager.CreateThreadAccessor();
        accessor.SelectInstrument(Device.MS, 1);
        accessor.IncludeReferenceAndExceptionData = true;

        var stream = new MemoryStream();
        var writerStorage = new ZipStreamArchiveWriter<MemoryStream>(stream);

        var writer = new ThermoMZPeakWriter(writerStorage, spectrumPeakArrayIndex: ThermoMZPeakWriter.PeakArrayIndex(true, true));
        writer.InitializeHelper(accessor);

        var startScan = accessor.RunHeader.FirstSpectrum;
        var lastScan = accessor.RunHeader.LastSpectrum;

        for(var scanNumber = startScan; scanNumber < lastScan; scanNumber++)
        {
            var scanFilter = accessor.GetFilterForScanNumber(scanNumber);
            var segments = accessor.GetSegmentedScanFromScanNumber(scanNumber);
            var statistics = accessor.GetScanStatsForScanNumber(scanNumber);
            var time = accessor.RetentionTimeFromScanNumber(scanNumber);

            var (spacingModel, auxArrays) = writer.AddSpectrumData(
                writer.CurrentSpectrum,
                segments,
                statistics);

            if (!statistics.IsCentroidScan)
            {
                auxArrays.AddRange(
                    writer.AddSpectrumPeakData(
                        writer.CurrentSpectrum,
                        accessor.GetCentroidStream(scanNumber, true)
                    )
                );
            }

            var key = writer.AddSpectrum(
                scanNumber,
                time,
                scanFilter,
                statistics,
                spacingModel?.Coefficients,
                auxiliaryArrays: auxArrays);

            var (precursorProps, acquisitionProperties) = writer.ExtractPrecursorAndTrailerMetadata(scanNumber, accessor, scanFilter, statistics);

            writer.AddScan(
                key,
                scanNumber,
                time,
                scanFilter,
                statistics,
                acquisitionProperties
            );

            if (precursorProps != null)
            {
                writer.AddPrecursor(
                    key,
                    precursorProps
                );
                writer.AddSelectedIon(
                    key,
                    precursorProps
                );
            }
        }


    }
}