using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Reader.Visitors;
using MZPeak.Storage;
using MZPeak.Writer;
using ThermoFisher.CommonCore.Data;
using ThermoFisher.CommonCore.Data.Business;
using ThermoFisher.CommonCore.Data.FilterEnums;
using ThermoFisher.CommonCore.Data.Interfaces;

namespace MZPeak.Thermo;


public class ConversionContextHelper
{
    private const string InjectionTimeKey = "Ion Injection Time (ms)";
    private const string ScanEventKey = "Scan Evnet";
    private const string MasterScanKey = "Master Scan";
    private const string MonoisotopicMZKey = "Monoisotopic M/Z";
    private const string ChargeStateKey = "Charge State";
    private static readonly string[] IsolationLevelKeys = [
        "MS2 Isolation Width",
        "MS3 Isolation Width",
        "MS4 Isolation Width",
        "MS5 Isolation Width",
        "MS6 Isolation Width",
        "MS7 Isolation Width",
        "MS8 Isolation Width",
        "MS9 Isolation Width",
        "MS10 Isolation Width"
    ];

    private static readonly string OrbitrapResolutionKey = "Orbitrap Resolution";
    private static readonly string FTResolutionKey = "FT Resolution";

    /// <summary>
    /// An index look up mapping trailer keys by index that lets us avoid
    /// looping over all trailer entries
    /// </summary>
    public Dictionary<string, int> TrailerMap;
    public List<HeaderItem> Headers;

    public ConversionContextHelper()
    {
        TrailerMap = new();
        Headers = new();
    }

    public bool GetShortTrailerExtraFor(IRawDataPlus accessor, int scanNumber, string key, out short value)
    {
        object tmp;
        HeaderItem header;
        int headerIdx;

        if (TrailerMap.TryGetValue(key, out headerIdx))
        {
            tmp = accessor.GetTrailerExtraValue(scanNumber, headerIdx);
            header = Headers[headerIdx];
            if (tmp != null)
            {
                try
                {
                    switch (header.DataType)
                    {
                        case GenericDataTypes.SHORT:
                            {
                                value = (short)tmp;
                                return true;
                            }
                        case GenericDataTypes.LONG:
                            {
                                value = Convert.ToInt16(tmp);
                                return true;
                            }
                        case GenericDataTypes.ULONG:
                            {
                                value = Convert.ToInt16(tmp);
                                return true;
                            }
                        case GenericDataTypes.USHORT:
                            {
                                value = (short)(ushort)tmp;
                                return true;
                            }
                        default:
                            {
                                value = Convert.ToInt16(tmp);
                                return true;
                            }
                    }
                }
                catch (InvalidCastException)
                {
                    value = Convert.ToInt16(tmp);
                    return true;
                }

            }
        }
        value = 0;
        return false;
    }

    public bool GetIntTrailerExtraFor(IRawDataPlus accessor, int scanNumber, string key, out int value, int defaultValue = 0)
    {
        object tmp;
        HeaderItem header;
        int headerIdx;

        if (TrailerMap.TryGetValue(key, out headerIdx))
        {
            tmp = accessor.GetTrailerExtraValue(scanNumber, headerIdx);
            header = Headers[headerIdx];
            if (tmp != null)
            {
                try
                {
                    switch (header.DataType)
                    {
                        case GenericDataTypes.SHORT:
                            {
                                value = (short)tmp;
                                return true;
                            }
                        case GenericDataTypes.LONG:
                            {
                                value = (int)(long)tmp;
                                return true;
                            }
                        case GenericDataTypes.ULONG:
                            {
                                value = (int)(ulong)tmp;
                                return true;
                            }
                        case GenericDataTypes.USHORT:
                            {
                                value = (ushort)tmp;
                                return true;
                            }
                        default:
                            {
                                value = Convert.ToInt32(tmp);
                                return true;
                            }
                    }
                }
                catch (InvalidCastException)
                {
                    value = Convert.ToInt32(tmp);
                    return true;
                }
            }
        }
        value = defaultValue;
        return false;
    }

    public bool GetDoubleTrailerExtraFor(IRawDataPlus accessor, int scanNumber, string key, out double value)
    {
        object tmp;
        HeaderItem header;
        int headerIdx;

        if (TrailerMap.TryGetValue(key, out headerIdx))
        {
            tmp = accessor.GetTrailerExtraValue(scanNumber, headerIdx);
            header = Headers[headerIdx];
            if (tmp != null)
            {
                try
                {
                    switch (header.DataType)
                    {
                        case GenericDataTypes.FLOAT:
                            {
                                value = (float)tmp;
                                return true;
                            }
                        case GenericDataTypes.DOUBLE:
                            {
                                value = (double)tmp;
                                return true;
                            }
                            ;
                        default:
                            {
                                value = Convert.ToDouble(tmp);
                                return true;
                            }
                    }
                }
                catch (InvalidCastException)
                {
                    value = Convert.ToDouble(tmp);
                    return true;
                }
            }
        }
        value = 0;
        return false;
    }

    public void Initialize(IRawDataPlus accessor)
    {
        var headers = accessor.GetTrailerExtraHeaderInformation();
        for (var i = 0; i < headers.Length; i++)
        {
            var header = headers[i];
            var label = header.Label.TrimEnd(':');
            TrailerMap[label] = i;
        }
        Headers = headers.ToList();
    }

    public Dictionary<(MassAnalyzerType, IonizationModeType), long> FindAllMassAnalyzers(IRawDataPlus accessor)
    {
        var analyzers = new Dictionary<(MassAnalyzerType, IonizationModeType), long>();
        var events = accessor.ScanEvents;

        int counter = 0;
        for (var segmentIdx = 0; segmentIdx < events.Segments; segmentIdx++)
        {
            for (var eventIdx = 0; eventIdx < events.GetEventCount(segmentIdx); eventIdx++)
            {
                var ev = events.GetEvent(segmentIdx, eventIdx);
                var a = ev.MassAnalyzer;
                var i = ev.IonizationMode;
                if (analyzers.ContainsKey((a, i)))
                {
                    continue;
                }
                analyzers.Add((a, i), counter);
                counter += 1;
            }
        }
        return analyzers;
    }
}


public class ThermoMZPeakWriter
{
    MZPeakWriter Writer;

    protected static ArrayIndex DefaultSpectrumArrayIndex()
    {
        var builder = ArrayIndexBuilder.PointBuilder(BufferContext.Spectrum);
        builder.Add(ArrayType.MZArray, BinaryDataType.Float64, Unit.MZ, 1);
        builder.Add(ArrayType.IntensityArray, BinaryDataType.Float32, Unit.NumberOfDetectorCounts);
        return builder.Build();
    }

    public ThermoMZPeakWriter(IMZPeakArchiveWriter storage, ArrayIndex? spectrumArrayIndex=null, ArrayIndex? chromatogramArrayIndex=null)
    {
        if (spectrumArrayIndex == null)
        {
            spectrumArrayIndex = DefaultSpectrumArrayIndex();
        }
        Writer = new MZPeakWriter(storage, spectrumArrayIndex);
    }

    /// <summary>
    /// A static lookup to convert Thermo's MSOrderType enum to an MS level integer
    /// </summary>
    private static Dictionary<MSOrderType, long> MSLevelMap = new Dictionary<MSOrderType, long>() {
            {MSOrderType.Ms, 1},
            {MSOrderType.Ms2, 2},
            {MSOrderType.Ms3, 3},
            {MSOrderType.Ms4, 4},
            {MSOrderType.Ms5, 5},
            {MSOrderType.Ms6, 6},
            {MSOrderType.Ms7, 7},
            {MSOrderType.Ms8, 8},
            {MSOrderType.Ms9, 9},
            {MSOrderType.Ms10, 10},
            {MSOrderType.Ng, 2},
            {MSOrderType.Nl, 2},
            {MSOrderType.Par, 2},
        };

    Param GetMSLevel(IScanFilter scanFilter)
    {
        var msLevel = MSLevelMap[scanFilter.MSOrder];
        return new Param(SpectrumProperties.MsLevel.Name(), SpectrumProperties.MsLevel.CURIE(), msLevel);
    }

    Param GetPolarity(IScanFilter scanFilter)
    {
        switch (scanFilter.Polarity)
        {
            case PolarityType.Negative:
                {
                    return new Param(ScanPolarity.ScanPolarity.Name(), ScanPolarity.ScanPolarity.CURIE(), -1);
                }
            case PolarityType.Positive:
                {
                    return new Param(ScanPolarity.ScanPolarity.Name(), ScanPolarity.ScanPolarity.CURIE(), 1);
                }
            case PolarityType.Any:
                {
                    return new Param(ScanPolarity.ScanPolarity.Name(), ScanPolarity.ScanPolarity.CURIE(), 0);
                }
            default: throw new InvalidOperationException();
        }
    }

    public ulong AddSpectrumMetadata(
        int scanNumber,
        double time,
        IScanFilter scanFilter,
        ScanStatistics scanStatistics,
        List<double>? spacingModel=null,
        List<Param>? @params=null,
        List<AuxiliaryArray>? auxiliaryArrays=null)
    {
        List<Param> paramList = @params ?? new();
        paramList.Add(GetMSLevel(scanFilter));
        paramList.Add(GetPolarity(scanFilter));
        paramList.Add(new Param(
            SpectrumProperties.BasePeakIntensity.Name(),
            SpectrumProperties.BasePeakIntensity.CURIE(),
            scanStatistics.BasePeakIntensity,
            Unit.NumberOfDetectorCounts.CURIE()
        ));
        paramList.Add(new Param(
            SpectrumProperties.BasePeakMZ.Name(),
            SpectrumProperties.BasePeakMZ.CURIE(),
            scanStatistics.BasePeakMass,
            Unit.MZ.CURIE()
        ));
        paramList.Add(new Param(
            SpectrumProperties.TotalIonCurrent.Name(),
            SpectrumProperties.TotalIonCurrent.CURIE(),
            scanStatistics.TIC,
            Unit.NumberOfDetectorCounts.CURIE()
        ));
        if (scanStatistics.IsCentroidScan)
            paramList.Add(SpectrumRepresentation.CentroidSpectrum.AsParam());
        else
            paramList.Add(SpectrumRepresentation.ProfileSpectrum.AsParam());

        var id = $"controllerType=0 controllerNumber=1 scan={scanNumber + 1}";
        var index = Writer.AddSpectrum(id, time, null, spacingModel, paramList, auxiliaryArrays);
        return index;
    }

    public void AddScanMetadata(
        ulong sourceIndex,
        int scanNumber,
        double time,
        IScanFilter scanFilter,
        ScanStatistics scanStatistics,
        List<Param>? @params = null
    )
    {
        var instrumentRef = 0u;
        List<Param> paramList = @params ?? new();
        if (scanFilter.CompensationVoltageCount == 1)
        {
            paramList.Add(new Param(
                ScanAttribute.FaimsCompensationVoltage.Name(),
                ScanAttribute.FaimsCompensationVoltage.CURIE(),
                scanFilter.CompensationVoltageValue(0),
                unit: Unit.Volt.CURIE()
            ));
        } else if (scanFilter.CompensationVoltageCount > 1)
            throw new NotImplementedException("Multiple FAIMS CV not yet supported");

        List<ScanWindow> scanWindows = [new ScanWindow(scanStatistics.LowMass, scanStatistics.HighMass, Unit.MZ)];

        paramList.Add(new Param(
            ScanAttribute.FilterString.Name(),
            ScanAttribute.FilterString.CURIE(),
            scanFilter.ToString())
        );

        Writer.AddScan(sourceIndex, instrumentRef, paramList);
    }

}
