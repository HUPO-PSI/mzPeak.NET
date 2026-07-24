using Apache.Arrow;
using MZPeak.ControlledVocabulary;
using MZPeak.Metadata;
using MZPeak.Writer.Data;

namespace MZPeak.Writer.Visitors;


public class ChromatogramMetadataBuilder
{
    public ChromatogramBuilder Chromatogram { get; }
    public PrecursorBuilder Precursor { get; }
    public SelectedIonBuilder SelectedIon { get; }
    public ulong ChromatogramCounter { get; protected set; }

    public int Length { get; private set; }

    public ChromatogramMetadataBuilder()
    {
        Chromatogram = new();
        Precursor = new();
        SelectedIon = new();
        ChromatogramCounter = 0;
    }
    /// <summary>
    /// Append a chromatogram row with all associated metadata.
    /// </summary>
    public ulong AppendChromatogram(
        string id,
        string? dataProcessingRef,
        List<Param> paramList,
        EntryDerivedMetadata? entryDerivedMetadata = null
    )
    {
        Chromatogram.Append(ChromatogramCounter, id, dataProcessingRef, paramList, entryDerivedMetadata);
        var index = ChromatogramCounter;
        ChromatogramCounter += 1;
        return index;
    }

    /// <summary>
    /// Append a precursor row associated with a chromatogram.
    /// </summary>
    public void AppendPrecursor(
        ulong sourceIndex,
        ulong? precursorIndex,
        string? precursorId,
        List<Param> isolationWindowParams,
        List<Param> activationParams
    )
    {
        if (sourceIndex >= ChromatogramCounter) throw new InvalidOperationException(string.Format("Source index {0} is greater than {1}", sourceIndex, ChromatogramCounter == 0 ? 0 : ChromatogramCounter - 1));
        if (precursorIndex >= ChromatogramCounter) throw new InvalidOperationException(string.Format("Precursor index {0} is greater than {1}", precursorIndex, ChromatogramCounter == 0 ? 0 : ChromatogramCounter - 1));
        Precursor.Append(sourceIndex, precursorIndex, precursorId, isolationWindowParams, activationParams);
    }

    /// <summary>
    /// Append a selected ion row associated with a precursor.
    /// </summary>
    public void AppendSelectedIon(
        ulong sourceIndex,
        ulong? precursorIndex,
        double? ionMobility,
        string? ionMobilityType,
        List<Param> selectedIonParams
    )
    {
        if (sourceIndex >= ChromatogramCounter) throw new InvalidOperationException(string.Format("Source index {0} is greater than {1}", sourceIndex, ChromatogramCounter == 0 ? 0 : ChromatogramCounter - 1));
        if (precursorIndex >= ChromatogramCounter) throw new InvalidOperationException(string.Format("Precursor index {0} is greater than {1}", precursorIndex, ChromatogramCounter == 0 ? 0 : ChromatogramCounter - 1));
        SelectedIon.Append(sourceIndex, precursorIndex, ionMobility, ionMobilityType, selectedIonParams);
    }

    /// <summary>
    /// Get the Arrow schema for the packed parallel metadata table.
    /// </summary>
    public Schema ArrowSchema(IReadOnlyDictionary<string, string>? metadata = null)
    {
        var fields = new List<Field>();
        fields.AddRange(Chromatogram.ArrowType());
        fields.AddRange(Precursor.ArrowType());
        fields.AddRange(SelectedIon.ArrowType());
        return new Schema(fields, metadata);
    }

    public void Clear()
    {
        Chromatogram.Clear();
        Precursor.Clear();
        SelectedIon.Clear();
    }
}