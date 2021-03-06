// <auto-generated>
//     Generated by the protocol buffer compiler.  DO NOT EDIT!
//     source: record.proto
// </auto-generated>
#pragma warning disable 1591, 0612, 3021
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace Walpb {

  /// <summary>Holder for reflection information generated from record.proto</summary>
  public static partial class RecordReflection {

    #region Descriptor
    /// <summary>File descriptor for record.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static RecordReflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            "CgxyZWNvcmQucHJvdG8SBXdhbHBiGgpnb2dvLnByb3RvIj0KBlJlY29yZBIS",
            "CgR0eXBlGAEgASgDQgTI3h8AEhEKA2NyYxgCIAEoDUIEyN4fABIMCgRkYXRh",
            "GAMgASgMIjMKCFNuYXBzaG90EhMKBWluZGV4GAEgASgEQgTI3h8AEhIKBHRl",
            "cm0YAiABKARCBMjeHwBCEMjiHgHg4h4B0OIeAcjhHgA="));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { global::Gogoproto.GogoReflection.Descriptor, },
          new pbr::GeneratedClrTypeInfo(null, null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::Walpb.Record), global::Walpb.Record.Parser, new[]{ "Type", "Crc", "Data" }, null, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::Walpb.Snapshot), global::Walpb.Snapshot.Parser, new[]{ "Index", "Term" }, null, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  public sealed partial class Record : pb::IMessage<Record> {
    private static readonly pb::MessageParser<Record> _parser = new pb::MessageParser<Record>(() => new Record());
    private pb::UnknownFieldSet _unknownFields;
    private int _hasBits0;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<Record> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Walpb.RecordReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Record() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Record(Record other) : this() {
      _hasBits0 = other._hasBits0;
      type_ = other.type_;
      crc_ = other.crc_;
      data_ = other.data_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Record Clone() {
      return new Record(this);
    }

    /// <summary>Field number for the "type" field.</summary>
    public const int TypeFieldNumber = 1;
    private readonly static long TypeDefaultValue = 0L;

    private long type_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public long Type {
      get { if ((_hasBits0 & 1) != 0) { return type_; } else { return TypeDefaultValue; } }
      set {
        _hasBits0 |= 1;
        type_ = value;
      }
    }
    /// <summary>Gets whether the "type" field is set</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool HasType {
      get { return (_hasBits0 & 1) != 0; }
    }
    /// <summary>Clears the value of the "type" field</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void ClearType() {
      _hasBits0 &= ~1;
    }

    /// <summary>Field number for the "crc" field.</summary>
    public const int CrcFieldNumber = 2;
    private readonly static uint CrcDefaultValue = 0;

    private uint crc_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public uint Crc {
      get { if ((_hasBits0 & 2) != 0) { return crc_; } else { return CrcDefaultValue; } }
      set {
        _hasBits0 |= 2;
        crc_ = value;
      }
    }
    /// <summary>Gets whether the "crc" field is set</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool HasCrc {
      get { return (_hasBits0 & 2) != 0; }
    }
    /// <summary>Clears the value of the "crc" field</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void ClearCrc() {
      _hasBits0 &= ~2;
    }

    /// <summary>Field number for the "data" field.</summary>
    public const int DataFieldNumber = 3;
    private readonly static pb::ByteString DataDefaultValue = pb::ByteString.Empty;

    private pb::ByteString data_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public pb::ByteString Data {
      get { return data_ ?? DataDefaultValue; }
      set {
        data_ = pb::ProtoPreconditions.CheckNotNull(value, "value");
      }
    }
    /// <summary>Gets whether the "data" field is set</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool HasData {
      get { return data_ != null; }
    }
    /// <summary>Clears the value of the "data" field</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void ClearData() {
      data_ = null;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as Record);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(Record other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Type != other.Type) return false;
      if (Crc != other.Crc) return false;
      if (Data != other.Data) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (HasType) hash ^= Type.GetHashCode();
      if (HasCrc) hash ^= Crc.GetHashCode();
      if (HasData) hash ^= Data.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (HasType) {
        output.WriteRawTag(8);
        output.WriteInt64(Type);
      }
      if (HasCrc) {
        output.WriteRawTag(16);
        output.WriteUInt32(Crc);
      }
      if (HasData) {
        output.WriteRawTag(26);
        output.WriteBytes(Data);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (HasType) {
        size += 1 + pb::CodedOutputStream.ComputeInt64Size(Type);
      }
      if (HasCrc) {
        size += 1 + pb::CodedOutputStream.ComputeUInt32Size(Crc);
      }
      if (HasData) {
        size += 1 + pb::CodedOutputStream.ComputeBytesSize(Data);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(Record other) {
      if (other == null) {
        return;
      }
      if (other.HasType) {
        Type = other.Type;
      }
      if (other.HasCrc) {
        Crc = other.Crc;
      }
      if (other.HasData) {
        Data = other.Data;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 8: {
            Type = input.ReadInt64();
            break;
          }
          case 16: {
            Crc = input.ReadUInt32();
            break;
          }
          case 26: {
            Data = input.ReadBytes();
            break;
          }
        }
      }
    }

  }

  public sealed partial class Snapshot : pb::IMessage<Snapshot> {
    private static readonly pb::MessageParser<Snapshot> _parser = new pb::MessageParser<Snapshot>(() => new Snapshot());
    private pb::UnknownFieldSet _unknownFields;
    private int _hasBits0;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pb::MessageParser<Snapshot> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::Walpb.RecordReflection.Descriptor.MessageTypes[1]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Snapshot() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Snapshot(Snapshot other) : this() {
      _hasBits0 = other._hasBits0;
      index_ = other.index_;
      term_ = other.term_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public Snapshot Clone() {
      return new Snapshot(this);
    }

    /// <summary>Field number for the "index" field.</summary>
    public const int IndexFieldNumber = 1;
    private readonly static ulong IndexDefaultValue = 0UL;

    private ulong index_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public ulong Index {
      get { if ((_hasBits0 & 1) != 0) { return index_; } else { return IndexDefaultValue; } }
      set {
        _hasBits0 |= 1;
        index_ = value;
      }
    }
    /// <summary>Gets whether the "index" field is set</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool HasIndex {
      get { return (_hasBits0 & 1) != 0; }
    }
    /// <summary>Clears the value of the "index" field</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void ClearIndex() {
      _hasBits0 &= ~1;
    }

    /// <summary>Field number for the "term" field.</summary>
    public const int TermFieldNumber = 2;
    private readonly static ulong TermDefaultValue = 0UL;

    private ulong term_;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public ulong Term {
      get { if ((_hasBits0 & 2) != 0) { return term_; } else { return TermDefaultValue; } }
      set {
        _hasBits0 |= 2;
        term_ = value;
      }
    }
    /// <summary>Gets whether the "term" field is set</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool HasTerm {
      get { return (_hasBits0 & 2) != 0; }
    }
    /// <summary>Clears the value of the "term" field</summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void ClearTerm() {
      _hasBits0 &= ~2;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override bool Equals(object other) {
      return Equals(other as Snapshot);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public bool Equals(Snapshot other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Index != other.Index) return false;
      if (Term != other.Term) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override int GetHashCode() {
      int hash = 1;
      if (HasIndex) hash ^= Index.GetHashCode();
      if (HasTerm) hash ^= Term.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void WriteTo(pb::CodedOutputStream output) {
      if (HasIndex) {
        output.WriteRawTag(8);
        output.WriteUInt64(Index);
      }
      if (HasTerm) {
        output.WriteRawTag(16);
        output.WriteUInt64(Term);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public int CalculateSize() {
      int size = 0;
      if (HasIndex) {
        size += 1 + pb::CodedOutputStream.ComputeUInt64Size(Index);
      }
      if (HasTerm) {
        size += 1 + pb::CodedOutputStream.ComputeUInt64Size(Term);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(Snapshot other) {
      if (other == null) {
        return;
      }
      if (other.HasIndex) {
        Index = other.Index;
      }
      if (other.HasTerm) {
        Term = other.Term;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    public void MergeFrom(pb::CodedInputStream input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 8: {
            Index = input.ReadUInt64();
            break;
          }
          case 16: {
            Term = input.ReadUInt64();
            break;
          }
        }
      }
    }

  }

  #endregion

}

#endregion Designer generated code
