// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: CustomFilter.proto

package test;

public final class CustomFilterProto {
  private CustomFilterProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface CustomFilterOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required bytes value = 1;
    /**
     * <code>required bytes value = 1;</code>
     */
    boolean hasValue();
    /**
     * <code>required bytes value = 1;</code>
     */
    com.google.protobuf.ByteString getValue();

    // required bool filterRow = 2 [default = true];
    /**
     * <code>required bool filterRow = 2 [default = true];</code>
     */
    boolean hasFilterRow();
    /**
     * <code>required bool filterRow = 2 [default = true];</code>
     */
    boolean getFilterRow();
  }
  /**
   * Protobuf type {@code CustomFilter}
   */
  public static final class CustomFilter extends
      com.google.protobuf.GeneratedMessage
      implements CustomFilterOrBuilder {
    // Use CustomFilter.newBuilder() to construct.
    private CustomFilter(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private CustomFilter(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final CustomFilter defaultInstance;
    public static CustomFilter getDefaultInstance() {
      return defaultInstance;
    }

    public CustomFilter getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private CustomFilter(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              value_ = input.readBytes();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              filterRow_ = input.readBool();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return test.CustomFilterProto.internal_static_CustomFilter_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return test.CustomFilterProto.internal_static_CustomFilter_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              test.CustomFilterProto.CustomFilter.class, test.CustomFilterProto.CustomFilter.Builder.class);
    }

    public static com.google.protobuf.Parser<CustomFilter> PARSER =
        new com.google.protobuf.AbstractParser<CustomFilter>() {
      public CustomFilter parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new CustomFilter(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<CustomFilter> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required bytes value = 1;
    public static final int VALUE_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString value_;
    /**
     * <code>required bytes value = 1;</code>
     */
    public boolean hasValue() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required bytes value = 1;</code>
     */
    public com.google.protobuf.ByteString getValue() {
      return value_;
    }

    // required bool filterRow = 2 [default = true];
    public static final int FILTERROW_FIELD_NUMBER = 2;
    private boolean filterRow_;
    /**
     * <code>required bool filterRow = 2 [default = true];</code>
     */
    public boolean hasFilterRow() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required bool filterRow = 2 [default = true];</code>
     */
    public boolean getFilterRow() {
      return filterRow_;
    }

    private void initFields() {
      value_ = com.google.protobuf.ByteString.EMPTY;
      filterRow_ = true;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasValue()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasFilterRow()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, value_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBool(2, filterRow_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, value_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(2, filterRow_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static test.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static test.CustomFilterProto.CustomFilter parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static test.CustomFilterProto.CustomFilter parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static test.CustomFilterProto.CustomFilter parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(test.CustomFilterProto.CustomFilter prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code CustomFilter}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements test.CustomFilterProto.CustomFilterOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return test.CustomFilterProto.internal_static_CustomFilter_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return test.CustomFilterProto.internal_static_CustomFilter_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                test.CustomFilterProto.CustomFilter.class, test.CustomFilterProto.CustomFilter.Builder.class);
      }

      // Construct using test.CustomFilterProto.CustomFilter.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        value_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        filterRow_ = true;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return test.CustomFilterProto.internal_static_CustomFilter_descriptor;
      }

      public test.CustomFilterProto.CustomFilter getDefaultInstanceForType() {
        return test.CustomFilterProto.CustomFilter.getDefaultInstance();
      }

      public test.CustomFilterProto.CustomFilter build() {
        test.CustomFilterProto.CustomFilter result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public test.CustomFilterProto.CustomFilter buildPartial() {
        test.CustomFilterProto.CustomFilter result = new test.CustomFilterProto.CustomFilter(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.value_ = value_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.filterRow_ = filterRow_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof test.CustomFilterProto.CustomFilter) {
          return mergeFrom((test.CustomFilterProto.CustomFilter)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(test.CustomFilterProto.CustomFilter other) {
        if (other == test.CustomFilterProto.CustomFilter.getDefaultInstance()) return this;
        if (other.hasValue()) {
          setValue(other.getValue());
        }
        if (other.hasFilterRow()) {
          setFilterRow(other.getFilterRow());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasValue()) {
          
          return false;
        }
        if (!hasFilterRow()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        test.CustomFilterProto.CustomFilter parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (test.CustomFilterProto.CustomFilter) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required bytes value = 1;
      private com.google.protobuf.ByteString value_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes value = 1;</code>
       */
      public boolean hasValue() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required bytes value = 1;</code>
       */
      public com.google.protobuf.ByteString getValue() {
        return value_;
      }
      /**
       * <code>required bytes value = 1;</code>
       */
      public Builder setValue(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes value = 1;</code>
       */
      public Builder clearValue() {
        bitField0_ = (bitField0_ & ~0x00000001);
        value_ = getDefaultInstance().getValue();
        onChanged();
        return this;
      }

      // required bool filterRow = 2 [default = true];
      private boolean filterRow_ = true;
      /**
       * <code>required bool filterRow = 2 [default = true];</code>
       */
      public boolean hasFilterRow() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required bool filterRow = 2 [default = true];</code>
       */
      public boolean getFilterRow() {
        return filterRow_;
      }
      /**
       * <code>required bool filterRow = 2 [default = true];</code>
       */
      public Builder setFilterRow(boolean value) {
        bitField0_ |= 0x00000002;
        filterRow_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bool filterRow = 2 [default = true];</code>
       */
      public Builder clearFilterRow() {
        bitField0_ = (bitField0_ & ~0x00000002);
        filterRow_ = true;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:CustomFilter)
    }

    static {
      defaultInstance = new CustomFilter(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:CustomFilter)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_CustomFilter_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_CustomFilter_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022CustomFilter.proto\"6\n\014CustomFilter\022\r\n\005" +
      "value\030\001 \002(\014\022\027\n\tfilterRow\030\002 \002(\010:\004trueB\031\n\004" +
      "testB\021CustomFilterProto"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_CustomFilter_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_CustomFilter_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_CustomFilter_descriptor,
              new java.lang.String[] { "Value", "FilterRow", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
