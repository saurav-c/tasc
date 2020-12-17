// Code generated by protoc-gen-go. DO NOT EDIT.
// source: anna.proto

package anna

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// An enum to differentiate between different KVS requests.
type RequestType int32

const (
	// A default type to capture unspecified requests.
	RequestType_RT_UNSPECIFIED RequestType = 0
	// A request to retrieve data from the KVS.
	RequestType_GET RequestType = 1
	// A request to put data into the KVS.
	RequestType_PUT RequestType = 2
)

var RequestType_name = map[int32]string{
	0: "RT_UNSPECIFIED",
	1: "GET",
	2: "PUT",
}

var RequestType_value = map[string]int32{
	"RT_UNSPECIFIED": 0,
	"GET":            1,
	"PUT":            2,
}

func (x RequestType) String() string {
	return proto.EnumName(RequestType_name, int32(x))
}

func (RequestType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{0}
}

type LatticeType int32

const (
	// No lattice type specified
	LatticeType_NONE LatticeType = 0
	// Last-writer wins lattice
	LatticeType_LWW LatticeType = 1
	// Unordered set lattice
	LatticeType_SET LatticeType = 2
	// Single-key causal lattice
	LatticeType_SINGLE_CAUSAL LatticeType = 3
	// Multi-key causal lattice
	LatticeType_MULTI_CAUSAL LatticeType = 4
	// Ordered-set lattice
	LatticeType_ORDERED_SET LatticeType = 5
	// Priority lattice
	LatticeType_PRIORITY LatticeType = 6
)

var LatticeType_name = map[int32]string{
	0: "NONE",
	1: "LWW",
	2: "SET",
	3: "SINGLE_CAUSAL",
	4: "MULTI_CAUSAL",
	5: "ORDERED_SET",
	6: "PRIORITY",
}

var LatticeType_value = map[string]int32{
	"NONE":          0,
	"LWW":           1,
	"SET":           2,
	"SINGLE_CAUSAL": 3,
	"MULTI_CAUSAL":  4,
	"ORDERED_SET":   5,
	"PRIORITY":      6,
}

func (x LatticeType) String() string {
	return proto.EnumName(LatticeType_name, int32(x))
}

func (LatticeType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{1}
}

type AnnaError int32

const (
	// The request did not have an error.
	AnnaError_NO_ERROR AnnaError = 0
	// The requested key does not exist.
	AnnaError_KEY_DNE AnnaError = 1
	// The request was sent to the wrong thread, which is not responsible for the
	// key.
	AnnaError_WRONG_THREAD AnnaError = 2
	// The request timed out.
	AnnaError_TIMEOUT AnnaError = 3
	// The lattice type was not correctly specified or conflicted with an
	// existing key.
	AnnaError_LATTICE AnnaError = 4
	// This error is returned by the router tier if no servers are in the
	// cluster.
	AnnaError_NO_SERVERS AnnaError = 5
)

var AnnaError_name = map[int32]string{
	0: "NO_ERROR",
	1: "KEY_DNE",
	2: "WRONG_THREAD",
	3: "TIMEOUT",
	4: "LATTICE",
	5: "NO_SERVERS",
}

var AnnaError_value = map[string]int32{
	"NO_ERROR":     0,
	"KEY_DNE":      1,
	"WRONG_THREAD": 2,
	"TIMEOUT":      3,
	"LATTICE":      4,
	"NO_SERVERS":   5,
}

func (x AnnaError) String() string {
	return proto.EnumName(AnnaError_name, int32(x))
}

func (AnnaError) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{2}
}

// A protobuf to represent an individual key, both for requests and responses.
type KeyTuple struct {
	// The key name for this request/response.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The lattice type for this key. Only required for server responses and PUT
	// requests.
	LatticeType LatticeType `protobuf:"varint,2,opt,name=lattice_type,json=latticeType,proto3,enum=LatticeType" json:"lattice_type,omitempty"`
	// The error type specified by the server (see AnnaError).
	Error AnnaError `protobuf:"varint,3,opt,name=error,proto3,enum=AnnaError" json:"error,omitempty"`
	// The data associated with this key.
	Payload []byte `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
	// The number of server addresses the client is aware of for a particular
	// key; used for DHT membership change optimization.
	AddressCacheSize uint32 `protobuf:"varint,5,opt,name=address_cache_size,json=addressCacheSize,proto3" json:"address_cache_size,omitempty"`
	// A boolean set by the server if the client's address_cache_size does not
	// match the metadata stored by the server.
	Invalidate           bool     `protobuf:"varint,6,opt,name=invalidate,proto3" json:"invalidate,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyTuple) Reset()         { *m = KeyTuple{} }
func (m *KeyTuple) String() string { return proto.CompactTextString(m) }
func (*KeyTuple) ProtoMessage()    {}
func (*KeyTuple) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{0}
}

func (m *KeyTuple) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyTuple.Unmarshal(m, b)
}
func (m *KeyTuple) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyTuple.Marshal(b, m, deterministic)
}
func (m *KeyTuple) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyTuple.Merge(m, src)
}
func (m *KeyTuple) XXX_Size() int {
	return xxx_messageInfo_KeyTuple.Size(m)
}
func (m *KeyTuple) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyTuple.DiscardUnknown(m)
}

var xxx_messageInfo_KeyTuple proto.InternalMessageInfo

func (m *KeyTuple) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyTuple) GetLatticeType() LatticeType {
	if m != nil {
		return m.LatticeType
	}
	return LatticeType_NONE
}

func (m *KeyTuple) GetError() AnnaError {
	if m != nil {
		return m.Error
	}
	return AnnaError_NO_ERROR
}

func (m *KeyTuple) GetPayload() []byte {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (m *KeyTuple) GetAddressCacheSize() uint32 {
	if m != nil {
		return m.AddressCacheSize
	}
	return 0
}

func (m *KeyTuple) GetInvalidate() bool {
	if m != nil {
		return m.Invalidate
	}
	return false
}

// An individual GET or PUT request; each request can batch multiple keys.
type KeyRequest struct {
	// The type of this request (see RequestType).
	Type RequestType `protobuf:"varint,1,opt,name=type,proto3,enum=RequestType" json:"type,omitempty"`
	// A list of KeyTuples batched in this request.
	Tuples []*KeyTuple `protobuf:"bytes,2,rep,name=tuples,proto3" json:"tuples,omitempty"`
	// The IP-port pair at which the client is waiting for the server's response.
	ResponseAddress string `protobuf:"bytes,3,opt,name=response_address,json=responseAddress,proto3" json:"response_address,omitempty"`
	// A client-specific ID used to match asynchronous requests with responses.
	RequestId            string   `protobuf:"bytes,4,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyRequest) Reset()         { *m = KeyRequest{} }
func (m *KeyRequest) String() string { return proto.CompactTextString(m) }
func (*KeyRequest) ProtoMessage()    {}
func (*KeyRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{1}
}

func (m *KeyRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyRequest.Unmarshal(m, b)
}
func (m *KeyRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyRequest.Marshal(b, m, deterministic)
}
func (m *KeyRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyRequest.Merge(m, src)
}
func (m *KeyRequest) XXX_Size() int {
	return xxx_messageInfo_KeyRequest.Size(m)
}
func (m *KeyRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyRequest.DiscardUnknown(m)
}

var xxx_messageInfo_KeyRequest proto.InternalMessageInfo

func (m *KeyRequest) GetType() RequestType {
	if m != nil {
		return m.Type
	}
	return RequestType_RT_UNSPECIFIED
}

func (m *KeyRequest) GetTuples() []*KeyTuple {
	if m != nil {
		return m.Tuples
	}
	return nil
}

func (m *KeyRequest) GetResponseAddress() string {
	if m != nil {
		return m.ResponseAddress
	}
	return ""
}

func (m *KeyRequest) GetRequestId() string {
	if m != nil {
		return m.RequestId
	}
	return ""
}

// A response to a KeyRequest.
type KeyResponse struct {
	// The type of response being sent back to the client (see RequestType).
	Type RequestType `protobuf:"varint,1,opt,name=type,proto3,enum=RequestType" json:"type,omitempty"`
	// The individual response pairs associated with this request. There is a
	// 1-to-1 mapping between these and the KeyTuples in the corresponding
	// KeyRequest.
	Tuples []*KeyTuple `protobuf:"bytes,2,rep,name=tuples,proto3" json:"tuples,omitempty"`
	// The request_id specified in the corresponding KeyRequest. Used to
	// associate asynchornous requests and responses.
	ResponseId string `protobuf:"bytes,3,opt,name=response_id,json=responseId,proto3" json:"response_id,omitempty"`
	// Any errors associated with the whole request. Individual tuple errors are
	// captured in the corresponding KeyTuple. This will only be set if the whole
	// request times out.
	Error                AnnaError `protobuf:"varint,4,opt,name=error,proto3,enum=AnnaError" json:"error,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *KeyResponse) Reset()         { *m = KeyResponse{} }
func (m *KeyResponse) String() string { return proto.CompactTextString(m) }
func (*KeyResponse) ProtoMessage()    {}
func (*KeyResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{2}
}

func (m *KeyResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyResponse.Unmarshal(m, b)
}
func (m *KeyResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyResponse.Marshal(b, m, deterministic)
}
func (m *KeyResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyResponse.Merge(m, src)
}
func (m *KeyResponse) XXX_Size() int {
	return xxx_messageInfo_KeyResponse.Size(m)
}
func (m *KeyResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyResponse.DiscardUnknown(m)
}

var xxx_messageInfo_KeyResponse proto.InternalMessageInfo

func (m *KeyResponse) GetType() RequestType {
	if m != nil {
		return m.Type
	}
	return RequestType_RT_UNSPECIFIED
}

func (m *KeyResponse) GetTuples() []*KeyTuple {
	if m != nil {
		return m.Tuples
	}
	return nil
}

func (m *KeyResponse) GetResponseId() string {
	if m != nil {
		return m.ResponseId
	}
	return ""
}

func (m *KeyResponse) GetError() AnnaError {
	if m != nil {
		return m.Error
	}
	return AnnaError_NO_ERROR
}

// A request to the router tier to retrieve server addresses corresponding to
// individual keys.
type KeyAddressRequest struct {
	// The IP-port pair at which the client will await a response.
	ResponseAddress string `protobuf:"bytes,1,opt,name=response_address,json=responseAddress,proto3" json:"response_address,omitempty"`
	// The names of the requested keys.
	Keys []string `protobuf:"bytes,2,rep,name=keys,proto3" json:"keys,omitempty"`
	// A unique ID used by the client to match asynchornous requests with
	// responses.
	RequestId            string   `protobuf:"bytes,3,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyAddressRequest) Reset()         { *m = KeyAddressRequest{} }
func (m *KeyAddressRequest) String() string { return proto.CompactTextString(m) }
func (*KeyAddressRequest) ProtoMessage()    {}
func (*KeyAddressRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{3}
}

func (m *KeyAddressRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyAddressRequest.Unmarshal(m, b)
}
func (m *KeyAddressRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyAddressRequest.Marshal(b, m, deterministic)
}
func (m *KeyAddressRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyAddressRequest.Merge(m, src)
}
func (m *KeyAddressRequest) XXX_Size() int {
	return xxx_messageInfo_KeyAddressRequest.Size(m)
}
func (m *KeyAddressRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyAddressRequest.DiscardUnknown(m)
}

var xxx_messageInfo_KeyAddressRequest proto.InternalMessageInfo

func (m *KeyAddressRequest) GetResponseAddress() string {
	if m != nil {
		return m.ResponseAddress
	}
	return ""
}

func (m *KeyAddressRequest) GetKeys() []string {
	if m != nil {
		return m.Keys
	}
	return nil
}

func (m *KeyAddressRequest) GetRequestId() string {
	if m != nil {
		return m.RequestId
	}
	return ""
}

// A 1-to-1 response from the router tier for individual KeyAddressRequests.
type KeyAddressResponse struct {
	// A batch of responses for individual keys.
	Addresses []*KeyAddressResponse_KeyAddress `protobuf:"bytes,1,rep,name=addresses,proto3" json:"addresses,omitempty"`
	// An error reported by the router tier. This should only ever be a timeout.
	Error AnnaError `protobuf:"varint,2,opt,name=error,proto3,enum=AnnaError" json:"error,omitempty"`
	// A unique ID used by the client to match asynchronous requests with
	// responses.
	ResponseId           string   `protobuf:"bytes,3,opt,name=response_id,json=responseId,proto3" json:"response_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyAddressResponse) Reset()         { *m = KeyAddressResponse{} }
func (m *KeyAddressResponse) String() string { return proto.CompactTextString(m) }
func (*KeyAddressResponse) ProtoMessage()    {}
func (*KeyAddressResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{4}
}

func (m *KeyAddressResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyAddressResponse.Unmarshal(m, b)
}
func (m *KeyAddressResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyAddressResponse.Marshal(b, m, deterministic)
}
func (m *KeyAddressResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyAddressResponse.Merge(m, src)
}
func (m *KeyAddressResponse) XXX_Size() int {
	return xxx_messageInfo_KeyAddressResponse.Size(m)
}
func (m *KeyAddressResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyAddressResponse.DiscardUnknown(m)
}

var xxx_messageInfo_KeyAddressResponse proto.InternalMessageInfo

func (m *KeyAddressResponse) GetAddresses() []*KeyAddressResponse_KeyAddress {
	if m != nil {
		return m.Addresses
	}
	return nil
}

func (m *KeyAddressResponse) GetError() AnnaError {
	if m != nil {
		return m.Error
	}
	return AnnaError_NO_ERROR
}

func (m *KeyAddressResponse) GetResponseId() string {
	if m != nil {
		return m.ResponseId
	}
	return ""
}

// A mapping from individual keys to the set of servers responsible for that
// key.
type KeyAddressResponse_KeyAddress struct {
	// The specified key.
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The IPs of the set of servers responsible for this key.
	Ips                  []string `protobuf:"bytes,2,rep,name=ips,proto3" json:"ips,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyAddressResponse_KeyAddress) Reset()         { *m = KeyAddressResponse_KeyAddress{} }
func (m *KeyAddressResponse_KeyAddress) String() string { return proto.CompactTextString(m) }
func (*KeyAddressResponse_KeyAddress) ProtoMessage()    {}
func (*KeyAddressResponse_KeyAddress) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{4, 0}
}

func (m *KeyAddressResponse_KeyAddress) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyAddressResponse_KeyAddress.Unmarshal(m, b)
}
func (m *KeyAddressResponse_KeyAddress) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyAddressResponse_KeyAddress.Marshal(b, m, deterministic)
}
func (m *KeyAddressResponse_KeyAddress) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyAddressResponse_KeyAddress.Merge(m, src)
}
func (m *KeyAddressResponse_KeyAddress) XXX_Size() int {
	return xxx_messageInfo_KeyAddressResponse_KeyAddress.Size(m)
}
func (m *KeyAddressResponse_KeyAddress) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyAddressResponse_KeyAddress.DiscardUnknown(m)
}

var xxx_messageInfo_KeyAddressResponse_KeyAddress proto.InternalMessageInfo

func (m *KeyAddressResponse_KeyAddress) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyAddressResponse_KeyAddress) GetIps() []string {
	if m != nil {
		return m.Ips
	}
	return nil
}

// Serialization of last-write wins lattices.
type LWWValue struct {
	// The last-writer wins timestamp associated with this data.
	Timestamp uint64 `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// The actual data stored by this LWWValue.
	Value                []byte   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *LWWValue) Reset()         { *m = LWWValue{} }
func (m *LWWValue) String() string { return proto.CompactTextString(m) }
func (*LWWValue) ProtoMessage()    {}
func (*LWWValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{5}
}

func (m *LWWValue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_LWWValue.Unmarshal(m, b)
}
func (m *LWWValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_LWWValue.Marshal(b, m, deterministic)
}
func (m *LWWValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_LWWValue.Merge(m, src)
}
func (m *LWWValue) XXX_Size() int {
	return xxx_messageInfo_LWWValue.Size(m)
}
func (m *LWWValue) XXX_DiscardUnknown() {
	xxx_messageInfo_LWWValue.DiscardUnknown(m)
}

var xxx_messageInfo_LWWValue proto.InternalMessageInfo

func (m *LWWValue) GetTimestamp() uint64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *LWWValue) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

// Serialization of unordered set lattices.
type SetValue struct {
	// An unordered set of values in this lattice.
	Values               [][]byte `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SetValue) Reset()         { *m = SetValue{} }
func (m *SetValue) String() string { return proto.CompactTextString(m) }
func (*SetValue) ProtoMessage()    {}
func (*SetValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{6}
}

func (m *SetValue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SetValue.Unmarshal(m, b)
}
func (m *SetValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SetValue.Marshal(b, m, deterministic)
}
func (m *SetValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SetValue.Merge(m, src)
}
func (m *SetValue) XXX_Size() int {
	return xxx_messageInfo_SetValue.Size(m)
}
func (m *SetValue) XXX_DiscardUnknown() {
	xxx_messageInfo_SetValue.DiscardUnknown(m)
}

var xxx_messageInfo_SetValue proto.InternalMessageInfo

func (m *SetValue) GetValues() [][]byte {
	if m != nil {
		return m.Values
	}
	return nil
}

// Serialization of a single-key causal lattice.
type SingleKeyCausalValue struct {
	// The vector clock for this key, which maps from unique client IDs to
	// monotonically increasing integers.
	VectorClock map[string]uint32 `protobuf:"bytes,1,rep,name=vector_clock,json=vectorClock,proto3" json:"vector_clock,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	// The set of values associated with this causal value. There will only be
	// more than one here if there are multiple causally concurrent updates.
	Values               [][]byte `protobuf:"bytes,2,rep,name=values,proto3" json:"values,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SingleKeyCausalValue) Reset()         { *m = SingleKeyCausalValue{} }
func (m *SingleKeyCausalValue) String() string { return proto.CompactTextString(m) }
func (*SingleKeyCausalValue) ProtoMessage()    {}
func (*SingleKeyCausalValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{7}
}

func (m *SingleKeyCausalValue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SingleKeyCausalValue.Unmarshal(m, b)
}
func (m *SingleKeyCausalValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SingleKeyCausalValue.Marshal(b, m, deterministic)
}
func (m *SingleKeyCausalValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SingleKeyCausalValue.Merge(m, src)
}
func (m *SingleKeyCausalValue) XXX_Size() int {
	return xxx_messageInfo_SingleKeyCausalValue.Size(m)
}
func (m *SingleKeyCausalValue) XXX_DiscardUnknown() {
	xxx_messageInfo_SingleKeyCausalValue.DiscardUnknown(m)
}

var xxx_messageInfo_SingleKeyCausalValue proto.InternalMessageInfo

func (m *SingleKeyCausalValue) GetVectorClock() map[string]uint32 {
	if m != nil {
		return m.VectorClock
	}
	return nil
}

func (m *SingleKeyCausalValue) GetValues() [][]byte {
	if m != nil {
		return m.Values
	}
	return nil
}

// An individual multi-key causal lattice, along with its associated
// dependencies.
type MultiKeyCausalValue struct {
	// The vector clock associated with this particular key.
	VectorClock map[string]uint32 `protobuf:"bytes,1,rep,name=vector_clock,json=vectorClock,proto3" json:"vector_clock,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	// The mapping from keys to vector clocks for each of the direct causal
	// dependencies this key has.
	Dependencies []*KeyVersion `protobuf:"bytes,2,rep,name=dependencies,proto3" json:"dependencies,omitempty"`
	// The set of potentially causally concurrent values for this key.
	Values               [][]byte `protobuf:"bytes,3,rep,name=values,proto3" json:"values,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MultiKeyCausalValue) Reset()         { *m = MultiKeyCausalValue{} }
func (m *MultiKeyCausalValue) String() string { return proto.CompactTextString(m) }
func (*MultiKeyCausalValue) ProtoMessage()    {}
func (*MultiKeyCausalValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{8}
}

func (m *MultiKeyCausalValue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MultiKeyCausalValue.Unmarshal(m, b)
}
func (m *MultiKeyCausalValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MultiKeyCausalValue.Marshal(b, m, deterministic)
}
func (m *MultiKeyCausalValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MultiKeyCausalValue.Merge(m, src)
}
func (m *MultiKeyCausalValue) XXX_Size() int {
	return xxx_messageInfo_MultiKeyCausalValue.Size(m)
}
func (m *MultiKeyCausalValue) XXX_DiscardUnknown() {
	xxx_messageInfo_MultiKeyCausalValue.DiscardUnknown(m)
}

var xxx_messageInfo_MultiKeyCausalValue proto.InternalMessageInfo

func (m *MultiKeyCausalValue) GetVectorClock() map[string]uint32 {
	if m != nil {
		return m.VectorClock
	}
	return nil
}

func (m *MultiKeyCausalValue) GetDependencies() []*KeyVersion {
	if m != nil {
		return m.Dependencies
	}
	return nil
}

func (m *MultiKeyCausalValue) GetValues() [][]byte {
	if m != nil {
		return m.Values
	}
	return nil
}

// Serialization of lowest-priority-wins lattices.
type PriorityValue struct {
	// The priority associated with this data
	Priority float64 `protobuf:"fixed64,1,opt,name=priority,proto3" json:"priority,omitempty"`
	// The actual data stored by this PriorityValue
	Value                []byte   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PriorityValue) Reset()         { *m = PriorityValue{} }
func (m *PriorityValue) String() string { return proto.CompactTextString(m) }
func (*PriorityValue) ProtoMessage()    {}
func (*PriorityValue) Descriptor() ([]byte, []int) {
	return fileDescriptor_0a446a6f28b5e890, []int{9}
}

func (m *PriorityValue) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PriorityValue.Unmarshal(m, b)
}
func (m *PriorityValue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PriorityValue.Marshal(b, m, deterministic)
}
func (m *PriorityValue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PriorityValue.Merge(m, src)
}
func (m *PriorityValue) XXX_Size() int {
	return xxx_messageInfo_PriorityValue.Size(m)
}
func (m *PriorityValue) XXX_DiscardUnknown() {
	xxx_messageInfo_PriorityValue.DiscardUnknown(m)
}

var xxx_messageInfo_PriorityValue proto.InternalMessageInfo

func (m *PriorityValue) GetPriority() float64 {
	if m != nil {
		return m.Priority
	}
	return 0
}

func (m *PriorityValue) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterEnum("RequestType", RequestType_name, RequestType_value)
	proto.RegisterEnum("LatticeType", LatticeType_name, LatticeType_value)
	proto.RegisterEnum("AnnaError", AnnaError_name, AnnaError_value)
	proto.RegisterType((*KeyTuple)(nil), "KeyTuple")
	proto.RegisterType((*KeyRequest)(nil), "KeyRequest")
	proto.RegisterType((*KeyResponse)(nil), "KeyResponse")
	proto.RegisterType((*KeyAddressRequest)(nil), "KeyAddressRequest")
	proto.RegisterType((*KeyAddressResponse)(nil), "KeyAddressResponse")
	proto.RegisterType((*KeyAddressResponse_KeyAddress)(nil), "KeyAddressResponse.KeyAddress")
	proto.RegisterType((*LWWValue)(nil), "LWWValue")
	proto.RegisterType((*SetValue)(nil), "SetValue")
	proto.RegisterType((*SingleKeyCausalValue)(nil), "SingleKeyCausalValue")
	proto.RegisterMapType((map[string]uint32)(nil), "SingleKeyCausalValue.VectorClockEntry")
	proto.RegisterType((*MultiKeyCausalValue)(nil), "MultiKeyCausalValue")
	proto.RegisterMapType((map[string]uint32)(nil), "MultiKeyCausalValue.VectorClockEntry")
	proto.RegisterType((*PriorityValue)(nil), "PriorityValue")
}

func init() { proto.RegisterFile("anna.proto", fileDescriptor_0a446a6f28b5e890) }

var fileDescriptor_0a446a6f28b5e890 = []byte{
	// 801 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x55, 0xdd, 0x8e, 0x9b, 0x46,
	0x14, 0xce, 0x18, 0xdb, 0x6b, 0x0e, 0xec, 0x66, 0x32, 0x8d, 0x2a, 0xb4, 0x6a, 0x53, 0x8a, 0xd4,
	0xca, 0x5d, 0x55, 0xa4, 0x4a, 0x6e, 0xaa, 0xaa, 0x8a, 0x64, 0xd9, 0xd3, 0x0d, 0xb2, 0x17, 0x56,
	0x03, 0x5e, 0x2b, 0x57, 0x88, 0x98, 0x51, 0x83, 0x96, 0x05, 0x02, 0x78, 0x25, 0xf2, 0x1e, 0xbd,
	0xea, 0x93, 0xf4, 0x09, 0xfa, 0x08, 0x7d, 0x85, 0x3e, 0x46, 0x34, 0xfc, 0xac, 0xc9, 0xc6, 0x51,
	0x6e, 0x72, 0x37, 0xe7, 0x3b, 0xdf, 0xe1, 0x7c, 0xdf, 0xe1, 0x0c, 0x00, 0x04, 0x49, 0x12, 0x98,
	0x59, 0x9e, 0x96, 0xe9, 0xa9, 0x5a, 0xbc, 0x09, 0x72, 0x1e, 0x36, 0x91, 0xf1, 0x1f, 0x82, 0xc9,
	0x92, 0x57, 0xde, 0x2e, 0x8b, 0x39, 0xc1, 0x20, 0x5d, 0xf3, 0x4a, 0x43, 0x3a, 0x9a, 0xca, 0x4c,
	0x1c, 0xc9, 0x53, 0x50, 0xe3, 0xa0, 0x2c, 0xa3, 0x2d, 0xf7, 0xcb, 0x2a, 0xe3, 0xda, 0x40, 0x47,
	0xd3, 0x93, 0x67, 0xaa, 0xb9, 0x6a, 0x40, 0xaf, 0xca, 0x38, 0x53, 0xe2, 0x7d, 0x40, 0x74, 0x18,
	0xf1, 0x3c, 0x4f, 0x73, 0x4d, 0xaa, 0x99, 0x60, 0xce, 0x92, 0x24, 0xa0, 0x02, 0x61, 0x4d, 0x82,
	0x68, 0x70, 0x94, 0x05, 0x55, 0x9c, 0x06, 0xa1, 0x36, 0xd4, 0xd1, 0x54, 0x65, 0x5d, 0x48, 0x7e,
	0x06, 0x12, 0x84, 0x61, 0xce, 0x8b, 0xc2, 0xdf, 0x06, 0xdb, 0x37, 0xdc, 0x2f, 0xa2, 0x77, 0x5c,
	0x1b, 0xe9, 0x68, 0x7a, 0xcc, 0x70, 0x9b, 0x99, 0x8b, 0x84, 0x1b, 0xbd, 0xe3, 0xe4, 0x09, 0x40,
	0x94, 0xdc, 0x06, 0x71, 0x14, 0x06, 0x25, 0xd7, 0xc6, 0x3a, 0x9a, 0x4e, 0x58, 0x0f, 0x31, 0xfe,
	0x46, 0x00, 0x4b, 0x5e, 0x31, 0xfe, 0x76, 0xc7, 0x8b, 0x92, 0xe8, 0x30, 0xac, 0x1d, 0xa0, 0xd6,
	0x41, 0x8b, 0xd7, 0x0e, 0xea, 0x0c, 0xf9, 0x1e, 0xc6, 0xa5, 0x18, 0x43, 0xa1, 0x0d, 0x74, 0x69,
	0xaa, 0x3c, 0x93, 0xcd, 0x6e, 0x30, 0xac, 0x4d, 0x90, 0x9f, 0x00, 0xe7, 0xbc, 0xc8, 0xd2, 0xa4,
	0xe0, 0x7e, 0x2b, 0xa8, 0x36, 0x2a, 0xb3, 0x87, 0x1d, 0x3e, 0x6b, 0x60, 0xf2, 0x2d, 0x40, 0xde,
	0xb4, 0xf0, 0xa3, 0xc6, 0xa9, 0xcc, 0xe4, 0x16, 0xb1, 0x42, 0xe3, 0x2f, 0x04, 0x4a, 0xad, 0xae,
	0xa9, 0xfa, 0x32, 0xf2, 0xbe, 0x03, 0xe5, 0x4e, 0x5e, 0x14, 0xb6, 0xca, 0xa0, 0x83, 0xac, 0x70,
	0xff, 0x76, 0x86, 0x9f, 0x78, 0x3b, 0xc6, 0x5b, 0x78, 0xb4, 0xe4, 0x55, 0x6b, 0xa2, 0x9b, 0xdd,
	0x21, 0xdb, 0xe8, 0xb0, 0x6d, 0x02, 0xc3, 0x6b, 0x5e, 0x35, 0x1a, 0x65, 0x56, 0x9f, 0xef, 0x8d,
	0x42, 0xba, 0x3f, 0x8a, 0x7f, 0x11, 0x90, 0x7e, 0xcf, 0x76, 0x22, 0xbf, 0x83, 0xdc, 0xf6, 0xe2,
	0xa2, 0x9b, 0xb0, 0xfc, 0xc4, 0xfc, 0x98, 0xd7, 0x87, 0xf6, 0x05, 0x7b, 0xa7, 0x83, 0x4f, 0xed,
	0xe1, 0xe7, 0x86, 0x75, 0xfa, 0x4b, 0xbd, 0x3f, 0x9d, 0xb1, 0x8f, 0xef, 0x06, 0x06, 0x29, 0xca,
	0x3a, 0xa7, 0xe2, 0x68, 0xbc, 0x80, 0xc9, 0x6a, 0xb3, 0xb9, 0x0a, 0xe2, 0x1d, 0x27, 0xdf, 0x80,
	0x5c, 0x46, 0x37, 0xbc, 0x28, 0x83, 0x9b, 0xac, 0xae, 0x1a, 0xb2, 0x3d, 0x40, 0x1e, 0xc3, 0xe8,
	0x56, 0xd0, 0x6a, 0x79, 0x2a, 0x6b, 0x02, 0xc3, 0x80, 0x89, 0xcb, 0xcb, 0xa6, 0xfe, 0x6b, 0x18,
	0xd7, 0x60, 0xe3, 0x5d, 0x65, 0x6d, 0x64, 0xfc, 0x83, 0xe0, 0xb1, 0x1b, 0x25, 0x7f, 0xc6, 0x7c,
	0xc9, 0xab, 0x79, 0xb0, 0x2b, 0x82, 0xb8, 0x29, 0xb0, 0x40, 0xbd, 0xe5, 0xdb, 0x32, 0xcd, 0xfd,
	0x6d, 0x9c, 0x6e, 0xaf, 0xdb, 0x91, 0xfd, 0x68, 0x1e, 0x22, 0x9b, 0x57, 0x35, 0x73, 0x2e, 0x88,
	0x34, 0x29, 0xf3, 0x8a, 0x29, 0xb7, 0x7b, 0xa4, 0xd7, 0x7b, 0xd0, 0xef, 0x7d, 0xfa, 0x02, 0xf0,
	0xfd, 0xc2, 0x03, 0x73, 0xf9, 0xc0, 0xdb, 0x71, 0xeb, 0xed, 0xb7, 0xc1, 0xaf, 0xc8, 0xf8, 0x1f,
	0xc1, 0x57, 0x17, 0xbb, 0xb8, 0x8c, 0xee, 0x49, 0x7f, 0x79, 0x50, 0xfa, 0x0f, 0xe6, 0x01, 0xee,
	0x67, 0x94, 0x3f, 0x05, 0x35, 0xe4, 0x19, 0x4f, 0x42, 0x9e, 0x6c, 0xa3, 0xbb, 0xab, 0xa2, 0x88,
	0x25, 0xb9, 0xe2, 0x79, 0x11, 0xa5, 0x09, 0xfb, 0x80, 0xd0, 0xb3, 0x2a, 0x7d, 0x51, 0xab, 0x33,
	0x38, 0xbe, 0xcc, 0xa3, 0x34, 0x8f, 0xca, 0xaa, 0xf1, 0x78, 0x0a, 0x93, 0xac, 0x05, 0xea, 0x27,
	0x20, 0x76, 0x17, 0x1f, 0xde, 0x86, 0xb3, 0xe7, 0xa0, 0xf4, 0xbe, 0x02, 0x84, 0xc0, 0x09, 0xf3,
	0xfc, 0xb5, 0xed, 0x5e, 0xd2, 0xb9, 0xf5, 0x87, 0x45, 0x17, 0xf8, 0x01, 0x39, 0x02, 0xe9, 0x9c,
	0x7a, 0x18, 0x89, 0xc3, 0xe5, 0xda, 0xc3, 0x83, 0xb3, 0x1b, 0x50, 0x7a, 0xdf, 0x66, 0x32, 0x81,
	0xa1, 0xed, 0xd8, 0xb4, 0xa1, 0xae, 0x36, 0x9b, 0x86, 0xea, 0x52, 0x0f, 0x0f, 0xc8, 0x23, 0x38,
	0x76, 0x2d, 0xfb, 0x7c, 0x45, 0xfd, 0xf9, 0x6c, 0xed, 0xce, 0x56, 0x58, 0x22, 0x18, 0xd4, 0x8b,
	0xf5, 0xca, 0xb3, 0x3a, 0x64, 0x48, 0x1e, 0x82, 0xe2, 0xb0, 0x05, 0x65, 0x74, 0xe1, 0x8b, 0xaa,
	0x11, 0x51, 0x61, 0x72, 0xc9, 0x2c, 0x87, 0x59, 0xde, 0x2b, 0x3c, 0x3e, 0x7b, 0x0d, 0xf2, 0xdd,
	0xc5, 0x12, 0x29, 0xdb, 0xf1, 0x29, 0x63, 0x0e, 0xc3, 0x0f, 0x88, 0x02, 0x47, 0x4b, 0xfa, 0xca,
	0x5f, 0xd8, 0x14, 0x23, 0xf1, 0xe0, 0x0d, 0x73, 0xec, 0x73, 0xdf, 0x7b, 0xc9, 0xe8, 0x6c, 0x81,
	0x07, 0x22, 0xed, 0x59, 0x17, 0xd4, 0x59, 0x7b, 0x58, 0x12, 0xc1, 0x6a, 0xe6, 0x79, 0xd6, 0x9c,
	0xe2, 0x21, 0x39, 0x01, 0xb0, 0x1d, 0xdf, 0xa5, 0xec, 0x8a, 0x32, 0x17, 0x8f, 0x5e, 0x8f, 0xeb,
	0x3f, 0xd5, 0xf3, 0xf7, 0x01, 0x00, 0x00, 0xff, 0xff, 0x44, 0x0b, 0xee, 0x10, 0xc5, 0x06, 0x00,
	0x00,
}
