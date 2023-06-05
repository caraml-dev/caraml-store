// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: feast/serving/ServingService.proto

package serving

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ServingService_GetFeastServingInfo_FullMethodName = "/feast.serving.ServingService/GetFeastServingInfo"
	ServingService_GetOnlineFeaturesV2_FullMethodName = "/feast.serving.ServingService/GetOnlineFeaturesV2"
	ServingService_GetOnlineFeatures_FullMethodName   = "/feast.serving.ServingService/GetOnlineFeatures"
)

// ServingServiceClient is the client API for ServingService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ServingServiceClient interface {
	// Get information about this Feast serving.
	GetFeastServingInfo(ctx context.Context, in *GetFeastServingInfoRequest, opts ...grpc.CallOption) (*GetFeastServingInfoResponse, error)
	// Get online features. To be deprecated in favor of GetOnlineFeatures.
	GetOnlineFeaturesV2(ctx context.Context, in *GetOnlineFeaturesRequest, opts ...grpc.CallOption) (*GetOnlineFeaturesResponseV2, error)
	// Get online features using optimized response message.
	GetOnlineFeatures(ctx context.Context, in *GetOnlineFeaturesRequest, opts ...grpc.CallOption) (*GetOnlineFeaturesResponse, error)
}

type servingServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewServingServiceClient(cc grpc.ClientConnInterface) ServingServiceClient {
	return &servingServiceClient{cc}
}

func (c *servingServiceClient) GetFeastServingInfo(ctx context.Context, in *GetFeastServingInfoRequest, opts ...grpc.CallOption) (*GetFeastServingInfoResponse, error) {
	out := new(GetFeastServingInfoResponse)
	err := c.cc.Invoke(ctx, ServingService_GetFeastServingInfo_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *servingServiceClient) GetOnlineFeaturesV2(ctx context.Context, in *GetOnlineFeaturesRequest, opts ...grpc.CallOption) (*GetOnlineFeaturesResponseV2, error) {
	out := new(GetOnlineFeaturesResponseV2)
	err := c.cc.Invoke(ctx, ServingService_GetOnlineFeaturesV2_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *servingServiceClient) GetOnlineFeatures(ctx context.Context, in *GetOnlineFeaturesRequest, opts ...grpc.CallOption) (*GetOnlineFeaturesResponse, error) {
	out := new(GetOnlineFeaturesResponse)
	err := c.cc.Invoke(ctx, ServingService_GetOnlineFeatures_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ServingServiceServer is the server API for ServingService service.
// All implementations should embed UnimplementedServingServiceServer
// for forward compatibility
type ServingServiceServer interface {
	// Get information about this Feast serving.
	GetFeastServingInfo(context.Context, *GetFeastServingInfoRequest) (*GetFeastServingInfoResponse, error)
	// Get online features. To be deprecated in favor of GetOnlineFeatures.
	GetOnlineFeaturesV2(context.Context, *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponseV2, error)
	// Get online features using optimized response message.
	GetOnlineFeatures(context.Context, *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponse, error)
}

// UnimplementedServingServiceServer should be embedded to have forward compatible implementations.
type UnimplementedServingServiceServer struct {
}

func (UnimplementedServingServiceServer) GetFeastServingInfo(context.Context, *GetFeastServingInfoRequest) (*GetFeastServingInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFeastServingInfo not implemented")
}
func (UnimplementedServingServiceServer) GetOnlineFeaturesV2(context.Context, *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponseV2, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOnlineFeaturesV2 not implemented")
}
func (UnimplementedServingServiceServer) GetOnlineFeatures(context.Context, *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOnlineFeatures not implemented")
}

// UnsafeServingServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ServingServiceServer will
// result in compilation errors.
type UnsafeServingServiceServer interface {
	mustEmbedUnimplementedServingServiceServer()
}

func RegisterServingServiceServer(s grpc.ServiceRegistrar, srv ServingServiceServer) {
	s.RegisterService(&ServingService_ServiceDesc, srv)
}

func _ServingService_GetFeastServingInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetFeastServingInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServingServiceServer).GetFeastServingInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServingService_GetFeastServingInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServingServiceServer).GetFeastServingInfo(ctx, req.(*GetFeastServingInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServingService_GetOnlineFeaturesV2_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOnlineFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServingServiceServer).GetOnlineFeaturesV2(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServingService_GetOnlineFeaturesV2_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServingServiceServer).GetOnlineFeaturesV2(ctx, req.(*GetOnlineFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServingService_GetOnlineFeatures_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOnlineFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServingServiceServer).GetOnlineFeatures(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ServingService_GetOnlineFeatures_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServingServiceServer).GetOnlineFeatures(ctx, req.(*GetOnlineFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ServingService_ServiceDesc is the grpc.ServiceDesc for ServingService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ServingService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "feast.serving.ServingService",
	HandlerType: (*ServingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFeastServingInfo",
			Handler:    _ServingService_GetFeastServingInfo_Handler,
		},
		{
			MethodName: "GetOnlineFeaturesV2",
			Handler:    _ServingService_GetOnlineFeaturesV2_Handler,
		},
		{
			MethodName: "GetOnlineFeatures",
			Handler:    _ServingService_GetOnlineFeatures_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "feast/serving/ServingService.proto",
}
