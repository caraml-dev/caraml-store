// Legacy compatibility endpoint for older version of feast client.
// The new endpoint is defined in feast_spark/api/JobService.proto

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: feast/core/LegacyJobService.proto

package core

import (
	context "context"
	api "github.com/caraml-dev/caraml-store/caraml-store-sdk/go/protos/feast_spark/api"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	JobService_StartOfflineToOnlineIngestionJob_FullMethodName = "/feast.core.JobService/StartOfflineToOnlineIngestionJob"
	JobService_GetHistoricalFeatures_FullMethodName            = "/feast.core.JobService/GetHistoricalFeatures"
	JobService_GetJob_FullMethodName                           = "/feast.core.JobService/GetJob"
)

// JobServiceClient is the client API for JobService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type JobServiceClient interface {
	// Start job to ingest data from offline store into online store
	StartOfflineToOnlineIngestionJob(ctx context.Context, in *api.StartOfflineToOnlineIngestionJobRequest, opts ...grpc.CallOption) (*api.StartOfflineToOnlineIngestionJobResponse, error)
	// Produce a training dataset, return a job id that will provide a file reference
	GetHistoricalFeatures(ctx context.Context, in *api.GetHistoricalFeaturesRequest, opts ...grpc.CallOption) (*api.GetHistoricalFeaturesResponse, error)
	// Get details of a single job
	GetJob(ctx context.Context, in *api.GetJobRequest, opts ...grpc.CallOption) (*api.GetJobResponse, error)
}

type jobServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewJobServiceClient(cc grpc.ClientConnInterface) JobServiceClient {
	return &jobServiceClient{cc}
}

func (c *jobServiceClient) StartOfflineToOnlineIngestionJob(ctx context.Context, in *api.StartOfflineToOnlineIngestionJobRequest, opts ...grpc.CallOption) (*api.StartOfflineToOnlineIngestionJobResponse, error) {
	out := new(api.StartOfflineToOnlineIngestionJobResponse)
	err := c.cc.Invoke(ctx, JobService_StartOfflineToOnlineIngestionJob_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) GetHistoricalFeatures(ctx context.Context, in *api.GetHistoricalFeaturesRequest, opts ...grpc.CallOption) (*api.GetHistoricalFeaturesResponse, error) {
	out := new(api.GetHistoricalFeaturesResponse)
	err := c.cc.Invoke(ctx, JobService_GetHistoricalFeatures_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) GetJob(ctx context.Context, in *api.GetJobRequest, opts ...grpc.CallOption) (*api.GetJobResponse, error) {
	out := new(api.GetJobResponse)
	err := c.cc.Invoke(ctx, JobService_GetJob_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobServiceServer is the server API for JobService service.
// All implementations should embed UnimplementedJobServiceServer
// for forward compatibility
type JobServiceServer interface {
	// Start job to ingest data from offline store into online store
	StartOfflineToOnlineIngestionJob(context.Context, *api.StartOfflineToOnlineIngestionJobRequest) (*api.StartOfflineToOnlineIngestionJobResponse, error)
	// Produce a training dataset, return a job id that will provide a file reference
	GetHistoricalFeatures(context.Context, *api.GetHistoricalFeaturesRequest) (*api.GetHistoricalFeaturesResponse, error)
	// Get details of a single job
	GetJob(context.Context, *api.GetJobRequest) (*api.GetJobResponse, error)
}

// UnimplementedJobServiceServer should be embedded to have forward compatible implementations.
type UnimplementedJobServiceServer struct {
}

func (UnimplementedJobServiceServer) StartOfflineToOnlineIngestionJob(context.Context, *api.StartOfflineToOnlineIngestionJobRequest) (*api.StartOfflineToOnlineIngestionJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartOfflineToOnlineIngestionJob not implemented")
}
func (UnimplementedJobServiceServer) GetHistoricalFeatures(context.Context, *api.GetHistoricalFeaturesRequest) (*api.GetHistoricalFeaturesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHistoricalFeatures not implemented")
}
func (UnimplementedJobServiceServer) GetJob(context.Context, *api.GetJobRequest) (*api.GetJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJob not implemented")
}

// UnsafeJobServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to JobServiceServer will
// result in compilation errors.
type UnsafeJobServiceServer interface {
	mustEmbedUnimplementedJobServiceServer()
}

func RegisterJobServiceServer(s grpc.ServiceRegistrar, srv JobServiceServer) {
	s.RegisterService(&JobService_ServiceDesc, srv)
}

func _JobService_StartOfflineToOnlineIngestionJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.StartOfflineToOnlineIngestionJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).StartOfflineToOnlineIngestionJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: JobService_StartOfflineToOnlineIngestionJob_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).StartOfflineToOnlineIngestionJob(ctx, req.(*api.StartOfflineToOnlineIngestionJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_GetHistoricalFeatures_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.GetHistoricalFeaturesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetHistoricalFeatures(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: JobService_GetHistoricalFeatures_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetHistoricalFeatures(ctx, req.(*api.GetHistoricalFeaturesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_GetJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.GetJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: JobService_GetJob_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetJob(ctx, req.(*api.GetJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// JobService_ServiceDesc is the grpc.ServiceDesc for JobService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var JobService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "feast.core.JobService",
	HandlerType: (*JobServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "StartOfflineToOnlineIngestionJob",
			Handler:    _JobService_StartOfflineToOnlineIngestionJob_Handler,
		},
		{
			MethodName: "GetHistoricalFeatures",
			Handler:    _JobService_GetHistoricalFeatures_Handler,
		},
		{
			MethodName: "GetJob",
			Handler:    _JobService_GetJob_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "feast/core/LegacyJobService.proto",
}
