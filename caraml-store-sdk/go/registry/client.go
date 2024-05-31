package serving

import (
	"context"
	"crypto/x509"
	"fmt"
	"github.com/caraml-dev/caraml-store/caraml-store-sdk/go/auth"
	registryproto "github.com/caraml-dev/caraml-store/caraml-store-sdk/go/protos/feast_spark/api"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"

	"github.com/opentracing/opentracing-go"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// GrpcClient is a grpc client for feast serving.
type GrpcClient struct {
	cli  registryproto.JobServiceClient
	conn *grpc.ClientConn
}

// SecurityConfig wraps security config for GrpcClient
type SecurityConfig struct {
	// Whether to enable TLS SSL trasnport security if true.
	EnableTLS bool
	// Optional: Provides path to TLS certificate used the verify Service identity.
	TLSCertPath string
	// Optional: Credential used for authentication.
	// Disables authentication if unspecified.
	Credential *auth.Credential
}

// NewGrpcClient constructs a client that can interact via grpc with the feast serving instance at the given host:port.
func NewGrpcClient(host string, port int) (*GrpcClient, error) {
	return NewSecureGrpcClient(host, port, SecurityConfig{
		EnableTLS:  false,
		Credential: nil,
	})
}

// NewSecureGrpcClient constructs a secure client that uses security features (ie authentication).
// host - hostname of the serving host/instance to connect to.
// port - post of the host to service host/instancf to connect to.
// securityConfig - security config configures client security.
func NewSecureGrpcClient(host string, port int, security SecurityConfig) (*GrpcClient, error) {
	return NewSecureGrpcClientWithDialOptions(host, port, security)
}

// NewSecureGrpcClientWithDialOptions constructs a secure client that uses security features (ie authentication) along with custom grpc dial options.
// host - hostname of the serving host/instance to connect to.
// port - post of the host to service host/instancf to connect to.
// securityConfig - security config configures client security.
// opts - grpc.DialOptions which should be used with this connection
func NewSecureGrpcClientWithDialOptions(host string, port int, security SecurityConfig, opts ...grpc.DialOption) (*GrpcClient, error) {
	feastCli := &GrpcClient{}
	adr := fmt.Sprintf("%s:%d", host, port)

	// Compile grpc dial options from security config.
	options := append(opts, grpc.WithStatsHandler(&ocgrpc.ClientHandler{}))
	// Configure client TLS.
	if !security.EnableTLS {
		options = append(options, grpc.WithInsecure())
	} else if security.EnableTLS && security.TLSCertPath != "" {
		// Read TLS certificate from given path.
		tlsCreds, err := credentials.NewClientTLSFromFile(security.TLSCertPath, "")
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.WithTransportCredentials(tlsCreds))
	} else {
		// Use system TLS certificate pool.
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		tlsCreds := credentials.NewClientTLSFromCert(certPool, "")
		options = append(options, grpc.WithTransportCredentials(tlsCreds))
	}

	// Enable authentication by attaching credentials if given
	if security.Credential != nil {
		options = append(options, grpc.WithPerRPCCredentials(security.Credential))
	}

	// Enable tracing if a global tracer is registered
	tracingInterceptor := grpc.WithUnaryInterceptor(
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()))
	options = append(options, tracingInterceptor)

	conn, err := grpc.Dial(adr, options...)
	if err != nil {
		return nil, err
	}
	feastCli.cli = registryproto.NewJobServiceClient(conn)
	feastCli.conn = conn
	return feastCli, nil
}

// ListJobs lists the spark jobs created by the registry service.
func (fc *GrpcClient) ListJobs(ctx context.Context, req *registryproto.ListJobsRequest) ([]*registryproto.Job, error) {
	resp, err := fc.cli.ListJobs(ctx, req)
	return resp.Jobs, err
}

// ListScheduledJobs lists the scheduled spark jobs created by the registry service.
func (fc *GrpcClient) ListScheduledJobs(ctx context.Context, req *registryproto.ListScheduledJobsRequest) ([]*registryproto.ScheduledJob, error) {
	resp, err := fc.cli.ListScheduledJobs(ctx, req)
	return resp.Jobs, err
}

// StartStreamingJob start or update a streaming ingestion job.
func (fc *GrpcClient) StartStreamingJob(ctx context.Context, req *registryproto.StartStreamIngestionJobRequest) error {
	_, err := fc.cli.StartStreamIngestionJob(ctx, req)
	return err
}

// ScheduleOfflineToOnlineIngestionJob schedule or update a batch ingestion job.
func (fc *GrpcClient) ScheduleOfflineToOnlineIngestionJob(ctx context.Context, req *registryproto.ScheduleOfflineToOnlineIngestionJobRequest) error {
	_, err := fc.cli.ScheduleOfflineToOnlineIngestionJob(ctx, req)
	return err
}

// Close the grpc connection.
func (fc *GrpcClient) Close() error {
	return fc.conn.Close()
}
