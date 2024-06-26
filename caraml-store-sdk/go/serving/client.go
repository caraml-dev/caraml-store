package serving

import (
	"context"
	"crypto/x509"
	"fmt"
	"github.com/caraml-dev/caraml-store/caraml-store-sdk/go/auth"
	servingproto "github.com/caraml-dev/caraml-store/caraml-store-sdk/go/protos/feast/serving"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"

	"github.com/opentracing/opentracing-go"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Client is a feast serving client.
type Client interface {
	GetOnlineFeatures(ctx context.Context, req *OnlineFeaturesRequest) (*OnlineFeaturesResponse, error)
	GetFeastServingInfo(ctx context.Context, in *servingproto.GetFeastServingInfoRequest) (*servingproto.GetFeastServingInfoResponse, error)
	Close() error
}

// GrpcClient is a grpc client for feast serving.
type GrpcClient struct {
	cli  servingproto.ServingServiceClient
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
	feastCli.cli = servingproto.NewServingServiceClient(conn)
	feastCli.conn = conn
	return feastCli, nil
}

// GetOnlineFeatures gets the latest values of the request features from the Feast serving instance provided.
func (fc *GrpcClient) GetOnlineFeatures(ctx context.Context, req *OnlineFeaturesRequest) (
	*OnlineFeaturesResponse, error) {
	featuresRequest, err := req.buildRequest()
	if err != nil {
		return nil, err
	}
	resp, err := fc.cli.GetOnlineFeatures(ctx, featuresRequest)

	// collect unique entity refs from entity rows
	entityRefs := make(map[string]struct{})
	for _, entityRows := range req.Entities {
		for ref := range entityRows {
			entityRefs[ref] = struct{}{}
		}
	}
	return &OnlineFeaturesResponse{RawResponse: resp}, err
}

// GetFeastServingInfo gets information about the feast serving instance this client is connected to.
func (fc *GrpcClient) GetFeastServingInfo(ctx context.Context, in *servingproto.GetFeastServingInfoRequest) (
	*servingproto.GetFeastServingInfoResponse, error) {
	return fc.cli.GetFeastServingInfo(ctx, in)
}

// Close the grpc connection.
func (fc *GrpcClient) Close() error {
	return fc.conn.Close()
}
