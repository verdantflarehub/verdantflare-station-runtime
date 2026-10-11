package executor

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// This server integration check runs on the development node, not locally.
func TestAppCenterRuntimeRPCRequiresSupport(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	// A legacy-only server has no protected service, regardless of whether it
	// would ignore unknown fields on the old protobuf message.
	pb.RegisterAppRuntimeServer(server, &pb.UnimplementedAppRuntimeServer{})
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	conn, err := grpc.NewClient("passthrough:///runtime-test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in, _ := protectedFixture()
	if _, err := pb.NewAppCenterRuntimeClient(conn).ReconcileBound(ctx, in); status.Code(err) != codes.Unimplemented {
		t.Fatal("legacy server did not reject protected RPC", err)
	}
	if _, err := pb.NewAppCenterRuntimeClient(conn).InspectBound(ctx, &pb.AppCenterInspection{SchemaVersion: 1}); status.Code(err) != codes.Unimplemented {
		t.Fatal("legacy server advertised protected inspection", err)
	}
}
