import grpc
import functools
from concurrent import futures
from signal import signal, SIGINT, SIGQUIT, SIGTERM

from teos.logger import setup_logging, get_logger

from teos.constants import SHUTDOWN_GRACE_TIME
from teos.protobuf.tower_services_pb2_grpc import (
    TowerServicesStub,
    TowerServicesServicer,
    add_TowerServicesServicer_to_server,
)


class RPC:
    """
    The RPC is an external RPC server offered by tower to receive requests from the CLI.
    This acts as a proxy between the internal api and the CLI.
    Args:
        rpc_bind (:obj:`str`): the IP or host where the RPC server will be hosted.
        rpc_port (:obj:`int`): the port where the RPC server will be hosted.
        internal_api_endpoint (:obj:`str`): the endpoint where to reach the internal (gRPC) api.
    Attributes:
        logger (:obj:`Logger <teos.logger.Logger>`): the logger for this component.
        endpoint (:obj:`str`): the endpoint where the RPC api will be served (external gRPC server).
        rpc_server (:obj:`Server <grpc.Server>`): the non-started gRPC server instance.
    """

    def __init__(self, rpc_bind, rpc_port, internal_api_endpoint):
        self.logger = get_logger(component=RPC.__name__)
        self.endpoint = f"{rpc_bind}:{rpc_port}"
        self.rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.rpc_server.add_insecure_port(self.endpoint)
        add_TowerServicesServicer_to_server(_RPC(internal_api_endpoint, self.logger), self.rpc_server)

    def handle_signals(self, signum, frame):
        self.teardown()

    def teardown(self):
        self.logger.info("Stopping")
        stopped_event = self.rpc_server.stop(SHUTDOWN_GRACE_TIME)
        stopped_event.wait()
        self.logger.info("Stopped")


def forward_errors(func):
    """
    Transforms ``func`` in order to forward any ``grpc.RPCError`` returned by the upstream grpc as the result of the
    current grpc call.
    """

    @functools.wraps(func)
    def wrapper(self, request, context, *args, **kwargs):
        try:
            return func(self, request, context, *args, **kwargs)
        except grpc.RpcError as e:
            context.set_details(e.details())
            context.set_code(e.code())

    return wrapper


class _RPC(TowerServicesServicer):
    """
    This represents the RPC server provider and implements all the methods that can be accessed using the CLI.

    Args:
        internal_api_endpoint (:obj:`str`): the endpoint where to reach the internal (gRPC) api.
        logger (:obj:`Logger <teos.logger.Logger>`): the logger for this component.

    Attributes:
        channel (:obj:`grpc.Channel`): the Channel object
        stub (:obj:`TowerServicesStub`): the rpc client stub
    """

    def __init__(self, internal_api_endpoint, logger):
        self.logger = logger
        self.internal_api_endpoint = internal_api_endpoint
        self.channel = grpc.insecure_channel(self.internal_api_endpoint)
        self.stub = TowerServicesStub(self.channel)

    @forward_errors
    def get_all_appointments(self, request, context):
        return self.stub.get_all_appointments(request)

    @forward_errors
    def get_tower_info(self, request, context):
        return self.stub.get_tower_info(request)

    @forward_errors
    def get_users(self, request, context):
        return self.stub.get_users(request)

    @forward_errors
    def get_user(self, request, context):
        return self.stub.get_user(request)

    @forward_errors
    def stop(self, request, context):
        return self.stub.stop(request)


def serve(rpc_bind, rpc_port, internal_api_endpoint, stop_event, log_file):
    """
    Serves the external RPC API at the given endpoint and connects it to the internal api.

    This method will serve and hold until the main process is stop or a stop signal is received. Notice the latter is
    not possible possible currently since the stop signal has to be passed to `rpc_server` and it is not returned. This
    may change once the stop command for the CLI is implemented.

    Args:
        rpc_bind (:obj:`str`): the IP or host where the RPC server will be hosted.
        rpc_port (:obj:`int`): the port where the RPC server will be hosted.
        internal_api_endpoint (:obj:`str`): the endpoint where to reach the internal (gRPC) api.
        log_file (:obj:`str`): the file_path where to store logs.
        stop_event (:obj:`multiprocessing.Event`) the Event that this service will monitor. The rpc server will
            initiate a graceful shutdown once this event is set.
    """

    setup_logging()
    rpc = RPC(rpc_bind, rpc_port, internal_api_endpoint)
    signal(SIGINT, rpc.handle_signals)
    signal(SIGTERM, rpc.handle_signals)
    signal(SIGQUIT, rpc.handle_signals)
    rpc.rpc_server.start()

    rpc.logger.info(f"Initialized. Serving at {rpc.endpoint}")

    stop_event.wait()

    rpc.logger.info("Stopping")
    stopped_event = rpc.rpc_server.stop(SHUTDOWN_GRACE_TIME)
    stopped_event.wait()
    rpc.logger.info("Stopped")
