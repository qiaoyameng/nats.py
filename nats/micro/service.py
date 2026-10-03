from __future__ import annotations

import json
import re
import time
from asyncio import CancelledError, Event
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    overload,
)

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from .request import Handler, Request, ServiceError

DEFAULT_QUEUE_GROUP = "q"
"""Queue Group name used across all services."""

DEFAULT_PREFIX = "$SRV"
"""The root of all control subjects."""

INFO_RESPONSE_TYPE = "io.nats.micro.v1.info_response"
PING_RESPONSE_TYPE = "io.nats.micro.v1.ping_response"
STATS_RESPONSE_TYPE = "io.nats.micro.v1.stats_response"

SEMVER_REGEX = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)
NAME_REGEX = re.compile(r"^[A-Za-z0-9\-_]+$")
SUBJECT_REGEX = re.compile(r"^[^ >]*[>]?$")


class ServiceVerb(str, Enum):
    PING = "PING"
    STATS = "STATS"
    INFO = "INFO"


@dataclass
class EndpointConfig:
    name: str
    """An alphanumeric human-readable string used to describe the endpoint.

    Multiple endpoints can have the same names.
    """

    handler: Handler
    """The handler of the endpoint."""

    queue_group: Optional[str] = None
    """The queue group of the endpoint. When queue group is not set, it defaults to the queue group of the parent group or service."""

    subject: Optional[str] = None
    """The subject of the endpoint. When subject is not set, it defaults to the name of the endpoint."""

    metadata: Optional[Dict[str, str]] = None
    """The metadata of the endpoint."""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Name cannot be empty.")

        if not NAME_REGEX.match(self.name):
            raise ValueError(
                "Invalid name. Name must contain only alphanumeric characters, underscores, and hyphens."
            )

        if self.subject:
            if not SUBJECT_REGEX.match(self.subject):
                raise ValueError(
                    "Invalid subject. Subject must not contain spaces, and can only have '>' at the end."
                )

        if self.queue_group:
            if not SUBJECT_REGEX.match(self.queue_group):
                raise ValueError(
                    "Invalid queue group. Queue group must not contain spaces."
                )


@dataclass
class EndpointStats:
    """
    Statistics about a specific service endpoint
    """

    name: str
    """
    The endpoint name
    """

    subject: str
    """
    The subject the endpoint listens on
    """

    queue_group: str
    """
    The queue group this endpoint listens on for requests
    """

    num_requests: int
    """
    The number of requests this endpoint has received
    """

    num_errors: int
    """
    The number of errors this endpoint has encountered
    """

    processing_time: int
    """
    The total processing time of requests in nanoseconds
    """

    average_processing_time: int
    """
    The average processing time of requests in nanoseconds
    """

    last_error: Optional[str] = None
    """
    The last error the service encountered, if any.
    """

    data: Optional[Any] = None
    """
    Additional statistics the endpoint makes available
    """

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EndpointStats:
        return cls(
            name=data["name"],
            subject=data["subject"],
            queue_group=data["queue_group"],
            num_requests=data["num_requests"],
            num_errors=data["num_errors"],
            last_error=data["last_error"],
            processing_time=data["processing_time"],
            average_processing_time=data["average_processing_time"],
            data=data["data"],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "subject": self.subject,
            "queue_group": self.queue_group,
            "num_requests": self.num_requests,
            "num_errors": self.num_errors,
            "last_error": self.last_error or "",
            "processing_time": self.processing_time,
            "average_processing_time": self.average_processing_time,
            "data": self.data,
        }


@dataclass
class EndpointInfo:
    """The information of an endpoint."""

    name: str
    """
    The endopoint name
    """

    subject: str
    """
    The subject the endpoint listens on
    """

    queue_group: str
    """
    The queue group this endpoint listens on for requests
    """

    metadata: Dict[str, Any] = field(default_factory=dict)
    """
    The endpoint metadata.
    """

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EndpointInfo:
        return cls(
            name=data["name"],
            subject=data["subject"],
            queue_group=data["queue_group"],
            metadata=data["metadata"],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "subject": self.subject,
            "queue_group": self.queue_group,
            "metadata": self.metadata,
        }


class _Period:
    """A single, self-contained statistics accumulation period.

    Requests are always charged to the period that was current when the
    request started, even if the service is reset while the request is still
    in flight. A period is finalized (its average processing time frozen and
    its `settled` event set) once every request that belongs to it has
    completed.
    """

    def __init__(self, started: datetime) -> None:
        self.started = started
        self.num_requests = 0
        self.num_errors = 0
        self.processing_time = 0
        self.average_processing_time = 0
        self.last_error: Optional[str] = None
        self.in_flight = 0
        self.settled = Event()

    def record_error(self, error: BaseException) -> None:
        self.num_errors += 1
        self.last_error = repr(error)

    def record_completion(self, elapsed_ns: int) -> None:
        self.processing_time += elapsed_ns
        self.average_processing_time = int(
            self.processing_time / self.num_requests
        )

    def finalize(self) -> None:
        """Freeze the period and mark it as fully settled."""
        if not self.settled.is_set():
            self.settled.set()


class Endpoint:
    """Endpoint manages a service endpoint."""

    def __init__(self, service: Service, config: EndpointConfig) -> None:
        self._service = service
        self._name = config.name
        self._subject = config.subject or config.name
        self._queue_group = config.queue_group or DEFAULT_QUEUE_GROUP
        self._handler = config.handler
        self._metadata = config.metadata

        self._current = _Period(service._started)
        # Closed periods in closing order. Each one is finalized and kept
        # until it is taken; a period with in-flight requests finalizes on
        # its own once those requests settle.
        self._closed: deque[_Period] = deque()

        self._subscription: Optional[Subscription] = None

    async def _start(self) -> None:
        assert not self._subscription

        self._subscription = await self._service._client.subscribe(
            subject=self._subject,
            queue=self._queue_group,
            cb=self._handle_request,
        )

    async def _stop(self) -> None:
        assert self._subscription
        await self._subscription.unsubscribe()
        self._subscription = None

    async def _rotate(self, started: datetime) -> None:
        """Begin a new statistics period.

        Requests already in flight keep charging the period they started in.
        The outgoing period is queued behind any earlier closed period.
        """
        outgoing = self._current
        self._closed.append(outgoing)
        self._current = _Period(started)

        if outgoing.in_flight == 0:
            outgoing.finalize()

    def _stats_for(self, period: _Period) -> EndpointStats:
        return EndpointStats(
            name=self._name,
            subject=self._subject,
            queue_group=self._queue_group,
            num_requests=period.num_requests,
            num_errors=period.num_errors,
            last_error=period.last_error,
            processing_time=period.processing_time,
            average_processing_time=period.average_processing_time,
        )

    def current_stats(self) -> EndpointStats:
        """Snapshot of the endpoint statistics for the current period."""
        return self._stats_for(self._current)

    async def take_previous_stats(self) -> Optional[EndpointStats]:
        """Take the next closed-period summary, waiting until it settles.

        Closed periods are handed out in closing order. If the oldest closed
        period still has in-flight requests, this waits for it rather than
        returning a newer period first. Returns None when no closed period
        remains.
        """
        if not self._closed:
            return None

        period = self._closed[0]
        await period.settled.wait()

        # Another reset may have queued more periods while awaiting; pop the
        # head only if it is still the period that was awaited.
        if self._closed and self._closed[0] is period:
            self._closed.popleft()
            return self._stats_for(period)

        return None

    async def _handle_request(self, msg: Msg) -> None:
        """Handle an endpoint message."""
        start_time = time.perf_counter_ns()
        period = self._current
        period.num_requests += 1
        period.in_flight += 1
        request = Request(msg)

        try:
            try:
                await self._handler(request)
            except CancelledError:
                # Cancellation (e.g. drain/unsubscribe) is not a service
                # failure: the request still settles into the period it
                # began in.
                raise
            except Exception as error:
                period.record_error(error)

                # A failure while delivering the error response is handled
                # explicitly: the handler failure stays the last recorded
                # error, and the secondary delivery failure is surfaced to
                # the dispatcher instead of being swallowed.
                if isinstance(error, ServiceError):
                    await request.respond_error(
                        error.code, error.description
                    )
                else:
                    await request.respond_error("500", repr(error))
        finally:
            self._settle(period, start_time)

    def _settle(self, period: _Period, start_time: int) -> None:
        period.in_flight -= 1
        period.record_completion(time.perf_counter_ns() - start_time)

        if period in self._closed and period.in_flight == 0:
            period.finalize()


@dataclass
class GroupConfig:
    """The configuration of a group."""

    name: str
    """The name of the group."""

    queue_group: Optional[str] = None
    """The default queue group of the group."""


class EndpointManager(Protocol):
    """
    Manages the endpoints of a service.
    """

    @overload
    async def add_endpoint(self, config: EndpointConfig) -> None:
        ...

    @overload
    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        queue_group: Optional[str] = None,
        subject: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        ...

    async def add_endpoint(
        self, config: Optional[EndpointConfig] = None, **kwargs
    ) -> None:
        ...


class GroupManager(Protocol):
    """
    Manages the groups of a service.
    """

    @overload
    def add_group(
        self, *, name: str, queue_group: Optional[str] = None
    ) -> Group:
        ...

    @overload
    def add_group(self, config: GroupConfig) -> Group:
        ...

    def add_group(
        self, config: Optional[GroupConfig] = None, **kwargs
    ) -> Group:
        ...


class Group(GroupManager, EndpointManager):

    def __init__(self, service: "Service", config: GroupConfig) -> None:
        self._service = service
        self._prefix = config.name
        self._queue_group = config.queue_group

    @overload
    async def add_endpoint(self, config: EndpointConfig) -> None:
        ...

    @overload
    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        queue_group: Optional[str] = None,
        subject: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        ...

    async def add_endpoint(
        self, config: Optional[EndpointConfig] = None, **kwargs
    ) -> None:
        if config is None:
            config = EndpointConfig(**kwargs)
        else:
            config = replace(config, **kwargs)

        config = replace(
            config,
            subject=f"{self._prefix.strip('.')}.{config.subject or config.name}"
            .strip("."),
            queue_group=config.queue_group or self._queue_group,
        )

        await self._service.add_endpoint(config)

    @overload
    def add_group(
        self, *, name: str, queue_group: Optional[str] = None
    ) -> Group:
        ...

    @overload
    def add_group(self, config: GroupConfig) -> Group:
        ...

    def add_group(
        self, config: Optional[GroupConfig] = None, **kwargs
    ) -> Group:
        if config:
            config = replace(config, **kwargs)
        else:
            config = GroupConfig(**kwargs)

        config = replace(
            config,
            name=f"{self._prefix}.{config.name}",
            queue_group=config.queue_group or self._queue_group,
        )

        return Group(self._service, config)


StatsHandler = Callable[[EndpointStats], Any]
"""
A handler function used to configure a custom *STATS* endpoint.
"""


@dataclass
class ServiceConfig:
    """The configuration of a service."""

    name: str
    """The kind of the service. Shared by all services that have the same name.
    This name can only have A-Z, a-z, 0-9, dash, underscore."""

    version: str
    """The version of the service.
    This verson must be a valid semantic version."""

    description: Optional[str] = None
    """The description of the service."""

    metadata: Optional[Dict[str, str]] = None
    """The metadata of the service."""

    queue_group: Optional[str] = None
    """The default queue group of the service."""

    stats_handler: Optional[StatsHandler] = None
    """
    A handler function used to configure a custom *STATS* endpoint.
    """

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Name cannot be empty.")

        if not NAME_REGEX.match(self.name):
            raise ValueError(
                "Invalid name. It must contain only alphanumeric characters, dashes, and underscores."
            )

        if not self.version:
            raise ValueError("Version cannot be empty.")

        if not SEMVER_REGEX.match(self.version):
            raise ValueError(
                "Invalid version. It must follow semantic versioning (e.g., 1.0.0, 2.1.3-alpha.1)."
            )

        if self.queue_group:
            if not SUBJECT_REGEX.match(self.queue_group):
                raise ValueError(
                    "Invalid queue group. It must contain only alphanumeric characters, dashes, and underscores."
                )


class ServiceIdentity(Protocol):
    """
    Defines fields helping to identity a service instance.
    """

    id: str
    name: str
    version: str
    metadata: Dict[str, str]


@dataclass
class ServicePing(ServiceIdentity):
    """The response to a ping message."""

    id: str
    name: str
    version: str
    metadata: Dict[str, str] = field(default_factory=dict)
    type: str = PING_RESPONSE_TYPE

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ServicePing:
        return cls(
            id=data["id"],
            name=data["name"],
            version=data["version"],
            metadata=data.get("metadata", {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "metadata": self.metadata,
        }


@dataclass
class ServiceStats(ServiceIdentity):
    """The statistics of a service."""

    id: str
    """
    A unique ID for this instance of a service
    """

    name: str
    """
    The kind of the service. Shared by all the services that have the same name
    """

    version: str
    """
    The version of the service
    """

    started: datetime
    """
    The time the service was started
    """

    endpoints: List[EndpointStats] = field(default_factory=list)
    """
    Statistics for each known endpoint
    """

    metadata: Dict[str, str] = field(default_factory=dict)
    """Service metadata."""

    type: str = STATS_RESPONSE_TYPE
    """
    The schema type of the message
    """

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ServiceStats:
        result = cls(
            id=data["id"],
            name=data["name"],
            version=data["version"],
            started=datetime.strptime(
                data["started"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            endpoints=[
                EndpointStats.from_dict(endpoint)
                for endpoint in data["endpoints"]
            ],
            metadata=data["metadata"],
        )

        return result

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "name": self.name,
            "id": self.id,
            "version": self.version,
            "started": self.started.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] +
                       "Z",
            "endpoints": [endpoint.to_dict() for endpoint in self.endpoints],
            "metadata": self.metadata,
        }


@dataclass
class ServiceInfo:
    """The information of a service."""

    name: str
    """
    The name of the service. Shared by all the services that have the same name
    """

    id: str
    """
    A unique ID for this instance of a service
    """

    version: str
    """
    The version of the service
    """

    description: Optional[str] = None
    """
    The description of the service supplied as configuration while creating the service
    """

    endpoints: List[EndpointInfo] = field(default_factory=list)
    """
    Information for all service endpoints
    """

    metadata: Dict[str, str] = field(default_factory=dict)
    """
    The service metadata
    """

    type: str = INFO_RESPONSE_TYPE
    """
    The type of the message
    """

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ServiceInfo:
        """
        Create a `ServiceInfo` from a dictionary.

        Notes:
            - Unknown fields are ignored.
        """

        return cls(
            id=data["id"],
            name=data["name"],
            version=data["version"],
            description=data.get("description"),
            endpoints=[
                EndpointInfo.from_dict(endpoint)
                for endpoint in data["endpoints"]
            ],
            metadata=data["metadata"],
            type=data.get("type", "io.nats.micro.v1.info_response"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the `ServiceInfo` to a dictionary.
        """

        return {
            "type": self.type,
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "endpoints": [endpoint.to_dict() for endpoint in self.endpoints],
            "metadata": self.metadata,
        }


class Service(AsyncContextManager):

    def __init__(self, client: Client, config: ServiceConfig) -> None:
        self._id = client._nuid.next().decode()
        self._name = config.name
        self._version = config.version
        self._description = config.description
        self._metadata = config.metadata or {}
        self._queue_group = config.queue_group
        self._stats_handler = config.stats_handler

        self._client = client
        self._subscriptions = {}
        self._endpoints = []
        self._started = datetime.utcnow()
        # Start timestamps of closed periods, aligned in order with the
        # closed-period queues of each endpoint.
        self._closed_started: deque[datetime] = deque()
        self._stopped = Event()
        self._prefix = DEFAULT_PREFIX

    @property
    def id(self) -> str:
        """
        Returns the unique identifier of the service.
        """
        return self._id

    async def start(self) -> None:
        if self._subscriptions:
            return

        verb_request_handlers = {
            ServiceVerb.PING: self._handle_ping_request,
            ServiceVerb.INFO: self._handle_info_request,
            ServiceVerb.STATS: self._handle_stats_request,
        }

        for verb, verb_handler in verb_request_handlers.items():
            verb_subjects = [
                (
                    f"{verb}-all",
                    control_subject(
                        verb, name=None, id=None, prefix=self._prefix
                    ),
                ),
                (
                    f"{verb}-kind",
                    control_subject(
                        verb, name=self._name, id=None, prefix=self._prefix
                    ),
                ),
                (
                    verb,
                    control_subject(
                        verb,
                        name=self._name,
                        id=self._id,
                        prefix=self._prefix
                    ),
                ),
            ]

            for key, subject in verb_subjects:
                self._subscriptions[key] = await self._client.subscribe(
                    subject, cb=verb_handler
                )

        self._started = datetime.utcnow()
        await self._client.flush()

    @overload
    async def add_endpoint(self, config: EndpointConfig) -> None:
        ...

    @overload
    async def add_endpoint(
        self,
        *,
        name: str,
        handler: Handler,
        queue_group: Optional[str] = None,
        subject: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        ...

    async def add_endpoint(
        self, config: Optional[EndpointConfig] = None, **kwargs
    ) -> None:
        if config is None:
            config = EndpointConfig(**kwargs)
        else:
            config = replace(config, **kwargs)

        config = replace(
            config, queue_group=config.queue_group or self._queue_group
        )

        endpoint = Endpoint(self, config)
        await endpoint._start()
        self._endpoints.append(endpoint)

    @overload
    def add_group(
        self, *, name: str, queue_group: Optional[str] = None
    ) -> Group:
        ...

    @overload
    def add_group(self, config: GroupConfig) -> Group:
        ...

    def add_group(
        self, config: Optional[GroupConfig] = None, **kwargs
    ) -> Group:
        if config:
            config = replace(config, **kwargs)
        else:
            config = GroupConfig(**kwargs)

        config = replace(
            config, queue_group=config.queue_group or self._queue_group
        )

        return Group(self, config)

    def stats(self) -> ServiceStats:
        """
        Returns the statistics for the service's current period.

        Requests that started before a `reset` and are still in flight keep
        contributing to the previous period, so they never pollute these
        counters. The returned object is an independent snapshot: the custom
        stats handler may mutate it without affecting internal counters.
        """
        stats = ServiceStats(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
            endpoints=[
                endpoint.current_stats() for endpoint in self._endpoints
            ],
            started=self._started,
        )

        if self._stats_handler:
            for endpoint_stats in stats.endpoints:
                endpoint_stats.data = self._stats_handler(endpoint_stats)

        return stats

    def _service_stats(
        self, started: datetime, endpoint_stats: List[EndpointStats]
    ) -> ServiceStats:
        return ServiceStats(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
            endpoints=endpoint_stats,
            started=started,
        )

    async def pop_previous_stats(self) -> Optional[ServiceStats]:
        """Take the finalized summary of the next closed period.

        Closed periods are handed out in the order they were created. This
        waits until every request that was in flight when the period closed
        has settled into it, then removes and returns its summary. Returns
        None if no closed period is pending. Each closed period is taken at
        most once.
        """
        if not self._closed_started:
            return None

        previous_started = self._closed_started[0]
        endpoint_stats = []
        for endpoint in self._endpoints:
            stats = await endpoint.take_previous_stats()
            if stats is None:
                # Endpoints added after the period closed saw no traffic in
                # it; report them with zeroed counters for a complete shape.
                stats = replace(
                    endpoint.current_stats(),
                    num_requests=0,
                    num_errors=0,
                    processing_time=0,
                    average_processing_time=0,
                    last_error=None,
                )
            endpoint_stats.append(stats)

        self._closed_started.popleft()
        return self._service_stats(previous_started, endpoint_stats)

    def info(self) -> ServiceInfo:
        return ServiceInfo(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
            description=self._description,
            endpoints=[
                EndpointInfo(
                    name=endpoint._name,
                    subject=endpoint._subject,
                    queue_group=endpoint._queue_group,
                    metadata=endpoint._metadata,
                ) for endpoint in self._endpoints
            ],
        )

    async def reset(self) -> None:
        """
        Starts a new statistics period for every endpoint.

        This does not simply change the `started` timestamp or zero shared
        counters. Requests that are already in flight are bound to the period
        in which they started and keep accumulating there once they complete;
        new requests accumulate in the fresh period. The closed period can be
        retrieved in closing order once fully settled via
        `pop_previous_stats`.
        """
        started = datetime.utcnow()

        self._closed_started.append(self._started)
        for endpoint in self._endpoints:
            await endpoint._rotate(started)

        self._started = started

    async def stop(self) -> None:
        if self._stopped.is_set():
            return

        for endpoint in self._endpoints:
            await endpoint._stop()

        for subscription in self._subscriptions.values():
            await subscription.drain()

        self._endpoints = []
        self._subscriptions = {}

        self._stopped.set()

    @property
    def stopped(self) -> Event:
        """
        Indicates whether `stop` was executed on the service.
        """
        return self._stopped

    async def __aenter__(self) -> "Service":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def _handle_ping_request(self, msg: Msg) -> None:
        """Handle a ping message."""
        ping = ServicePing(
            id=self._id,
            name=self._name,
            version=self._version,
            metadata=self._metadata,
        ).to_dict()

        await msg.respond(data=json.dumps(ping).encode())

    async def _handle_info_request(self, msg: Msg) -> None:
        """Handle an info message."""
        info = self.info().to_dict()

        await msg.respond(data=json.dumps(info).encode())

    async def _handle_stats_request(self, msg: Msg) -> None:
        """Handle a stats message."""
        stats = self.stats().to_dict()
        await msg.respond(data=json.dumps(stats).encode())


def control_subject(
    verb: ServiceVerb,
    name: Optional[str] = None,
    id: Optional[str] = None,
    prefix=DEFAULT_PREFIX,
) -> str:
    if name is None and id is None:
        return f"{prefix}.{verb.value}"
    elif id is None:
        return f"{prefix}.{verb.value}.{name}"
    else:
        return f"{prefix}.{verb.value}.{name}.{id}"
