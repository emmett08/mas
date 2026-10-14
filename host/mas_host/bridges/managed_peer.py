"""Authorised use of immutable, administrator-registered federation peers."""
from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from ..peer_registry import PeerRegistry
from ..service_contracts import ServiceError
from .orchestrator import OrchestratorConnector, PeerContract
from .peer_clients import A2ATaskServiceClient, MCPTaskServiceClient
from .service_client import TaskServiceClient


class ManagedPeerConnector(OrchestratorConnector):
    """Trusted caller identity is never taken from workflow arguments.

    The selected peer version remains fixed for this connector. Every operation
    rechecks local caller grants and revocation, then negotiates remote identity.
    A remote effect already sent at revocation can still complete. The remote
    deployment owns its own admission and budget; no shared budget is implied.
    """
    def __init__(self, registry: PeerRegistry, name, caller, *, token_file,
                 version=None, identity=None, timeout_seconds=30, transport_policy=None):
        registration = registry.resolve(name, caller, version=version, identity=identity)
        binding = registration.descriptor
        self.registry, self.caller, self.registration = registry, caller, registration
        options = {'token_file': token_file, 'principal': binding['outbound_principal'],
                   'timeout_seconds': timeout_seconds, 'allow_loopback_http': binding['allow_loopback_http'],
                   'transport_policy': transport_policy}
        clients = {'mas-http/1': TaskServiceClient, 'mas-mcp/1': MCPTaskServiceClient,
                   'mas-a2a/1': A2ATaskServiceClient}
        super().__init__(clients[binding['profile']](binding['endpoint'], **options),
            binding['outbound_principal'], peer_contract=PeerContract(binding['deployment_id'],
                binding['workflows'], tuple(binding['capabilities'])))
        parsed = urlsplit(binding['endpoint'])
        self._artifact_service = TaskServiceClient(urlunsplit(parsed._replace(path='')), **options)

    def descriptor(self):
        return {'format': 'mas-managed-peer/1', 'name': self.registration.descriptor['name'],
                'version': self.registration.descriptor['version'], 'identity': self.registration.identity,
                'caller': self.caller, 'principal': self.principal}

    def _authorise(self, capability=None):
        binding = self.registration.descriptor
        self.registry.resolve(binding['name'], self.caller,
            version=binding['version'], identity=self.registration.identity)
        if capability is not None and capability not in binding['capabilities']:
            raise ServiceError('peer_capability_forbidden')

    def negotiate(self):
        self._authorise()
        observed = super().negotiate()
        registrations = observed.get('registrations')
        if observed.get('principal') != self.principal or type(registrations) is not list:
            raise ServiceError('peer_identity_mismatch')
        identities = {}
        for item in registrations:
            if (type(item) is not dict or type(item.get('name')) is not str
                    or item['name'] in identities):
                raise ServiceError('peer_contract_mismatch')
            identities[item['name']] = item.get('identity') if item.get('available') is True else None
        if any(identities.get(name) != identity for name, identity in self.peer_contract.workflows.items()):
            raise ServiceError('peer_contract_mismatch')
        return observed

    def submit(self, request_key, workflow, input):
        self._authorise('submit')
        return super().submit(request_key, workflow, input)

    def inspect(self, task_id):
        self._authorise('inspect')
        self.negotiate()
        return super().inspect(task_id)

    def result(self, task_id):
        self._authorise('result')
        return super().result(task_id)

    def cancel(self, task_id):
        self._authorise('cancel')
        self.negotiate()
        # Inspect the pinned workflow before issuing an effect on a remote ID.
        super().inspect(task_id)
        return super().cancel(task_id)

    def update(self, task_id, request_key, input):
        self._authorise('update')
        return super().update(task_id, request_key, input)

    def artifacts(self, **options):
        """Use the same-origin artefact route, with per-chunk local revocation.

        Only sessions opened through this instance are accepted. After losing
        a transfer response, the server's operator surface performs recovery;
        a token alone never bypasses the registered scope allowlist.
        """
        from .artifact_client import ArtifactTransferClient
        sessions = set()
        def exchange(operation, payload):
            self._authorise()
            if 'scope' in payload:
                if payload['scope'] not in self.registration.descriptor['artifact_scopes']:
                    raise ServiceError('peer_artifact_forbidden')
                self.negotiate()
            elif payload.get('transfer_id') not in sessions:
                raise ServiceError('peer_artifact_forbidden')
            value = self._artifact_service.artifact(self.principal, operation, payload)
            if operation in {'download_begin', 'upload_begin'}:
                sessions.add(value.get('transfer_id'))
            if operation == 'close':
                sessions.discard(payload.get('transfer_id'))
            return value
        return ArtifactTransferClient(exchange, descriptor=self.descriptor(), **options)
