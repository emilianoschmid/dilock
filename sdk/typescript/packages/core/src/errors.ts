export class DilockError extends Error { constructor(message: string, options?: ErrorOptions) { super(message, options); this.name = new.target.name; } }
export class InvalidArgumentError extends DilockError {}
export class BackendUnavailableError extends DilockError {}
export class ProtocolMismatchError extends DilockError {}
export class AcquisitionUnknownError extends DilockError {}
export class AcquisitionTimeoutError extends DilockError {}
export class LeaseLostError extends DilockError {}
export class ReleaseUnknownError extends DilockError {}
export class UnsupportedCapabilityError extends DilockError {}
