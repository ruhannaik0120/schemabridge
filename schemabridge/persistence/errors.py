"""Fixed redacted control-plane persistence exceptions."""
class WorkflowError(RuntimeError): pass
class WorkflowNotFoundError(WorkflowError):
    def __init__(self): super().__init__("The workflow is unavailable.")
class WorkflowConflictError(WorkflowError):
    def __init__(self): super().__init__("The workflow was modified by another command.")
class InvalidWorkflowTransitionError(WorkflowError):
    def __init__(self): super().__init__("The workflow status transition is invalid.")
class WorkflowIdempotencyConflictError(WorkflowError):
    def __init__(self): super().__init__("The idempotency key was used for a different command.")
class WorkflowPersistenceError(WorkflowError):
    def __init__(self): super().__init__("The workflow persistence operation failed.")
class WorkflowArtifactValidationError(WorkflowError):
    def __init__(self): super().__init__("The workflow artifact is invalid.")
class WorkflowMigrationError(WorkflowError):
    def __init__(self): super().__init__("The control-plane migration failed.")
class WorkflowRequiredArtifactError(WorkflowError):
    def __init__(self): super().__init__("A required workflow artifact is unavailable.")
class WorkflowStaleArtifactReferenceError(WorkflowError):
    def __init__(self): super().__init__("The workflow artifact reference is stale.")
class WorkflowOperationUnavailableError(WorkflowError):
    def __init__(self): super().__init__("The workflow operation is unavailable in the current state.")
class WorkflowMappingApprovalRequiredError(WorkflowError):
    def __init__(self): super().__init__("An approved mapping artifact is required.")
class WorkflowConnectorOperationError(WorkflowError):
    def __init__(self): super().__init__("Workflow schema discovery failed.")
class WorkflowPreviewCompilationError(WorkflowError):
    def __init__(self): super().__init__("Workflow transformation preview compilation failed.")
class WorkflowTargetProfileUnavailableError(WorkflowError):
    def __init__(self): super().__init__("The target profile is unavailable for execution.")
class WorkflowTargetProfileNotWriteCapableError(WorkflowError):
    def __init__(self): super().__init__("The target profile is not write-enabled.")
class WorkflowUnsupportedExecutionConnectorError(WorkflowError):
    def __init__(self): super().__init__("The target connector does not support migration execution.")
class WorkflowUnsafeGeneratedStatementError(WorkflowError):
    def __init__(self): super().__init__("The persisted transformation is not safe to execute.")
class WorkflowExecutionAlreadyInProgressError(WorkflowError):
    def __init__(self): super().__init__("A workflow execution is already in progress.")
class WorkflowExecutionOutcomeUncertainError(WorkflowError):
    def __init__(self): super().__init__("The target execution outcome requires manual investigation.")
class WorkflowExecutionConfirmedFailureError(WorkflowError):
    def __init__(self): super().__init__("The target execution failed and was rolled back.")
class WorkflowValidationNotReadyError(WorkflowError):
    def __init__(self): super().__init__("The workflow is not ready for validation.")
class WorkflowValidationAlreadyInProgressError(WorkflowError):
    def __init__(self): super().__init__("Workflow validation is already in progress.")
class WorkflowValidationExecutionError(WorkflowError):
    def __init__(self): super().__init__("Workflow validation execution failed.")
class WorkflowValidationOutcomeUncertainError(WorkflowError):
    def __init__(self): super().__init__("The validation outcome requires manual investigation.")
class WorkflowUnsafeValidationQueryError(WorkflowError):
    def __init__(self): super().__init__("The generated validation query is unsafe.")
class WorkflowTransportAlreadyInProgressError(WorkflowError):
    def __init__(self): super().__init__("Workflow staging transport is already in progress.")
class WorkflowTransportConfirmedFailureError(WorkflowError):
    def __init__(self): super().__init__("The staging load failed and was safely cleaned up.")
class WorkflowTransportOutcomeUncertainError(WorkflowError):
    def __init__(self): super().__init__("The staging load outcome requires manual investigation.")
class WorkflowStagingCleanupError(WorkflowError):
    def __init__(self): super().__init__("The committed migration's managed staging table could not be cleaned up.")
class MigrationJobNotFoundError(WorkflowError):
    def __init__(self): super().__init__("The migration job is unavailable.")
class MigrationJobAlreadyActiveError(WorkflowError):
    def __init__(self): super().__init__("The workflow already has an active migration job.")
class MigrationJobTransitionError(WorkflowError):
    def __init__(self): super().__init__("The migration job stage transition is invalid.")
