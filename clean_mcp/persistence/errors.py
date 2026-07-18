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
