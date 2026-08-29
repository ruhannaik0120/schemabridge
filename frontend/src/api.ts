export type ApiConnection = 'checking' | 'connected' | 'offline'
export type DatabaseSystem = 'postgresql' | 'mysql' | 'snowflake'

export interface RelationInput { system: DatabaseSystem; catalogName: string; schemaName: string; objectName: string }
export interface WorkflowCreateInput { displayName: string; sourceProfileId: string; targetProfileId: string; sourceRelation: RelationInput; targetRelation: RelationInput }
export interface Workflow { workflow_id: string; display_name: string; source_profile_id: string; target_profile_id: string; status: string; version: number; source_relation: { system: string; catalog_name: string | null; schema_name: string; object_name: string }; target_relation: { system: string; catalog_name: string | null; schema_name: string; object_name: string } }
export interface DiscoveredColumn { column_name: string; native_type: string | null; canonical_type: string; nullable: boolean | null }
export interface DiscoveryResult { workflow: Workflow; artifact: { artifact_version: number }; result: { columns: DiscoveredColumn[]; estimated_row_count: number | null } }
export interface MappingSuggestion { source_column: string; target_column: string | null; confidence: number; compatibility: string; decision: string; evidence: Array<{ code: string; explanation: string | null }> }
export interface MappingPlanResult { workflow: Workflow; artifact: { artifact_version: number }; result: { suggestions: MappingSuggestion[]; unmatched_source_columns: string[]; unmatched_target_columns: string[]; ambiguous_source_columns: string[]; warnings: string[] } }
export interface MappingDecision { source_column: string; target_column?: string | null; status: 'APPROVED' | 'REJECTED' }
export interface MappingApprovalResult { workflow: Workflow; artifact: { artifact_version: number }; result: { approved_mappings: Array<{ source_column: string; target_column: string | null }> } }
export interface StagingResult { workflow: Workflow; artifact: { artifact_version: number }; result: { staging_relation: { schema_name: string; object_name: string }; rows_read: number; rows_written: number; batch_count: number; duration_ms: number } }
export interface TransformationPreviewResult { workflow: Workflow; artifact: { artifact_version: number }; result: { preview_only: true; dialect: string; statement_type: string; sql: string; parameters: unknown[]; warnings: string[] } }
export interface ExecutionResult { workflow: Workflow; artifact: { artifact_version: number }; result: { status: string; affected_rows: number | null; duration_ms: number; transaction_outcome: string }; cleanup: { staging_relation: { schema_name: string; object_name: string }; duration_ms: number } | null }
export interface ValidationResult { workflow: Workflow; result: { validation_report: { status: string; matched_count: number; mismatched_count: number; unavailable_count: number; warnings: string[]; check_results: Array<{ check_id: string; check_type: string; source_value: number | null; target_value: number | null; status: string; difference: number | null }> }; source_execution_status: string; target_execution_status: string; primary_key_reconciliation: { source_key_count: number; target_key_count: number; missing_key_count: number; extra_key_count: number; source_duplicate_key_count: number; target_duplicate_key_count: number } | null; warnings: string[] } }
export interface WorkflowHistory { workflow: Workflow; artifacts: Array<{ artifact_type: string; artifact_version: number; payload: unknown; created_at: string }>; events: Array<{ sequence_number: number; event_type: string; occurred_at: string; new_status: string | null }> }
export interface MigrationJob { job_id: string; workflow_id: string; status: string; stage: string; queued_at: string; batch_size: number; timeout_seconds: number; batch_progress: { batches_completed: number; rows_read: number; rows_written: number; total_rows_estimate: number | null; estimated_percent_complete: number | null } | null; failure_category: string | null }
export interface ConnectionProfileSummary { name: string; db_type: string; database: string; database_present: boolean; write_enabled: boolean }

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? ''

/** Check only the backend process; this never opens a database connection. */
export async function checkApiHealth(): Promise<ApiConnection> {
  try { const response = await fetch(`${apiBaseUrl}/health/live`, { headers: { Accept: 'application/json' } }); return response.ok ? 'connected' : 'offline' } catch { return 'offline' }
}

export async function listConnectionProfiles(): Promise<ConnectionProfileSummary[]> {
  const response = await fetch(`${apiBaseUrl}/api/v1/profiles`, { headers: { Accept: 'application/json' } })
  if (response.ok) return (await response.json() as { items: ConnectionProfileSummary[] }).items
  throw new Error('SchemaBridge could not load configured connection profiles.')
}

/** Create only the durable workflow record. All database actions are separate approval-gated steps. */
export async function createWorkflow(input: WorkflowCreateInput): Promise<Workflow> {
  const response = await fetch(`${apiBaseUrl}/api/v1/migrations/workflows`, { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ display_name: input.displayName, source_profile_id: input.sourceProfileId, target_profile_id: input.targetProfileId, source_relation: toApiRelation(input.sourceRelation), target_relation: toApiRelation(input.targetRelation) }) })
  if (response.ok) return response.json() as Promise<Workflow>
  const payload = await response.json().catch(() => null) as { error?: { message?: string } } | null
  throw new Error(payload?.error?.message ?? 'SchemaBridge could not create this workflow.')
}

function toApiRelation(relation: RelationInput) { return { system: relation.system, catalog_name: relation.catalogName || null, schema_name: relation.schemaName, object_name: relation.objectName } }

/** Discover the saved source or target relation and store its immutable metadata artifact. */
export async function discoverRelation(workflow: Workflow, side: 'source' | 'target'): Promise<DiscoveryResult> {
  const response = await fetch(`${apiBaseUrl}/api/v1/migrations/workflows/${workflow.workflow_id}/discover-${side}`, { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_version: workflow.version }) })
  if (response.ok) return response.json() as Promise<DiscoveryResult>
  const payload = await response.json().catch(() => null) as { error?: { message?: string } } | null
  throw new Error(payload?.error?.message ?? `SchemaBridge could not discover the ${side} table.`)
}

export async function generateMapping(workflow: Workflow): Promise<MappingPlanResult> { return workflowCommand<MappingPlanResult>(workflow, 'mapping-proposals', {}) }
export async function approveMapping(workflow: Workflow, mappingArtifactVersion: number, decisions: MappingDecision[]): Promise<MappingApprovalResult> { return workflowCommand<MappingApprovalResult>(workflow, 'mapping-approvals', { mapping_artifact_version: mappingArtifactVersion, decisions }) }

async function workflowCommand<T>(workflow: Workflow, path: string, command: Record<string, unknown>): Promise<T> {
  const response = await fetch(`${apiBaseUrl}/api/v1/migrations/workflows/${workflow.workflow_id}/${path}`, { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_version: workflow.version, ...command }) })
  if (response.ok) return response.json() as Promise<T>
  const payload = await response.json().catch(() => null) as { error?: { message?: string } } | null
  throw new Error(payload?.error?.message ?? 'SchemaBridge could not complete that workflow step.')
}

export async function loadStaging(workflow: Workflow, sourceDiscoveryArtifactVersion: number, approvedMappingArtifactVersion: number, batchSize: number, timeoutSeconds: number): Promise<StagingResult> {
  return workflowCommand<StagingResult>(workflow, 'load-staging', { source_discovery_artifact_version: sourceDiscoveryArtifactVersion, approved_mapping_artifact_version: approvedMappingArtifactVersion, source_profile_id: workflow.source_profile_id, target_profile_id: workflow.target_profile_id, batch_size: batchSize, timeout_seconds: timeoutSeconds })
}

export async function previewTransformation(workflow: Workflow, approvedMappingArtifactVersion: number, staging: { schema_name: string; object_name: string }): Promise<TransformationPreviewResult> {
  return workflowCommand<TransformationPreviewResult>(workflow, 'transformation-previews', { approved_mapping_artifact_version: approvedMappingArtifactVersion, staging_database: workflow.target_relation.catalog_name ?? undefined, staging_schema: staging.schema_name, staging_table: staging.object_name, statement_type: 'INSERT_SELECT' })
}

export async function executeTransformation(workflow: Workflow, approvedMappingArtifactVersion: number, previewArtifactVersion: number): Promise<ExecutionResult> {
  return workflowCommand<ExecutionResult>(workflow, 'execute', { approved_mapping_artifact_version: approvedMappingArtifactVersion, transformation_preview_artifact_version: previewArtifactVersion, target_profile_id: workflow.target_profile_id, timeout_seconds: 300 })
}

export async function validateMigration(workflow: Workflow, approvedMappingArtifactVersion: number, executionEvidenceArtifactVersion: number, strictPrimaryKey: boolean): Promise<ValidationResult> {
  return workflowCommand<ValidationResult>(workflow, 'validate', { execution_evidence_artifact_version: executionEvidenceArtifactVersion, approved_mapping_artifact_version: approvedMappingArtifactVersion, source_profile_id: workflow.source_profile_id, target_profile_id: workflow.target_profile_id, timeout_seconds: 300, strict_primary_key: strictPrimaryKey, primary_key_batch_size: 500 })
}

export async function getWorkflowHistory(workflowId: string): Promise<WorkflowHistory> {
  const base = `${apiBaseUrl}/api/v1/migrations/workflows/${workflowId}`
  const [workflowResponse, artifactsResponse, eventsResponse] = await Promise.all([fetch(base, { headers: { Accept: 'application/json' } }), fetch(`${base}/artifacts?limit=100`, { headers: { Accept: 'application/json' } }), fetch(`${base}/audit-events?limit=100`, { headers: { Accept: 'application/json' } })])
  if (workflowResponse.ok && artifactsResponse.ok && eventsResponse.ok) return { workflow: await workflowResponse.json() as Workflow, artifacts: (await artifactsResponse.json() as { items: WorkflowHistory['artifacts'] }).items, events: (await eventsResponse.json() as { items: WorkflowHistory['events'] }).items }
  const payload = await workflowResponse.json().catch(() => null) as { error?: { message?: string } } | null
  throw new Error(payload?.error?.message ?? 'SchemaBridge could not load this workflow history.')
}

export async function listWorkflows(): Promise<Workflow[]> {
  const response = await fetch(`${apiBaseUrl}/api/v1/migrations/workflows?limit=20`, { headers: { Accept: 'application/json' } })
  if (response.ok) return (await response.json() as { items: Workflow[] }).items
  throw new Error('SchemaBridge could not load workflows.')
}

export async function listMigrationJobs(): Promise<MigrationJob[]> {
  const response = await fetch(`${apiBaseUrl}/api/v1/migrations/jobs?limit=20`, { headers: { Accept: 'application/json' } })
  if (response.ok) return (await response.json() as { items: MigrationJob[] }).items
  throw new Error('SchemaBridge could not load migration jobs.')
}

export async function queueMigrationJob(workflow: Workflow, sourceDiscoveryArtifactVersion: number, approvedMappingArtifactVersion: number): Promise<MigrationJob> {
  const response = await fetch(`${apiBaseUrl}/api/v1/migrations/workflows/${workflow.workflow_id}/jobs`, { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ expected_version: workflow.version, source_discovery_artifact_version: sourceDiscoveryArtifactVersion, approved_mapping_artifact_version: approvedMappingArtifactVersion, batch_size: 1000, timeout_seconds: 300 }) })
  if (response.ok) return (await response.json() as { job: MigrationJob }).job
  const payload = await response.json().catch(() => null) as { error?: { message?: string } } | null
  throw new Error(payload?.error?.message ?? 'SchemaBridge could not queue the migration job.')
}
