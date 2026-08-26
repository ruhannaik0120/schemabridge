import { expect, test } from '@playwright/test'

const sourceTable = process.env.SCHEMABRIDGE_UI_E2E_SOURCE_TABLE
const targetTable = process.env.SCHEMABRIDGE_UI_E2E_TARGET_TABLE
const backgroundSourceTable = process.env.SCHEMABRIDGE_UI_E2E_BACKGROUND_SOURCE_TABLE
const backgroundTargetTable = process.env.SCHEMABRIDGE_UI_E2E_BACKGROUND_TARGET_TABLE
const resumeSourceTable = process.env.SCHEMABRIDGE_UI_E2E_RESUME_SOURCE_TABLE
const resumeTargetTable = process.env.SCHEMABRIDGE_UI_E2E_RESUME_TARGET_TABLE
const catalog = process.env.SCHEMABRIDGE_UI_E2E_CATALOG ?? 'schemabridge'

async function createApprovedWorkflow(page: import('@playwright/test').Page, name: string, source: string, target: string) {
  await page.goto('/')
  await expect(page.getByText('API connected')).toBeVisible()
  await page.getByRole('button', { name: 'Start a migration' }).click()
  await page.getByLabel('Migration name').fill(name)

  const profiles = page.getByLabel('Connection profile ID')
  await profiles.nth(0).fill('ui-e2e-source')
  await profiles.nth(1).fill('ui-e2e-target')
  const catalogs = page.getByLabel('Database / catalog')
  await catalogs.nth(0).fill(catalog)
  await catalogs.nth(1).fill(catalog)
  const tables = page.getByLabel('Table')
  await tables.nth(0).fill(source)
  await tables.nth(1).fill(target)

  await page.getByRole('button', { name: 'Create workflow' }).click()
  await page.getByRole('button', { name: 'Discover source' }).click()
  await expect(page.getByText('3 columns')).toBeVisible()
  await page.getByRole('button', { name: 'Discover target' }).click()
  await page.getByRole('button', { name: 'Generate mapping suggestions' }).click()
  await expect(page.getByText('Approved mappings: 3 of 3')).toBeVisible()
  await page.getByRole('button', { name: 'Approve mapping' }).click()
}

test('migrates a tiny PostgreSQL table through the governed UI', async ({ page }) => {
  if (!sourceTable || !targetTable) throw new Error('The live test table names are required.')

  await createApprovedWorkflow(page, 'Automated UI PostgreSQL proof', sourceTable, targetTable)
  await page.getByRole('button', { name: 'Continue guided staging' }).click()

  await page.getByRole('checkbox', { name: 'I understand this will read source data and create a temporary staging table.' }).check()
  await page.getByRole('button', { name: 'Load staging table' }).click()
  await expect(page.getByText('Rows staged')).toBeVisible()
  await expect(page.getByText('Rows staged').locator('..').getByText('3', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Generate SQL preview' }).click()
  await expect(page.getByText('INSERT_SELECT')).toBeVisible()

  await page.getByRole('checkbox', { name: 'I reviewed this SQL and approve applying it to the final target table.' }).check()
  await page.getByRole('button', { name: 'Execute final migration' }).click()
  await expect(page.getByText('Transaction')).toBeVisible()
  await page.getByRole('button', { name: 'Run validation' }).click()
  await expect(page.getByRole('heading', { name: 'Migration checks passed' })).toBeVisible()
  await expect(page.getByText('7', { exact: true })).toBeVisible()
  await expect(page.getByText('0', { exact: true }).first()).toBeVisible()
})

test('reopens a saved mapping proposal for review', async ({ page }) => {
  if (!backgroundSourceTable || !backgroundTargetTable) throw new Error('The mapping review live test table names are required.')

  await page.goto('/')
  await page.getByRole('button', { name: 'Start a migration' }).click()
  await page.getByLabel('Migration name').fill('Automated UI mapping proposal proof')
  const profiles = page.getByLabel('Connection profile ID')
  await profiles.nth(0).fill('ui-e2e-source')
  await profiles.nth(1).fill('ui-e2e-target')
  const catalogs = page.getByLabel('Database / catalog')
  await catalogs.nth(0).fill(catalog)
  await catalogs.nth(1).fill(catalog)
  const tables = page.getByLabel('Table')
  await tables.nth(0).fill(backgroundSourceTable)
  await tables.nth(1).fill(backgroundTargetTable)
  await page.getByRole('button', { name: 'Create workflow' }).click()
  await page.getByRole('button', { name: 'Discover source' }).click()
  await page.getByRole('button', { name: 'Discover target' }).click()
  await page.getByRole('button', { name: 'Generate mapping suggestions' }).click()

  await page.reload()
  await page.getByRole('button', { name: 'Workflows' }).click()
  await page.locator('.workflow-list article').filter({ hasText: 'Automated UI mapping proposal proof' }).getByRole('button', { name: 'Open' }).click()
  await page.getByRole('button', { name: 'Open workflow' }).click()
  await page.getByRole('button', { name: 'Review saved mapping proposal' }).click()
  await expect(page.getByRole('heading', { name: 'Review the proposed mapping' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Approve mapping' })).toBeVisible()
})

test('reopens a staged workflow at its saved SQL preview', async ({ page }) => {
  if (!backgroundSourceTable || !backgroundTargetTable) throw new Error('The staged resume live test table names are required.')

  await createApprovedWorkflow(page, 'Automated UI staged resume proof', backgroundSourceTable, backgroundTargetTable)
  await page.getByRole('button', { name: 'Continue guided staging' }).click()
  await page.getByRole('checkbox', { name: 'I understand this will read source data and create a temporary staging table.' }).check()
  await page.getByRole('button', { name: 'Load staging table' }).click()
  await expect(page.getByText('Rows staged')).toBeVisible()

  await page.reload()
  await page.getByRole('button', { name: 'Workflows' }).click()
  await page.locator('.workflow-list article').filter({ hasText: 'Automated UI staged resume proof' }).getByRole('button', { name: 'Open' }).click()
  await page.getByRole('button', { name: 'Open workflow' }).click()
  await page.getByRole('button', { name: 'Review saved SQL preview' }).click()
  await expect(page.getByRole('heading', { name: 'Data is safely staged' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Generate SQL preview' })).toBeVisible()
})

test('reopens an execution-ready workflow for final confirmation', async ({ page }) => {
  if (!backgroundSourceTable || !backgroundTargetTable) throw new Error('The execution resume live test table names are required.')

  await createApprovedWorkflow(page, 'Automated UI execution resume proof', backgroundSourceTable, backgroundTargetTable)
  await page.getByRole('button', { name: 'Continue guided staging' }).click()
  await page.getByRole('checkbox', { name: 'I understand this will read source data and create a temporary staging table.' }).check()
  await page.getByRole('button', { name: 'Load staging table' }).click()
  await page.getByRole('button', { name: 'Generate SQL preview' }).click()
  await expect(page.getByText('INSERT_SELECT')).toBeVisible()

  await page.reload()
  await page.getByRole('button', { name: 'Workflows' }).click()
  await page.locator('.workflow-list article').filter({ hasText: 'Automated UI execution resume proof' }).getByRole('button', { name: 'Open' }).click()
  await page.getByRole('button', { name: 'Open workflow' }).click()
  await page.getByRole('button', { name: 'Review final execution' }).click()
  await expect(page.getByRole('heading', { name: 'Execute the final target migration' })).toBeVisible()
  await expect(page.getByRole('checkbox', { name: 'I reviewed this SQL and approve applying it to the final target table.' })).not.toBeChecked()
})

test('reopens an executed workflow for validation', async ({ page }) => {
  if (!resumeSourceTable || !resumeTargetTable) throw new Error('The validation resume live test table names are required.')
  await createApprovedWorkflow(page, 'Automated UI validation resume proof', resumeSourceTable, resumeTargetTable)
  await page.getByRole('button', { name: 'Continue guided staging' }).click()
  await page.getByRole('checkbox', { name: 'I understand this will read source data and create a temporary staging table.' }).check()
  await page.getByRole('button', { name: 'Load staging table' }).click()
  await page.getByRole('button', { name: 'Generate SQL preview' }).click()
  await page.getByRole('checkbox', { name: 'I reviewed this SQL and approve applying it to the final target table.' }).check()
  await page.getByRole('button', { name: 'Execute final migration' }).click()
  await page.reload()
  await page.getByRole('button', { name: 'Workflows' }).click()
  await page.locator('.workflow-list article').filter({ hasText: 'Automated UI validation resume proof' }).getByRole('button', { name: 'Open' }).click()
  await page.getByRole('button', { name: 'Open workflow' }).click()
  await page.getByRole('button', { name: 'Continue approved workflow' }).click()
  await expect(page.getByRole('heading', { name: 'Validate and reconcile results' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Run validation' })).toBeVisible()
})

test('queues an approved migration job through the UI', async ({ page }) => {
  if (!backgroundSourceTable || !backgroundTargetTable) throw new Error('The background live test table names are required.')

  await createApprovedWorkflow(page, 'Automated UI background job proof', backgroundSourceTable, backgroundTargetTable)
  await page.reload()
  await page.getByRole('button', { name: 'Workflows' }).click()
  await page.locator('.workflow-list article').filter({ hasText: 'Automated UI background job proof' }).getByRole('button', { name: 'Open' }).click()
  await page.getByRole('button', { name: 'Open workflow' }).click()
  await page.getByRole('button', { name: 'Continue approved workflow' }).click()
  await page.getByRole('button', { name: 'Queue background job' }).click()
  await page.getByRole('button', { name: 'Queue migration job' }).click()
  await expect(page.getByRole('heading', { name: 'Worker pickup is pending' })).toBeVisible()
  await expect(page.getByText('Status').locator('..').getByText('QUEUED', { exact: true })).toBeVisible()
})

test('shows the completed background job in Jobs', async ({ page }) => {
  test.skip(process.env.SCHEMABRIDGE_UI_E2E_VERIFY_WORKER !== '1', 'This check runs after the local worker cycle.')

  await page.goto('/')
  await page.getByRole('button', { name: 'Jobs' }).click()
  await expect(page.getByRole('heading', { name: 'Migration jobs' })).toBeVisible()
  await expect(page.getByText('SUCCEEDED', { exact: true })).toBeVisible()
  await expect(page.getByText('3 rows written')).toBeVisible()
})
