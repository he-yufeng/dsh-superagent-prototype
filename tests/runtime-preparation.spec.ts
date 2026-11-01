/** Workspace preparation outcomes and recovery through the Runtime's public task lifecycle. */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { Store } from '../src/app/store.ts'
import { KV } from '../src/kv.ts'
import { deferred, waitUntil } from './fixtures/async.ts'
import { feishu, startRuntime, type Host } from './fixtures/host.ts'

let h: Host
beforeEach(async () => { h = await startRuntime() })
afterEach(async () => { await h.stop(); vi.useRealTimers() })
function request(id: string, ticketId: string) {
  const report = h.runtime.store.tickets.get(ticketId)!.assignments.at(-1)!.report
  if (!report) throw new Error('Expected a preparation failure before the follow-up')
  const now = Math.max(Date.now(), Date.parse(report.at) + 1)
  vi.useRealTimers(); vi.useFakeTimers({ toFake: ['Date'], now })
  h.runtime.receive(feishu(id, '请重试准备，保留已有文件', { root_id: 'om_1', mentions: [] }))
}
async function followup(id: string, ticketId: string) { request(id, ticketId); await h.runtime.idle() }

function failingPreparation(nonempty: boolean, failures = 1) {
  const settings = h.runtime.store.settings()
  h.runtime.store.saveSettings({ ...settings, executor: { ...settings.executor, prepare: 'prepare checkout' } })
  const spawn = h.subprocess.spawn.getMockImplementation()!
  let attempts = 0
  h.subprocess.spawn.mockImplementation(spec => {
    const handle = spawn(spec)
    if (spec.argv[0] !== 'sh') return handle
    const directory = spec.env?.DSH_SUPERAGENT_DIR
    if (!directory) throw new Error('Prepare is missing its ticket directory')
    attempts++
    mkdirSync(directory, { recursive: true })
    if (nonempty && attempts === 1) writeFileSync(join(directory, 'partial.txt'), 'work left by the failed command\n')
    const exitCode = attempts <= failures ? 1 : 0
    return { ...handle, done: handle.done.then(() => ({ exitCode, signal: null })) }
  })
  return () => attempts
}

async function pendingPreparation() {
  const settings = h.runtime.store.settings(), command = deferred()
  h.runtime.store.saveSettings({ ...settings, executor: { ...settings.executor, prepare: 'prepare checkout' } })
  h.hooks.prepare = command.promise
  const trigger = feishu('1', '@bot 修一')
  h.runtime.receive(trigger); await h.runtime.idle()
  await h.route({ kind: 'accept', trigger_id: trigger.id, title: 'fix', worktree_name: 'fix-one', acknowledgement: '收到。' })
  await waitUntil(() => h.subprocess.spawn.mock.calls.some(([spec]) => spec.argv[0] === 'sh'))
  return { command, ticket: h.runtime.store.tickets.list()[0]! }
}

async function recoverUnfinished(ticketId: string, command: { resolve(): void }) {
  const key = KV.executor.preparation(ticketId), started = h.runtime.store.get(key)!
  expect(started.state).toBe('started')
  const stopped = h.runtime.stop()
  command.resolve(); await stopped
  // Restore the actual pre-shutdown record, as a crash would leave it, without abandoning a running test process.
  const snapshot = new Store(join(h.directory, 'orchestrator.sqlite'))
  try { snapshot.set(key, started) } finally { snapshot.close() }
  h.hooks.prepare = undefined
  return h.restart()
}

it('records a preparation in progress before its command can finish', async () => {
  const one = await pendingPreparation()
  try {
    expect(h.runtime.store.get(KV.executor.preparation(one.ticket.id))).toMatchObject({ directory: one.ticket.execution!.directory, state: 'started' })
    expect(h.agents.has(`worker-${one.ticket.id}`)).toBe(false)
  } finally { one.command.resolve(); await h.runtime.idle() }
})

it('treats a recovered started preparation as unknown until an explicit retry', async () => {
  const one = await pendingPreparation(), directory = one.ticket.execution!.directory
  mkdirSync(directory, { recursive: true }); writeFileSync(join(directory, 'partial.txt'), 'unfinished work\n')
  const runs = () => h.subprocess.spawn.mock.calls.filter(([spec]) => spec.argv[0] === 'sh').length
  try {
    const next = await recoverUnfinished(one.ticket.id, one.command); await next.idle()
    expect(runs()).toBe(1)
    const failed = next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.report
    expect(failed).toMatchObject({ state: 'failed', summary: expect.stringContaining('unknown') })
    expect(next.store.get(KV.executor.preparation(one.ticket.id))!.state).toBe('started')
    next.executor.resume(); await next.pump(); await next.idle()
    expect(runs()).toBe(1)
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.report).toEqual(failed)
    next.engine.retryTicket(one.ticket.id); await next.idle()
    expect(runs()).toBe(2)
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
    expect(readFileSync(join(directory, 'partial.txt'), 'utf8')).toBe('unfinished work\n')
  } finally { one.command.resolve(); await h.runtime.idle() }
})

it('does not reuse the human request that already started an interrupted attempt on the same Assignment', async () => {
  const attempts = failingPreparation(true), command = deferred()
  const one = await h.acceptedTicket(feishu('1', '@bot 修一')), assignment = one.ticket.assignments.at(-1)!.id
  h.hooks.prepare = command.promise
  try {
    request('2', one.ticket.id)
    await waitUntil(() => attempts() === 2)
    const next = await recoverUnfinished(one.ticket.id, command); await next.idle()
    expect(attempts()).toBe(2)
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)).toMatchObject({ id: assignment, state: 'pending', report: { state: 'failed', summary: expect.stringContaining('unknown') } })
    await followup('3', one.ticket.id)
    expect(attempts()).toBe(3)
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)).toMatchObject({ id: assignment, state: 'running' })
  } finally { command.resolve(); await h.runtime.idle() }
})

async function unconfirmedPreparation() {
  const deadline = new AbortController(), cleanup = new AbortController(), timeout = AbortSignal.timeout.bind(AbortSignal)
  const timer = vi.spyOn(AbortSignal, 'timeout').mockImplementation(ms => ms === 10 * 60_000 ? deadline.signal : ms === 10_000 ? cleanup.signal : timeout(ms))
  const settings = h.runtime.store.settings()
  h.runtime.store.saveSettings({ ...settings, executor: { ...settings.executor, maxWorkers: 1 } })
  const range = deferred(); h.hooks.prepareExit = range.promise
  const one = await pendingPreparation()
  try {
    const result = h.subprocess.spawn.mock.results[h.subprocess.spawn.mock.calls.findIndex(([spec]) => spec.argv[0] === 'sh')]!
    if (result.type !== 'return') throw new Error('Prepare did not spawn')
    deadline.abort(); one.command.resolve()
    await waitUntil(() => result.value.waitForExit.mock.calls.some(([signal]) => signal === cleanup.signal))
    cleanup.abort(); await h.runtime.idle()
    expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.report).toMatchObject({ state: 'failed', summary: expect.stringContaining('Could not confirm') })
    return { ...one, range, handle: result.value, timeout }
  } catch (error) { one.command.resolve(); range.resolve(); throw error }
  finally { timer.mockRestore(); h.hooks.prepare = undefined; h.hooks.prepareExit = undefined }
}
const runs = () => h.subprocess.spawn.mock.calls.filter(([spec]) => spec.argv[0] === 'sh').length

it('does not let a Runtime reload forget a preparation whose range exit was unconfirmed', async () => {
  const one = await unconfirmedPreparation()
  try {
    const next = await h.restart(); await next.idle()
    const recovery = new AbortController(), checks = one.handle.waitForExit.mock.calls.length
    const observation = vi.spyOn(AbortSignal, 'timeout').mockImplementation(ms => ms === 10_000 ? recovery.signal : one.timeout(ms))
    next.engine.retryTicket(one.ticket.id)
    const idle = next.idle()
    await waitUntil(() => one.handle.waitForExit.mock.calls.length > checks)
    recovery.abort(); await idle; observation.mockRestore()
    expect(runs()).toBe(1)
    expect(h.agents.has(`worker-${one.ticket.id}`)).toBe(false)
    expect(next.store.get(KV.executor.preparation(one.ticket.id))!.state).toBe('unsettled')
    expect(next.health.worker).toMatchObject({ state: 'error', detail: expect.stringContaining('Could not confirm') })
    next.engine.stopTicket(one.ticket.id)
    await h.acceptedTicket(feishu('2', '@bot 修二'))
    expect(runs()).toBe(1)
  } finally { one.command.resolve(); one.range.resolve(); await h.runtime.idle() }
})

it('can retry after a reload once the retained provider handle confirms the old range exited', async () => {
  const one = await unconfirmedPreparation()
  try {
    one.range.resolve()
    expect(await one.handle.waitForExit(AbortSignal.timeout(1000))).toBe(true)
    const next = await h.restart()
    next.engine.retryTicket(one.ticket.id); await next.pump(); await next.idle()
    expect(runs()).toBe(2)
    expect(next.store.get(KV.executor.preparation(one.ticket.id))!.state).toBe('ready')
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
    expect(next.health.worker?.state).not.toBe('error')
  } finally { one.command.resolve(); one.range.resolve(); await h.runtime.idle() }
})

it('can confirm an old range while paused without starting the requested preparation', async () => {
  const one = await unconfirmedPreparation()
  try {
    one.range.resolve()
    await h.runtime.settings.pause(true, 'panel')
    h.runtime.engine.retryTicket(one.ticket.id); await h.runtime.idle()
    expect(runs()).toBe(1)
    expect(h.runtime.store.get(KV.executor.preparation(one.ticket.id))!.state).toBe('failed')
    expect(h.agents.has(`worker-${one.ticket.id}`)).toBe(false)
    await h.runtime.settings.pause(false, 'panel'); await h.runtime.idle()
    expect(runs()).toBe(2)
    expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
  } finally { one.range.resolve(); await h.runtime.idle() }
})

it('keeps an unconfirmed range blocked when the provider has lost its handle', async () => {
  const one = await unconfirmedPreparation()
  try {
    one.range.resolve()
    const next = await h.restart({ subprocess: { ...h.subprocess } })
    next.engine.retryTicket(one.ticket.id); await next.idle()
    expect(runs()).toBe(1)
    expect(next.store.get(KV.executor.preparation(one.ticket.id))!.state).toBe('unsettled')
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.report).toMatchObject({ state: 'failed', summary: expect.stringContaining('no longer has this preparation handle') })
    expect(next.health.worker?.state).toBe('error')
  } finally { one.range.resolve(); await h.runtime.idle() }
})

it('does not let a late observation failure consume a newer human recovery request', async () => {
  const one = await unconfirmedPreparation(), oldCheck = deferred()
  const wait = one.handle.waitForExit.getMockImplementation()!
  let checks = 0
  one.handle.waitForExit.mockImplementation(async signal => {
    if (++checks === 1) { await oldCheck.promise; throw new Error('old observation failed') }
    return wait(signal)
  })
  try {
    const next = await h.restart()
    next.engine.retryTicket(one.ticket.id)
    await waitUntil(() => checks === 1)
    const trigger = feishu('2', '请重新检查退出后继续准备', { root_id: 'om_1', mentions: [] })
    next.receive(trigger); await next.pump()
    const input = next.store.tickets.get(one.ticket.id)!.inputs.find(item => item.kind === 'trigger' && item.triggerId === trigger.id)!
    oldCheck.resolve()
    await waitUntil(() => checks === 2)
    expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.report).toBeUndefined()
    expect(runs()).toBe(1)
    one.range.resolve(); await next.idle()
    expect(runs()).toBe(2)
    expect(next.store.get(KV.executor.preparation(one.ticket.id))).toMatchObject({ state: 'ready', inputId: input.id })
  } finally { oldCheck.resolve(); one.range.resolve(); await h.runtime.idle() }
})

it('releases a confirmed range without relaunching a Ticket stopped during observation', async () => {
  const one = await unconfirmedPreparation()
  try {
    const next = await h.restart(), checks = one.handle.waitForExit.mock.calls.length
    next.engine.retryTicket(one.ticket.id)
    await waitUntil(() => one.handle.waitForExit.mock.calls.length > checks)
    next.engine.stopTicket(one.ticket.id)
    one.range.resolve(); await next.idle()
    expect(runs()).toBe(1)
    expect(next.store.tickets.get(one.ticket.id)).toMatchObject({ ended: { by: 'human' } })
    expect(h.agents.has(`worker-${one.ticket.id}`)).toBe(false)
    expect(next.health.worker?.state).not.toBe('error')
  } finally { one.range.resolve(); await h.runtime.idle() }
})

it('cancels an exit observation on shutdown without claiming that the range exited', async () => {
  const one = await unconfirmedPreparation()
  try {
    const next = await h.restart(), checks = one.handle.waitForExit.mock.calls.length
    next.engine.retryTicket(one.ticket.id)
    await waitUntil(() => one.handle.waitForExit.mock.calls.length > checks)
    await next.stop()
    const saved = new Store(join(h.directory, 'orchestrator.sqlite'))
    try { expect(saved.get(KV.executor.preparation(one.ticket.id))!.state).toBe('unsettled') }
    finally { saved.close() }
    expect(runs()).toBe(1)
  } finally { one.range.resolve() }
})

it.each([0, 1])('does not overwrite a newer preparation record with a late command result (exit: %s)', async exitCode => {
  const spawn = h.subprocess.spawn.getMockImplementation()!
  h.subprocess.spawn.mockImplementation(spec => {
    const handle = spawn(spec)
    return spec.argv[0] === 'sh' ? { ...handle, done: handle.done.then(() => ({ exitCode, signal: null })) } : handle
  })
  const one = await pendingPreparation(), key = KV.executor.preparation(one.ticket.id)
  const newer = { ...h.runtime.store.get(key)!, attemptId: 'newer-attempt', state: 'ready' as const }
  // Exercise the KV ownership guard independently of the Engine's normal launch serialization.
  h.runtime.store.set(key, newer)
  try {
    one.command.resolve(); await h.runtime.idle()
    expect(h.runtime.store.get(key)).toEqual(newer)
  } finally { one.command.resolve(); await h.runtime.idle() }
})

it.each([false, true])('runs preparation again after a failed command on explicit retry (nonempty: %s)', async nonempty => {
  const attempts = failingPreparation(nonempty)
  const one = await h.acceptedTicket(feishu('1', '@bot 修一'))
  const directory = one.ticket.execution!.directory
  expect(one.ticket.assignments.at(-1)!.report).toMatchObject({ state: 'failed' })
  expect(attempts()).toBe(1)
  expect(existsSync(directory)).toBe(nonempty)
  expect(h.agents.has(one.sessionId)).toBe(false)
  h.runtime.engine.retryTicket(one.ticket.id); await h.runtime.idle()
  expect(attempts()).toBe(2)
  expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
  if (nonempty) expect(readFileSync(join(directory, 'partial.txt'), 'utf8')).toBe('work left by the failed command\n')
})

it('does not let a stale launch effect repeat a failed prepare without a new request', async () => {
  const attempts = failingPreparation(true)
  const one = await h.acceptedTicket(feishu('1', '@bot 修一'))
  const assignment = one.ticket.assignments.at(-1)!, report = assignment.report
  h.runtime.effects.enqueue('launch', assignment.id, { ticketId: one.ticket.id, assignmentId: assignment.id })
  await h.runtime.idle()
  expect(attempts()).toBe(1)
  expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.report).toEqual(report)
  expect(h.agents.has(one.sessionId)).toBe(false)
})

it('lets each new human request retry preparation on the same pending Assignment', async () => {
  const attempts = failingPreparation(true, 2)
  const one = await h.acceptedTicket(feishu('1', '@bot 修一'))
  const assignment = one.ticket.assignments.at(-1)!.id
  await followup('2', one.ticket.id)
  expect(attempts()).toBe(2)
  expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)).toMatchObject({ id: assignment, state: 'pending', report: { state: 'failed' } })
  await followup('3', one.ticket.id)
  expect(attempts()).toBe(3)
  expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)).toMatchObject({ id: assignment, state: 'running' })
  expect(readFileSync(join(one.ticket.execution!.directory, 'partial.txt'), 'utf8')).toBe('work left by the failed command\n')
})

it('reuses a successful preparation across a restart and an explicit retry', async () => {
  const attempts = failingPreparation(false, 0)
  const one = await h.acceptedTicket(feishu('1', '@bot 修一'))
  const ready = h.runtime.store.get(KV.executor.preparation(one.ticket.id))
  expect(ready?.state).toBe('ready')
  await h.runTool(one.sessionId, 'orchestrator_report', { state: 'failed', summary: 'worker needs a retry' })
  const next = await h.restart()
  next.engine.retryTicket(one.ticket.id); await next.idle()
  expect(attempts()).toBe(1)
  expect(next.store.get(KV.executor.preparation(one.ticket.id))).toEqual(ready)
  expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
})

it('keeps an existing legacy workspace with no preparation record usable', async () => {
  const one = await h.acceptedTicket(feishu('1', '@bot 修一')), directory = one.ticket.execution!.directory
  writeFileSync(join(directory, 'legacy.txt'), 'existing work\n')
  await h.runTool(one.sessionId, 'orchestrator_report', { state: 'failed', summary: 'worker needs a retry' })
  h.runtime.store.delete(KV.executor.preparation(one.ticket.id))
  const attempts = failingPreparation(true)
  h.runtime.engine.retryTicket(one.ticket.id); await h.runtime.idle()
  expect(attempts()).toBe(0)
  expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
  expect(readFileSync(join(directory, 'legacy.txt'), 'utf8')).toBe('existing work\n')
})

it('records ready only after directory verification succeeds', async () => {
  const attempts = failingPreparation(true, 0), spawn = h.subprocess.spawn.getMockImplementation()!
  let checks = 0
  h.subprocess.spawn.mockImplementation(spec => {
    const handle = spawn(spec)
    if (spec.argv.includes('--show-scope') && spec.cwd.startsWith(join(h.directory, 'work')) && checks++ === 0) {
      const text = 'local\0credential.helper\ncustom-helper\0'
      return { ...handle, collected: { ...handle.collected, stdout: { readFrom: () => ({ text, nextOffset: text.length, lossy: false }) } } }
    }
    return handle
  })
  const one = await h.acceptedTicket(feishu('1', '@bot 修一'))
  expect(h.runtime.store.get(KV.executor.preparation(one.ticket.id))).toMatchObject({ state: 'failed', error: expect.stringContaining('credential.helper') })
  expect(h.agents.has(one.sessionId)).toBe(false)
  h.runtime.engine.retryTicket(one.ticket.id); await h.runtime.idle()
  expect(attempts()).toBe(2)
  expect(h.runtime.store.get(KV.executor.preparation(one.ticket.id))!.state).toBe('ready')
  expect(h.runtime.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
})

it('keeps the failed outcome across a Runtime restart and retries only on a person\'s request', async () => {
  const attempts = failingPreparation(true)
  const one = await h.acceptedTicket(feishu('1', '@bot 修一'))
  const saved = h.runtime.store.get(KV.executor.preparation(one.ticket.id))
  const next = await h.restart(); await next.idle()
  expect(attempts()).toBe(1)
  expect(next.store.get(KV.executor.preparation(one.ticket.id))).toEqual(saved)
  expect(h.agents.has(one.sessionId)).toBe(false)
  next.engine.retryTicket(one.ticket.id); await next.idle()
  expect(attempts()).toBe(2)
  expect(next.store.get(KV.executor.preparation(one.ticket.id))).toMatchObject({ directory: one.ticket.execution!.directory, state: 'ready' })
  expect(next.store.tickets.get(one.ticket.id)!.assignments.at(-1)!.state).toBe('running')
})
