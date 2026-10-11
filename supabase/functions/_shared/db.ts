/**
 * Index tables and checkpoint persistence.
 *
 * Uses the service-role key, which bypasses RLS. This code runs **only** in the
 * trusted indexer function: the key must never be shipped to a client, embedded
 * in a response, or written to a log.
 *
 * RLS remains enabled on these tables and grants browser roles no access, so a
 * leaked anon key cannot read or write index state. The service role is not a
 * substitute for those protections — it is an additional, server-only path.
 */

import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import type { Checkpoint } from './checkpoint.ts';
import type { NewGroup } from './discovery.ts';
import type { IngestPlan } from './ingest.ts';
import type { GroupFacts, GroupState, StoredGroupState } from './state.ts';

type MutableFacts = {
  members: { position: number }[];
  contributions: { round: number; amount: string }[];
  payouts: { round: number; recipient_amount: string }[];
  fees: { round: number; fee: string }[];
  started: boolean;
  completed: boolean;
  lastEventLedger: number;
};

export type IndexedEventRow = {
  event_identity: string;
  ledger: number;
  tx_hash: string;
  tx_index: number;
  event_index: number;
  contract_id: string;
  topic: string[];
  value: string;
};

export type RejectedEventRow = {
  event_id: string;
  ledger: number;
  tx_hash: string;
  tx_index: number;
  event_index: number;
  contract_id: string;
  reason: string;
};

/** Stable unique ordering keys used before #selectIn applies offset pagination. */
const SELECT_IN_ORDER_COLUMNS: Record<string, readonly string[]> = {
  groups: ['contract_id'],
  group_members: ['contract_id', 'member'],
  contributions: ['contract_id', 'event_identity'],
  payouts: ['contract_id', 'event_identity'],
  protocol_fees: ['contract_id', 'event_identity'],
  decoded_events: ['contract_id', 'event_identity'],
};

export class IndexerDb {
  #client: SupabaseClient;

  constructor(supabaseUrl: string, serviceRoleKey: string, client?: SupabaseClient) {
    this.#client = client ??
      createClient(supabaseUrl, serviceRoleKey, {
        auth: { persistSession: false, autoRefreshToken: false },
      });
  }

  async getCheckpoint(): Promise<Checkpoint | undefined> {
    const { data, error } = await this.#client
      .from('indexer_checkpoints')
      .select('last_processed_ledger, start_ledger, updated_at')
      .eq('id', 'default')
      .maybeSingle();

    if (error) throw new Error(`Failed to read indexer checkpoint: ${error.message}`);
    if (!data) return undefined;

    return {
      lastProcessedLedger: Number(data.last_processed_ledger),
      startLedger: Number(data.start_ledger),
      updatedAt: String(data.updated_at),
    };
  }

  async upsertEvents(rows: readonly IndexedEventRow[]): Promise<void> {
    if (rows.length === 0) return;
    const { error } = await this.#client
      .from('indexed_events')
      .upsert([...rows], { onConflict: 'event_identity', ignoreDuplicates: true });
    if (error) throw new Error(`Failed to upsert indexed events: ${error.message}`);
  }

  async recordRejectedEvents(
    correlationId: string,
    rows: readonly RejectedEventRow[],
  ): Promise<void> {
    if (rows.length === 0) return;
    const { error } = await this.#client
      .from('indexer_rejected_events')
      .upsert(
        rows.map((row) => ({ ...row, reason: row.reason.slice(0, 500), correlation_id: correlationId })),
        { onConflict: 'event_id', ignoreDuplicates: true },
      );
    if (error) throw new Error(`Failed to persist rejected events: ${error.message}`);
  }

  async listGroupContractIds(pageSize = 1000): Promise<string[]> {
    const contractIds: string[] = [];
    let from = 0;
    while (true) {
      const to = from + pageSize - 1;
      const selectQuery = this.#client.from('groups').select('contract_id');
      const orderedQuery = typeof (selectQuery as { order?: unknown }).order === 'function'
        ? (selectQuery as { order: (col: string, opts?: unknown) => typeof selectQuery }).order(
          'contract_id',
          { ascending: true },
        )
        : selectQuery;
      const { data, error } = await orderedQuery.range(from, to);
      if (error) throw new Error(`Failed to read indexed group contracts: ${error.message}`);
      if (!data || data.length === 0) break;
      for (const row of data) contractIds.push(String(row.contract_id));
      if (data.length < pageSize) break;
      from += pageSize;
    }
    return contractIds;
  }

  async upsertGroups(rows: readonly NewGroup[]): Promise<void> {
    if (rows.length === 0) return;
    const { error } = await this.#client
      .from('groups')
      .upsert([...rows], { onConflict: 'contract_id', ignoreDuplicates: true });
    if (error) throw new Error(`Failed to upsert groups: ${error.message}`);
  }

  async persistPlan(plan: IngestPlan): Promise<void> {
    await this.#insertIgnoringDuplicates('decoded_events', plan.decoded, 'event_identity');
    await this.#insertIgnoringDuplicates('group_members', plan.members, 'contract_id,member');
    await this.#insertIgnoringDuplicates('contributions', plan.contributions, 'event_identity');
    await this.#insertIgnoringDuplicates('payouts', plan.payouts, 'event_identity');
    await this.#insertIgnoringDuplicates('protocol_fees', plan.fees, 'event_identity');
  }

  async #insertIgnoringDuplicates(table: string, rows: readonly object[], onConflict: string): Promise<void> {
    if (rows.length === 0) return;
    const { error } = await this.#client
      .from(table)
      .upsert([...rows], { onConflict, ignoreDuplicates: true });
    if (error) throw new Error(`Failed to record ${table}: ${error.message}`);
  }

  async readGroupFacts(
    contractIds: readonly string[],
    pageSize = 1000,
  ): Promise<Map<string, GroupFacts>> {
    const facts = new Map<string, GroupFacts>();
    if (contractIds.length === 0) return facts;
    const ids = [...contractIds];

    const forGroup = (contractId: string): MutableFacts => {
      let existing = facts.get(contractId) as MutableFacts | undefined;
      if (existing === undefined) {
        existing = {
          members: [], contributions: [], payouts: [], fees: [],
          started: false, completed: false, lastEventLedger: 0,
        };
        facts.set(contractId, existing);
      }
      return existing;
    };

    const rows = await Promise.all([
      this.#selectIn('group_members', 'contract_id, position', ids, pageSize),
      this.#selectIn('contributions', 'contract_id, round, amount::text', ids, pageSize),
      this.#selectIn('payouts', 'contract_id, round, recipient_amount::text', ids, pageSize),
      this.#selectIn('protocol_fees', 'contract_id, round, fee::text', ids, pageSize),
      this.#selectIn('decoded_events', 'contract_id, name, ledger', ids, pageSize),
    ]);

    const [members, contributions, payouts, fees, events] = rows as [
      Record<string, unknown>[], Record<string, unknown>[], Record<string, unknown>[],
      Record<string, unknown>[], Record<string, unknown>[],
    ];

    for (const row of members) {
      forGroup(String(row['contract_id'])).members.push({ position: Number(row['position']) });
    }
    for (const row of contributions) {
      forGroup(String(row['contract_id'])).contributions.push({
        round: Number(row['round']), amount: String(row['amount']),
      });
    }
    for (const row of payouts) {
      forGroup(String(row['contract_id'])).payouts.push({
        round: Number(row['round']), recipient_amount: String(row['recipient_amount']),
      });
    }
    for (const row of fees) {
      forGroup(String(row['contract_id'])).fees.push({
        round: Number(row['round']), fee: String(row['fee']),
      });
    }
    for (const row of events) {
      const group = forGroup(String(row['contract_id']));
      const name = String(row['name']);
      const ledger = Number(row['ledger']);
      if (name === 'start') group.started = true;
      if (name === 'completed') group.completed = true;
      if (ledger > group.lastEventLedger) group.lastEventLedger = ledger;
    }
    return facts;
  }

  async readDerivedGroupState(contractIds: readonly string[]): Promise<GroupState[]> {
    if (contractIds.length === 0) return [];
    const { data, error } = await this.#client.rpc('derive_group_state', {
      p_contract_ids: [...contractIds],
    });
    if (error) throw new Error(`Failed to derive group state: ${error.message}`);
    return ((data ?? []) as Record<string, unknown>[]).map((row) => ({
      contract_id: String(row['contract_id']),
      status: (row['completed'] ? 'completed' : row['started'] ? 'active' : 'open') as GroupState['status'],
      member_count: Number(row['member_count']),
      current_round: Number(row['current_round']),
      completed_rounds: Number(row['completed_rounds']),
      contributed_total: String(row['contributed_total']),
      paid_out_total: String(row['paid_out_total']),
      fee_total: String(row['fee_total']),
      last_event_ledger: Number(row['last_event_ledger']),
    }));
  }

  async readGroupState(
    contractIds: readonly string[],
    pageSize = 1000,
  ): Promise<Map<string, StoredGroupState>> {
    const states = new Map<string, StoredGroupState>();
    if (contractIds.length === 0) return states;
    const rows = await this.#selectIn(
      'groups',
      'contract_id, status, member_count, current_round, completed_rounds, ' +
        'contributed_total::text, paid_out_total::text, fee_total::text, last_event_ledger',
      contractIds,
      pageSize,
    );
    for (const row of rows) {
      states.set(String(row['contract_id']), {
        status: String(row['status']) as StoredGroupState['status'],
        member_count: Number(row['member_count']), current_round: Number(row['current_round']),
        completed_rounds: Number(row['completed_rounds']), contributed_total: String(row['contributed_total']),
        paid_out_total: String(row['paid_out_total']), fee_total: String(row['fee_total']),
        last_event_ledger: Number(row['last_event_ledger']),
      });
    }
    return states;
  }

  async upsertGroupState(states: readonly GroupState[]): Promise<void> {
    if (states.length === 0) return;
    const updatedAt = new Date().toISOString();
    for (const state of states) {
      const { contract_id: contractId, ...figures } = state;
      const { error, count } = await this.#client
        .from('groups').update({ ...figures, updated_at: updatedAt }, { count: 'exact' })
        .eq('contract_id', contractId);
      if (error) throw new Error(`Failed to record group state: ${error.message}`);
      if (count === 0) {
        throw new Error(
          `Failed to record group state: no groups row for ${contractId}. ` +
            'Reconciliation updates existing groups; it never creates them.',
        );
      }
    }
  }

  async #selectIn(
    table: string,
    columns: string,
    contractIds: readonly string[],
    pageSize = 1000,
  ): Promise<Record<string, unknown>[]> {
    if (contractIds.length === 0) return [];
    if (pageSize <= 0) throw new Error(`pageSize must be positive, got ${pageSize}`);

    const orderColumns = SELECT_IN_ORDER_COLUMNS[table];
    if (!orderColumns) throw new Error(`Failed to read ${table}: no stable ordering key configured`);

    const CONTRACT_CHUNK_SIZE = 100;
    const allRows: Record<string, unknown>[] = [];

    for (let c = 0; c < contractIds.length; c += CONTRACT_CHUNK_SIZE) {
      const chunk = contractIds.slice(c, c + CONTRACT_CHUNK_SIZE);
      let from = 0;

      while (true) {
        const to = from + pageSize - 1;
        const selectQuery = this.#client.from(table).select(columns).in('contract_id', chunk);
        let orderedQuery = selectQuery as typeof selectQuery & { order?: Function };

        if (typeof orderedQuery.order === 'function') {
          for (const column of orderColumns) {
            orderedQuery = orderedQuery.order(column, { ascending: true });
          }
        }

        const pagedQuery = typeof (orderedQuery as { range?: unknown }).range === 'function'
          ? (orderedQuery as { range: (from: number, to: number) => typeof orderedQuery }).range(from, to)
          : orderedQuery;

        const { data, error } = await pagedQuery;
        if (error) throw new Error(`Failed to read ${table}: ${error.message}`);

        const rows = (data ?? []) as unknown as Record<string, unknown>[];
        for (const row of rows) allRows.push(row);

        if (typeof (orderedQuery as { range?: unknown }).range !== 'function') {
          if (rows.length >= pageSize) {
            throw new Error(
              `Failed to read ${table}: returned ${rows.length} rows at server cap without pagination support`,
            );
          }
          break;
        }
        if (rows.length < pageSize) break;
        from += pageSize;
      }
    }
    return allRows;
  }

  async advanceCheckpoint(params: {
    lastProcessedLedger: number;
    startLedger: number;
    lastSeenLatestLedger?: number;
  }): Promise<void> {
    const { data, error } = await this.#client
      .from('indexer_checkpoints')
      .update({
        last_processed_ledger: params.lastProcessedLedger,
        start_ledger: params.startLedger,
        updated_at: new Date().toISOString(),
      })
      .eq('id', 'default')
      .lt('last_processed_ledger', params.lastProcessedLedger)
      .select('id');
    if (error) throw new Error(`Failed to advance indexer checkpoint: ${error.message}`);
    if (data && data.length > 0) return;

    const { error: insertError } = await this.#client
      .from('indexer_checkpoints')
      .upsert(
        {
          id: 'default', last_processed_ledger: params.lastProcessedLedger,
          start_ledger: params.startLedger, updated_at: new Date().toISOString(),
          ...(params.lastSeenLatestLedger !== undefined
            ? { last_seen_latest_ledger: params.lastSeenLatestLedger } : {}),
        },
        { onConflict: 'id', ignoreDuplicates: true },
      );
    if (insertError) throw new Error(`Failed to advance indexer checkpoint: ${insertError.message}`);

    const { error: recheckError } = await this.#client
      .from('indexer_checkpoints')
      .update({
        last_processed_ledger: params.lastProcessedLedger,
        start_ledger: params.startLedger,
        updated_at: new Date().toISOString(),
      })
      .eq('id', 'default')
      .lt('last_processed_ledger', params.lastProcessedLedger);
    if (recheckError) throw new Error(`Failed to advance indexer checkpoint: ${recheckError.message}`);
  }

  async recordLatestLedger(latestLedger: number): Promise<void> {
    const { error } = await this.#client
      .from('indexer_checkpoints')
      .update({ last_seen_latest_ledger: latestLedger, updated_at: new Date().toISOString() })
      .eq('id', 'default');
    if (error) throw new Error(`Failed to record latest ledger: ${error.message}`);
  }

  async recordRunFailure(params: {
    correlationId: string;
    ledgerFrom: number;
    ledgerTo: number;
    reason: string;
  }): Promise<void> {
    try {
      const { error } = await this.#client.from('indexer_runs').insert({
        correlation_id: params.correlationId,
        ledger_from: params.ledgerFrom,
        ledger_to: params.ledgerTo,
        status: 'failed',
        reason: params.reason.slice(0, 500),
      });
      if (error) {
        console.error(JSON.stringify({
          level: 'error', message: 'Failed to record indexer run failure',
          correlationId: params.correlationId, error: error.message,
        }));
      }
    } catch (err) {
      console.error(JSON.stringify({
        level: 'error', message: 'Exception recording indexer run failure',
        correlationId: params.correlationId,
        error: err instanceof Error ? err.message : String(err),
      }));
    }
  }
}
