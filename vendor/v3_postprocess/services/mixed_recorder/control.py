"""Durable shared lifecycle; emits desired feeds, never invokes market APIs.

Adapter evidence is mandatory at readiness/release boundaries. This component
does not itself certify receipt bodies or run a native receive loop.
"""
from copy import deepcopy
from functools import wraps
from task_control.contracts import require, integer, digest, evidence, seal,verified
from task5.selection import native_terminal
from .planner import limits, plan, validate_offer,venue_types,task_types,_validated_footprint,discovery_targets
from .shared_feeds import merge


def transaction(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        try:return method(self, *args, **kwargs)
        except BaseException:
            # Rename may have committed before a lost caller reply. Reload the
            # actual chain, never retain a partially mutated in-memory session.
            self.state = deepcopy(self.store.reload())
            raise
    return run


class Coordinator:
    def __init__(self, store, capacity, *, at_ns, segment_seconds=600,collect_trades=True):
        self.store = store; self.capacity = limits(capacity)
        require(type(collect_trades) is bool,'boolean_trade_scope_required')
        require(not self.capacity.get('shared_polymarket') or not collect_trades,'shared_feeds_books_only')
        integer(segment_seconds, 'positive_segment_duration_required', 1)
        require(segment_seconds<=3600,'bounded_native_segment_duration_required')
        self.segment_duration_ns=segment_seconds*10**9
        if store.last is None:
            self.state = dict(schema='mixed-lifecycle-state/1', capacity_hash=digest(self.capacity),
                              sessions={}, terminal_members={}, admissions_paused=False,tail_reservations={},
                              segment_duration_ns=self.segment_duration_ns,collect_trades=collect_trades)
            self._save('initialized', at_ns)
        else:
            self.state = deepcopy(store.state)
            require(self.state['capacity_hash'] == digest(self.capacity), 'capacity_change_requires_explicit_migration')
            require(self.state.get('segment_duration_ns')==self.segment_duration_ns,'segment_policy_change_requires_explicit_migration')
            require(self.state.get('collect_trades',True)==collect_trades,'collection_scope_change_requires_explicit_migration')

    def _save(self, event, at_ns):
        revision = self.store.last['revision'] if self.store.last else 0
        self.store.commit(self.state, event=event, at_ns=at_ns, expected_revision=revision)

    def discovery_status(self,*,baseline=False):
        """Read current committed reservations, including unreleased and tails."""
        sessions=self.state['sessions'];tasks=task_types(self.capacity);venues=venue_types(self.capacity)
        reserved=[s['offer'] for s in sessions.values() if s['lifecycle']!='closed' or not s['feeds_released']]
        usage=_validated_footprint(reserved,tasks=tasks,venues=venues,shared_polymarket=self.capacity.get('shared_polymarket',False))
        for sid,row in self.state.get('tail_reservations',{}).items():
            if sessions[sid]['feeds_released']:
                for member,rpm in row.items():usage['request_rpm_by_venue'][member.split(':',1)[0]]+=rpm
        counts={t:sum(s['task_type']==t and s['lifecycle']!='closed' for s in sessions.values()) for t in tasks}
        targets=discovery_targets(self.capacity,counts,baseline=baseline,usage=usage)
        if self.state['admissions_paused']:targets={t:0 for t in targets}
        return dict(usage=usage,counts=counts,targets=targets,can_discover=any(targets.values()))

    @transaction
    def admit(self, offers, *, at_ns):
        active = []; draining = []
        for session in self.state['sessions'].values():
            if session['lifecycle'] != 'closed':active.append(session['offer'])
            elif not session['feeds_released']:draining.append(session['offer'])
        eligible = []; blocked = []
        for row in offers:
            row = validate_offer(row)
            c = row['candidate']
            if any(c['observed_at_ns'] <= self.state['terminal_members'].get(m, -1) for m in c['members']):
                blocked.append(dict(offer_hash=row['contract_hash'], reason='post_terminal_filter_refresh_required'))
            else:eligible.append(row)
        tail={v:0 for v in venue_types(self.capacity)}
        for sid,row in self.state.get('tail_reservations',{}).items():
            if self.state['sessions'][sid]['feeds_released']:
                for member,rpm in row.items():tail[member.split(':',1)[0]]+=rpm
        result = plan(eligible, active=active, draining=draining, capacity=self.capacity,tail_rpm_by_venue=tail,
                      at_ns=at_ns, allow_new=not self.state['admissions_paused'])
        physical_owners={m:r['session_id'] for m,r in self.desired_feeds().items()} if self.capacity.get('shared_polymarket') else None
        for row in result['admitted']:
            gid = row['candidate']['group_id']
            ordinal = sum(s['group_id'] == gid for s in self.state['sessions'].values())
            sid = digest([gid, ordinal, at_ns])
            physical={}
            if physical_owners is not None:
                physical={m:physical_owners.get(m) or digest(['physical-feed',m,sid]) for m in row['candidate']['members']}
                physical_owners.update(physical)
            self.state['sessions'][sid] = dict(session_id=sid, group_id=gid, offer=row,
                task_type=row['candidate']['task_type'], end_policy='required_member_official_terminal',
                expires_after_24h=False, native_capture='all_received_events',
                admitted_at_ns=at_ns, actual_started_at_ns=None, lifecycle='warming',
                ready={}, reconnect_gaps=[], segments=[], closed_at_ns=None,
                terminal_evidence=None, feeds_released=False, release_receipt=None)
            if physical:self.state['sessions'][sid]['physical_sessions']=physical
        self._save(dict(admission_plan=result['contract_hash'], terminal_blocked=blocked), at_ns)
        return result

    @transaction
    def ready(self, session_id, member_receipts, *, at_ns, max_age_ns):
        self._ready(session_id,member_receipts,at_ns=at_ns,max_age_ns=max_age_ns)
        self._save(dict(session_ready=session_id), at_ns)

    @transaction
    def inherit_open(self,previous,refreshed,proof,*,terminal_members,at_ns):
        """Resume already-recorded open groups; never pretend an old filter is new."""
        proof=verified(proof,'native-cohort-continuation/1')
        require(not self.state['sessions'] and not self.state.get('cohort_continuation') and not self.state.get('collect_trades',True) and
            self.capacity.get('shared_polymarket') is True,'continuation_requires_empty_shared_books_epoch')
        require(proof['previous_sessions_sha256']==digest(previous) and set(refreshed)==set(previous) and
            proof['refreshed_offer_hashes']=={sid:r['contract_hash'] for sid,r in refreshed.items()},'continuation_session_binding_changed')
        require(proof['old_admission_clocks_preserved'] is True and proof['liquidity_reselection_performed'] is False,
            'continuation_cannot_invent_filter_freshness')
        require(type(proof['admissions_paused']) is bool,'continuation_admission_pause_required')
        source=verified(proof['source'],'native-cohort-continuation-source/1');boundary=source['uncertainty_start_ns']
        integer(boundary,'continuation_uncertainty_clock_required');require(boundary<=at_ns,'continuation_clock_regressed')
        sessions={}
        for old_sid,old in previous.items():
            row=validate_offer(refreshed[old_sid]);original=validate_offer(old['offer'])
            require(old['session_id']==old_sid and old['lifecycle']!='closed' and old['terminal_evidence'] is None and
                old['actual_started_at_ns'] is not None and old['actual_started_at_ns']<=at_ns,'continuation_open_recorded_session_required')
            require(row['candidate']==original['candidate'] and row['request_rpm_by_member']==original['request_rpm_by_member'] and
                {m['member']:m['instruments'] for m in row['markets']}=={m['member']:m['instruments'] for m in original['markets']},
                'continuation_cannot_replace_members_or_instruments')
            sid=digest(['continued-logical-session',old_sid,proof['contract_hash']]);session=deepcopy(old)
            members=row['candidate']['members'];start=max(boundary,old['actual_started_at_ns'])
            session.update(session_id=sid,offer=row,lifecycle='recovering',closed_at_ns=None,terminal_evidence=None,
                feeds_released=False,release_receipt=None,segments=[],
                continued_from=dict(session_id=old_sid,offer_hash=original['contract_hash'],continuation_hash=proof['contract_hash'],
                    previous_gaps_sha256=digest(old['reconnect_gaps']),previous_segments_sha256=digest(old['segments'])),
                physical_sessions={m:digest(['continued-physical-feed',m,proof['contract_hash']]) for m in members},
                reconnect_gaps=[dict(start_ns=start,end_ns=None,fresh_after_ns=at_ns,affected_members=list(members),
                    fresh_after_by_member={m:at_ns for m in members},reason='owned_service_epoch_continuation',
                    interval_is_uncertain_not_exact_loss=True)])
            session.pop('recording_end_reason',None);sessions[sid]=session
        usage=_validated_footprint(list(refreshed.values()),tasks=task_types(self.capacity),venues=venue_types(self.capacity),shared_polymarket=True)
        from .planner import group_total,group_caps
        require(sum(usage['groups_by_task'].values())<=group_total(self.capacity) and all(usage['groups_by_task'][t]<=group_caps(self.capacity)[t]
            for t in task_types(self.capacity)) and usage['physical_markets']<=self.capacity['physical_markets'] and
            usage['outcome_streams']<=self.capacity['outcome_streams'] and all(usage['markets_by_venue'][v]<=self.capacity['markets_by_venue'][v] and
            usage['request_rpm_by_venue'][v]<=self.capacity['request_rpm_by_venue'][v] for v in venue_types(self.capacity)),
            'continuation_exceeds_current_resource_caps')
        self.state.update(sessions=sessions,terminal_members=deepcopy(terminal_members),cohort_continuation=proof,
            admissions_paused=proof['admissions_paused'])
        self._save(dict(open_cohort_continued=proof['contract_hash'],sessions=len(sessions)),at_ns)

    @transaction
    def ready_many(self, receipts_by_session, *, at_ns, max_age_ns):
        """Validate one observed readiness cut, then persist one atomic batch.

        Freshness is checked at the decision clock, not after serial durable
        commits for unrelated groups. Any invalid member rolls back the batch.
        """
        require(isinstance(receipts_by_session,dict),'readiness_batch_required')
        for sid,receipts in receipts_by_session.items():
            self._ready(sid,receipts,at_ns=at_ns,max_age_ns=max_age_ns)
        if receipts_by_session:self._save(dict(sessions_ready=sorted(receipts_by_session)),at_ns)

    def _ready(self, session_id, member_receipts, *, at_ns, max_age_ns):
        s = self.state['sessions'][session_id]
        require(s['lifecycle'] in {'warming', 'recovering'}, 'session_not_waiting_readiness')
        integer(at_ns, 'invalid_ready_clock'); integer(max_age_ns, 'positive_ready_max_age', 1)
        members = s['offer']['candidate']['members']
        gap=s['reconnect_gaps'][-1] if s['lifecycle']=='recovering' else None
        affected=set(gap.get('affected_members',members)) if gap else set()
        require(set(member_receipts) == set(members), 'all_group_members_must_be_ready')
        for member,row in member_receipts.items():
            fields={'epoch', 'sequence', 'snapshot_ref', 'ack_ref', 'available_at_ns'}
            require(set(row) in (fields,fields|{'current_book'}), 'native_readiness_fields_required')
            require(isinstance(row['epoch'], str) and bool(row['epoch']), 'connection_epoch_required')
            integer(row['sequence'], 'durable_snapshot_sequence_required')
            integer(row['available_at_ns'], 'snapshot_clock_required')
            clock=row['available_at_ns']
            if 'current_book' in row:
                proof=verified(row['current_book'],'native-continuous-book-evidence/1')
                require(proof['fence'].get('schema') in ('native-prefix-fence/1','native-live-prefix-evidence/1'),'current_book_prefix_schema_required')
                fence=verified(proof['fence'],proof['fence']['schema'])
                if fence['schema']=='native-live-prefix-evidence/1':
                    require(fence['export_or_retirement_fence'] is False and fence['previously_verified_rows']+fence['new_rows_read']==fence['row_count'],
                        'live_prefix_evidence_role_changed')
                require(proof['member']==member and proof['epoch']==row['epoch'] and proof['initial_snapshot_ref']==row['snapshot_ref'] and
                    proof['basis_snapshot_at_ns']==row['available_at_ns'] and proof['basis_not_before_ns']<=row['available_at_ns'] and
                    proof['basis_sequence']<=proof['through_sequence']==row['sequence']==fence['through_sequence'] and
                    proof['basis_sequence']>>48==fence['generation'] and
                    (fence['generation']<<48)-1<=fence['after_sequence']<=fence['through_sequence']<(fence['generation']+1)<<48 and
                    fence['row_count']==fence['through_sequence']-fence['after_sequence'] and
                    fence['table']==('accept_poly_v3' if member.startswith('polymarket:') else 'accept_extra_v3'),
                    'current_book_evidence_binding_changed')
                clock=integer(proof['verified_at_ns'],'current_book_verification_clock_required')
                require(row['available_at_ns']<=clock<=at_ns and 0<=at_ns-fence['observed_at_ns']<=max_age_ns,
                    'current_book_evidence_stale')
            require(s['admitted_at_ns'] <= clock <= at_ns, 'snapshot_stale_or_before_admission')
            if gap and member not in affected:
                require(row['epoch']==s['ready'][member]['epoch'] and row['sequence']>=s['ready'][member]['sequence'],
                        'unaffected_member_epoch_change_requires_invalidation')
            else:
                require(at_ns-clock <= max_age_ns,'snapshot_stale_or_before_admission')
                if gap:
                    floor=gap.get('fresh_after_by_member',{}).get(member,gap.get('fresh_after_ns'))
                    require(floor is not None,'legacy_recovery_requires_new_boundary')
                    require(row['available_at_ns']>=floor,'recovery_requires_fresh_snapshot')
            evidence([row['snapshot_ref'], row['ack_ref']])
        if s['lifecycle'] == 'recovering':
            require(at_ns >= gap['start_ns'], 'recovery_clock_regressed')
            gap['end_ns'] = at_ns
        s['ready'] = deepcopy(member_receipts); s['lifecycle'] = 'active'
        if s['actual_started_at_ns'] is None:s['actual_started_at_ns'] = at_ns

    @transaction
    def recovery_boundary(self, *, at_ns, reason='controller_restart_uncertain_interval'):
        """Call before adapters reconcile after restart or a durable feed gap."""
        integer(at_ns, 'invalid_recovery_clock')
        before = self.store.last['at_ns']
        require(at_ns >= before, 'recovery_clock_regressed')
        for s in self.state['sessions'].values():
            if s['lifecycle'] == 'active':
                s['lifecycle'] = 'recovering'
                last_boundary = s['segments'][-1]['end_ns'] if s['segments'] else s['actual_started_at_ns']
                s['reconnect_gaps'].append(dict(start_ns=last_boundary, end_ns=None, reason=reason,fresh_after_ns=at_ns,
                                                affected_members=list(s['offer']['candidate']['members']),
                                                fresh_after_by_member={m:at_ns for m in s['offer']['candidate']['members']},
                                                interval_is_uncertain_not_exact_loss=True))
            elif s['lifecycle']=='recovering':
                gap=s['reconnect_gaps'][-1];members=list(s['offer']['candidate']['members'])
                gap.update(fresh_after_ns=at_ns,affected_members=members,fresh_after_by_member={m:at_ns for m in members})
                gap.setdefault('restart_boundaries',[]).append(dict(at_ns=at_ns,reason=reason))
        self._save('recovery_boundary', at_ns)

    @transaction
    def invalidate_member(self,member,*,last_good_ns,fresh_after_ns,transition_ref,at_ns):
        """A single native reconnect cannot reset unrelated task groups."""
        affected=self._invalidate_member(member,last_good_ns=last_good_ns,fresh_after_ns=fresh_after_ns,transition_ref=transition_ref,at_ns=at_ns)
        if affected:self._save(dict(native_member_invalidated=member,sessions=affected),at_ns)
        return affected

    @transaction
    def invalidate_members(self,transitions,*,at_ns):
        require(isinstance(transitions,dict) and len(transitions)<=8192,'bounded_native_transition_batch_required')
        affected={}
        for member,row in transitions.items():
            sessions=self._invalidate_member(member,**row,at_ns=at_ns)
            if sessions:affected[member]=sessions
        if affected:self._save(dict(native_members_invalidated=affected),at_ns)
        return affected

    def _invalidate_member(self,member,*,last_good_ns,fresh_after_ns,transition_ref,at_ns):
        evidence([transition_ref]);require(transition_ref['kind']=='native_connection_transition','native_transition_evidence_required')
        for value in (last_good_ns,fresh_after_ns,at_ns):integer(value,'native_transition_clock_required')
        require(last_good_ns<=fresh_after_ns<=at_ns and transition_ref['observed_at_ns']<=at_ns,'native_transition_clock_invalid')
        affected=[]
        for sid,s in self.state['sessions'].items():
            if s['lifecycle'] not in {'active','recovering'} or member not in s['offer']['candidate']['members']:continue
            boundary=s['segments'][-1]['end_ns'] if s['segments'] else s['actual_started_at_ns']
            require(last_good_ns>=boundary,'native_transition_before_sealed_boundary_requires_correction')
            if s['lifecycle']=='active':
                s['lifecycle']='recovering';s['reconnect_gaps'].append(dict(start_ns=last_good_ns,end_ns=None,
                    fresh_after_ns=fresh_after_ns,affected_members=[member],reason='native_member_connection_transition',
                    fresh_after_by_member={member:fresh_after_ns},
                    transition_refs=[transition_ref],interval_is_uncertain_not_exact_loss=True))
            else:
                gap=s['reconnect_gaps'][-1]
                floors=gap.setdefault('fresh_after_by_member',{m:gap.get('fresh_after_ns',gap['start_ns'])
                    for m in gap.get('affected_members',s['offer']['candidate']['members'])})
                floors[member]=max(floors.get(member,0),fresh_after_ns)
                gap['start_ns']=min(gap['start_ns'],last_good_ns);gap['fresh_after_ns']=max(gap.get('fresh_after_ns',gap['start_ns']),fresh_after_ns)
                gap['affected_members']=sorted(set(gap.get('affected_members',s['offer']['candidate']['members']))|{member})
                refs=gap.setdefault('transition_refs',[])
                if transition_ref not in refs:refs.append(transition_ref)
            affected.append(sid)
        return affected

    @transaction
    def terminal(self, member, metadata, metadata_ref, *, at_ns):
        venue, mid = member.split(':', 1)
        if venue=='myriad':
            from myriad.protocol import identity
            require(identity(metadata)==mid,'scoped_terminal_metadata_required')
        else:require(venue in {'polymarket', 'predictfun'} and str(metadata.get('id')) == mid, 'scoped_terminal_metadata_required')
        evidence([metadata_ref]); require(type(metadata_ref.get('observed_at_ns')) is int and metadata_ref['observed_at_ns']<=at_ns, 'terminal_observation_clock_required')
        require(metadata_ref['sha256'] == digest(metadata) and metadata_ref['kind'] == 'native_market_metadata',
                'terminal_metadata_body_binding_required')
        if not native_terminal(venue, metadata):return []
        self.state['terminal_members'][member] = metadata_ref['observed_at_ns']
        closed = []
        for sid, s in self.state['sessions'].items():
            if s['lifecycle'] != 'closed' and member in s['offer']['candidate']['members']:
                require(at_ns >= s['admitted_at_ns'], 'terminal_before_admission')
                s['lifecycle'] = 'closed'; s['closed_at_ns'] = at_ns
                s['terminal_evidence'] = dict(member=member, metadata_ref=metadata_ref, payload_hash=digest(metadata),
                    reason='required_member_official_terminal', settlement_payout_certified=False)
                if self.state.get('collect_trades',True):
                    self.state.setdefault('tail_reservations',{})[sid]=deepcopy(s['offer']['request_rpm_by_member'])
                else:s['trade_collection']='not_requested'
                closed.append(sid)
        self._save(dict(terminal_sessions=closed, terminal_member=member), at_ns)
        return closed

    @transaction
    def simulate_recording_end(self, session_id, receipt, *, at_ns):
        """Close a debug recording epoch, never the exchange market identity."""
        evidence([receipt])
        require(receipt['kind']=='simulated_recording_end' and receipt.get('observed_at_ns')==at_ns,
                'explicit_simulated_recording_end_receipt_required')
        s=self.state['sessions'][session_id]
        if s['lifecycle']=='closed':
            require(s.get('simulation_evidence')==receipt,'simulation_cannot_relabel_official_terminal')
            return []
        require(s['lifecycle']=='active' and s['actual_started_at_ns'] is not None and at_ns>=s['actual_started_at_ns'],
                'simulation_requires_active_recording')
        require(not self.state.get('collect_trades',True),'simulation_requires_external_physical_trade_queue')
        s.update(lifecycle='closed',closed_at_ns=at_ns,terminal_evidence=None,
                 end_policy='authorized_simulated_recording_epoch_only',
                 recording_end_reason='simulated_recording_end_not_official_terminal',
                 simulation_evidence=deepcopy(receipt),trade_collection='external_physical_queue')
        # No terminal_members update, no fabricated official metadata/payout.
        self._save(dict(simulated_recording_end=session_id,official_market_terminal=False),at_ns)
        return [session_id]

    @transaction
    def release(self, session_id, receipt, *, at_ns):
        s = self.state['sessions'][session_id]
        require(s['lifecycle'] == 'closed', 'cannot_release_open_group')
        evidence([receipt]); require(receipt['kind'] == 'native_feed_release_receipt', 'native_feed_release_receipt_required')
        require(receipt.get('observed_at_ns') == at_ns and at_ns >= s['closed_at_ns'], 'release_clock_required')
        s['feeds_released'] = True; s['release_receipt'] = receipt
        self._save(dict(feeds_released=session_id), at_ns)

    @transaction
    def tail_finished(self,session_id,receipt,*,at_ns):
        s=self.state['sessions'][session_id];require(s['lifecycle']=='closed','only_closed_session_tail_can_release')
        evidence([receipt]);require(receipt['kind']=='native_trade_tail_finished' and receipt.get('observed_at_ns')==at_ns,
            'native_tail_completion_receipt_required')
        if session_id in self.state.get('tail_reservations',{}):
            del self.state['tail_reservations'][session_id];s['tail_receipt']=receipt
            self._save(dict(tail_resource_released=session_id),at_ns)

    @transaction
    def cut(self, session_id, *, end_ns, watermarks, at_ns):
        s = self.state['sessions'][session_id]
        require(s['actual_started_at_ns'] is not None, 'cannot_seal_unstarted_session')
        require(s['lifecycle'] in {'active', 'closed'}, 'fresh_boundary_required')
        start = s['segments'][-1]['end_ns'] if s['segments'] else s['actual_started_at_ns']
        require(start < end_ns <= at_ns and (s['closed_at_ns'] is None or end_ns <= s['closed_at_ns']), 'invalid_segment_boundary')
        require(end_ns-start<=self.segment_duration_ns,'segment_duration_exceeds_rotation_policy')
        require(set(watermarks) == set(s['ready']), 'all_member_watermarks_required')
        previous = s['segments'][-1]['watermarks'] if s['segments'] else s['ready']
        for member, row in watermarks.items():
            require(set(row) == {'epoch', 'sequence', 'durable_sequence', 'checkpoint_ref'}, 'durable_boundary_fields_required')
            integer(row['sequence'], 'invalid_boundary_sequence'); integer(row['durable_sequence'], 'invalid_durable_sequence')
            require(row['durable_sequence'] >= row['sequence'], 'boundary_not_persisted')
            require(row['epoch'] == s['ready'][member]['epoch'], 'unreconciled_connection_epoch')
            if row['epoch'] == previous[member]['epoch']:
                require(row['sequence'] >= previous[member]['sequence'], 'receive_sequence_regressed')
            evidence([row['checkpoint_ref']])
        segment = seal('mixed-segment-reference/1', session_id=session_id, group_id=s['group_id'],
            index=len(s['segments']), start_ns=start, end_ns=end_ns, boundary='[start_ns,end_ns)',
            previous=s['segments'][-1]['contract_hash'] if s['segments'] else None,
            watermarks=watermarks, gap_refs=deepcopy(s['reconnect_gaps']), archive_profile='nautilus-source/2',
            market_payload_copied=False, remote_publication_verified=False)
        s['segments'].append(segment); self._save(dict(segment_sealed=segment['contract_hash']), at_ns)
        return segment

    @transaction
    def pause_admission(self, paused, *, at_ns):
        require(type(paused) is bool, 'boolean_admission_pause_required')
        self.state['admissions_paused'] = paused; self._save(dict(admissions_paused=paused), at_ns)

    def desired_feeds(self):
        if self.capacity.get('shared_polymarket'):
            feeds={};identities={}
            for sid,s in self.state['sessions'].items():
                if s['feeds_released']:continue
                for market in s['offer']['markets']:
                    member=market['member'];merge(identities,market,True)
                    physical=s['physical_sessions'][member]
                    if member not in feeds:
                        feeds[member]=dict(session_id=physical,market=deepcopy(market),lifecycle=s['lifecycle'],
                            reconnect_requires_snapshot=False,consumer_sessions=[])
                    row=feeds[member];require(row['session_id']==physical,'shared_physical_session_changed')
                    row['consumer_sessions'].append(sid)
                    if s['lifecycle']!='closed':row['lifecycle']=s['lifecycle']
                    if market['metadata_ref']['observed_at_ns']>row['market']['metadata_ref']['observed_at_ns']:
                        row['market']=deepcopy(market)
                    row['reconnect_requires_snapshot']|=s['lifecycle']=='recovering' and member in s['reconnect_gaps'][-1].get('affected_members',s['offer']['candidate']['members'])
            return feeds
        feeds = {}
        for sid, s in self.state['sessions'].items():
            if s['feeds_released']:continue
            for market in s['offer']['markets']:
                member = market['member']; require(member not in feeds, 'duplicate_physical_owner')
                feeds[member] = dict(session_id=sid, market=deepcopy(market),
                                     lifecycle=s['lifecycle'], reconnect_requires_snapshot=s['lifecycle'] == 'recovering' and
                                         member in s['reconnect_gaps'][-1].get('affected_members',s['offer']['candidate']['members']))
        return feeds
