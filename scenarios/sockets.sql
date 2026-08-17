-- The table a sockets run subscribes to and writes.
--
-- Two columns carry the measurement. shard is what each socket filters
-- on, so a row lands on every socket holding that shard and one commit
-- becomes a fan out. sent_us is the microsecond the generator stamped
-- the row at, on the generator's own clock, which is also the clock the
-- delivery is timed against: the latency in the result has one clock in
-- it and no ntp skew between two boxes.
--
-- Written to be applied twice, because a run applies it every time and a
-- run measured against whatever the last run left behind is not a number
-- about this scenario.

drop table if exists public.pulse;

create table public.pulse (
    id bigserial primary key,
    shard int not null,
    sent_us bigint not null
);

-- The subscriber reads as anon, and a change is only sent to a socket
-- that could have selected the row itself, so without this grant the
-- rows go into the tap and reach nobody.
grant select on public.pulse to anon, authenticated;

-- Row level security is off here on purpose, and that is part of what
-- the number means. With policies on, the server selects the changed row
-- back as the subscriber before sending it, which is a second round trip
-- per set of claims per change. sockets-100k-rls.json is that other
-- thing, measured separately rather than averaged into this one.

-- The publication is the project's own, created on boot by any zou and
-- by the dashboard on Supabase, but a bare postgres has never heard of
-- it and a scenario should not need a server to have been up first.
do $$
begin
    if not exists (select 1 from pg_publication where pubname = 'supabase_realtime') then
        create publication supabase_realtime;
    end if;
end
$$;

-- The drop above took the table out of the publication with it, so this
-- is always the add of a table that is not in it yet.
alter publication supabase_realtime add table public.pulse;

-- No index on shard, deliberately. A subscription's filter is applied in
-- the server against the row the decoder handed it, not by a query, and
-- the visibility check a policy needs is a primary key lookup, which the
-- key already has an index for. An index nothing reads is write cost the
-- run would then be measuring.
