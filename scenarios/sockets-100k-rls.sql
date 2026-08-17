-- The same table as sockets.sql with row level security on it.
--
-- The policy is one every project writes: rows are readable, and the
-- server has to ask before it may say so. That question is a select of
-- the changed row back as the subscriber, inside the transaction that
-- became them, once per set of claims per change. It is the cost a
-- filtered subscription really carries on Supabase, so it is measured on
-- its own rather than left out of the headline or hidden inside it.
--
-- The policy says true rather than naming a person on purpose. A policy
-- that hid the rows would measure a run where nothing is delivered, and
-- what is under measurement here is the check, not the hiding.

drop table if exists public.pulse;

create table public.pulse (
    id bigserial primary key,
    shard int not null,
    sent_us bigint not null
);

grant select on public.pulse to anon, authenticated;

alter table public.pulse enable row level security;
create policy readable on public.pulse for select using (true);

do $$
begin
    if not exists (select 1 from pg_publication where pubname = 'supabase_realtime') then
        create publication supabase_realtime;
    end if;
end
$$;

alter publication supabase_realtime add table public.pulse;
