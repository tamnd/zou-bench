-- The demo tenant the warm read scenario measures against: a todo app
-- with lists, todos, a public table, and row level security tying every
-- private row to the token's own sub.
--
-- Applied before every rest run and idempotent, so the same file sets
-- up zou and a vanilla postgres behind PostgREST and neither leg can
-- accidentally measure a different database.

begin;

-- The roles and the auth helpers exist already on a zou tenant. On a
-- plain postgres they have to be created, and the definitions are
-- Supabase's own, copied so both legs answer auth.uid() identically.
do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'anon') then
    create role anon nologin noinherit;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'authenticated') then
    create role authenticated nologin noinherit;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'service_role') then
    create role service_role nologin noinherit bypassrls;
  end if;
end
$$;

create schema if not exists auth;

create or replace function auth.jwt() returns jsonb
  language sql stable as $$
  select coalesce(
    nullif(current_setting('request.jwt.claim', true), ''),
    nullif(current_setting('request.jwt.claims', true), '')
  )::jsonb
$$;

create or replace function auth.uid() returns uuid
  language sql stable as $$
  select coalesce(
    nullif(current_setting('request.jwt.claim.sub', true), ''),
    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
  )::uuid
$$;

grant usage on schema auth to anon, authenticated, service_role;
grant usage on schema public to anon, authenticated, service_role;

drop table if exists bench_todos cascade;
drop table if exists bench_lists cascade;
drop table if exists bench_notes cascade;

create table bench_lists (
  id int primary key,
  owner uuid not null,
  name text not null
);

create table bench_todos (
  id int primary key,
  list int not null references bench_lists (id),
  owner uuid not null,
  title text not null,
  done boolean not null default false,
  created_at timestamptz not null default now()
);

-- A table anon reads, because an app's first paint usually happens
-- before anybody has signed in and that request is on the same path.
create table bench_notes (
  id int primary key,
  title text not null,
  body text not null
);

-- Two owners: the token the run carries, and somebody else. Half the
-- rows belonging to the other user is the point, a policy that filters
-- nothing is not being measured.
insert into bench_lists
select n,
       case when n % 2 = 0
            then '11111111-1111-1111-1111-111111111111'::uuid
            else '22222222-2222-2222-2222-222222222222'::uuid end,
       'list ' || n
from generate_series(1, 200) as n;

insert into bench_todos
select n,
       ((n - 1) % 200) + 1,
       case when ((n - 1) % 200) % 2 = 0
            then '11111111-1111-1111-1111-111111111111'::uuid
            else '22222222-2222-2222-2222-222222222222'::uuid end,
       'todo ' || n,
       n % 3 = 0,
       now() - (n || ' minutes')::interval
from generate_series(1, 20000) as n;

insert into bench_notes
select n, 'note ' || n, repeat('body ', 20)
from generate_series(1, 1000) as n;

create index on bench_todos (owner);
create index on bench_todos (list);
create index on bench_todos (done);
create index on bench_lists (owner);

alter table bench_lists enable row level security;
alter table bench_todos enable row level security;
alter table bench_notes enable row level security;

create policy own on bench_lists for all to authenticated
  using (owner = auth.uid()) with check (owner = auth.uid());
create policy own on bench_todos for all to authenticated
  using (owner = auth.uid()) with check (owner = auth.uid());
create policy readable on bench_notes for select to anon, authenticated
  using (true);

-- What an app's dashboard asks for, as one round trip.
create or replace function bench_todo_stats() returns table (done bigint, open bigint)
  language sql stable as $$
  select count(*) filter (where done), count(*) filter (where not done)
  from bench_todos
$$;

grant select, insert, update, delete on bench_lists, bench_todos to authenticated;
grant select on bench_notes to anon, authenticated;
grant execute on function bench_todo_stats() to anon, authenticated;
grant all on bench_lists, bench_todos, bench_notes to service_role;

commit;

-- Warm means warm: the planner has statistics and the pages are in
-- shared buffers before the first timed request, not after the first
-- thousand.
analyze bench_lists;
analyze bench_todos;
analyze bench_notes;
