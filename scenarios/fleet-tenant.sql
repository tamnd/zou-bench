-- The small project a fleet is made of: one table, a hundred rows, and
-- row level security on, which is what makes it a Supabase project
-- rather than a table in a shared database.
--
-- Applied once per tenant over the postgres door, and idempotent, so a
-- provisioning run that was interrupted can be run again.

begin;

create table if not exists bench_rows (
    id    int primary key,
    title text not null,
    done  boolean not null default false,
    tag   int not null
);

insert into bench_rows (id, title, done, tag)
select i, 'row ' || i, i % 4 = 0, i % 10
from generate_series(1, 100) as i
on conflict (id) do nothing;

alter table bench_rows enable row level security;

drop policy if exists bench_rows_readable on bench_rows;
create policy bench_rows_readable on bench_rows for select using (true);

drop policy if exists bench_rows_writable on bench_rows;
create policy bench_rows_writable on bench_rows for insert
    to authenticated with check (true);

grant select on bench_rows to anon, authenticated;
grant insert on bench_rows to authenticated;

commit;
