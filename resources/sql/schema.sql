create table sales (
  id integer primary key,
  created_at timestamp with time zone not null,
  completed boolean,
  method varchar(255) default 'store',
  total numeric not null
);
--
create table items (
  id              integer primary key,
  created_at      timestamp with time zone not null,
  sku             varchar(255) not null,
  manufacturer_id integer references manufacturers,
  category_id     integer references categories,
  description     varchar(255) not null,
  msrp            numeric not null,
  online_price    numeric not null,
  default_price   numeric not null,
  archived        boolean
);
--
create table sale_lines (
  id         integer primary key,
  sale_id    integer references sales,
  item_id    integer references items,
  created_at timestamp with time zone not null,

  qty        integer not null default 1,
  unit_price numeric not null,
  total      numeric not null,
  subtotal   numeric not null,
  fifo_price numeric not null,
  discount   numeric
);
--
create table manufacturers (
  id   integer primary key,
  name varchar(255)
);
--
create table categories (
  id     integer primary key,
  name   varchar(255),
  gender varchar(255)
);
--
create table vendors (
  id integer primary key,
  name varchar(255)
)
--
create or replace view sale_lines_with_relations as
  select l.*, i.default_price, i.msrp, i.online_price,
    l.subtotal - l.fifo_price * l.qty as profit,
    case when m.name is null then 'unknown' else m.name end as manufacturer_name,
    case when c.name is null then 'unknown' else c.name end as category_name,
    case when c.gender is null then 'unknown' else c.gender end as gender,
    case when v.name is null then 'unknown' else v.name end as vendor_name
  from sale_lines l
  join items i on l.item_id = i.id
  join sales s on l.sale_id = s.id
  left outer join categories c on i.category_id = c.id
  left outer join manufacturers m on i.manufacturer_id = m.id
  left outer join vendors v on i.vendor_id = v.id
  where s.completed = true;
--
