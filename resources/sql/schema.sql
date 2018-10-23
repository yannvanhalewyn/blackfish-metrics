create table sales (
  id integer primary key,
  created_at timestamp with time zone not null,
  completed boolean,
  total integer not null
);
--
create table items (
  id              integer primary key,
  created_at      timestamp with time zone not null,
  sku             varchar(255) not null,
  manufacturer_id integer references manufacturers,
  category_id     integer references categories,
  description     varchar(255) not null,
  msrp            integer not null,
  online_price    integer not null,
  default_price   integer not null,
  archived        boolean
);
--
create table sale_lines (
  id         integer primary key,
  sale_id    integer references sales,
  item_id    integer references items,
  created_at timestamp with time zone not null,

  qty        integer not null default 1,
  unit_price integer not null,
  total      integer not null,
  subtotal   integer not null,
  fifo_price integer not null,
  discount   integer
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
create view sale_lines_with_prices as
  select l.*, i.default_price, i.msrp, i.online_price
  from sale_lines l
  join items i on l.item_id = i.id
  join sales s on l.sale_id = s.id
  where s.completed = true;
--
