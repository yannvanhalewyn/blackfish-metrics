create table sales (
  id integer primary key,
  created_at timestamp not null,
  completed boolean,
  total integer not null
);
--
create table items (
  id integer primary key,
  created_at timestamp not null,
  sku varchar(255) not null,
  description varchar(255) not null,
  msrp integer not null,
  online_price integer not null,
  default_price integer not null
);
--
create table sale_lines (
  id integer primary key,
  sale_id integer references sales not null,
  item_id integer references items not null,
  created_at timestamp not null,

  unit_qty integer not null default 1,
  unit_price integer not null,
  price integer not null,
  discount integer
);
--
create view sale_lines_with_prices as
  select l.*, i.default_price, i.msrp, i.online_price
  from sale_lines l
  join items i on l.item_id = i.id;
