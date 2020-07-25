DROP TABLE IF EXISTS "stockData".stockinfo;
CREATE TABLE IF NOT EXISTS "stockData".stockinfo
(
  ticker character(10) COLLATE pg_catalog."default" NOT NULL,
  date timestamp without time zone NOT NULL,
  open numeric(13,4),
	high numeric(13,4),
	low numeric(13,4),
	close numeric(13,4),
	volume integer,
  CONSTRAINT stockinfo_pkey PRIMARY KEY (ticker, date)
)

TABLESPACE pg_default;
